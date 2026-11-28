package pointer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/cozy/cozy-stack/model/instance"
	"github.com/cozy/cozy-stack/model/instance/lifecycle"
	"github.com/cozy/cozy-stack/model/sharing"
	"github.com/cozy/cozy-stack/model/vfs"
	"github.com/cozy/cozy-stack/pkg/config/config"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/pkg/couchdb/mango"
	"github.com/cozy/cozy-stack/pkg/logger"
	"github.com/cozy/cozy-stack/pkg/messaging"
	"github.com/cozy/cozy-stack/pkg/metadata"
	"github.com/cozy/cozy-stack/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func init() {
	rabbitmq.RegisterHandler(Queue, func(p messaging.Publisher) rabbitmq.Handler {
		return NewHandler(p)
	})
}

var log = logger.WithNamespace("pointer")

// ExternalIDKey is the metadata key of the document a pointer stands for.
const ExternalIDKey = "externalId"

// ExternalRoleKey is the metadata key of the role of the owner of the instance
// on the document.
const ExternalRoleKey = "externalRole"

// ExternalAsOfKey is the metadata key of the asOf of the last command applied
// to the pointer.
const ExternalAsOfKey = "externalAsOf"

// Outcome is what an item did.
type Outcome string

// The outcomes of an item.
const (
	Done  Outcome = "ok"
	Stale Outcome = "stale"
	Gone  Outcome = "gone"
)

// errDefinitive marks an item that a redelivery cannot fix.
var errDefinitive = errors.New("definitive")

// Handler applies the batches of pointer commands.
type Handler struct{}

// NewHandler returns the handler of the queue. The commands have no reply.
func NewHandler(_ messaging.Publisher) *Handler {
	return &Handler{}
}

// Handle applies every item of a batch. An item that can never be applied is
// logged and counted as handled; any other failure asks for the redelivery of
// the whole batch, whose items already applied then change nothing.
func (h *Handler) Handle(_ context.Context, d amqp.Delivery) error {
	if d.RoutingKey != RoutingKeyBatch {
		return malformed("unknown routing key %s", d.RoutingKey)
	}
	b, err := ParseBatch(d.Body)
	if err != nil {
		return err
	}
	var transient error
	for i := range b.Items {
		it := &b.Items[i]
		outcome, err := Apply(b, it)
		switch {
		case err == nil:
			log.WithDomain(it.Instance).Debugf("%s %s: %s", it.Op, it.ExternalID, outcome)
		case errors.Is(err, errDefinitive):
			log.WithDomain(it.Instance).Warnf("%s %s dropped: %s", it.Op, it.ExternalID, err)
		default:
			log.WithDomain(it.Instance).Errorf("%s %s failed: %s", it.Op, it.ExternalID, err)
			transient = err
		}
	}
	if transient != nil {
		return fmt.Errorf("%s: %w", RoutingKeyBatch, transient)
	}
	return nil
}

// permanentVFSErrors are the answers of the VFS that a redelivery cannot change.
var permanentVFSErrors = []error{
	vfs.ErrParentDoesNotExist, vfs.ErrParentInTrash, vfs.ErrForbiddenDocMove,
	vfs.ErrIllegalFilename, vfs.ErrIllegalPath, vfs.ErrFileInTrash,
}

// Apply applies one item, under a lock on its document on its instance. An
// error it returns is definitive when a redelivery cannot fix it.
func Apply(b *Batch, it *Item) (Outcome, error) {
	outcome, err := apply(b, it)
	return outcome, permanentOrNot(err)
}

// permanentOrNot marks as definitive an error of the VFS a redelivery cannot change.
func permanentOrNot(err error) error {
	for _, permanent := range permanentVFSErrors {
		if errors.Is(err, permanent) && !errors.Is(err, errDefinitive) {
			return fmt.Errorf("%w: %w", errDefinitive, err)
		}
	}
	return err
}

func apply(b *Batch, it *Item) (Outcome, error) {
	if err := it.check(b); err != nil {
		return "", fmt.Errorf("%w: %s", errDefinitive, err)
	}
	inst, err := lifecycle.GetInstance(it.Instance)
	if err != nil {
		if errors.Is(err, instance.ErrNotFound) {
			return "", fmt.Errorf("%w: unknown instance %s", errDefinitive, it.Instance)
		}
		return "", err
	}
	if it.Op == OpEnsure && it.DirID != "" {
		// The folder may be gone (deleted meanwhile, or lost): the pointer goes
		// to its fallback, or stays out of it
		if _, err := inst.VFS().DirByID(it.DirID); err != nil {
			if !os.IsNotExist(err) && !couchdb.IsNotFoundError(err) {
				return "", err
			}
			log.WithDomain(inst.Domain).Warnf("ensure %s: folder %s is gone", it.ExternalID, it.DirID)
			if it.Fallback != nil {
				return applyFallback(b, it)
			}
			it.DirID, it.Member = "", ""
		}
	}
	if it.Op == OpEnsure && it.Member != "" && it.DirID != "" {
		if sharing.WritableDriveOfDir(inst, it.DirID, it.Member) == "" {
			if it.Fallback == nil {
				return "", fmt.Errorf("%w: %s writes in no drive of %s holding %s", errDefinitive, it.Member, it.Instance, it.DirID)
			}
			return applyFallback(b, it)
		}
	}
	mu := config.Lock().ReadWrite(inst, "pointers/"+it.ExternalID)
	if err := mu.Lock(); err != nil {
		return "", err
	}
	defer mu.Unlock()

	file, err := find(inst, it.FileID, it.ExternalID)
	if err != nil {
		return "", err
	}
	if file != nil && olderThanApplied(file, it.AsOf) {
		return Stale, nil
	}
	switch it.Op {
	case OpEnsure:
		if file == nil && it.OnlyIfExists {
			return Gone, nil
		}
		_, err = Ensure(inst, b, it, file)
		return Done, err
	case OpRemove:
		if file == nil {
			return Gone, nil
		}
		return Done, inst.VFS().DestroyFile(file)
	case OpTrash, OpRestore:
		if file == nil {
			return Gone, nil
		}
		if (it.Op == OpTrash) == file.Trashed {
			return Done, nil
		}
		markWrite(inst, file, b.App, it.AsOf)
		if it.Op == OpTrash {
			_, err = vfs.TrashFile(inst.VFS(), file)
		} else {
			_, err = vfs.RestoreFile(inst.VFS(), file)
		}
		return Done, err
	}
	return "", fmt.Errorf("%w: unknown op %q", errDefinitive, it.Op)
}

// applyFallback applies an ensure to its fallback instance and folder.
func applyFallback(b *Batch, it *Item) (Outcome, error) {
	fb := *it
	fb.Instance, fb.DirID, fb.Member, fb.Fallback = it.Fallback.Instance, "", "", nil
	fbBatch := *b
	fbBatch.Folder = it.Fallback.Folder
	return apply(&fbBatch, &fb)
}

// olderThanApplied tells if asOf is older than the last command applied to
// the pointer. The writes of a person leave externalAsOf as it is.
func olderThanApplied(file *vfs.FileDoc, asOf time.Time) bool {
	raw, _ := file.Metadata[ExternalAsOfKey].(string)
	applied, err := time.Parse(time.RFC3339Nano, raw)
	return err == nil && asOf.Before(applied)
}

// Ensure creates the pointer of a document, or brings the existing one to the
// wanted name, folder and metadata. A pointer in the trash is left there.
func Ensure(inst *instance.Instance, b *Batch, it *Item, file *vfs.FileDoc) (*vfs.FileDoc, error) {
	fs := inst.VFS()
	meta := vfs.Metadata{ExternalIDKey: it.ExternalID, ExternalAsOfKey: it.AsOf.UTC().Format(time.RFC3339Nano)}
	if it.Role != "" {
		meta[ExternalRoleKey] = it.Role
	}
	if file != nil {
		return update(inst, file, b, it, meta)
	}
	dirID := it.DirID
	if dirID == "" {
		dir, err := vfs.MkdirAll(fs, "/"+b.Folder)
		if err != nil {
			return nil, err
		}
		dirID = dir.ID()
	}
	now := time.Now()
	doc, err := vfs.NewFileDoc(it.Name, dirID, 0, nil, b.Mime, "", now, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	doc.Metadata = meta
	doc.CozyMetadata = vfs.NewCozyMetadata(inst.PageURL("/", nil))
	doc.CozyMetadata.CreatedByApp = b.App
	doc.CozyMetadata.UpdatedByApp(&metadata.UpdatedByAppEntry{Slug: b.App, Date: now, Instance: inst.PageURL("/", nil)})
	doc.DocName = uniqueName(fs, dirID, doc.DocName)
	f, err := fs.CreateFile(doc, nil)
	if err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	return doc, nil
}

func find(inst *instance.Instance, fileID, externalID string) (*vfs.FileDoc, error) {
	if fileID != "" {
		file, err := inst.VFS().FileByID(fileID)
		if err == nil {
			return file, nil
		}
		if !os.IsNotExist(err) && !couchdb.IsNotFoundError(err) {
			return nil, err
		}
		return nil, nil
	}
	return FindByExternalID(inst, externalID)
}

var externalIDIndex = mango.MakeIndex(consts.Files, "by-external-id", mango.IndexDef{Fields: []string{"metadata.externalId"}})

// FindByExternalID returns the pointer of a document on the instance, or nil.
// A pointer in the trash counts: it is the same file.
func FindByExternalID(inst *instance.Instance, externalID string) (*vfs.FileDoc, error) {
	if externalID == "" {
		return nil, nil
	}
	files, err := findPointers(inst, mango.Equal("metadata.externalId", externalID), 2)
	if err != nil || len(files) == 0 {
		return nil, err
	}
	return files[0], nil
}

// List returns every pointer of the instance.
func List(inst *instance.Instance) ([]*vfs.FileDoc, error) {
	return findPointers(inst, mango.Exists("metadata.externalId"), 10000)
}

// indexedDBs are the instances whose index by externalId this process has
// declared: declaring it on every lookup saturates CouchDB under load.
var indexedDBs sync.Map

// defineIndex is replaced by the tests.
var defineIndex = couchdb.DefineIndex

func ensureIndex(inst *instance.Instance) error {
	if _, ok := indexedDBs.Load(inst.DBPrefix()); ok {
		return nil
	}
	if err := defineIndex(inst, externalIDIndex); err != nil {
		return err
	}
	indexedDBs.Store(inst.DBPrefix(), struct{}{})
	return nil
}

func findPointers(inst *instance.Instance, selector mango.Filter, limit int) ([]*vfs.FileDoc, error) {
	if err := ensureIndex(inst); err != nil {
		return nil, err
	}
	var files []*vfs.FileDoc
	req := &couchdb.FindRequest{UseIndex: "by-external-id", Selector: selector, Limit: limit}
	if err := couchdb.FindDocs(inst, consts.Files, req, &files); err != nil {
		return nil, err
	}
	return files, nil
}

func update(inst *instance.Instance, file *vfs.FileDoc, b *Batch, it *Item, meta vfs.Metadata) (*vfs.FileDoc, error) {
	fs := inst.VFS()
	patch := &vfs.DocPatch{}
	merged := vfs.Metadata{}
	for k, v := range file.Metadata {
		merged[k] = v
	}
	for k, v := range meta {
		merged[k] = v
	}
	if it.Role == "" {
		delete(merged, ExternalRoleKey)
	}
	// The asOf is written even when nothing else changes: an older command
	// arriving later must find it
	patch.Metadata = &merged
	if !file.Trashed && file.DocName != it.Name {
		name := uniqueName(fs, file.DirID, it.Name)
		if name != file.DocName {
			patch.Name = &name
		}
	}
	if !file.Trashed && it.DirID != "" && file.DirID != it.DirID {
		dirID := it.DirID
		patch.DirID = &dirID
	}
	now := time.Now()
	patch.UpdatedAt = &now
	markWrite(inst, file, b.App, it.AsOf)
	return vfs.ModifyFileMetadata(fs, file, patch)
}

// uniqueName suffixes the name when another file of the folder has it, as Drive
// does: "A (2).docs-note".
func uniqueName(fs vfs.VFS, dirID, name string) string {
	dir, err := fs.DirByID(dirID)
	if err != nil {
		return name
	}
	ext := path.Ext(name)
	base := name[:len(name)-len(ext)]
	candidate := name
	for i := 2; i < 100; i++ {
		_, err := fs.FileByPath(path.Join(dir.Fullpath, candidate))
		if os.IsNotExist(err) {
			return candidate
		}
		if err != nil {
			return candidate
		}
		candidate = fmt.Sprintf("%s (%d)%s", base, i, ext)
	}
	return candidate
}

// markWrite names the service in the file, so the consumers of the file events
// tell its writes from the ones of a person.
func markWrite(inst *instance.Instance, file *vfs.FileDoc, app string, asOf time.Time) {
	if file.CozyMetadata == nil {
		file.CozyMetadata = vfs.NewCozyMetadata(inst.PageURL("/", nil))
	}
	file.CozyMetadata.UpdatedByApp(&metadata.UpdatedByAppEntry{Slug: app, Date: time.Now(), Instance: inst.PageURL("/", nil)})
	if file.Metadata == nil {
		file.Metadata = vfs.Metadata{}
	}
	file.Metadata[ExternalAsOfKey] = asOf.UTC().Format(time.RFC3339Nano)
}
