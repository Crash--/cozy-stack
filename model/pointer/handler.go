package pointer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/cozy/cozy-stack/model/instance"
	"github.com/cozy/cozy-stack/model/instance/lifecycle"
	"github.com/cozy/cozy-stack/model/vfs"
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

// ReadyData is the data of com.twake.drive.pointer.ready.v1.
type ReadyData struct {
	RequestID  string `json:"requestId,omitempty"`
	Command    string `json:"command"`
	Instance   string `json:"instance"`
	FileID     string `json:"fileId,omitempty"`
	DirID      string `json:"dirId,omitempty"`
	ExternalID string `json:"externalId,omitempty"`
	// Gone tells that remove or restore found no file: nothing to do.
	Gone bool `json:"gone,omitempty"`
}

// Handler applies the pointer commands.
type Handler struct {
	publisher messaging.Publisher
}

// NewHandler returns a handler replying with this publisher.
func NewHandler(publisher messaging.Publisher) *Handler {
	return &Handler{publisher: publisher}
}

// Handle applies one command and publishes its reply.
func (h *Handler) Handle(ctx context.Context, d amqp.Delivery) error {
	cmd, err := ParseCommand(d.RoutingKey, d.Body)
	if err != nil {
		return err
	}
	inst, err := lifecycle.GetInstance(cmd.Instance)
	if err != nil {
		if errors.Is(err, instance.ErrNotFound) {
			return fmt.Errorf("%s: %w: unknown instance %s", d.RoutingKey, rabbitmq.ErrMalformed, cmd.Instance)
		}
		return fmt.Errorf("%s: %w", d.RoutingKey, err)
	}
	data := ReadyData{RequestID: cmd.RequestID, Command: d.RoutingKey, Instance: inst.Domain, ExternalID: cmd.ExternalID}
	switch d.RoutingKey {
	case RoutingKeyEnsure:
		var file *vfs.FileDoc
		file, err = Ensure(inst, cmd)
		if err == nil {
			data.FileID, data.DirID = file.ID(), file.DirID
		}
	case RoutingKeyRemove:
		data.Gone, err = Remove(inst, cmd.FileID)
	case RoutingKeyTrash:
		var file *vfs.FileDoc
		file, err = Trash(inst, cmd.FileID, cmd.App)
		if err == nil && file == nil {
			data.Gone = true
		} else if err == nil {
			data.FileID, data.DirID = file.ID(), file.DirID
			data.ExternalID, _ = file.Metadata[ExternalIDKey].(string)
		}
	case RoutingKeyRestore:
		var file *vfs.FileDoc
		file, err = Restore(inst, cmd.FileID, cmd.App)
		if err == nil && file == nil {
			data.Gone = true
		} else if err == nil {
			data.FileID, data.DirID = file.ID(), file.DirID
			data.ExternalID, _ = file.Metadata[ExternalIDKey].(string)
		}
	}
	if err != nil {
		return fmt.Errorf("%s on %s: %w", d.RoutingKey, inst.Domain, err)
	}
	req, err := rabbitmq.NewActivityRequest(inst.ContextName, RoutingKeyReady, inst.OrgID, data)
	if err != nil {
		return err
	}
	if err := h.publisher.Publish(ctx, req); err != nil {
		return fmt.Errorf("%s: reply: %w", d.RoutingKey, err)
	}
	log.WithDomain(inst.Domain).Infof("%s %s: %s", d.RoutingKey, cmd.ExternalID, data.FileID)
	return nil
}

// Ensure creates the pointer of a document, or brings an existing one to the
// wanted name, folder and metadata. The pointer is found by fileId when given,
// else by externalId. A pointer in the trash is left there.
func Ensure(inst *instance.Instance, cmd *Command) (*vfs.FileDoc, error) {
	fs := inst.VFS()
	file, err := find(inst, cmd)
	if err != nil {
		return nil, err
	}
	meta := vfs.Metadata{ExternalIDKey: cmd.ExternalID}
	if cmd.Role != "" {
		meta[ExternalRoleKey] = cmd.Role
	}
	if file != nil {
		return update(fs, file, cmd, meta)
	}
	dirID := cmd.DirID
	if dirID == "" {
		dir, err := vfs.MkdirAll(fs, "/"+cmd.Folder)
		if err != nil {
			return nil, err
		}
		dirID = dir.ID()
	}
	now := time.Now()
	doc, err := vfs.NewFileDoc(cmd.Name, dirID, 0, nil, cmd.Mime, "", now, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	doc.Metadata = meta
	doc.CozyMetadata = vfs.NewCozyMetadata(inst.PageURL("/", nil))
	doc.CozyMetadata.CreatedByApp = cmd.App
	doc.CozyMetadata.UpdatedByApp(&metadata.UpdatedByAppEntry{Slug: cmd.App, Date: now, Instance: inst.PageURL("/", nil)})
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

func find(inst *instance.Instance, cmd *Command) (*vfs.FileDoc, error) {
	fs := inst.VFS()
	if cmd.FileID != "" {
		file, err := fs.FileByID(cmd.FileID)
		if err == nil {
			return file, nil
		}
		if !os.IsNotExist(err) && !couchdb.IsNotFoundError(err) {
			return nil, err
		}
	}
	return FindByExternalID(inst, cmd.ExternalID)
}

var externalIDIndex = mango.MakeIndex(consts.Files, "by-external-id", mango.IndexDef{Fields: []string{"metadata.externalId"}})

// FindByExternalID returns the pointer of a document on the instance, or nil.
// A pointer in the trash counts: it is the same file.
func FindByExternalID(inst *instance.Instance, externalID string) (*vfs.FileDoc, error) {
	if externalID == "" {
		return nil, nil
	}
	if err := couchdb.DefineIndex(inst, externalIDIndex); err != nil {
		return nil, err
	}
	var files []*vfs.FileDoc
	req := &couchdb.FindRequest{
		UseIndex: "by-external-id",
		Selector: mango.Equal("metadata.externalId", externalID),
		Limit:    2,
	}
	if err := couchdb.FindDocs(inst, consts.Files, req, &files); err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}
	return files[0], nil
}

func update(fs vfs.VFS, file *vfs.FileDoc, cmd *Command, meta vfs.Metadata) (*vfs.FileDoc, error) {
	patch := &vfs.DocPatch{}
	changed := false
	if file.Metadata == nil || file.Metadata[ExternalIDKey] != meta[ExternalIDKey] || file.Metadata[ExternalRoleKey] != meta[ExternalRoleKey] {
		merged := vfs.Metadata{}
		for k, v := range file.Metadata {
			merged[k] = v
		}
		for k, v := range meta {
			merged[k] = v
		}
		if cmd.Role == "" {
			delete(merged, ExternalRoleKey)
		}
		patch.Metadata = &merged
		changed = true
	}
	if !file.Trashed && file.DocName != cmd.Name {
		name := uniqueName(fs, file.DirID, cmd.Name)
		if name != file.DocName {
			patch.Name = &name
			changed = true
		}
	}
	if !file.Trashed && cmd.DirID != "" && file.DirID != cmd.DirID {
		dirID := cmd.DirID
		patch.DirID = &dirID
		changed = true
	}
	if !changed {
		return file, nil
	}
	now := time.Now()
	patch.UpdatedAt = &now
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

// Remove destroys a pointer. It returns true when the file is already gone.
func Remove(inst *instance.Instance, fileID string) (bool, error) {
	fs := inst.VFS()
	file, err := fs.FileByID(fileID)
	if os.IsNotExist(err) || couchdb.IsNotFoundError(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, fs.DestroyFile(file)
}

// Trash puts a pointer in the trash of Drive, where its owner can restore it.
// It returns nil when the file is gone, and the file unchanged when it is
// already in the trash.
func Trash(inst *instance.Instance, fileID, app string) (*vfs.FileDoc, error) {
	fs := inst.VFS()
	file, err := fs.FileByID(fileID)
	if os.IsNotExist(err) || couchdb.IsNotFoundError(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if file.Trashed {
		return file, nil
	}
	markWrite(inst, file, app)
	return vfs.TrashFile(fs, file)
}

// markWrite names the service in the file, so the consumers of the file events
// tell its writes from the ones of a person.
func markWrite(inst *instance.Instance, file *vfs.FileDoc, app string) {
	if app == "" {
		return
	}
	if file.CozyMetadata == nil {
		file.CozyMetadata = vfs.NewCozyMetadata(inst.PageURL("/", nil))
	}
	file.CozyMetadata.UpdatedByApp(&metadata.UpdatedByAppEntry{Slug: app, Date: time.Now(), Instance: inst.PageURL("/", nil)})
}

// Restore takes a pointer out of the trash. It returns nil when the file is
// gone, and the file unchanged when it is not in the trash.
func Restore(inst *instance.Instance, fileID, app string) (*vfs.FileDoc, error) {
	fs := inst.VFS()
	file, err := fs.FileByID(fileID)
	if os.IsNotExist(err) || couchdb.IsNotFoundError(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !file.Trashed {
		return file, nil
	}
	markWrite(inst, file, app)
	return vfs.RestoreFile(fs, file)
}
