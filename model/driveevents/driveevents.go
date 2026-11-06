// Package driveevents publishes on RabbitMQ what happens to the files, the
// sharings and the share by links of an instance, as CloudEvents on the
// activity exchange. Each event is carried by a job: the job system is the
// outbox, and a job keeps the id of its event across retries, so a consumer
// can drop the duplicates.
package driveevents

import (
	"time"

	"github.com/cozy/cozy-stack/model/job"
	"github.com/cozy/cozy-stack/model/permission"
	"github.com/cozy/cozy-stack/model/vfs"
	"github.com/cozy/cozy-stack/pkg/config/config"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/logger"
	"github.com/cozy/cozy-stack/pkg/prefixer"
	"github.com/cozy/cozy-stack/pkg/rabbitmq"
	"github.com/gofrs/uuid/v5"
)

// WorkerType is the type of the jobs publishing the events.
const WorkerType = "drive-events"

// The event types, used as routing keys.
const (
	FileCopied    = "file.copied"
	FileMoved     = "file.moved"
	FileTrashed   = "file.trashed"
	FileRestored  = "file.restored"
	FileDestroyed = "file.destroyed"

	MemberAdded   = "sharing.member.added"
	MemberUpdated = "sharing.member.updated"
	MemberRemoved = "sharing.member.removed"

	LinkCreated = "sharing.link.created"
	LinkUpdated = "sharing.link.updated"
	LinkRevoked = "sharing.link.revoked"
)

func init() {
	job.AddWorker(&job.WorkerConfig{
		WorkerType:  WorkerType,
		Concurrency: 1,
		// 5 s, 10 s, 20 s... : the last try comes about 40 minutes after the
		// first one, long enough to outlast a restart of RabbitMQ.
		MaxExecCount: 10,
		RetryDelay:   5 * time.Second,
		Reserved:     true,
		Timeout:      time.Minute,
		WorkerFunc:   work,
	})
	vfs.FileChanged = fileChanged
	permission.LinkChanged = linkChanged
}

// File is a file in the data of the file events.
type File struct {
	ID       string       `json:"id"`
	Instance string       `json:"instance"`
	DirID    string       `json:"dir_id,omitempty"`
	Name     string       `json:"name"`
	Mime     string       `json:"mime,omitempty"`
	Metadata vfs.Metadata `json:"metadata,omitempty"`
	// From is the file it was copied or moved from.
	From *File `json:"from,omitempty"`
}

// Member is the data of the sharing.member events.
type Member struct {
	SharingID string   `json:"sharing_id"`
	Instance  string   `json:"instance"`
	Owner     bool     `json:"owner"`
	Sharer    string   `json:"sharer"`
	Drive     bool     `json:"drive"`
	Member    string   `json:"member,omitempty"`
	ReadOnly  bool     `json:"read_only"`
	Status    string   `json:"status"`
	FileIDs   []string `json:"file_ids"`
}

// Link is the data of the sharing.link events.
type Link struct {
	PermissionID string   `json:"permission_id"`
	Instance     string   `json:"instance"`
	FileIDs      []string `json:"file_ids"`
	Verbs        []string `json:"verbs"`
}

func work(ctx *job.TaskContext) error {
	var event rabbitmq.CloudEvent
	if err := ctx.UnmarshalMessage(&event); err != nil {
		return err
	}
	event.TwakeOrg = ctx.Instance.OrgID
	if ctx.Publisher == nil {
		return nil
	}
	return ctx.Publisher.Publish(ctx, rabbitmq.ActivityRequest(ctx.Instance.ContextName, event))
}

// Emit pushes the job publishing an event. The event id is chosen here, once.
func Emit(db prefixer.Prefixer, eventType string, data any) {
	log := logger.WithDomain(db.DomainName()).WithNamespace("drive-events")
	id, err := uuid.NewV7()
	if err != nil {
		log.Errorf("Cannot emit %s: %s", eventType, err)
		return
	}
	msg, err := job.NewMessage(rabbitmq.CloudEvent{
		ID:   id.String(),
		Type: eventType,
		Time: time.Now().UTC(),
		Data: data,
	})
	sys := job.SystemUnsafe()
	if sys == nil || !config.GetConfig().RabbitMQ.Enabled {
		return // nowhere to publish
	}
	if err == nil {
		_, err = sys.PushJob(db, &job.JobRequest{WorkerType: WorkerType, Message: msg})
	}
	if err != nil {
		// shortcut: the event is lost if the job cannot be pushed right after
		// the write; an outbox written in the same CouchDB update would not lose it
		log.Errorf("Cannot emit %s: %s", eventType, err)
		return
	}
	log.Infof("Emitted %s %s", eventType, id)
}

var emit = Emit

func fileChanged(db prefixer.Prefixer, olddoc, newdoc *vfs.FileDoc) {
	domain := db.DomainName()
	switch {
	case olddoc == nil:
		if from := newdoc.CopiedFrom(); from != nil {
			emit(db, FileCopied, fileData(domain, newdoc, from))
		}
	case newdoc == nil:
		if to := olddoc.MovedTo(); to != nil {
			moved := fileData(domain, olddoc, nil)
			emit(db, FileMoved, &File{ID: to.ID, Instance: to.Instance, Name: olddoc.DocName, Mime: olddoc.Mime, Metadata: olddoc.Metadata, From: moved})
		} else {
			emit(db, FileDestroyed, fileData(domain, olddoc, nil))
		}
	case !olddoc.Trashed && newdoc.Trashed:
		emit(db, FileTrashed, fileData(domain, newdoc, nil))
	case olddoc.Trashed && !newdoc.Trashed:
		emit(db, FileRestored, fileData(domain, newdoc, nil))
	case olddoc.DirID != newdoc.DirID:
		emit(db, FileMoved, fileData(domain, newdoc, &vfs.FileRef{ID: olddoc.ID(), Instance: domain}))
	}
}

func fileData(domain string, doc *vfs.FileDoc, from *vfs.FileRef) *File {
	f := &File{ID: doc.ID(), Instance: domain, DirID: doc.DirID, Name: doc.DocName, Mime: doc.Mime, Metadata: doc.Metadata}
	if from != nil {
		f.From = &File{ID: from.ID, Instance: from.Instance}
		if f.From.Instance == "" {
			f.From.Instance = domain
		}
	}
	return f
}

func linkChanged(db prefixer.Prefixer, change string, p *permission.Permission) {
	var ids, verbs []string
	for _, rule := range p.Permissions {
		if rule.Type == consts.Files && rule.Selector == "" {
			ids = append(ids, rule.Values...)
		}
		for v := range rule.Verbs {
			verbs = append(verbs, string(v))
		}
	}
	if len(ids) == 0 {
		return
	}
	types := map[string]string{permission.LinkCreated: LinkCreated, permission.LinkUpdated: LinkUpdated, permission.LinkRevoked: LinkRevoked}
	emit(db, types[change], &Link{PermissionID: p.ID(), Instance: db.DomainName(), FileIDs: ids, Verbs: verbs})
}
