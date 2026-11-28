// Package driveevents publishes on RabbitMQ what happens to the files that
// point to a document of another app, and to the members and links of the
// sharings, as CloudEvents on the activity exchange. Each event is carried by
// a job: the job system is the outbox, and a job keeps the id of its event
// across retries, so a consumer can drop the duplicates.
package driveevents

import (
	"time"

	"github.com/cozy/cozy-stack/model/instance"
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

// ExternalIDKey is the metadata key that makes a file a pointer to a document
// of another app. Only those files have events: the bus is not flooded by
// every upload.
const ExternalIDKey = "externalId"

// ExternalRoleKey is the metadata key of the role of the owner of the instance
// on the external document.
const ExternalRoleKey = "externalRole"

// The event types, used as routing keys.
const (
	FileCreated   = "com.twake.drive.file.created.v1"
	FileUpdated   = "com.twake.drive.file.updated.v1"
	FileCopied    = "com.twake.drive.file.copied.v1"
	FileMoved     = "com.twake.drive.file.moved.v1"
	FileTrashed   = "com.twake.drive.file.trashed.v1"
	FileRestored  = "com.twake.drive.file.restored.v1"
	FileDestroyed = "com.twake.drive.file.destroyed.v1"

	MemberAdded     = "com.twake.drive.sharing.member.added.v1"
	MemberUpdated   = "com.twake.drive.sharing.member.updated.v1"
	MemberRemoved   = "com.twake.drive.sharing.member.removed.v1"
	DriveStateEvent = "com.twake.drive.sharing.drive.state.v1"

	LinkCreated = "com.twake.drive.sharing.link.created.v1"
	LinkUpdated = "com.twake.drive.sharing.link.updated.v1"
	LinkRevoked = "com.twake.drive.sharing.link.revoked.v1"
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
	// UpdatedByApps are the slugs of the apps that wrote the file.
	UpdatedByApps []string `json:"updated_by_apps,omitempty"`
	// WrittenBy is the app that made this write, when the file names it:
	// absent for a client that names no app.
	WrittenBy string `json:"written_by,omitempty"`
	// Actor is the owner of the instance, absent on an organization instance.
	Actor *Actor `json:"actor,omitempty"`
	// DriveID is the shared drive of this instance holding the file, if any,
	// and DriveName its name.
	DriveID   string `json:"drive_id,omitempty"`
	DriveName string `json:"drive_name,omitempty"`
	Trashed   bool   `json:"trashed"`
	// UpdatedAt is the time of the write the event tells: the date of a
	// gesture, which the consumers compare to keep the latest one.
	UpdatedAt time.Time `json:"updated_at"`
	// From is the file it was copied or moved from.
	From *File `json:"from,omitempty"`
}

// DriveState is the data of a sharing.drive.state event: a shared drive and
// its members, as its owner holds them at At.
type DriveState struct {
	SharingID string        `json:"sharing_id"`
	Instance  string        `json:"instance"`
	Name      string        `json:"name,omitempty"`
	FileIDs   []string      `json:"file_ids"`
	Members   []DriveMember `json:"members"`
	At        time.Time     `json:"at"`
}

// DriveMember is a recipient of a shared drive, in a sharing.drive.state event.
type DriveMember struct {
	Member   string `json:"member"`
	Email    string `json:"email,omitempty"`
	ReadOnly bool   `json:"read_only"`
	Status   string `json:"status"`
}

// Actor is the person who owns the instance of a file.
type Actor struct {
	Sub   string `json:"sub"`
	Email string `json:"email,omitempty"`
}

// writtenByWindow is how close to the write the date of an app entry must be
// for that app to have made it.
const writtenByWindow = 2 * time.Second

func actorOf(db prefixer.Prefixer) *Actor {
	if inst, ok := db.(*instance.Instance); ok {
		return actorOfInstance(inst)
	}
	return actorOfDomain(db.DomainName())
}

// actorOfDomain is replaced by the tests.
var actorOfDomain = func(domain string) *Actor {
	inst, err := instance.Get(domain)
	if err != nil {
		return nil
	}
	return actorOfInstance(inst)
}

func actorOfInstance(inst *instance.Instance) *Actor {
	// An organization instance may carry an oidc_id, but nobody owns it
	if inst.OIDCID == "" || inst.IsOrganizationInstance() {
		return nil
	}
	email, _ := inst.SettingsEMail()
	return &Actor{Sub: inst.OIDCID, Email: email}
}

// writtenBy returns the app whose entry is dated at the write, or "".
func writtenBy(doc *vfs.FileDoc, at time.Time) string {
	if doc.CozyMetadata == nil {
		return ""
	}
	var slug string
	var latest time.Time
	for _, entry := range doc.CozyMetadata.UpdatedByApps {
		if entry.Date.After(latest) {
			slug, latest = entry.Slug, entry.Date
		}
	}
	if slug == "" || at.Sub(latest).Abs() > writtenByWindow {
		return ""
	}
	return slug
}

// DriveOf names the shared drive owned by the instance whose root holds the
// file, or "" outside any drive. Set by the sharing package, which this one
// cannot import.
var DriveOf func(db prefixer.Prefixer, doc *vfs.FileDoc) string

// DriveName gives the name of a shared drive owned by the instance of the
// domain, or "". Set by the sharing package.
var DriveName func(domain, sharingID string) string

func driveName(domain, sharingID string) string {
	if DriveName == nil || sharingID == "" {
		return ""
	}
	return DriveName(domain, sharingID)
}

// Member is the data of the sharing.member events.
type Member struct {
	SharingID string   `json:"sharing_id"`
	Instance  string   `json:"instance"`
	Owner     bool     `json:"owner"`
	Sharer    string   `json:"sharer"`
	Drive     bool     `json:"drive"`
	Member    string   `json:"member,omitempty"`
	Email     string   `json:"email,omitempty"`
	ReadOnly  bool     `json:"read_only"`
	Status    string   `json:"status"`
	FileIDs   []string `json:"file_ids"`
	// Name is the description of the sharing: the name of a shared drive.
	Name string `json:"name,omitempty"`
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

// IsPointer tells if a file points to a document of another app.
func IsPointer(doc *vfs.FileDoc) bool {
	if doc == nil || doc.Metadata == nil {
		return false
	}
	id, _ := doc.Metadata[ExternalIDKey].(string)
	return id != ""
}

func fileChanged(db prefixer.Prefixer, olddoc, newdoc *vfs.FileDoc) {
	if !IsPointer(olddoc) && !IsPointer(newdoc) {
		return
	}
	switch {
	case olddoc == nil:
		if from := newdoc.CopiedFrom(); from != nil {
			emit(db, FileCopied, fileData(db, newdoc, from))
		} else {
			emit(db, FileCreated, fileData(db, newdoc, nil))
		}
	case newdoc == nil:
		if to := olddoc.MovedTo(); to != nil {
			moved := fileData(db, olddoc, nil)
			actor := moved.Actor
			if actor == nil {
				actor = actorOfDomain(to.Instance)
			}
			emit(db, FileMoved, &File{ID: to.ID, Instance: to.Instance, DirID: to.DirID, DriveID: to.DriveID, DriveName: driveName(to.Instance, to.DriveID), UpdatedAt: moved.UpdatedAt, Name: olddoc.DocName, Mime: olddoc.Mime, Metadata: olddoc.Metadata, Actor: actor, From: moved})
		} else {
			emit(db, FileDestroyed, fileData(db, olddoc, nil))
		}
	case !olddoc.Trashed && newdoc.Trashed:
		emit(db, FileTrashed, fileData(db, newdoc, nil))
	case olddoc.Trashed && !newdoc.Trashed:
		emit(db, FileRestored, fileData(db, newdoc, nil))
	case olddoc.DirID != newdoc.DirID:
		from := &vfs.FileRef{ID: olddoc.ID(), Instance: db.DomainName(), DirID: olddoc.DirID}
		if DriveOf != nil {
			from.DriveID = DriveOf(db, olddoc)
		}
		emit(db, FileMoved, fileData(db, newdoc, from))
	case olddoc.DocName != newdoc.DocName || metadataChanged(olddoc.Metadata, newdoc.Metadata):
		emit(db, FileUpdated, fileData(db, newdoc, nil))
	}
}

func metadataChanged(old, updated vfs.Metadata) bool {
	for _, key := range []string{ExternalIDKey, ExternalRoleKey} {
		if old[key] != updated[key] {
			return true
		}
	}
	return false
}

func fileData(db prefixer.Prefixer, doc *vfs.FileDoc, from *vfs.FileRef) *File {
	domain := db.DomainName()
	// The event is emitted during the write: now is its time
	now := time.Now().UTC()
	f := &File{ID: doc.ID(), Instance: domain, DirID: doc.DirID, Name: doc.DocName, Mime: doc.Mime, Metadata: doc.Metadata, Trashed: doc.Trashed, UpdatedAt: now}
	if DriveOf != nil && !doc.Trashed {
		f.DriveID = DriveOf(db, doc)
		f.DriveName = driveName(domain, f.DriveID)
	}
	if doc.CozyMetadata != nil {
		for _, entry := range doc.CozyMetadata.UpdatedByApps {
			f.UpdatedByApps = append(f.UpdatedByApps, entry.Slug)
		}
	}
	f.WrittenBy = writtenBy(doc, now)
	f.Actor = actorOf(db)
	if from != nil {
		f.From = &File{ID: from.ID, Instance: from.Instance, DirID: from.DirID, DriveID: from.DriveID, UpdatedAt: now}
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
