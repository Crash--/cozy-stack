package driveevents

import (
	"testing"
	"time"

	"github.com/cozy/cozy-stack/model/vfs"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/metadata"
	"github.com/cozy/cozy-stack/pkg/prefixer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileChanged(t *testing.T) {
	type emitted struct {
		eventType string
		file      *File
	}
	var got []emitted
	emit = func(_ prefixer.Prefixer, eventType string, data any) {
		got = append(got, emitted{eventType, data.(*File)})
	}
	DriveOf = func(_ prefixer.Prefixer, doc *vfs.FileDoc) string {
		if doc.DirID == "d2" {
			return "drive-1"
		}
		return ""
	}
	alice := &Actor{Sub: "alice", Email: "alice@example.net"}
	realActorOfDomain := actorOfDomain
	actorOfDomain = func(domain string) *Actor {
		if domain == "alice.example" {
			return alice
		}
		return nil
	}
	t.Cleanup(func() { emit = Emit; DriveOf = nil; actorOfDomain = realActorOfDomain })
	db := prefixer.NewPrefixer(0, "alice.example", "alice")

	meta := vfs.Metadata{ExternalIDKey: "doc-1"}
	file := &vfs.FileDoc{DocID: "f1", DocName: "a.docs-note", DirID: "d1", Metadata: meta}
	trashed := &vfs.FileDoc{DocID: "f1", DocName: "a.docs-note", DirID: consts.TrashDirID, Trashed: true, Metadata: meta}
	moved := &vfs.FileDoc{DocID: "f1", DocName: "a.docs-note", DirID: "d2", Metadata: meta}
	renamed := &vfs.FileDoc{DocID: "f1", DocName: "b.docs-note", DirID: "d1", Metadata: meta}
	copied, err := vfs.CreateFileDocCopy(file, "", "")
	require.NoError(t, err)
	copied.DocID = "f2"
	created := &vfs.FileDoc{DocID: "f3", DocName: "new.docs-note", DirID: "d1", Metadata: meta}
	leaving := &vfs.FileDoc{DocID: "f1", DocName: "a.docs-note", DirID: "d1", Metadata: meta}
	leaving.MarkMovedTo(vfs.FileRef{ID: "f9", Instance: "bob.example", DirID: "d9", DriveID: "drive-9"})
	plain := &vfs.FileDoc{DocID: "p1", DocName: "report.pdf", DirID: "d1"}
	plainRenamed := &vfs.FileDoc{DocID: "p1", DocName: "report2.pdf", DirID: "d1"}

	fileChanged(db, file, trashed)
	fileChanged(db, trashed, file)
	fileChanged(db, file, moved)
	fileChanged(db, file, renamed)
	fileChanged(db, nil, copied)
	fileChanged(db, nil, created)
	fileChanged(db, trashed, nil)
	fileChanged(db, leaving, nil)
	// A file that points to nothing has no event
	fileChanged(db, nil, plain)
	fileChanged(db, plain, plainRenamed)
	fileChanged(db, plain, nil)

	types := make([]string, len(got))
	for i, e := range got {
		types[i] = e.eventType
	}
	assert.Equal(t, []string{FileTrashed, FileRestored, FileMoved, FileUpdated, FileCopied, FileCreated, FileDestroyed, FileMoved}, types)
	// A move names the folder it left, and the owner of the instance
	assert.Equal(t, &File{ID: "f1", Instance: "alice.example", DirID: "d1"}, got[2].file.From)
	assert.Equal(t, alice, got[2].file.Actor)
	// Moved into the folder of a drive: the event names the drive
	assert.Equal(t, "drive-1", got[2].file.DriveID)
	assert.Equal(t, "", got[3].file.DriveID)
	assert.Equal(t, "b.docs-note", got[3].file.Name)
	assert.Equal(t, &File{ID: "f1", Instance: "alice.example"}, got[4].file.From)
	assert.Nil(t, got[5].file.From)
	assert.Equal(t, "f9", got[7].file.ID)
	assert.Equal(t, "bob.example", got[7].file.Instance)
	assert.Equal(t, "d9", got[7].file.DirID)
	assert.Equal(t, "drive-9", got[7].file.DriveID)
	assert.Equal(t, &File{ID: "f1", Instance: "alice.example", DirID: "d1", Name: "a.docs-note", Metadata: meta, Actor: alice}, got[7].file.From)
	assert.Equal(t, alice, got[7].file.Actor, "the person who moved it, from the instance it left")

	// Moved out of a drive: the event names the drive it left
	got = nil
	fileChanged(db, moved, file)
	require.Len(t, got, 1)
	assert.Equal(t, &File{ID: "f1", Instance: "alice.example", DirID: "d2", DriveID: "drive-1"}, got[0].file.From)
}

func TestWrittenBy(t *testing.T) {
	now := time.Now()
	doc := &vfs.FileDoc{CozyMetadata: &vfs.FilesCozyMetadata{}}
	doc.CozyMetadata.UpdatedByApps = []*metadata.UpdatedByAppEntry{
		{Slug: "drive", Date: now},
		{Slug: "docs-side-service", Date: now.Add(-time.Hour)},
	}
	// The entry dated at the write, not the last of the list
	assert.Equal(t, "drive", writtenBy(doc, now))
	// A client that names no app: the latest entry is older than the write
	assert.Equal(t, "", writtenBy(doc, now.Add(time.Minute)))
	assert.Equal(t, "", writtenBy(&vfs.FileDoc{}, now))
}
