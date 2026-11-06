package driveevents

import (
	"testing"

	"github.com/cozy/cozy-stack/model/vfs"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/prefixer"
	"github.com/stretchr/testify/assert"
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
	t.Cleanup(func() { emit = Emit })
	db := prefixer.NewPrefixer(0, "alice.example", "alice")

	file := &vfs.FileDoc{DocID: "f1", DocName: "a.docs-note", DirID: "d1"}
	trashed := &vfs.FileDoc{DocID: "f1", DocName: "a.docs-note", DirID: consts.TrashDirID, Trashed: true}
	moved := &vfs.FileDoc{DocID: "f1", DocName: "a.docs-note", DirID: "d2"}
	renamed := &vfs.FileDoc{DocID: "f1", DocName: "b.docs-note", DirID: "d1"}
	copied := vfs.CreateFileDocCopy(file, "", "")
	copied.DocID = "f2"
	created := &vfs.FileDoc{DocID: "f3", DocName: "new.docs-note", DirID: "d1"}
	leaving := &vfs.FileDoc{DocID: "f1", DocName: "a.docs-note", DirID: "d1"}
	leaving.MarkMovedTo(vfs.FileRef{ID: "f9", Instance: "bob.example"})

	fileChanged(db, file, trashed)
	fileChanged(db, trashed, file)
	fileChanged(db, file, moved)
	fileChanged(db, file, renamed)
	fileChanged(db, nil, copied)
	fileChanged(db, nil, created)
	fileChanged(db, trashed, nil)
	fileChanged(db, leaving, nil)

	types := make([]string, len(got))
	for i, e := range got {
		types[i] = e.eventType
	}
	assert.Equal(t, []string{FileTrashed, FileRestored, FileMoved, FileCopied, FileCreated, FileDestroyed, FileMoved}, types)
	assert.Equal(t, &File{ID: "f1", Instance: "alice.example"}, got[2].file.From)
	assert.Equal(t, &File{ID: "f1", Instance: "alice.example"}, got[3].file.From)
	assert.Nil(t, got[4].file.From)
	assert.Equal(t, "f9", got[6].file.ID)
	assert.Equal(t, "bob.example", got[6].file.Instance)
	assert.Equal(t, &File{ID: "f1", Instance: "alice.example", DirID: "d1", Name: "a.docs-note"}, got[6].file.From)
}
