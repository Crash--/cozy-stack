package sharing

import (
	"fmt"
	"testing"
	"time"

	"github.com/cozy/cozy-stack/model/driveevents"
	"github.com/cozy/cozy-stack/model/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDriveOf(t *testing.T) {
	ownerInst, recipientInst := setupTwoTestInstances(t)
	owner, _ := setupAcceptedDrive(t, ownerInst, recipientInst)
	fs := ownerInst.VFS()
	rootID, err := owner.DriveRootID()
	require.NoError(t, err)
	sub, err := vfs.Mkdir(fs, "/Projects/2026", nil)
	require.NoError(t, err)
	outside, err := vfs.Mkdir(fs, "/Elsewhere", nil)
	require.NoError(t, err)

	n := 0
	newFile := func(dirID string, meta vfs.Metadata) *vfs.FileDoc {
		n++
		doc, err := vfs.NewFileDoc(fmt.Sprintf("%d.docs-note", n), dirID, 0, nil, "application/vnd.twake.docs", "", time.Now(), false, false, false, nil)
		require.NoError(t, err)
		doc.Metadata = meta
		f, err := fs.CreateFile(doc, nil)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		return doc
	}

	// A pointer: the VFS calls the drive events hook, and DriveOf with it, while
	// it holds its lock; the creation must come back
	pointer := newFile(rootID, vfs.Metadata{driveevents.ExternalIDKey: "doc-1"})
	assert.Equal(t, owner.SID, DriveOf(ownerInst, pointer))
	assert.Equal(t, owner.SID, DriveOf(ownerInst, newFile(rootID, nil)))
	assert.Equal(t, owner.SID, DriveOf(ownerInst, newFile(sub.DocID, nil)))
	assert.Equal(t, "", DriveOf(ownerInst, newFile(outside.DocID, nil)))
	// The recipient's copy of the drive is not a drive it owns
	assert.Equal(t, "", DriveOf(recipientInst, &vfs.FileDoc{DocName: "a", DirID: "nowhere"}))
}
