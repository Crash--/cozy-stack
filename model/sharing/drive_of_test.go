package sharing

import (
	"testing"
	"time"

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

	newFile := func(dirID string) *vfs.FileDoc {
		doc, err := vfs.NewFileDoc("a.docs-note", dirID, 0, nil, "application/vnd.twake.docs", "", time.Now(), false, false, false, nil)
		require.NoError(t, err)
		f, err := fs.CreateFile(doc, nil)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		return doc
	}

	assert.Equal(t, owner.SID, DriveOf(ownerInst, newFile(rootID)))
	assert.Equal(t, owner.SID, DriveOf(ownerInst, newFile(sub.DocID)))
	assert.Equal(t, "", DriveOf(ownerInst, newFile(outside.DocID)))
	// The recipient's copy of the drive is not a drive it owns
	assert.Equal(t, "", DriveOf(recipientInst, &vfs.FileDoc{DocName: "a", DirID: "nowhere"}))
}
