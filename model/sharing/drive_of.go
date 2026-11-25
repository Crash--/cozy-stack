package sharing

import (
	"strings"

	"github.com/cozy/cozy-stack/model/driveevents"
	"github.com/cozy/cozy-stack/model/instance"
	"github.com/cozy/cozy-stack/model/vfs"
	"github.com/cozy/cozy-stack/pkg/prefixer"
)

func init() {
	driveevents.DriveOf = func(db prefixer.Prefixer, doc *vfs.FileDoc) string {
		inst, ok := db.(*instance.Instance)
		if !ok {
			var err error
			if inst, err = instance.Get(db.DomainName()); err != nil {
				return ""
			}
		}
		return DriveOf(inst, doc)
	}
}

// DriveOf returns the id of the shared drive owned by the instance whose root
// directory holds the file, or "" when the file is in no drive.
//
// It reads the index directly: the VFS calls FileChanged while it holds its
// lock, and its own DirByID would wait for that lock forever.
func DriveOf(inst *instance.Instance, doc *vfs.FileDoc) string {
	drives, err := ListDrives(inst)
	if err != nil {
		return ""
	}
	fs := vfs.NewCouchdbIndexer(inst)
	fpath, err := doc.Path(fs)
	if err != nil {
		return ""
	}
	for _, s := range drives {
		if !s.Owner || !s.HasDirectoryDriveRoot() {
			continue
		}
		rootID, err := s.DriveRootID()
		if err != nil {
			continue
		}
		root, err := fs.DirByID(rootID)
		if err != nil {
			continue
		}
		if strings.HasPrefix(fpath, root.Fullpath+"/") {
			return s.SID
		}
	}
	return ""
}
