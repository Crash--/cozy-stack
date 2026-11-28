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
	roots, err := DriveRootsOf(inst)
	if err != nil {
		return ""
	}
	return roots.Of(inst, doc)
}

// DriveRoots are the root directories of the shared drives an instance owns,
// by sharing id: read once, they name the drive of many files.
type DriveRoots map[string]string

// DriveRootsOf reads the roots of the shared drives the instance owns.
func DriveRootsOf(inst *instance.Instance) (DriveRoots, error) {
	drives, err := ListDrives(inst)
	if err != nil {
		return nil, err
	}
	fs := vfs.NewCouchdbIndexer(inst)
	roots := DriveRoots{}
	for _, s := range drives {
		if !s.Owner || !s.HasDirectoryDriveRoot() {
			continue
		}
		rootID, err := s.DriveRootID()
		if err != nil {
			continue
		}
		if root, err := fs.DirByID(rootID); err == nil {
			roots[s.SID] = root.Fullpath
		}
	}
	return roots, nil
}

// Of returns the drive whose root holds the file, or "".
func (roots DriveRoots) Of(inst *instance.Instance, doc *vfs.FileDoc) string {
	if len(roots) == 0 {
		return ""
	}
	fpath, err := doc.Path(vfs.NewCouchdbIndexer(inst))
	if err != nil {
		return ""
	}
	for sid, root := range roots {
		if strings.HasPrefix(fpath, root+"/") {
			return sid
		}
	}
	return ""
}

// WritableDriveOfDir returns the id of the shared drive owned by the instance
// whose root is the directory or one of its parents, when member (a domain)
// is a ready member of it with write access; "" otherwise.
func WritableDriveOfDir(inst *instance.Instance, dirID, member string) string {
	drives, err := ListDrives(inst)
	if err != nil {
		return ""
	}
	fs := vfs.NewCouchdbIndexer(inst)
	dir, err := fs.DirByID(dirID)
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
		if err != nil || (dir.Fullpath != root.Fullpath && !strings.HasPrefix(dir.Fullpath, root.Fullpath+"/")) {
			continue
		}
		for i, m := range s.Members {
			if i > 0 && hostOf(m.Instance) == hostOf(member) && m.Status == MemberStatusReady && !m.ReadOnly {
				return s.SID
			}
		}
	}
	return ""
}

// OwnerOfDriveDir returns the domain of the instance owning a shared drive the
// member belongs to and whose tree holds the directory, or "". A member's Drive
// shows the folders of a shared drive with the ids they have on its owner.
func OwnerOfDriveDir(member *instance.Instance, dirID string) string {
	drives, err := ListDrives(member)
	if err != nil {
		return ""
	}
	for _, s := range drives {
		if s.Owner || len(s.Members) == 0 {
			continue
		}
		rootID, err := s.DriveRootID()
		if err != nil {
			continue
		}
		ownerDomain := hostOf(s.Members[0].Instance)
		if dirID == rootID {
			return ownerDomain
		}
		owner, err := instance.Get(ownerDomain)
		if err != nil {
			continue
		}
		fs := vfs.NewCouchdbIndexer(owner)
		dir, err := fs.DirByID(dirID)
		if err != nil {
			continue
		}
		if root, err := fs.DirByID(rootID); err == nil && strings.HasPrefix(dir.Fullpath, root.Fullpath+"/") {
			return ownerDomain
		}
	}
	return ""
}

func hostOf(instanceURL string) string {
	host := strings.TrimPrefix(strings.TrimPrefix(instanceURL, "https://"), "http://")
	return strings.TrimSuffix(host, "/")
}
