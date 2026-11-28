package pointer

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/cozy/cozy-stack/model/instance"
	"github.com/cozy/cozy-stack/model/instance/lifecycle"
	"github.com/cozy/cozy-stack/model/sharing"
	"github.com/cozy/cozy-stack/model/vfs"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/pkg/rabbitmq"
)

// applySync brings the pointers of the scope to the wanted state: each wanted
// pointer is ensured, every other one goes, and so does a second file for the
// same document. Each step is an item of its own, under the asOf of the sync.
func applySync(inst *instance.Instance, b *Batch, it *Item) (Outcome, error) {
	files, err := List(inst)
	if err != nil {
		return "", err
	}
	roots, err := sharing.DriveRootsOf(inst)
	if err != nil {
		return "", err
	}
	current := map[string][]*vfs.FileDoc{}
	for _, f := range files {
		if f.Mime != b.Mime || inDrive(inst, roots, f) {
			continue
		}
		id, _ := f.Metadata[ExternalIDKey].(string)
		current[id] = append(current[id], f)
	}

	var steps []Item
	remove := func(id string, f *vfs.FileDoc) {
		steps = append(steps, Item{Op: OpRemove, Instance: it.Instance, ExternalID: id, FileID: f.ID(), AsOf: it.AsOf})
	}
	for _, w := range it.Wanted {
		existing := current[w.ExternalID]
		delete(current, w.ExternalID)
		var kept *vfs.FileDoc
		if len(existing) > 0 {
			// The first one made stays: the others are leftovers
			sort.SliceStable(existing, func(i, j int) bool { return existing[i].CreatedAt.Before(existing[j].CreatedAt) })
			kept = existing[0]
			for _, extra := range existing[1:] {
				remove(w.ExternalID, extra)
			}
		}
		if kept == nil && w.OnlyIfExists {
			continue
		}
		// An unchanged pointer is not written: the night would write them all
		if kept != nil && unchanged(kept, w) {
			continue
		}
		step := Item{Op: OpEnsure, Instance: it.Instance, ExternalID: w.ExternalID, Name: w.Name, Role: w.Role, OnlyIfExists: w.OnlyIfExists, Trashed: w.Trashed, AsOf: it.AsOf}
		if kept != nil {
			step.FileID = kept.ID()
		}
		steps = append(steps, step)
	}
	for id, extra := range current {
		for _, f := range extra {
			remove(id, f)
		}
	}

	var transient error
	for i := range steps {
		step := &steps[i]
		if _, err := Apply(b, step); err != nil {
			if errors.Is(err, errDefinitive) {
				log.WithDomain(it.Instance).Warnf("sync %s %s dropped: %s", step.Op, step.ExternalID, err)
			} else {
				transient = err
			}
		}
	}
	return Done, transient
}

// inDrive tells if the file is in the tree of a shared drive the instance
// owns, or was before going to the trash.
func inDrive(inst *instance.Instance, roots sharing.DriveRoots, f *vfs.FileDoc) bool {
	if !f.Trashed {
		return roots.Of(inst, f) != ""
	}
	for _, root := range roots {
		if f.RestorePath == root || strings.HasPrefix(f.RestorePath, root+"/") {
			return true
		}
	}
	return false
}

var takenSuffix = regexp.MustCompile(` \(\d+\)$`)

// unchanged tells if an ensure of w would change nothing in the pointer: the
// same role, the same place in or out of the trash, and the same name but for
// the suffix the stack takes in a folder where the name is taken, "A (2)".
func unchanged(f *vfs.FileDoc, w Wanted) bool {
	role, _ := f.Metadata[ExternalRoleKey].(string)
	if role != w.Role || (w.Trashed != nil && *w.Trashed != f.Trashed) {
		return false
	}
	if f.Trashed || f.DocName == w.Name {
		return true
	}
	ext := path.Ext(w.Name)
	if path.Ext(f.DocName) != ext {
		return false
	}
	return takenSuffix.ReplaceAllString(strings.TrimSuffix(f.DocName, ext), "") == strings.TrimSuffix(w.Name, ext)
}

// describeRequest is the payload of drive.sharing.describe.
type describeRequest struct {
	Instance  string `json:"instance"`
	SharingID string `json:"sharing_id"`
}

// describe publishes the state of a shared drive its owner holds. A drive the
// instance does not own, or no longer has, publishes nothing.
func describe(body []byte) error {
	var req describeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return describeMalformed("%s", err)
	}
	req.Instance = strings.TrimSpace(req.Instance)
	req.SharingID = strings.TrimSpace(req.SharingID)
	if req.Instance == "" || req.SharingID == "" {
		return describeMalformed("missing instance or sharing_id")
	}
	inst, err := lifecycle.GetInstance(req.Instance)
	if err != nil {
		if errors.Is(err, instance.ErrNotFound) {
			log.WithDomain(req.Instance).Warnf("describe %s: unknown instance", req.SharingID)
			return nil
		}
		return err
	}
	s, err := sharing.FindSharing(inst, req.SharingID)
	if err != nil {
		if couchdb.IsNotFoundError(err) || couchdb.IsNoDatabaseError(err) {
			log.WithDomain(inst.Domain).Warnf("describe %s: no such sharing", req.SharingID)
			return nil
		}
		return err
	}
	if !s.Drive || !s.Owner {
		log.WithDomain(inst.Domain).Warnf("describe %s: not a shared drive this instance owns", req.SharingID)
		return nil
	}
	s.EmitDriveState(inst)
	return nil
}

func describeMalformed(format string, args ...any) error {
	return fmt.Errorf("%s: %w: %s", RoutingKeyDescribe, rabbitmq.ErrMalformed, fmt.Sprintf(format, args...))
}
