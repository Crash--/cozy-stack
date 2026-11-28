package sharing

import (
	"time"

	"github.com/cozy/cozy-stack/model/driveevents"
	"github.com/cozy/cozy-stack/model/instance"
)

const (
	memberAdded   = driveevents.MemberAdded
	memberUpdated = driveevents.MemberUpdated
	memberRemoved = driveevents.MemberRemoved
)

// emitMember publishes a sharing.member event for m. Only the owner's instance
// publishes: it learns every change of a member, and a recipient's instance
// would publish the same change a second time.
func (s *Sharing) emitMember(inst *instance.Instance, eventType string, m *Member) {
	if !s.Owner {
		return
	}
	data := &driveevents.Member{
		SharingID: s.SID,
		Instance:  inst.Domain,
		Owner:     s.Owner,
		Drive:     s.Drive,
		FileIDs:   []string{},
		Name:      s.Description,
		Member:    m.Instance,
		Email:     m.Email,
		ReadOnly:  m.ReadOnly,
		Status:    m.Status,
	}
	if len(s.Members) > 0 {
		data.Sharer = s.Members[0].Instance
	}
	if rule := s.FirstFilesRule(); rule != nil {
		data.FileIDs = rule.Values
	}
	emitEvent(inst, eventType, data)
}

// emitEvent is replaced by the tests.
var emitEvent = driveevents.Emit

// EmitDriveState publishes a sharing.drive.state event: the drive and all its
// recipients as the owner holds them now, for a consumer that asked for it.
func (s *Sharing) EmitDriveState(inst *instance.Instance) {
	data := &driveevents.DriveState{
		SharingID: s.SID,
		Instance:  inst.Domain,
		Name:      s.Description,
		FileIDs:   []string{},
		Members:   []driveevents.DriveMember{},
		At:        time.Now().UTC(),
	}
	if rule := s.FirstFilesRule(); rule != nil {
		data.FileIDs = rule.Values
	}
	for i, m := range s.Members {
		if i > 0 {
			data.Members = append(data.Members, driveevents.DriveMember{Member: m.Instance, Email: m.Email, ReadOnly: m.ReadOnly, Status: m.Status})
		}
	}
	emitEvent(inst, driveevents.DriveStateEvent, data)
}

// EmitMembers publishes a sharing.member.updated event for every recipient of
// the sharing, as the owner knows them: the initial state of a consumer that
// missed the events, or a resynchronisation.
func (s *Sharing) EmitMembers(inst *instance.Instance) {
	for i := range s.Members {
		if i > 0 {
			s.emitMember(inst, memberUpdated, &s.Members[i])
		}
	}
}
