package sharing

import (
	"github.com/cozy/cozy-stack/model/driveevents"
	"github.com/cozy/cozy-stack/model/instance"
)

const (
	memberAdded   = driveevents.MemberAdded
	memberUpdated = driveevents.MemberUpdated
	memberRemoved = driveevents.MemberRemoved
)

// emitMember publishes a sharing.member event for m, or for the recipient this
// instance is when m is nil.
func (s *Sharing) emitMember(inst *instance.Instance, eventType string, m *Member) {
	if m == nil {
		for i := range s.Members {
			if i > 0 && s.Members[i].Instance != "" {
				m = &s.Members[i]
				break
			}
		}
	}
	data := &driveevents.Member{
		SharingID: s.SID,
		Instance:  inst.Domain,
		Owner:     s.Owner,
		Drive:     s.Drive,
		FileIDs:   []string{},
		Name:      s.Description,
	}
	if len(s.Members) > 0 {
		data.Sharer = s.Members[0].Instance
	}
	if rule := s.FirstFilesRule(); rule != nil {
		data.FileIDs = rule.Values
	}
	if m != nil {
		data.Member = m.Instance
		data.ReadOnly = m.ReadOnly
		data.Status = m.Status
	}
	driveevents.Emit(inst, eventType, data)
}

// EmitMembers publishes a sharing.member.updated event for every recipient of
// the sharing, as the owner knows them: the initial state of a consumer that
// missed the events, or a resynchronisation.
func (s *Sharing) EmitMembers(inst *instance.Instance) {
	if !s.Owner {
		return
	}
	for i := range s.Members {
		if i > 0 {
			s.emitMember(inst, memberUpdated, &s.Members[i])
		}
	}
}
