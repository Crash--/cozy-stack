package sharing

import (
	"testing"

	"github.com/cozy/cozy-stack/model/driveevents"
	"github.com/cozy/cozy-stack/model/instance"
	"github.com/cozy/cozy-stack/pkg/prefixer"
	"github.com/stretchr/testify/assert"
)

func TestMemberEventsFromTheOwnerOnly(t *testing.T) {
	var got []string
	emitEvent = func(_ prefixer.Prefixer, eventType string, data any) {
		m := data.(*driveevents.Member)
		got = append(got, eventType+" "+m.Instance+" "+m.Member+" "+m.Status)
	}
	t.Cleanup(func() { emitEvent = driveevents.Emit })

	members := func() []Member {
		return []Member{
			{Status: MemberStatusOwner, Instance: "https://acme.example"},
			{Status: MemberStatusReady, Instance: "https://bob.example"},
		}
	}
	owner := &Sharing{SID: "s1", Owner: true, Drive: true, Members: members()}
	recipient := &Sharing{SID: "s1", Drive: true, Members: members()}
	acme := &instance.Instance{Domain: "acme.example"}
	bob := &instance.Instance{Domain: "bob.example"}

	// The same acceptance, seen by both instances: one event, the owner's
	owner.emitMember(acme, memberAdded, &owner.Members[1])
	recipient.emitMember(bob, memberAdded, &recipient.Members[1])
	owner.EmitMembers(acme)
	recipient.EmitMembers(bob)

	assert.Equal(t, []string{
		driveevents.MemberAdded + " acme.example https://bob.example ready",
		driveevents.MemberUpdated + " acme.example https://bob.example ready",
	}, got)
}
