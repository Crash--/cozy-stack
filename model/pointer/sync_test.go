package pointer

import (
	"context"
	"testing"
	"time"

	"github.com/cozy/cozy-stack/model/sharing"
	"github.com/cozy/cozy-stack/model/vfs"
	"github.com/cozy/cozy-stack/pkg/config/config"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/pkg/rabbitmq"
	"github.com/cozy/cozy-stack/tests/testutils"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSync(t *testing.T) {
	if testing.Short() {
		t.Skip("an instance is required for this test: test skipped due to the use of --short flag")
	}
	config.UseTestFile(t)
	testutils.NeedCouchdb(t)
	inst := testutils.NewSetup(t, t.Name()).GetTestInstance()
	b := &Batch{App: "docs-side-service", Mime: "application/vnd.twake.docs", Folder: "Docs"}
	t0 := time.Date(2026, 11, 28, 10, 0, 0, 0, time.UTC)
	t1, t2 := t0.Add(time.Minute), t0.Add(2*time.Minute)
	yes, no := true, false

	ensure := func(it Item) *vfs.FileDoc {
		t.Helper()
		it.Op, it.Instance = OpEnsure, inst.Domain
		if it.AsOf.IsZero() {
			it.AsOf = t0
		}
		_, err := Apply(b, &it)
		require.NoError(t, err)
		f, err := FindByExternalID(inst, it.ExternalID)
		require.NoError(t, err)
		return f
	}
	exists := func(f *vfs.FileDoc) bool {
		t.Helper()
		_, err := inst.VFS().FileByID(f.ID())
		return err == nil
	}

	// A shared drive this instance owns: its pointers follow the drive grants
	team, err := vfs.MkdirAll(inst.VFS(), "/Team")
	require.NoError(t, err)
	require.NoError(t, couchdb.CreateNamedDocWithDB(inst, &sharing.Sharing{
		SID: "drive-team", Drive: true, Active: true, Owner: true,
		Members: []sharing.Member{{Instance: "https://" + inst.Domain, Status: sharing.MemberStatusOwner}, {Instance: "https://bob.example", Status: sharing.MemberStatusReady}},
		Rules:   []sharing.Rule{{Title: "Team", DocType: consts.Files, Values: []string{team.ID()}}},
	}))

	kept := ensure(Item{ExternalID: "doc-kept", Name: "Kept.docs-note", Role: "reader"})
	gone := ensure(Item{ExternalID: "doc-gone", Name: "Gone.docs-note"})
	inTeam := ensure(Item{ExternalID: "doc-team", Name: "Team.docs-note", DirID: team.ID()})
	newer := ensure(Item{ExternalID: "doc-newer", Name: "Newer.docs-note", AsOf: t2})
	renamed := ensure(Item{ExternalID: "doc-renamed", Name: "Old.docs-note"})
	toTrash := ensure(Item{ExternalID: "doc-trash", Name: "Trash.docs-note", Role: "owner"})
	toRestore := ensure(Item{ExternalID: "doc-restore", Name: "Restore.docs-note", Role: "owner"})
	_, err = Apply(b, &Item{Op: OpTrash, Instance: inst.Domain, ExternalID: "doc-restore", AsOf: t0})
	require.NoError(t, err)
	// A second file for the same document, as a lost command could leave
	second, err := Ensure(inst, b, &Item{ExternalID: "doc-kept", Name: "Kept.docs-note", Role: "reader", AsOf: t0}, nil)
	require.NoError(t, err)
	// A pointer of another app
	other, err := Ensure(inst, &Batch{App: "other", Mime: "application/x-other", Folder: "Other"}, &Item{ExternalID: "doc-other", Name: "Other.x", AsOf: t0}, nil)
	require.NoError(t, err)

	body := map[string]any{"app": b.App, "mime": b.Mime, "folder": b.Folder, "items": []any{map[string]any{
		"op": "sync", "instance": inst.Domain, "scope": "personal", "asOf": t1,
		"wanted": []map[string]any{
			{"externalId": "doc-kept", "name": "Kept.docs-note", "role": "reader"},
			{"externalId": "doc-renamed", "name": "New.docs-note"},
			{"externalId": "doc-created", "name": "Created.docs-note", "role": "editor"},
			{"externalId": "doc-trash", "name": "Trash.docs-note", "role": "owner", "onlyIfExists": true, "trashed": yes},
			{"externalId": "doc-restore", "name": "Restore.docs-note", "role": "owner", "trashed": no},
			{"externalId": "doc-absent", "name": "Absent.docs-note", "role": "owner", "onlyIfExists": true, "trashed": yes},
		},
	}}}
	require.NoError(t, NewHandler(nil).Handle(context.Background(), amqp.Delivery{RoutingKey: RoutingKeyBatch, Body: mustJSON(t, body)}))

	t.Run("AnUnchangedPointerIsNotWritten", func(t *testing.T) {
		f, err := inst.VFS().FileByID(kept.ID())
		require.NoError(t, err)
		assert.Equal(t, "2026-11-28T10:00:00Z", f.Metadata[ExternalAsOfKey])
	})
	t.Run("ASecondFileForTheSameDocumentGoes", func(t *testing.T) {
		assert.False(t, exists(second))
	})
	t.Run("APointerNotWantedGoes", func(t *testing.T) {
		assert.False(t, exists(gone))
	})
	t.Run("ThePointersOfAnOwnedDriveStay", func(t *testing.T) {
		assert.True(t, exists(inTeam))
	})
	t.Run("APointerWrittenAfterTheSyncStays", func(t *testing.T) {
		assert.True(t, exists(newer))
	})
	t.Run("ThePointersOfAnotherAppStay", func(t *testing.T) {
		assert.True(t, exists(other))
	})
	t.Run("AWantedPointerIsRenamedOrCreated", func(t *testing.T) {
		f, err := inst.VFS().FileByID(renamed.ID())
		require.NoError(t, err)
		assert.Equal(t, "New.docs-note", f.DocName)
		created, err := FindByExternalID(inst, "doc-created")
		require.NoError(t, err)
		require.NotNil(t, created)
		assert.Equal(t, "editor", created.Metadata[ExternalRoleKey])
	})
	t.Run("TheTrashFollowsTheWantedState", func(t *testing.T) {
		f, err := inst.VFS().FileByID(toTrash.ID())
		require.NoError(t, err)
		assert.True(t, f.Trashed)
		f, err = inst.VFS().FileByID(toRestore.ID())
		require.NoError(t, err)
		assert.False(t, f.Trashed)
		absent, err := FindByExternalID(inst, "doc-absent")
		require.NoError(t, err)
		assert.Nil(t, absent, "onlyIfExists creates nothing")
	})
	t.Run("ASyncWithoutScopeIsDropped", func(t *testing.T) {
		_, err := Apply(b, &Item{Op: OpSync, Instance: inst.Domain, AsOf: t1})
		assert.ErrorIs(t, err, errDefinitive)
	})
}

func TestUnchanged(t *testing.T) {
	f := &vfs.FileDoc{DocName: "Plan (2).docs-note", Metadata: vfs.Metadata{ExternalRoleKey: "reader"}}
	assert.True(t, unchanged(f, Wanted{Name: "Plan.docs-note", Role: "reader"}), "the suffix the stack took")
	assert.False(t, unchanged(f, Wanted{Name: "Plan.docs-note", Role: "editor"}))
	assert.False(t, unchanged(f, Wanted{Name: "Other.docs-note", Role: "reader"}))
	trashed := true
	assert.False(t, unchanged(f, Wanted{Name: "Plan.docs-note", Role: "reader", Trashed: &trashed}))
}

func TestDescribe(t *testing.T) {
	h := NewHandler(nil)
	err := h.Handle(context.Background(), amqp.Delivery{RoutingKey: RoutingKeyDescribe, Body: []byte(`{"instance":""}`)})
	assert.ErrorIs(t, err, rabbitmq.ErrMalformed)
	if testing.Short() {
		return
	}
	config.UseTestFile(t)
	testutils.NeedCouchdb(t)
	inst := testutils.NewSetup(t, t.Name()).GetTestInstance()
	// A drive the instance does not have publishes nothing, and is not retried
	assert.NoError(t, h.Handle(context.Background(), amqp.Delivery{RoutingKey: RoutingKeyDescribe, Body: mustJSON(t, map[string]any{"instance": inst.Domain, "sharing_id": "nope"})}))
}
