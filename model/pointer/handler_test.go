package pointer

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/cozy/cozy-stack/model/vfs"
	"github.com/cozy/cozy-stack/pkg/config/config"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/rabbitmq"
	"github.com/cozy/cozy-stack/tests/testutils"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	os.Exit(testutils.RunTestMainWithCouchDB(m))
}

func TestParseBatch(t *testing.T) {
	valid := map[string]any{"app": "docs-side-service", "mime": "application/vnd.twake.docs", "folder": "Docs",
		"items": []any{map[string]any{"op": "remove", "instance": "a", "externalId": "d", "asOf": "2026-11-27T10:00:00Z"}}}
	b, err := ParseBatch(mustJSON(t, valid))
	require.NoError(t, err)
	assert.Len(t, b.Items, 1)

	tooMany := make([]any, MaxItems+1)
	for i := range tooMany {
		tooMany[i] = valid["items"].([]any)[0]
	}
	for name, change := range map[string]func(map[string]any){
		"no app":    func(m map[string]any) { delete(m, "app") },
		"no mime":   func(m map[string]any) { delete(m, "mime") },
		"no items":  func(m map[string]any) { m["items"] = []any{} },
		"too large": func(m map[string]any) { m["items"] = tooMany },
	} {
		t.Run(name, func(t *testing.T) {
			m := map[string]any{}
			for k, v := range valid {
				m[k] = v
			}
			change(m)
			_, err := ParseBatch(mustJSON(t, m))
			assert.ErrorIs(t, err, rabbitmq.ErrMalformed)
		})
	}
	_, err = ParseBatch([]byte("{"))
	assert.ErrorIs(t, err, rabbitmq.ErrMalformed)
}

func TestHandler(t *testing.T) {
	if testing.Short() {
		t.Skip("an instance is required for this test: test skipped due to the use of --short flag")
	}
	config.UseTestFile(t)
	testutils.NeedCouchdb(t)
	ctx := context.Background()
	setup := testutils.NewSetup(t, t.Name())
	inst := setup.GetTestInstance()
	h := NewHandler(nil)
	t0 := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)

	batch := func(items ...map[string]any) amqp.Delivery {
		for _, it := range items {
			if it["instance"] == nil {
				it["instance"] = inst.Domain
			}
			if it["asOf"] == nil {
				it["asOf"] = t0
			}
		}
		body := map[string]any{"app": "docs-side-service", "mime": "application/vnd.twake.docs", "folder": "Docs", "items": items}
		return amqp.Delivery{RoutingKey: RoutingKeyBatch, Body: mustJSON(t, body)}
	}
	pointerOf := func(externalID string) *vfs.FileDoc {
		t.Helper()
		file, err := FindByExternalID(inst, externalID)
		require.NoError(t, err)
		return file
	}

	t.Run("EnsureCreatesThePointerInTheAppFolder", func(t *testing.T) {
		require.NoError(t, h.Handle(ctx, batch(map[string]any{"op": "ensure", "externalId": "doc-1", "name": "A.docs-note", "role": "owner"})))
		file := pointerOf("doc-1")
		require.NotNil(t, file)
		assert.Equal(t, "A.docs-note", file.DocName)
		assert.Equal(t, "application/vnd.twake.docs", file.Mime)
		assert.Equal(t, "owner", file.Metadata[ExternalRoleKey])
		assert.Equal(t, "2026-11-27T10:00:00Z", file.Metadata[ExternalAsOfKey])
		assert.Equal(t, "docs-side-service", file.CozyMetadata.CreatedByApp)
		dir, err := inst.VFS().DirByID(file.DirID)
		require.NoError(t, err)
		assert.Equal(t, "/Docs", dir.Fullpath)
	})

	t.Run("EnsureIsIdempotentAndRenames", func(t *testing.T) {
		first := pointerOf("doc-1").ID()
		ensure := map[string]any{"op": "ensure", "externalId": "doc-1", "name": "B.docs-note", "role": "reader", "asOf": t0.Add(time.Second)}
		require.NoError(t, h.Handle(ctx, batch(ensure)))
		require.NoError(t, h.Handle(ctx, batch(map[string]any{"op": "ensure", "externalId": "doc-1", "name": "B.docs-note", "role": "reader", "asOf": t0.Add(time.Second)})))
		file := pointerOf("doc-1")
		assert.Equal(t, first, file.ID(), "the same file, found by externalId")
		assert.Equal(t, "B.docs-note", file.DocName)
		assert.Equal(t, "reader", file.Metadata[ExternalRoleKey])
		apps := file.CozyMetadata.UpdatedByApps
		assert.Equal(t, "docs-side-service", apps[len(apps)-1].Slug)
	})

	t.Run("AnOlderCommandChangesNothing", func(t *testing.T) {
		// Read before the rename above, applied after it
		require.NoError(t, h.Handle(ctx, batch(map[string]any{"op": "remove", "externalId": "doc-1", "asOf": t0})))
		file := pointerOf("doc-1")
		require.NotNil(t, file, "the stale remove is ignored")
		assert.Equal(t, "B.docs-note", file.DocName)
		// A later read removes it
		require.NoError(t, h.Handle(ctx, batch(map[string]any{"op": "remove", "externalId": "doc-1", "asOf": t0.Add(2 * time.Second)})))
		assert.Nil(t, pointerOf("doc-1"))
		// A redelivery finds nothing
		require.NoError(t, h.Handle(ctx, batch(map[string]any{"op": "remove", "externalId": "doc-1", "asOf": t0.Add(2 * time.Second)})))
	})

	t.Run("EnsureMovesToTheGivenFolder", func(t *testing.T) {
		dir, err := vfs.MkdirAll(inst.VFS(), "/Projects")
		require.NoError(t, err)
		require.NoError(t, h.Handle(ctx, batch(
			map[string]any{"op": "ensure", "externalId": "doc-2", "name": "B.docs-note", "dirId": dir.ID()},
			// a second document with the same name in the same folder gets a suffix
			map[string]any{"op": "ensure", "externalId": "doc-3", "name": "B.docs-note", "dirId": dir.ID()},
		)))
		assert.Equal(t, dir.ID(), pointerOf("doc-2").DirID)
		assert.Equal(t, "B (2).docs-note", pointerOf("doc-3").DocName)
	})

	t.Run("TrashAndRestore", func(t *testing.T) {
		require.NoError(t, h.Handle(ctx, batch(map[string]any{"op": "trash", "externalId": "doc-2", "asOf": t0.Add(time.Second)})))
		file := pointerOf("doc-2")
		assert.True(t, file.Trashed)
		apps := file.CozyMetadata.UpdatedByApps
		assert.Equal(t, "docs-side-service", apps[len(apps)-1].Slug)
		assert.Equal(t, "2026-11-27T10:00:01Z", file.Metadata[ExternalAsOfKey])
		// already in the trash: nothing changes
		require.NoError(t, h.Handle(ctx, batch(map[string]any{"op": "trash", "externalId": "doc-2", "asOf": t0.Add(time.Second)})))

		require.NoError(t, h.Handle(ctx, batch(map[string]any{"op": "restore", "externalId": "doc-2", "asOf": t0.Add(2 * time.Second)})))
		file = pointerOf("doc-2")
		assert.False(t, file.Trashed)
		assert.NotEqual(t, consts.TrashDirID, file.DirID)
	})

	t.Run("AFailedItemDoesNotStopTheOthers", func(t *testing.T) {
		require.NoError(t, h.Handle(ctx, batch(
			map[string]any{"op": "ensure", "instance": "nobody.cozy.local", "externalId": "doc-4", "name": "D.docs-note"},
			map[string]any{"op": "ensure", "externalId": "doc-4"}, // no name
			map[string]any{"op": "explode", "externalId": "doc-4"},
			map[string]any{"op": "ensure", "externalId": "doc-4", "name": "D.docs-note"},
		)))
		require.NotNil(t, pointerOf("doc-4"))
	})

	t.Run("AMemberOutsideAnyDriveFallsBack", func(t *testing.T) {
		dir, err := vfs.MkdirAll(inst.VFS(), "/Not a drive")
		require.NoError(t, err)
		require.NoError(t, h.Handle(ctx, batch(map[string]any{
			"op": "ensure", "externalId": "doc-5", "name": "E.docs-note", "dirId": dir.ID(), "member": "frank.cozy.local",
			"fallback": map[string]any{"instance": inst.Domain, "folder": "Fallback"},
		})))
		file := pointerOf("doc-5")
		require.NotNil(t, file)
		parent, err := inst.VFS().DirByID(file.DirID)
		require.NoError(t, err)
		assert.Equal(t, "/Fallback", parent.Fullpath)
	})

	t.Run("OnlyIfExistsCreatesNothing", func(t *testing.T) {
		require.NoError(t, h.Handle(ctx, batch(map[string]any{"op": "ensure", "externalId": "doc-6", "name": "F.docs-note", "onlyIfExists": true})))
		assert.Nil(t, pointerOf("doc-6"))
		require.NoError(t, h.Handle(ctx, batch(map[string]any{"op": "ensure", "externalId": "doc-4", "name": "G.docs-note", "onlyIfExists": true, "asOf": t0.Add(time.Second)})))
		assert.Equal(t, "G.docs-note", pointerOf("doc-4").DocName)
	})

	t.Run("AFolderGoneIsNotRetriedForever", func(t *testing.T) {
		gone, err := vfs.MkdirAll(inst.VFS(), "/Gone")
		require.NoError(t, err)
		require.NoError(t, inst.VFS().DestroyDirAndContent(gone, func(vfs.TrashJournal) error { return nil }))
		// A new pointer for a folder that no longer exists lands in the folder of the app
		require.NoError(t, h.Handle(ctx, batch(map[string]any{"op": "ensure", "externalId": "doc-7", "name": "H.docs-note", "dirId": gone.ID()})))
		file := pointerOf("doc-7")
		require.NotNil(t, file)
		parent, err := inst.VFS().DirByID(file.DirID)
		require.NoError(t, err)
		assert.Equal(t, "/Docs", parent.Fullpath)
		// An existing one stays where it is, and takes its new name
		require.NoError(t, h.Handle(ctx, batch(map[string]any{"op": "ensure", "externalId": "doc-7", "name": "I.docs-note", "dirId": gone.ID(), "asOf": t0.Add(time.Second)})))
		assert.Equal(t, "I.docs-note", pointerOf("doc-7").DocName)
		assert.Equal(t, parent.ID(), pointerOf("doc-7").DirID)
		// With a fallback, the fallback
		require.NoError(t, h.Handle(ctx, batch(map[string]any{
			"op": "ensure", "externalId": "doc-8", "name": "J.docs-note", "dirId": gone.ID(), "member": "frank.cozy.local",
			"fallback": map[string]any{"instance": inst.Domain, "folder": "Fallback"},
		})))
		fb, err := inst.VFS().DirByID(pointerOf("doc-8").DirID)
		require.NoError(t, err)
		assert.Equal(t, "/Fallback", fb.Fullpath)
	})

	t.Run("APermanentErrorOfTheVFSIsDefinitive", func(t *testing.T) {
		assert.ErrorIs(t, permanentOrNot(vfs.ErrParentDoesNotExist), errDefinitive)
		assert.NotErrorIs(t, permanentOrNot(os.ErrDeadlineExceeded), errDefinitive)
	})

	t.Run("WrongRoutingKeyIsMalformed", func(t *testing.T) {
		err := h.Handle(ctx, amqp.Delivery{RoutingKey: "drive.pointer.ensure", Body: []byte("{}")})
		assert.ErrorIs(t, err, rabbitmq.ErrMalformed)
	})
}

func TestDriveQueueUsesThePointerHandler(t *testing.T) {
	svc := new(rabbitmq.RabbitMQService)
	specs := svc.BuildExchangeSpecs([]config.RabbitExchange{{
		Name:   rabbitmq.ExchangeDrive,
		Kind:   "topic",
		Queues: []config.RabbitQueue{{Name: Queue}},
	}})
	require.Len(t, specs, 1)
	require.Len(t, specs[0].Queues, 1)
	assert.Equal(t, NewHandler(svc), specs[0].Queues[0].Handler)
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}
