package pointer

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/cozy/cozy-stack/model/instance"
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

type fakePublisher struct {
	requests []rabbitmq.PublishRequest
}

func (p *fakePublisher) Publish(_ context.Context, req rabbitmq.PublishRequest) error {
	p.requests = append(p.requests, req)
	return nil
}

func (p *fakePublisher) last(t *testing.T) ReadyData {
	t.Helper()
	require.NotEmpty(t, p.requests)
	req := p.requests[len(p.requests)-1]
	assert.Equal(t, rabbitmq.ExchangeActivity, req.Exchange)
	assert.Equal(t, RoutingKeyReady, req.RoutingKey)
	event := req.Payload.(rabbitmq.CloudEvent)
	assert.Equal(t, "twake://drive", event.Source)
	return event.Data.(ReadyData)
}

func TestParseCommand(t *testing.T) {
	valid := map[string]any{"instance": "alice.cozy.local", "externalId": "doc-1", "name": "A.docs-note", "mime": "application/vnd.twake.docs", "folder": "Docs"}
	cmd, err := ParseCommand(RoutingKeyEnsure, mustJSON(t, valid))
	require.NoError(t, err)
	assert.Equal(t, "doc-1", cmd.ExternalID)

	for name, change := range map[string]func(map[string]any){
		"no instance":   func(m map[string]any) { delete(m, "instance") },
		"no externalId": func(m map[string]any) { delete(m, "externalId") },
		"no name":       func(m map[string]any) { m["name"] = " " },
		"slash in name": func(m map[string]any) { m["name"] = "a/b" },
		"no mime":       func(m map[string]any) { delete(m, "mime") },
		"no folder":     func(m map[string]any) { delete(m, "folder") },
	} {
		t.Run(name, func(t *testing.T) {
			m := map[string]any{}
			for k, v := range valid {
				m[k] = v
			}
			change(m)
			_, err := ParseCommand(RoutingKeyEnsure, mustJSON(t, m))
			assert.ErrorIs(t, err, rabbitmq.ErrMalformed)
		})
	}
	_, err = ParseCommand(RoutingKeyRemove, mustJSON(t, map[string]any{"instance": "alice.cozy.local"}))
	assert.ErrorIs(t, err, rabbitmq.ErrMalformed)
	_, err = ParseCommand("drive.pointer.explode", mustJSON(t, valid))
	assert.ErrorIs(t, err, rabbitmq.ErrMalformed)
	_, err = ParseCommand(RoutingKeyEnsure, []byte("{"))
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
	pub := &fakePublisher{}
	h := NewHandler(pub)

	ensure := func(fields map[string]any) amqp.Delivery {
		body := map[string]any{"instance": inst.Domain, "externalId": "doc-1", "name": "A.docs-note", "mime": "application/vnd.twake.docs", "folder": "Docs", "role": "owner", "app": "docs-side-service", "requestId": "r1"}
		for k, v := range fields {
			body[k] = v
		}
		return amqp.Delivery{RoutingKey: RoutingKeyEnsure, Body: mustJSON(t, body)}
	}

	t.Run("EnsureCreatesThePointerInTheAppFolder", func(t *testing.T) {
		require.NoError(t, h.Handle(ctx, ensure(nil)))
		data := pub.last(t)
		assert.Equal(t, "r1", data.RequestID)
		assert.Equal(t, "doc-1", data.ExternalID)
		file, err := inst.VFS().FileByID(data.FileID)
		require.NoError(t, err)
		assert.Equal(t, "A.docs-note", file.DocName)
		assert.Equal(t, "application/vnd.twake.docs", file.Mime)
		assert.Equal(t, "doc-1", file.Metadata[ExternalIDKey])
		assert.Equal(t, "owner", file.Metadata[ExternalRoleKey])
		assert.Equal(t, "docs-side-service", file.CozyMetadata.CreatedByApp)
		dir, err := inst.VFS().DirByID(file.DirID)
		require.NoError(t, err)
		assert.Equal(t, "/Docs", dir.Fullpath)
		assert.Equal(t, data.DirID, dir.ID())
	})

	t.Run("EnsureIsIdempotentAndRenames", func(t *testing.T) {
		require.NoError(t, h.Handle(ctx, ensure(nil)))
		first := pub.last(t).FileID
		require.NoError(t, h.Handle(ctx, ensure(map[string]any{"name": "B.docs-note", "role": "reader"})))
		data := pub.last(t)
		assert.Equal(t, first, data.FileID, "the same file, found by externalId")
		file, err := inst.VFS().FileByID(first)
		require.NoError(t, err)
		assert.Equal(t, "B.docs-note", file.DocName)
		assert.Equal(t, "reader", file.Metadata[ExternalRoleKey])
		assert.Equal(t, "doc-1", file.Metadata[ExternalIDKey])
	})

	t.Run("EnsureMovesToTheGivenFolder", func(t *testing.T) {
		dir, err := vfs.MkdirAll(inst.VFS(), "/Projects")
		require.NoError(t, err)
		require.NoError(t, h.Handle(ctx, ensure(map[string]any{"dirId": dir.ID(), "name": "B.docs-note"})))
		data := pub.last(t)
		assert.Equal(t, dir.ID(), data.DirID)
		// a second document with the same name in the same folder gets a suffix
		require.NoError(t, h.Handle(ctx, ensure(map[string]any{"dirId": dir.ID(), "name": "B.docs-note", "externalId": "doc-2"})))
		file, err := inst.VFS().FileByID(pub.last(t).FileID)
		require.NoError(t, err)
		assert.Equal(t, "B (2).docs-note", file.DocName)
	})

	t.Run("RemoveAndRestore", func(t *testing.T) {
		require.NoError(t, h.Handle(ctx, ensure(map[string]any{"externalId": "doc-3", "name": "C.docs-note"})))
		fileID := pub.last(t).FileID
		trash := amqp.Delivery{RoutingKey: RoutingKeyTrash, Body: mustJSON(t, map[string]any{"instance": inst.Domain, "fileId": fileID, "app": "docs-side-service"})}
		require.NoError(t, h.Handle(ctx, trash))
		file, err := inst.VFS().FileByID(fileID)
		require.NoError(t, err)
		assert.True(t, file.Trashed)
		// the write is marked as the service's, for the consumers of the file events
		apps := file.CozyMetadata.UpdatedByApps
		assert.Equal(t, "docs-side-service", apps[len(apps)-1].Slug)
		assert.Equal(t, "doc-3", pub.last(t).ExternalID)
		// already in the trash: nothing changes
		require.NoError(t, h.Handle(ctx, trash))
		assert.False(t, pub.last(t).Gone)

		restore := amqp.Delivery{RoutingKey: RoutingKeyRestore, Body: mustJSON(t, map[string]any{"instance": inst.Domain, "fileId": fileID})}
		require.NoError(t, h.Handle(ctx, restore))
		data := pub.last(t)
		assert.False(t, data.Gone)
		assert.Equal(t, "doc-3", data.ExternalID)
		file, err = inst.VFS().FileByID(fileID)
		require.NoError(t, err)
		assert.False(t, file.Trashed)
		assert.NotEqual(t, consts.TrashDirID, file.DirID)

		remove := amqp.Delivery{RoutingKey: RoutingKeyRemove, Body: mustJSON(t, map[string]any{"instance": inst.Domain, "fileId": fileID})}
		require.NoError(t, h.Handle(ctx, remove))
		assert.False(t, pub.last(t).Gone)
		_, err = inst.VFS().FileByID(fileID)
		assert.True(t, os.IsNotExist(err))
		// a redelivery finds nothing and says so
		require.NoError(t, h.Handle(ctx, remove))
		assert.True(t, pub.last(t).Gone)
		found, err := FindByExternalID(inst, "doc-3")
		require.NoError(t, err)
		assert.Nil(t, found)
	})

	t.Run("UnknownInstanceIsMalformed", func(t *testing.T) {
		err := h.Handle(ctx, amqp.Delivery{RoutingKey: RoutingKeyRemove, Body: mustJSON(t, map[string]any{"instance": "nobody.cozy.local", "fileId": "x"})})
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

var _ = instance.ErrNotFound

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}
