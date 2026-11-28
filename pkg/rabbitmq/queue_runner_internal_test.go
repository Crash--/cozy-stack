package rabbitmq

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cozy/cozy-stack/pkg/config/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

type recordingAcknowledger struct{ calls []string }

func (a *recordingAcknowledger) Ack(_ uint64, _ bool) error {
	a.calls = append(a.calls, "ack")
	return nil
}

func (a *recordingAcknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	a.calls = append(a.calls, fmt.Sprintf("nack requeue=%t", requeue))
	return nil
}

func (a *recordingAcknowledger) Reject(_ uint64, requeue bool) error {
	a.calls = append(a.calls, fmt.Sprintf("reject requeue=%t", requeue))
	return nil
}

func TestSettle(t *testing.T) {
	r := &queueRunner{queue: QueueSpec{cfg: &config.RabbitQueue{Name: "q"}}}
	settle := func(err error) []string {
		ack := &recordingAcknowledger{}
		r.settle(amqp.Delivery{Acknowledger: ack}, 1, err)
		return ack.calls
	}
	assert.Equal(t, []string{"ack"}, settle(nil))
	assert.Equal(t, []string{"nack requeue=false"}, settle(fmt.Errorf("x: %w", ErrMalformed)), "dead-lettered at once")
	// A nack with requeue is never counted by the delivery limit of a quorum
	// queue (RabbitMQ 4.3 and later): a failure must be a reject
	assert.Equal(t, []string{"reject requeue=true"}, settle(errors.New("couchdb is down")))
}
