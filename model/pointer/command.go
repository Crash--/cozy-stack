// Package pointer applies the commands of a side service on the pointers of
// an instance: the files that stand for a document of another app
// (`metadata.externalId`). It is the only writer of those files on behalf of
// a service: no admin token leaves the stack.
package pointer

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cozy/cozy-stack/pkg/rabbitmq"
)

// The routing keys of the commands, on the drive exchange.
const (
	RoutingKeyEnsure  = "drive.pointer.ensure"
	RoutingKeyRemove  = "drive.pointer.remove"
	RoutingKeyRestore = "drive.pointer.restore"
	RoutingKeyTrash   = "drive.pointer.trash"
	// RoutingKeyReady is the reply of a command, on the activity exchange.
	RoutingKeyReady = "com.twake.drive.pointer.ready.v1"
)

// Queue is the queue of the commands on the drive exchange.
const Queue = "stack.drive.pointers"

// Command is the payload of the three commands.
type Command struct {
	// Instance is the domain of the instance that holds the pointer.
	Instance string `json:"instance"`
	// RequestID comes back in the reply, for the side service to match it.
	RequestID string `json:"requestId,omitempty"`
	// FileID names an existing pointer. Without it, ensure finds the pointer
	// by externalId, or creates it.
	FileID string `json:"fileId,omitempty"`
	// Ensure only:
	ExternalID string `json:"externalId,omitempty"`
	Name       string `json:"name,omitempty"`
	Mime       string `json:"mime,omitempty"`
	Role       string `json:"role,omitempty"`
	// DirID is where a new pointer goes; Folder is a folder at the root of the
	// instance, created if missing, used when DirID is empty.
	DirID  string `json:"dirId,omitempty"`
	Folder string `json:"folder,omitempty"`
	// App is the slug written in cozyMetadata, so the consumers of the file
	// events recognize the writes of the service.
	App string `json:"app,omitempty"`
}

// ParseCommand decodes and validates a command. Its errors wrap
// rabbitmq.ErrMalformed, since a redelivery cannot fix them.
func ParseCommand(routingKey string, body []byte) (*Command, error) {
	var cmd Command
	if err := json.Unmarshal(body, &cmd); err != nil {
		return nil, malformed(routingKey, "%s", err)
	}
	cmd.Instance = strings.TrimSpace(cmd.Instance)
	cmd.FileID = strings.TrimSpace(cmd.FileID)
	cmd.ExternalID = strings.TrimSpace(cmd.ExternalID)
	cmd.Name = strings.TrimSpace(cmd.Name)
	if cmd.Instance == "" {
		return nil, malformed(routingKey, "missing instance")
	}
	switch routingKey {
	case RoutingKeyEnsure:
		switch {
		case cmd.ExternalID == "":
			return nil, malformed(routingKey, "missing externalId")
		case cmd.Name == "" || strings.ContainsAny(cmd.Name, "/\x00"):
			return nil, malformed(routingKey, "invalid name %q", cmd.Name)
		case cmd.Mime == "":
			return nil, malformed(routingKey, "missing mime")
		case cmd.DirID == "" && cmd.Folder == "":
			return nil, malformed(routingKey, "missing dirId or folder")
		}
	case RoutingKeyRemove, RoutingKeyRestore, RoutingKeyTrash:
		if cmd.FileID == "" {
			return nil, malformed(routingKey, "missing fileId")
		}
	default:
		return nil, malformed(routingKey, "unknown command")
	}
	return &cmd, nil
}

func malformed(routingKey, format string, args ...any) error {
	return fmt.Errorf("%s: %w: %s", routingKey, rabbitmq.ErrMalformed, fmt.Sprintf(format, args...))
}
