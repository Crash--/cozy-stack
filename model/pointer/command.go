// Package pointer applies the commands of a side service on the pointers of
// the instances: the files that stand for a document of another app
// (`metadata.externalId`). It is the only writer of those files on behalf of
// a service: no admin token leaves the stack.
package pointer

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cozy/cozy-stack/pkg/rabbitmq"
)

// RoutingKeyBatch is the routing key of a batch of commands, on the drive
// exchange.
const RoutingKeyBatch = "drive.pointer.batch"

// RoutingKeyDescribe asks the owner of a shared drive to publish its state.
const RoutingKeyDescribe = "drive.sharing.describe"

// Queue is the queue of the commands on the drive exchange.
const Queue = "stack.drive.pointers"

// MaxItems is the largest batch.
const MaxItems = 200

// The operations of an item.
const (
	OpEnsure  = "ensure"
	OpRemove  = "remove"
	OpTrash   = "trash"
	OpRestore = "restore"
	OpSync    = "sync"
)

// ScopePersonal is the scope of a sync: the pointers of the app on the
// instance, outside the shared drives it owns.
const ScopePersonal = "personal"

// MaxWanted is the largest wanted state of a sync.
const MaxWanted = 10000

// Batch is the payload of drive.pointer.batch: commands on the pointers of
// any instances, applied one by one.
type Batch struct {
	// App is the slug written in cozyMetadata, so the consumers of the file
	// events recognize the writes of the service.
	App string `json:"app"`
	// Mime is the type of the pointers.
	Mime string `json:"mime"`
	// Folder is the folder at the root of an instance where a pointer goes
	// when its item names no dirId; created if missing.
	Folder string `json:"folder"`
	Items  []Item `json:"items"`
}

// Item is one command on the pointer of a document on an instance.
type Item struct {
	Op         string `json:"op"`
	Instance   string `json:"instance"`
	ExternalID string `json:"externalId"`
	// FileID names a pointer file, when the instance may hold another one for
	// the document (a file just moved there).
	FileID string `json:"fileId,omitempty"`
	Name   string `json:"name,omitempty"`
	Role   string `json:"role,omitempty"`
	DirID  string `json:"dirId,omitempty"`
	// Member, with DirID, puts the pointer in a shared drive of the instance
	// only if this instance is one of its members with write access.
	Member   string    `json:"member,omitempty"`
	Fallback *Fallback `json:"fallback,omitempty"`
	// OnlyIfExists makes an ensure update the pointer without creating it.
	OnlyIfExists bool `json:"onlyIfExists,omitempty"`
	// Trashed, on an ensure, puts the existing pointer in the trash or takes it
	// out; nil leaves it where it is.
	Trashed *bool `json:"trashed,omitempty"`
	// AsOf is when the service read the state this command brings: an older
	// command than the last one applied to the pointer changes nothing.
	AsOf time.Time `json:"asOf"`
	// Scope and Wanted are the state a sync brings: every pointer of the scope
	// not in Wanted goes.
	Scope  string   `json:"scope,omitempty"`
	Wanted []Wanted `json:"wanted,omitempty"`
}

// Wanted is a pointer a sync keeps or creates.
type Wanted struct {
	ExternalID   string `json:"externalId"`
	Name         string `json:"name"`
	Role         string `json:"role,omitempty"`
	OnlyIfExists bool   `json:"onlyIfExists,omitempty"`
	Trashed      *bool  `json:"trashed,omitempty"`
}

// Fallback is where a pointer goes when its drive refuses the member.
type Fallback struct {
	Instance string `json:"instance"`
	Folder   string `json:"folder"`
}

// ParseBatch decodes a batch. Its errors wrap rabbitmq.ErrMalformed, since a
// redelivery cannot fix them. The items are checked one by one when applied.
func ParseBatch(body []byte) (*Batch, error) {
	var b Batch
	if err := json.Unmarshal(body, &b); err != nil {
		return nil, malformed("%s", err)
	}
	b.App = strings.TrimSpace(b.App)
	b.Mime = strings.TrimSpace(b.Mime)
	b.Folder = strings.TrimSpace(b.Folder)
	switch {
	case b.App == "":
		return nil, malformed("missing app")
	case b.Mime == "":
		return nil, malformed("missing mime")
	case len(b.Items) == 0:
		return nil, malformed("no items")
	case len(b.Items) > MaxItems:
		return nil, malformed("%d items, at most %d", len(b.Items), MaxItems)
	}
	return &b, nil
}

// check tells why an item cannot be applied, or nil.
func (it *Item) check(b *Batch) error {
	it.Instance = strings.TrimSpace(it.Instance)
	it.ExternalID = strings.TrimSpace(it.ExternalID)
	it.FileID = strings.TrimSpace(it.FileID)
	it.Name = strings.TrimSpace(it.Name)
	switch {
	case it.Instance == "":
		return fmt.Errorf("missing instance")
	case it.ExternalID == "" && it.Op != OpSync:
		return fmt.Errorf("missing externalId")
	case it.AsOf.IsZero():
		return fmt.Errorf("missing asOf")
	}
	switch it.Op {
	case OpSync:
		if it.Scope != ScopePersonal {
			return fmt.Errorf("unknown scope %q", it.Scope)
		}
		if len(it.Wanted) > MaxWanted {
			return fmt.Errorf("%d wanted, at most %d", len(it.Wanted), MaxWanted)
		}
		for _, w := range it.Wanted {
			if strings.TrimSpace(w.ExternalID) == "" || w.Name == "" || strings.ContainsAny(w.Name, "/\x00") {
				return fmt.Errorf("invalid wanted %q %q", w.ExternalID, w.Name)
			}
		}
		if b.Folder == "" {
			return fmt.Errorf("missing folder")
		}
	case OpEnsure:
		if it.Name == "" || strings.ContainsAny(it.Name, "/\x00") {
			return fmt.Errorf("invalid name %q", it.Name)
		}
		if it.DirID == "" && b.Folder == "" {
			return fmt.Errorf("missing dirId or folder")
		}
	case OpRemove, OpTrash, OpRestore:
	default:
		return fmt.Errorf("unknown op %q", it.Op)
	}
	return nil
}

func malformed(format string, args ...any) error {
	return fmt.Errorf("%s: %w: %s", RoutingKeyBatch, rabbitmq.ErrMalformed, fmt.Sprintf(format, args...))
}
