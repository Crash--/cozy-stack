package instances

import (
	"errors"
	"net/http"
	"strings"

	"github.com/cozy/cozy-stack/model/instance"
	"github.com/cozy/cozy-stack/model/instance/lifecycle"
	"github.com/cozy/cozy-stack/model/pointer"
	"github.com/cozy/cozy-stack/model/sharing"
	"github.com/cozy/cozy-stack/pkg/jsonapi"
	"github.com/labstack/echo/v4"
)

type pointersQuery struct {
	Instances []string `json:"instances"`
}

type pointerInfo struct {
	FileID     string `json:"fileId"`
	ExternalID string `json:"externalId"`
	Name       string `json:"name"`
	Mime       string `json:"mime"`
	Role       string `json:"role,omitempty"`
	DirID      string `json:"dirId"`
	Trashed    bool   `json:"trashed"`
	DriveID    string `json:"driveId,omitempty"`
	AsOf       string `json:"asOf,omitempty"`
}

// queryPointers returns the pointers of several instances, for a side service
// that compares them with the documents of its product.
func queryPointers(c echo.Context) error {
	var q pointersQuery
	if err := c.Bind(&q); err != nil {
		return jsonapi.BadJSON()
	}
	if len(q.Instances) == 0 || len(q.Instances) > pointer.MaxItems {
		return jsonapi.InvalidParameter("instances", errors.New("between 1 and 200 instances"))
	}
	result := make(map[string]any, len(q.Instances))
	for _, domain := range q.Instances {
		domain = strings.TrimSpace(domain)
		inst, err := lifecycle.GetInstance(domain)
		if err != nil {
			if errors.Is(err, instance.ErrNotFound) {
				result[domain] = echo.Map{"error": "not found"}
				continue
			}
			return err
		}
		files, err := pointer.List(inst)
		if err != nil {
			return err
		}
		infos := make([]pointerInfo, 0, len(files))
		for _, f := range files {
			info := pointerInfo{FileID: f.ID(), Name: f.DocName, Mime: f.Mime, DirID: f.DirID, Trashed: f.Trashed}
			info.ExternalID, _ = f.Metadata[pointer.ExternalIDKey].(string)
			info.Role, _ = f.Metadata[pointer.ExternalRoleKey].(string)
			info.AsOf, _ = f.Metadata[pointer.ExternalAsOfKey].(string)
			if !f.Trashed {
				info.DriveID = sharing.DriveOf(inst, f)
			}
			infos = append(infos, info)
		}
		result[domain] = infos
	}
	return c.JSON(http.StatusOK, echo.Map{"instances": result})
}

type driveMember struct {
	Instance string `json:"instance"`
	Email    string `json:"email"`
	ReadOnly bool   `json:"readOnly"`
	Status   string `json:"status"`
}

// showDrive returns a shared drive owned by the instance, with its members.
func showDrive(c echo.Context) error {
	inst, err := lifecycle.GetInstance(c.Param("domain"))
	if err != nil {
		return err
	}
	s, err := sharing.FindSharing(inst, c.Param("sharing-id"))
	if err != nil {
		return err
	}
	if !s.Drive || !s.Owner {
		return jsonapi.NotFound(errors.New("not a shared drive owned by this instance"))
	}
	rootIDs := []string{}
	if rule := s.FirstFilesRule(); rule != nil {
		rootIDs = rule.Values
	}
	members := []driveMember{}
	for i, m := range s.Members {
		if i == 0 {
			continue // the owner
		}
		host := strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(m.Instance, "https://"), "http://"), "/")
		members = append(members, driveMember{Instance: host, Email: m.Email, ReadOnly: m.ReadOnly, Status: m.Status})
	}
	return c.JSON(http.StatusOK, echo.Map{"id": s.SID, "name": s.Description, "rootIds": rootIDs, "members": members})
}
