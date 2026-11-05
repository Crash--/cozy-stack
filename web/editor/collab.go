package editor

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/cozy/cozy-stack/model/instance"
	"github.com/cozy/cozy-stack/model/permission"
	"github.com/cozy/cozy-stack/model/vfs"
	"github.com/cozy/cozy-stack/pkg/config/config"
	"github.com/cozy/cozy-stack/pkg/crypto"
	"github.com/cozy/cozy-stack/pkg/jsonapi"
	"github.com/cozy/cozy-stack/pkg/metadata"
	"github.com/cozy/cozy-stack/web/middlewares"
	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo/v4"
)

// A Yjs collaboration server edits the file on behalf of the users. The stack
// gives the browser a short JWT (file, mode) signed with a secret shared with
// that server, and the server calls back the content routes with its own JWT
// signed with the same secret, like the OnlyOffice Document Server does.

const (
	collabUserAudience   = "collab"
	collabServerAudience = "cozy-stack"
	collabTokenTTL       = 10 * time.Minute
	collabSlug           = "collab-server"
	attachmentsRoot      = "/.cozy_editor_attachments"
	maxCollabContentSize = 64 << 20
	revHeader            = "X-Cozy-Rev"
)

type collabClaims struct {
	jwt.RegisteredClaims
	Mode     string `json:"mode,omitempty"`
	Instance string `json:"instance,omitempty"`
}

func getCollabConfig(inst *instance.Instance) *config.Collab {
	all := config.GetConfig().Collab
	if c, ok := all[inst.ContextName]; ok && c.Secret != "" {
		return &c
	}
	if c, ok := all[config.DefaultInstanceContext]; ok && c.Secret != "" {
		return &c
	}
	return nil
}

// CollabOpen is the API handler for GET /editor/:file-id/collab. It returns
// where the collaboration server is, the room of the file, and a token that
// lets the caller join the room in edit or view mode.
func CollabOpen(c echo.Context) error {
	inst := middlewares.GetInstance(c)
	cfg := getCollabConfig(inst)
	if cfg == nil {
		return jsonapi.NotFound(errors.New("no collaboration server"))
	}
	file, err := inst.VFS().FileByID(c.Param("file-id"))
	if err != nil {
		return wrapError(err)
	}
	if err := middlewares.AllowVFS(c, permission.GET, file); err != nil {
		return err
	}
	mode := "edit"
	if file.Trashed || middlewares.AllowVFS(c, permission.PUT, file) != nil {
		mode = "view"
	}

	now := time.Now()
	claims := collabClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    inst.Domain,
			Subject:   file.ID(),
			Audience:  jwt.ClaimStrings{collabUserAudience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(collabTokenTTL)),
		},
		Mode:     mode,
		Instance: strings.TrimRight(inst.PageURL("/", nil), "/"),
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, &claims).SignedString([]byte(cfg.Secret))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, echo.Map{
		"url":   cfg.URL,
		"room":  inst.Domain + "/" + file.ID(),
		"mode":  mode,
		"token": token,
	})
}

// checkServerToken verifies the JWT that the collaboration server sends on
// the content routes: signed with the shared secret, for this file.
func checkServerToken(c echo.Context, inst *instance.Instance, fileID string) error {
	cfg := getCollabConfig(inst)
	if cfg == nil {
		return jsonapi.NotFound(errors.New("no collaboration server"))
	}
	raw := strings.TrimPrefix(c.Request().Header.Get(echo.HeaderAuthorization), "Bearer ")
	var claims collabClaims
	err := crypto.ParseJWT(raw, func(token *jwt.Token) (interface{}, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, permission.ErrInvalidToken
		}
		return []byte(cfg.Secret), nil
	}, &claims)
	if err != nil || claims.Subject != fileID || claims.Issuer != inst.Domain {
		return jsonapi.NewError(http.StatusUnauthorized, "invalid collab token")
	}
	for _, aud := range claims.Audience {
		if aud == collabServerAudience {
			return nil
		}
	}
	return jsonapi.NewError(http.StatusUnauthorized, "invalid collab token")
}

// CollabContent is the handler for GET /editor/collab/:file-id, used by the
// collaboration server to load the document. The revision is in the
// X-Cozy-Rev header (ServeFileContent owns the ETag, an md5).
func CollabContent(c echo.Context) error {
	inst := middlewares.GetInstance(c)
	fileID := c.Param("file-id")
	if err := checkServerToken(c, inst, fileID); err != nil {
		return err
	}
	file, err := inst.VFS().FileByID(fileID)
	if err != nil {
		return wrapError(err)
	}
	c.Response().Header().Set(revHeader, file.Rev())
	if c.Request().Method == http.MethodHead {
		return c.NoContent(http.StatusOK)
	}
	return vfs.ServeFileContent(inst.VFS(), file, nil, "", "inline", c.Request(), c.Response())
}

type collabSave struct {
	Content  string `json:"content"`
	Markdown string `json:"markdown"`
}

// CollabSave is the handler for PUT /editor/collab/:file-id. It replaces the
// file content with the Yjs state, keeps the readable export in the file
// metadata (versions keep it too), and refuses a stale If-Match.
func CollabSave(c echo.Context) error {
	inst := middlewares.GetInstance(c)
	fileID := c.Param("file-id")
	if err := checkServerToken(c, inst, fileID); err != nil {
		return err
	}
	var body collabSave
	if err := c.Bind(&body); err != nil {
		return jsonapi.BadRequest(err)
	}
	content, err := base64.StdEncoding.DecodeString(body.Content)
	if err != nil || len(content) > maxCollabContentSize {
		return jsonapi.BadRequest(errors.New("bad content"))
	}

	fs := inst.VFS()
	file, err := fs.FileByID(fileID)
	if err != nil {
		return wrapError(err)
	}
	if rev := c.Request().Header.Get("If-Match"); rev != "" && rev != file.Rev() {
		return jsonapi.PreconditionFailed("If-Match", errors.New("revision changed"))
	}

	newfile := file.Clone().(*vfs.FileDoc)
	newfile.MD5Sum = nil
	newfile.ByteSize = int64(len(content))
	newfile.UpdatedAt = time.Now()
	newfile.Metadata = vfs.Metadata{}
	for k, v := range file.Metadata {
		newfile.Metadata[k] = v
	}
	newfile.Metadata["markdown"] = body.Markdown
	instanceURL := inst.PageURL("/", nil)
	if newfile.CozyMetadata == nil {
		newfile.CozyMetadata = vfs.NewCozyMetadata(instanceURL)
	}
	newfile.CozyMetadata.UpdatedByApp(&metadata.UpdatedByAppEntry{
		Slug: collabSlug, Date: newfile.UpdatedAt, Instance: instanceURL,
	})
	newfile.CozyMetadata.UpdatedAt = newfile.UpdatedAt
	newfile.CozyMetadata.UploadedAt = &newfile.UpdatedAt
	newfile.CozyMetadata.UploadedBy = &vfs.UploadedByEntry{Slug: collabSlug}

	f, err := fs.CreateFile(newfile, file)
	if err != nil {
		return wrapError(err)
	}
	_, err = f.Write(content)
	if cerr := f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		return wrapError(err)
	}
	return c.JSON(http.StatusOK, echo.Map{"rev": newfile.Rev()})
}

// UploadAttachment is the handler for POST /editor/:file-id/attachments. The
// attachment is a regular file, in a directory kept for the note, and whoever
// can write the note can add one.
func UploadAttachment(c echo.Context) error {
	inst := middlewares.GetInstance(c)
	fs := inst.VFS()
	note, err := fs.FileByID(c.Param("file-id"))
	if err != nil {
		return wrapError(err)
	}
	if err := middlewares.AllowVFS(c, permission.PUT, note); err != nil {
		return err
	}
	// shortcut: a plain directory, visible at the root of the owner's Drive;
	// a system directory hidden by Drive (or the thumbs storage used by
	// cozy-notes) before shipping.
	dir, err := vfs.MkdirAll(fs, path.Join(attachmentsRoot, note.ID()))
	if err != nil {
		return wrapError(err)
	}
	mime := c.Request().Header.Get(echo.HeaderContentType)
	if !strings.HasPrefix(mime, "image/") {
		return jsonapi.BadRequest(errors.New("only images"))
	}
	name := fmt.Sprintf("%d%s", time.Now().UnixNano(), path.Ext(path.Base(c.QueryParam("Name"))))
	doc, err := vfs.NewFileDoc(name, dir.ID(), c.Request().ContentLength, nil, mime, "image",
		time.Now(), false, false, false, nil)
	if err != nil {
		return jsonapi.BadRequest(err)
	}
	f, err := fs.CreateFile(doc, nil)
	if err != nil {
		return wrapError(err)
	}
	_, err = io.Copy(f, io.LimitReader(c.Request().Body, maxCollabContentSize))
	if cerr := f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		return wrapError(err)
	}
	return c.JSON(http.StatusCreated, echo.Map{"id": doc.ID()})
}

// GetAttachment is the handler for GET /editor/:file-id/attachments/:id. It
// follows the rights of the note, not of the attachment directory.
func GetAttachment(c echo.Context) error {
	inst := middlewares.GetInstance(c)
	fs := inst.VFS()
	note, err := fs.FileByID(c.Param("file-id"))
	if err != nil {
		return wrapError(err)
	}
	if err := middlewares.AllowVFS(c, permission.GET, note); err != nil {
		return err
	}
	dir, err := fs.DirByPath(path.Join(attachmentsRoot, note.ID()))
	if err != nil {
		return wrapError(err)
	}
	att, err := fs.FileByID(c.Param("attachment-id"))
	if err != nil {
		return wrapError(err)
	}
	if att.DirID != dir.ID() {
		return jsonapi.NotFound(os.ErrNotExist)
	}
	return vfs.ServeFileContent(fs, att, nil, "", "inline", c.Request(), c.Response())
}
