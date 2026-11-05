package editor

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/cozy/cozy-stack/pkg/config/config"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/tests/testutils"
	"github.com/cozy/cozy-stack/web/errors"
	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestCollab(t *testing.T) {
	if testing.Short() {
		t.Skip("an instance is required for this test: test skipped due to the use of --short flag")
	}

	config.UseTestFile(t)
	testutils.NeedCouchdb(t)
	const secret = "collab-test-secret"
	config.GetConfig().Collab = map[string]config.Collab{
		config.DefaultInstanceContext: {URL: "ws://collab.example", Secret: secret},
	}
	setup := testutils.NewSetup(t, t.Name())
	inst := setup.GetTestInstance()
	_, token := setup.GetTestClient(consts.Files)
	fileID := createEditorFile(t, inst, "note.docsnote", "application/octet-stream", "application")

	ts := setup.GetTestServer("/editor", Routes)
	ts.Config.Handler.(*echo.Echo).HTTPErrorHandler = errors.ErrorHandler
	t.Cleanup(ts.Close)

	serverToken := func(sub string) string {
		claims := jwt.RegisteredClaims{
			Issuer:    inst.Domain,
			Subject:   sub,
			Audience:  jwt.ClaimStrings{collabServerAudience},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
		}
		signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, &claims).SignedString([]byte(secret))
		require.NoError(t, err)
		return signed
	}

	t.Run("OpenGivesAnEditTokenForTheFile", func(t *testing.T) {
		e := testutils.CreateTestClient(t, ts.URL)
		obj := e.GET("/editor/"+fileID+"/collab").
			WithHeader("Authorization", "Bearer "+token).
			Expect().Status(200).JSON().Object()
		obj.Value("mode").IsEqual("edit")
		obj.Value("room").IsEqual(inst.Domain + "/" + fileID)

		var claims collabClaims
		_, err := jwt.ParseWithClaims(obj.Value("token").String().Raw(), &claims,
			func(*jwt.Token) (interface{}, error) { return []byte(secret), nil })
		require.NoError(t, err)
		require.Equal(t, fileID, claims.Subject)
		require.Equal(t, "edit", claims.Mode)
		require.Equal(t, "owner:"+inst.Domain, claims.User)
	})

	t.Run("PeopleListsTheOwner", func(t *testing.T) {
		e := testutils.CreateTestClient(t, ts.URL)
		people := e.GET("/editor/"+fileID+"/people").
			WithHeader("Authorization", "Bearer "+token).
			Expect().Status(200).JSON().Array()
		people.Value(0).Object().Value("id").IsEqual("owner:" + inst.Domain)
	})

	t.Run("AttachmentsLiveInTheFixedDirectory", func(t *testing.T) {
		e := testutils.CreateTestClient(t, ts.URL)
		id := e.POST("/editor/"+fileID+"/attachments").
			WithQuery("Name", "a.png").
			WithHeader("Authorization", "Bearer "+token).
			WithHeader("Content-Type", "image/png").
			WithBytes([]byte("png")).
			Expect().Status(201).JSON().Object().Value("id").String().Raw()
		att, err := inst.VFS().FileByID(id)
		require.NoError(t, err)
		require.Equal(t, AttachmentsDirID, att.DirID)
		e.GET("/editor/"+fileID+"/attachments/"+id).
			WithHeader("Authorization", "Bearer "+token).
			Expect().Status(200).Body().IsEqual("png")
		other := createEditorFile(t, inst, "other.docsnote", "application/octet-stream", "application")
		e.GET("/editor/"+other+"/attachments/"+id).
			WithHeader("Authorization", "Bearer "+token).
			Expect().Status(404)
	})

	t.Run("ContentRoutesNeedTheServerToken", func(t *testing.T) {
		e := testutils.CreateTestClient(t, ts.URL)
		e.GET("/editor/collab/"+fileID).
			WithHeader("Authorization", "Bearer "+token).
			Expect().Status(401)
		e.GET("/editor/collab/"+fileID).
			WithHeader("Authorization", "Bearer "+serverToken("another-file")).
			Expect().Status(401)
	})

	t.Run("SaveKeepsTheExportAndChecksTheRevision", func(t *testing.T) {
		e := testutils.CreateTestClient(t, ts.URL)
		rev := e.HEAD("/editor/collab/"+fileID).
			WithHeader("Authorization", "Bearer "+serverToken(fileID)).
			Expect().Status(200).Header(revHeader).Raw()

		body := map[string]string{
			"content":  base64.StdEncoding.EncodeToString([]byte("yjs-state")),
			"markdown": "# Hello",
		}
		e.PUT("/editor/collab/"+fileID).
			WithHeader("Authorization", "Bearer "+serverToken(fileID)).
			WithHeader("If-Match", rev).
			WithJSON(body).
			Expect().Status(200)
		e.PUT("/editor/collab/"+fileID).
			WithHeader("Authorization", "Bearer "+serverToken(fileID)).
			WithHeader("If-Match", rev).
			WithJSON(body).
			Expect().Status(412)

		e.GET("/editor/collab/"+fileID).
			WithHeader("Authorization", "Bearer "+serverToken(fileID)).
			Expect().Status(200).Body().IsEqual("yjs-state")
		file, err := inst.VFS().FileByID(fileID)
		require.NoError(t, err)
		require.Equal(t, "# Hello", file.Metadata["markdown"])
	})
}
