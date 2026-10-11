package operations_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/fs/rc/jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// listStreamItem decodes the fields of a streamed item these tests check.
type listStreamItem struct {
	Path  string
	Name  string
	Size  int64
	IsDir bool
}

// listStreamLine decodes either kind of operations/liststream line.
type listStreamLine struct {
	List  []listStreamItem `json:"list"`
	Done  bool             `json:"done"`
	Count int64            `json:"count"`
	Error string           `json:"error"`
}

// A synchronous rc call runs on the context of its HTTP request, which carries the server.
func listStreamContext() context.Context {
	return context.WithValue(context.Background(), http.ServerContextKey, &http.Server{})
}

// readListStream splits a stream into its batches and its trailer, which must be the last line.
func readListStream(t *testing.T, body string) (batches [][]listStreamItem, trailer listStreamLine) {
	t.Helper()
	scanner := bufio.NewScanner(strings.NewReader(body))
	var lines []listStreamLine
	for scanner.Scan() {
		var line listStreamLine
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &line), "every line is one JSON object")
		lines = append(lines, line)
	}
	require.NoError(t, scanner.Err())
	require.NotEmpty(t, lines, "a stream always ends with its trailer")
	trailer = lines[len(lines)-1]
	require.True(t, trailer.Done, "the last line is the trailer")
	for _, line := range lines[:len(lines)-1] {
		require.False(t, line.Done, "only the last line is a trailer")
		require.NotEmpty(t, line.List, "a batch line is never empty")
		batches = append(batches, line.List)
	}
	return batches, trailer
}

func runListStream(t *testing.T, call *rc.Call, in rc.Params) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	in["_response"] = recorder
	out, err := call.Fn(listStreamContext(), in)
	require.NoError(t, err)
	assert.Nil(t, out, "the listing is never kept as a job output")
	assert.Equal(t, "application/x-ndjson", recorder.Header().Get("Content-Type"))
	return recorder.Body.String()
}

// operations/liststream: Stream the listing of the given remote and path as JSON lines.
func TestRcListStream(t *testing.T) {
	r, call := rcNewRun(t, "operations/liststream")
	ctx := context.Background()
	file1 := r.WriteObject(ctx, "a", "a", t1)
	file2 := r.WriteObject(ctx, "subdir/b", "bb", t2)
	file3 := r.WriteObject(ctx, "subdir/c", "ccc", t2)
	file4 := r.WriteObject(ctx, "subdir/deeper/d", "dddd", t2)
	r.CheckRemoteItems(t, file1, file2, file3, file4)

	body := runListStream(t, call, rc.Params{
		"fs":     r.FremoteName,
		"remote": "",
		"opt":    rc.Params{"recurse": true, "noModTime": true, "noMimeType": true},
	})
	batches, trailer := readListStream(t, body)

	assert.Empty(t, trailer.Error)
	var paths []string
	sizes := map[string]int64{}
	for _, batch := range batches {
		// The local backend is walked directory by directory, so one batch is one directory.
		parent := path.Dir(batch[0].Path)
		for _, item := range batch {
			assert.Equal(t, parent, path.Dir(item.Path), "a batch holds the entries of one directory")
			paths = append(paths, item.Path)
			sizes[item.Path] = item.Size
		}
	}
	sort.Strings(paths)
	assert.Equal(t, []string{"a", "subdir", "subdir/b", "subdir/c", "subdir/deeper", "subdir/deeper/d"}, paths)
	assert.Equal(t, int64(len(paths)), trailer.Count)
	assert.Equal(t, int64(1), sizes["a"])
	assert.Equal(t, int64(4), sizes["subdir/deeper/d"])

	body = runListStream(t, call, rc.Params{"fs": r.FremoteName, "remote": "subdir"})
	batches, trailer = readListStream(t, body)
	require.Len(t, batches, 1)
	paths = paths[:0]
	for _, item := range batches[0] {
		paths = append(paths, item.Path)
	}
	sort.Strings(paths)
	assert.Equal(t, []string{"subdir/b", "subdir/c", "subdir/deeper"}, paths, "without recurse only the directory itself is listed")
	assert.Equal(t, int64(3), trailer.Count)
}

// Items are encoded one at a time into a reused item, so each one must still have exactly the
// bytes operations/list gives it, with nothing carried over from the previous entry.
func TestRcListStreamEncodesItemsLikeOperationsList(t *testing.T) {
	r, call := rcNewRun(t, "operations/liststream")
	ctx := context.Background()
	r.WriteObject(ctx, "plain", "a", t1)
	r.WriteObject(ctx, "tom & jerry <quoted \"name\">", "bb", t2)
	r.WriteObject(ctx, "dossier é/fichier ünïcode", "ccc", t2)
	r.WriteObject(ctx, "dossier é/sub/deep", "dddd", t1)
	opt := operations.ListJSONOpt{Recurse: true, ShowHash: true}

	expected := map[string]string{}
	require.NoError(t, operations.ListJSON(ctx, r.Fremote, "", &opt, func(item *operations.ListJSONItem) error {
		encoded, err := json.Marshal(item)
		expected[item.Path] = string(encoded)
		return err
	}))
	body := runListStream(t, call, rc.Params{
		"fs":     r.FremoteName,
		"remote": "",
		"opt":    rc.Params{"recurse": true, "showHash": true},
	})

	lines := strings.Split(strings.TrimSuffix(body, "\n"), "\n")
	streamed := 0
	for _, line := range lines[:len(lines)-1] {
		var batch struct {
			List []json.RawMessage `json:"list"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &batch))
		items := make([]string, len(batch.List))
		for i, raw := range batch.List {
			var item struct{ Path string }
			require.NoError(t, json.Unmarshal(raw, &item))
			assert.Equal(t, expected[item.Path], string(raw), "item %q", item.Path)
			items[i] = string(raw)
		}
		assert.Equal(t, `{"list":[`+strings.Join(items, ",")+`]}`, line, "the line is the encoded batch")
		streamed += len(items)
	}
	assert.Equal(t, len(expected), streamed)
	_, trailer := readListStream(t, body)
	assert.Equal(t, int64(len(expected)), trailer.Count)
}

func TestRcListStreamSendsItsJobIdentityBeforeTheListing(t *testing.T) {
	r, call := rcNewRun(t, "operations/liststream")
	r.WriteObject(context.Background(), "a", "a", t1)
	recorder := httptest.NewRecorder()

	job, out, err := jobs.NewJob(listStreamContext(), call.Fn, rc.Params{
		"fs":        r.FremoteName,
		"remote":    "",
		"_response": recorder,
	})
	require.NoError(t, err)

	assert.Empty(t, out, "a synchronous stream returns no output")
	assert.Equal(t, strconv.FormatInt(job.ID, 10), recorder.Result().Header.Get("x-rclone-jobid"))
	assert.Equal(t, job.ExecuteID, recorder.Result().Header.Get("x-rclone-executeid"))
	assert.True(t, recorder.Flushed, "the headers are sent before the first full buffer")
	batches, trailer := readListStream(t, recorder.Body.String())
	require.Len(t, batches, 1)
	assert.Equal(t, int64(1), trailer.Count)
}

func TestRcListStreamReportsListingErrorsInTheTrailer(t *testing.T) {
	r, call := rcNewRun(t, "operations/liststream")
	r.Mkdir(context.Background(), r.Fremote)

	body := runListStream(t, call, rc.Params{
		"fs":     r.FremoteName,
		"remote": "missing",
		"opt":    rc.Params{"recurse": true},
	})
	batches, trailer := readListStream(t, body)

	assert.Empty(t, batches)
	assert.Equal(t, int64(0), trailer.Count)
	assert.Contains(t, trailer.Error, "directory not found")
}

func TestRcListStreamRefusesToRunWithoutItsHTTPRequest(t *testing.T) {
	r, call := rcNewRun(t, "operations/liststream")
	r.Mkdir(context.Background(), r.Fremote)
	recorder := httptest.NewRecorder()

	// An _async job runs on a background context once its request has been answered.
	job, _, err := jobs.NewJob(listStreamContext(), call.Fn, rc.Params{
		"fs":        r.FremoteName,
		"remote":    "",
		"_async":    true,
		"_response": recorder,
	})
	require.NoError(t, err)
	finished := make(chan struct{})
	job.OnFinish(func() { close(finished) })
	<-finished

	assert.False(t, job.Success)
	assert.Contains(t, job.Error, "must be called synchronously")
	assert.Zero(t, recorder.Body.Len(), "nothing is written to a response that has already been sent")

	_, err = call.Fn(listStreamContext(), rc.Params{"fs": r.FremoteName, "remote": ""})
	assert.Error(t, err, "the response writer is required")
}
