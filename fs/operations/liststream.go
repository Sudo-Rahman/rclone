package operations

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/fs/rc/jobs"
	"github.com/rclone/rclone/fs/walk"
)

func init() {
	rc.Add(rc.Call{
		Path:          "operations/liststream",
		Fn:            rcListStream,
		NeedsResponse: true,
		Title:         "Stream the listing of the given remote and path as JSON lines",
		Help: `This takes the same parameters as operations/list:

- fs - a remote name string e.g. "drive:"
- remote - a path within that remote e.g. "dir"
- opt - a dictionary of options to control the listing (optional)
    - see operations/list for the available options

Unlike operations/list, the listing is never held in memory or kept as
a job output. It is written to the HTTP response while the remote is
listed, as newline-delimited JSON (application/x-ndjson):

- one line per listing batch, in the same shape as the operations/list
  reply: {"list": [items]}. A batch holds the entries rclone received in
  one listing callback; when the remote is walked directory by
  directory, that is the whole listing of one directory.
- one final line once the listing has ended:
  {"done": true, "count": N, "error": "..."}. count is the number of
  items written; error is set only if the listing failed, in which case
  the batches already written are incomplete.

A stream which ends without the final line was interrupted, and
anything after it is not part of the listing. Closing the connection
cancels the listing. The response headers are sent as soon as the
listing starts; their x-rclone-jobid and x-rclone-executeid identify the
job while it streams, so that it can be stopped with job/stop and
observed with job/list. This call writes to the HTTP response,
so it must be called synchronously over HTTP: "_async" is refused.
`,
	})
}

// listStreamTrailer is the last line of a stream whose listing ran to its end.
type listStreamTrailer struct {
	Done  bool   `json:"done"`
	Count int64  `json:"count"`
	Error string `json:"error,omitempty"`
}

// listStreamBufferSize batches small lines into fewer writes to the connection.
const listStreamBufferSize = 64 * 1024

// listStreamBatchWriter writes the {"list": [...]} line of one listing callback item by item,
// so a directory of millions of entries is never held as items or as one encoded line. The
// bytes are those json.Encoder writes for the whole batch.
type listStreamBatchWriter struct {
	out     *bufio.Writer
	item    ListJSONItem // refilled for every entry
	encoded bytes.Buffer
	encoder *json.Encoder
}

func newListStreamBatchWriter(out *bufio.Writer) *listStreamBatchWriter {
	b := &listStreamBatchWriter{out: out}
	b.encoder = json.NewEncoder(&b.encoded)
	return b
}

// write writes the listed entries as one line and returns how many were written. Without any
// listed entry nothing is written. If an item cannot be encoded, the line is closed so the
// stream stays line-delimited, and the error is returned.
func (b *listStreamBatchWriter) write(ctx context.Context, lj *listJSON, entries fs.DirEntries) (written int64, err error) {
	for _, entry := range entries {
		if !lj.fill(ctx, entry, &b.item) {
			continue
		}
		b.encoded.Reset()
		if err = b.encoder.Encode(&b.item); err != nil {
			err = fmt.Errorf("creating entry failed in ListJSON: %w", err)
			break
		}
		separator := ","
		if written == 0 {
			separator = `{"list":[`
		}
		// bufio keeps the first write error and returns it from every later write, so checking
		// the item's write also covers its separator.
		_, _ = b.out.WriteString(separator)
		// Encode ends the item with a newline, which belongs only after the whole line.
		if _, werr := b.out.Write(b.encoded.Bytes()[:b.encoded.Len()-1]); werr != nil {
			return written, fmt.Errorf("writing the listing stream failed: %w", werr)
		}
		written++
	}
	b.item = ListJSONItem{} // keep no reference to the last entry
	if written == 0 {
		return 0, err
	}
	if _, werr := b.out.WriteString("]}\n"); werr != nil && err == nil {
		err = fmt.Errorf("writing the listing stream failed: %w", werr)
	}
	return written, err
}

// Stream the listing of the directory
func rcListStream(ctx context.Context, in rc.Params) (out rc.Params, err error) {
	// The stream is written to the response of the HTTP request that called it. An _async job
	// runs on a background context after that response has been sent, so it is refused here,
	// before anything is listed or written.
	if ctx.Value(http.ServerContextKey) == nil {
		return nil, errors.New("operations/liststream must be called synchronously over HTTP")
	}
	w, err := in.GetHTTPResponseWriter()
	if err != nil {
		return nil, err
	}
	f, remote, err := rc.GetFsAndRemote(ctx, in)
	if err != nil {
		return nil, err
	}
	var opt ListJSONOpt
	err = in.GetStruct("opt", &opt)
	if rc.NotErrParamNotFound(err) {
		return nil, err
	}
	lj, err := newListJSON(ctx, f, remote, &opt)
	if err != nil {
		return nil, err
	}

	// rclone adds x-rclone-jobid only once a call has returned, after the whole stream. Send the
	// identity of this job with the stream instead, so the caller can stop it and see it settle.
	if job, ok := jobs.GetJob(ctx); ok {
		w.Header().Set("x-rclone-jobid", strconv.FormatInt(job.ID, 10))
		w.Header().Set("x-rclone-executeid", job.ExecuteID)
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	// Send the headers now rather than with the first full buffer, which a slow remote may take
	// long to fill, so the caller learns the job identity as soon as the listing starts.
	w.WriteHeader(http.StatusOK)
	if err := http.NewResponseController(w).Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		fs.Debugf(f, "operations/liststream: failed to send the headers: %v", err)
	}
	buffered := bufio.NewWriterSize(w, listStreamBufferSize)
	batches := newListStreamBatchWriter(buffered)
	var (
		// walk already calls back one batch at a time; the lock also keeps each line whole for a
		// backend that would not.
		mu    sync.Mutex
		count int64
	)
	listErr := walk.ListR(ctx, f, remote, false, ConfigMaxDepth(ctx, lj.opt.Recurse), walk.ListAll, func(entries fs.DirEntries) error {
		mu.Lock()
		defer mu.Unlock()
		written, err := batches.write(ctx, lj, entries)
		count += written
		return err
	})
	// Every listing callback has returned, so the trailer needs no lock.
	trailer := listStreamTrailer{Done: true, Count: count}
	if listErr != nil {
		trailer.Error = fmt.Errorf("error in ListJSON: %w", listErr).Error()
	}
	if err := json.NewEncoder(buffered).Encode(trailer); err != nil {
		fs.Debugf(f, "operations/liststream: failed to write the trailer: %v", err)
		return nil, nil
	}
	if err := buffered.Flush(); err != nil {
		fs.Debugf(f, "operations/liststream: failed to flush the stream: %v", err)
	}
	// The listing has been written, so the job keeps no output. Errors are reported in the
	// trailer, because the response status was sent with the first line.
	return nil, nil
}
