package unzip

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"

	"github.com/google/uuid"
)

// ObjectRef is one object named in an S3 event notification.
type ObjectRef struct {
	Bucket string
	Key    string
	Size   int64
}

// s3Event is the part of an S3 event notification we use. S3 puts this JSON
// in the SQS message body. The full shape:
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
type s3Event struct {
	Event   string `json:"Event"` // "s3:TestEvent" when the notification is first configured
	Records []struct {
		EventName string `json:"eventName"` // e.g. "ObjectCreated:CompleteMultipartUpload"
		S3        struct {
			Bucket struct {
				Name string `json:"name"`
			} `json:"bucket"`
			Object struct {
				Key  string `json:"key"`
				Size int64  `json:"size"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

// ParseS3Event extracts the objects from an S3 event notification. S3 sends a
// one-off s3:TestEvent when a notification is configured; that yields nothing.
func ParseS3Event(body []byte) ([]ObjectRef, error) {
	var ev s3Event
	if err := json.Unmarshal(body, &ev); err != nil {
		return nil, fmt.Errorf("not an S3 event: %w", err)
	}
	if ev.Event == "s3:TestEvent" {
		return nil, nil
	}
	refs := make([]ObjectRef, 0, len(ev.Records))
	for _, r := range ev.Records {
		// Keys arrive URL-encoded, with spaces as '+': "my file.zip" is "my+file.zip".
		key, err := url.QueryUnescape(r.S3.Object.Key)
		if err != nil {
			return nil, fmt.Errorf("bad key %q: %w", r.S3.Object.Key, err)
		}
		refs = append(refs, ObjectRef{Bucket: r.S3.Bucket.Name, Key: key, Size: r.S3.Object.Size})
	}
	return refs, nil
}

var uploadKey = regexp.MustCompile(`^uploads/([^/]+)/upload\.zip$`)

// BatchIDFromKey returns the batch ID for an upload key, which is the upload's
// UUID: uploads/{id}/upload.zip. The browser already knows it, so it can poll
// for the batch before processing even starts.
func BatchIDFromKey(key string) (string, bool) {
	m := uploadKey.FindStringSubmatch(key)
	if m == nil || uuid.Validate(m[1]) != nil {
		return "", false
	}
	return m[1], true
}
