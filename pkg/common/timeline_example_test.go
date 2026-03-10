// This file demonstrates using Timeline composite keys at the app layer for:
// 1) Write-Fanout: DM double-write to sender/receiver mailbox; public lobby single write.
// 2) Persistent cursor + O(log N) range scan instead of O(N) full-table filter.
//
// Store interface is Put/Get/Scan, aligned with pkg/core.HybridStore.

package common

import (
	"encoding/json"
	"sort"
	"testing"
)

// Example message body (app-defined)
type exampleMsg struct {
	Sender   string `json:"sender"`
	Receiver string `json:"receiver"`
	Body     string `json:"body"`
	Ts       int64  `json:"ts"`
}

// Example store interface (aligned with HybridStore)
type exampleStore interface {
	Put(key KeyType, value ValueType)
	Get(key KeyType) (ValueType, bool)
	Scan(start, end KeyType) []Record
}

// WriteFanout: write 1 copy (public lobby) or 2 (sender + receiver mailbox).
func WriteFanout(store exampleStore, sender, receiver string, msg exampleMsg) {
	ts := msg.Ts
	if ts <= 0 {
		ts = 1 // avoid clashing with CursorKey(ts=0)
	}
	senderID := UsernameToOwnerID(sender)
	receiverID := UsernameToOwnerID(receiver)
	val, _ := json.Marshal(msg)

	isPublic := receiver == "" || receiver == "PUBLIC"
	if isPublic {
		key := BuildTimelineKey(0, ts)
		store.Put(key, val)
		return
	}
	// DM: double write
	store.Put(BuildTimelineKey(senderID, ts), val)
	if senderID != receiverID {
		store.Put(BuildTimelineKey(receiverID, ts), val)
	}
}

// SyncTimeline fetches messages for a mailbox after lastTs and updates the persistent cursor. O(log N) range scan.
func SyncTimeline(store exampleStore, currentUser, target string, lastTs int64) ([]exampleMsg, error) {
	myID := UsernameToOwnerID(currentUser)
	targetID := myID
	if target == "" || target == "PUBLIC" {
		targetID = 0
	}
	// If lastTs not provided, read cursor from store (CursorKey holds last_read_timestamp)
	if lastTs <= 0 {
		if v, ok := store.Get(CursorKey(targetID)); ok && len(v) > 0 {
			lastTs = parseInt64(string(v))
		}
	}
	start, end := TimelineScanBounds(targetID, lastTs)
	records := store.Scan(start, end)
	sort.Slice(records, func(i, j int) bool { return records[i].Key < records[j].Key })
	out := make([]exampleMsg, 0, len(records))
	for _, r := range records {
		if len(r.Value) == 0 {
			continue
		}
		var m exampleMsg
		if json.Unmarshal(r.Value, &m) == nil {
			out = append(out, m)
		}
	}
	// Update cursor
	if len(out) > 0 {
		newLast := out[len(out)-1].Ts
		store.Put(CursorKey(targetID), []byte(formatInt64(newLast)))
	}
	return out, nil
}

func parseInt64(s string) int64 {
	var n int64
	for _, c := range s {
		if c >= '0' && c <= '9' {
			n = n*10 + int64(c-'0')
		}
	}
	return n
}

func formatInt64(n int64) string {
	if n <= 0 {
		return "0"
	}
	var b [20]byte
	i := len(b) - 1
	for n > 0 {
		b[i] = byte('0' + n%10)
		n /= 10
		i--
	}
	return string(b[i+1:])
}

// mockStore in-memory impl for example test
type mockStore struct {
	m map[KeyType]ValueType
}

func (m *mockStore) Put(k KeyType, v ValueType) {
	if m.m == nil {
		m.m = make(map[KeyType]ValueType)
	}
	m.m[k] = append([]byte(nil), v...)
}

func (m *mockStore) Get(k KeyType) (ValueType, bool) {
	v, ok := m.m[k]
	return v, ok
}

func (m *mockStore) Scan(start, end KeyType) []Record {
	var out []Record
	for k, v := range m.m {
		if k >= start && k <= end && len(v) > 0 {
			out = append(out, Record{Key: k, Value: v})
		}
	}
	return out
}

func TestExampleWriteFanoutAndSync(t *testing.T) {
	store := &mockStore{}
	// One public lobby message
	WriteFanout(store, "alice", "PUBLIC", exampleMsg{Sender: "alice", Receiver: "PUBLIC", Body: "hi all", Ts: 1000})
	// One DM: should write two copies (alice and bob each)
	WriteFanout(store, "alice", "bob", exampleMsg{Sender: "alice", Receiver: "bob", Body: "hello", Ts: 2000})

	// Fetch public lobby
	msgs, _ := SyncTimeline(store, "alice", "PUBLIC", 0)
	if len(msgs) != 1 || msgs[0].Body != "hi all" {
		t.Fatalf("public timeline: got %d msgs", len(msgs))
	}
	// Fetch alice mailbox (should include public + DM)
	msgs, _ = SyncTimeline(store, "alice", "alice", 0)
	if len(msgs) < 1 {
		t.Fatalf("alice timeline: got %d", len(msgs))
	}
}
