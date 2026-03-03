// 本文件演示如何在应用层使用 Timeline 组合键实现：
// 1) 写扩散 (Write-Fanout)：私聊双写发件箱/收件箱，公共大厅单写；
// 2) 持久化游标 + O(log N) 范围扫描替代 O(N) 全表过滤。
//
// 存储接口假设为 Put/Get/Scan，与 pkg/core.HybridStore 一致。

package common

import (
	"encoding/json"
	"sort"
	"testing"
)

// 示例：消息体（应用层定义）
type exampleMsg struct {
	Sender   string `json:"sender"`
	Receiver string `json:"receiver"`
	Body     string `json:"body"`
	Ts       int64  `json:"ts"`
}

// 假设的存储接口（与 HybridStore 对齐）
type exampleStore interface {
	Put(key KeyType, value ValueType)
	Get(key KeyType) (ValueType, bool)
	Scan(start, end KeyType) []Record
}

// WriteFanout 写扩散：根据是否公共大厅决定写 1 份还是 2 份（发件人+收件人信箱）。
func WriteFanout(store exampleStore, sender, receiver string, msg exampleMsg) {
	ts := msg.Ts
	if ts <= 0 {
		ts = 1 // 避免与 CursorKey(ts=0) 冲突
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
	// 私聊：双写
	store.Put(BuildTimelineKey(senderID, ts), val)
	if senderID != receiverID {
		store.Put(BuildTimelineKey(receiverID, ts), val)
	}
}

// SyncTimeline 拉取某信箱从 lastTs 之后的消息，并更新持久化游标。读路径为 O(log N) 范围扫描。
func SyncTimeline(store exampleStore, currentUser, target string, lastTs int64) ([]exampleMsg, error) {
	myID := UsernameToOwnerID(currentUser)
	targetID := myID
	if target == "" || target == "PUBLIC" {
		targetID = 0
	}
	// 若未传 lastTs，从存储中读游标（CursorKey 存 last_read_timestamp）
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
	// 更新游标
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

// mockStore 内存实现，用于示例测试
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
	// 公共大厅一条
	WriteFanout(store, "alice", "PUBLIC", exampleMsg{Sender: "alice", Receiver: "PUBLIC", Body: "hi all", Ts: 1000})
	// 私聊一条：应写两份（alice 与 bob 各一份）
	WriteFanout(store, "alice", "bob", exampleMsg{Sender: "alice", Receiver: "bob", Body: "hello", Ts: 2000})

	// 拉取公共大厅
	msgs, _ := SyncTimeline(store, "alice", "PUBLIC", 0)
	if len(msgs) != 1 || msgs[0].Body != "hi all" {
		t.Fatalf("public timeline: got %d msgs", len(msgs))
	}
	// 拉取 alice 信箱（应包含 public + 发件）
	msgs, _ = SyncTimeline(store, "alice", "alice", 0)
	if len(msgs) < 1 {
		t.Fatalf("alice timeline: got %d", len(msgs))
	}
}
