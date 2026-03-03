package common

import (
	"testing"
)

func TestBuildTimelineKey(t *testing.T) {
	// 公共大厅 owner=0
	k0 := BuildTimelineKey(0, 1000)
	if GetTimelineOwnerID(k0) != 0 || GetTimelineTimestamp(k0) != 1000 {
		t.Fatalf("owner=0, ts=1000: got owner=%d ts=%d", GetTimelineOwnerID(k0), GetTimelineTimestamp(k0))
	}

	// 普通用户
	k1 := BuildTimelineKey(12345, 1609459200000)
	if GetTimelineOwnerID(k1) != 12345 || GetTimelineTimestamp(k1) != 1609459200000 {
		t.Fatalf("owner=12345: got owner=%d ts=%d", GetTimelineOwnerID(k1), GetTimelineTimestamp(k1))
	}

	// 游标 Key：timestamp=0
	cursor := CursorKey(7)
	if GetTimelineTimestamp(cursor) != 0 || GetTimelineOwnerID(cursor) != 7 {
		t.Fatalf("CursorKey(7): got owner=%d ts=%d", GetTimelineOwnerID(cursor), GetTimelineTimestamp(cursor))
	}
}

func TestTimelineOrdering(t *testing.T) {
	// 同 owner 下按时间有序
	owner := 1
	k1 := BuildTimelineKey(owner, 100)
	k2 := BuildTimelineKey(owner, 200)
	k3 := BuildTimelineKey(owner, 300)
	if k1 >= k2 || k2 >= k3 {
		t.Fatalf("keys should be ordered: %d %d %d", k1, k2, k3)
	}

	// 不同 owner 不重叠
	kA := BuildTimelineKey(0, 1000)
	kB := BuildTimelineKey(1, 1000)
	if kA >= kB {
		t.Fatalf("owner 0 < owner 1 for same ts")
	}
}

func TestUsernameToOwnerID(t *testing.T) {
	if UsernameToOwnerID("") != 0 || UsernameToOwnerID("PUBLIC") != 0 {
		t.Fatalf("empty/PUBLIC should be 0")
	}
	id := UsernameToOwnerID("alice")
	if id < 0 || id > (1<<TimelineOwnerBits)-1 {
		t.Fatalf("owner id out of range: %d", id)
	}
	if UsernameToOwnerID("alice") != UsernameToOwnerID("alice") {
		t.Fatalf("same username should yield same id")
	}
}

func TestTimelineScanBounds(t *testing.T) {
	start, end := TimelineScanBounds(5, 100)
	if GetTimelineOwnerID(start) != 5 || GetTimelineTimestamp(start) != 101 {
		t.Fatalf("start: owner=%d ts=%d", GetTimelineOwnerID(start), GetTimelineTimestamp(start))
	}
	if GetTimelineOwnerID(end) != 5 || GetTimelineTimestamp(end) != timelineTimestampMask {
		t.Fatalf("end: owner=%d ts=%d", GetTimelineOwnerID(end), GetTimelineTimestamp(end))
	}
	if start >= end {
		t.Fatalf("start should be < end")
	}
}
