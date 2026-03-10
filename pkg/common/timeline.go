package common

// Timeline composite key: 64-bit [OwnerID 22bit | Timestamp 42bit]
// Used on LSM-Tree for write-fanout + ordered mailbox; pushes O(N) app filtering down to O(log N) range scan.
const (
	// Timestamp in low 42 bits (~139 years at ms granularity)
	TimelineTimestampBits = 42
	// OwnerID in high 22 bits (~4.19M users); 0 reserved for public lobby
	TimelineOwnerBits = 22
)

var (
	timelineTimestampMask = (int64(1) << TimelineTimestampBits) - 1
	timelineOwnerMask     = (int64(1) << TimelineOwnerBits) - 1
)

// BuildTimelineKey builds composite key: [OwnerID (22 bit)] | [Timestamp (42 bit)]
// ownerID in [0, 2^22-1]; 0 = public lobby.
func BuildTimelineKey(ownerID int, timestampMs int64) KeyType {
	return KeyType((int64(ownerID)&timelineOwnerMask)<<TimelineTimestampBits | (timestampMs & timelineTimestampMask))
}

// GetTimelineTimestamp extracts timestamp (low 42 bits) from the key.
func GetTimelineTimestamp(key KeyType) int64 {
	return int64(key) & timelineTimestampMask
}

// GetTimelineOwnerID extracts Owner ID (high 22 bits) from the key.
func GetTimelineOwnerID(key KeyType) int {
	return int((int64(key) >> TimelineTimestampBits) & timelineOwnerMask)
}

// UsernameToOwnerID maps username to a stable 22-bit numeric ID (simple hash).
// In production use an assigned incremental UID. "" / "PUBLIC" return 0 (public lobby).
func UsernameToOwnerID(username string) int {
	if username == "" || username == "PUBLIC" {
		return 0
	}
	h := int64(0)
	for _, c := range username {
		h = h*31 + int64(c)
	}
	if h < 0 {
		h = -h
	}
	return int(h & timelineOwnerMask)
}

// CursorKey returns the persistent cursor key for a mailbox: same owner, timestamp=0 stores last_read_timestamp.
func CursorKey(ownerID int) KeyType {
	return BuildTimelineKey(ownerID, 0)
}

// TimelineScanBounds builds [start, end] for O(log N) range scan.
// For Sync: fetch all messages for owner after lastTimestampMs.
func TimelineScanBounds(ownerID int, lastTimestampMs int64) (start, end KeyType) {
	start = BuildTimelineKey(ownerID, lastTimestampMs+1)
	end = BuildTimelineKey(ownerID, timelineTimestampMask) // max timestamp for same owner
	return start, end
}
