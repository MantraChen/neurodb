package common

// Timeline 组合键：64 位 [OwnerID 22bit | Timestamp 42bit]
// 用于 LSM-Tree 上实现“写扩散 + 有序信箱”，将 O(N) 应用层过滤下推为 O(log N) 范围扫描。
const (
	// Timestamp 占低 42 位，约 139 年（毫秒）
	TimelineTimestampBits = 42
	// OwnerID 占高 22 位，约 419 万用户；0 保留给公共大厅
	TimelineOwnerBits = 22
)

var (
	timelineTimestampMask = (int64(1) << TimelineTimestampBits) - 1
	timelineOwnerMask     = (int64(1) << TimelineOwnerBits) - 1
)

// BuildTimelineKey 构建组合键: [OwnerID (22 bit)] | [Timestamp (42 bit)]
// ownerID 需在 [0, 2^22-1]，0 表示公共大厅。
func BuildTimelineKey(ownerID int, timestampMs int64) KeyType {
	return KeyType((int64(ownerID)&timelineOwnerMask)<<TimelineTimestampBits | (timestampMs & timelineTimestampMask))
}

// GetTimelineTimestamp 从组合键中提取时间戳（低 42 位）。
func GetTimelineTimestamp(key KeyType) int64 {
	return int64(key) & timelineTimestampMask
}

// GetTimelineOwnerID 从组合键中提取 Owner ID（高 22 位）。
func GetTimelineOwnerID(key KeyType) int {
	return int((int64(key) >> TimelineTimestampBits) & timelineOwnerMask)
}

// UsernameToOwnerID 将用户名映射为稳定的 22 位数字 ID（简易哈希）。
// 生产环境应由注册时分配的自增 UID 替代。"" / "PUBLIC" 返回 0（公共大厅）。
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

// CursorKey 返回某信箱的持久化游标 Key：同一 owner 下 timestamp=0 的 Key 专用于存 last_read_timestamp。
func CursorKey(ownerID int) KeyType {
	return BuildTimelineKey(ownerID, 0)
}

// TimelineScanBounds 构造 O(log N) 范围扫描的 [start, end]。
// 用于 Sync：从 lastTimestampMs 之后拉取该 owner 的所有消息。
func TimelineScanBounds(ownerID int, lastTimestampMs int64) (start, end KeyType) {
	start = BuildTimelineKey(ownerID, lastTimestampMs+1)
	end = BuildTimelineKey(ownerID, timelineTimestampMask) // 同 owner 下最大时间戳
	return start, end
}
