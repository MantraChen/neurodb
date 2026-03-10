package network

import "neurodb/pkg/network/tcp"

// TCPServer 与 NewTCPServer 保留为兼容性别名，实际实现位于 pkg/network/tcp。
type TCPServer = tcp.Server

// NewTCPServer 创建自定义二进制协议 TCP 服务端。
var NewTCPServer = tcp.NewServer
