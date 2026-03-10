package network

import "neurodb/pkg/network/tcp"

// TCPServer and NewTCPServer are compatibility aliases; implementation lives in pkg/network/tcp.
type TCPServer = tcp.Server

// NewTCPServer creates the custom binary-protocol TCP server.
var NewTCPServer = tcp.NewServer
