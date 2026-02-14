package client

import (
	"testing"
)

func TestDialInvalidAddr(t *testing.T) {
	_, err := Dial("invalid:invalid:invalid")
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestDialUnreachable(t *testing.T) {
	// Connect to non-routable IP (RFC 5737) - expect error
	_, err := Dial("192.0.2.1:9999")
	if err == nil {
		t.Skip("connection unexpectedly succeeded (e.g. in sandbox)")
	}
}
