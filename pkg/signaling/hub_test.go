package signaling

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

func TestHubRegistrationAndSignaling(t *testing.T) {
	hub := NewHub()

	c1 := &Client{
		Hub:    hub,
		Send:   make(chan []byte, 10),
		RoomID: "test-room",
		ID:     "peer-1",
		Name:   "Alice",
	}

	c2 := &Client{
		Hub:    hub,
		Send:   make(chan []byte, 10),
		RoomID: "test-room",
		ID:     "peer-2",
		Name:   "Bob",
	}

	// Register c1
	hub.Register(c1)

	// c1 should get existing-peers with 0 peers
	msgBytes := <-c1.Send
	var msg Message
	if err := json.Unmarshal(msgBytes, &msg); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if msg.Type != TypeExistingPeers || len(msg.Peers) != 0 {
		t.Fatalf("expected 0 existing peers for c1, got %v", msg)
	}

	// Register c2
	hub.Register(c2)

	// c2 should get existing-peers with c1
	msgBytes2 := <-c2.Send
	var msg2 Message
	if err := json.Unmarshal(msgBytes2, &msg2); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if msg2.Type != TypeExistingPeers || len(msg2.Peers) != 1 || msg2.Peers[0].ID != "peer-1" {
		t.Fatalf("expected c2 to see c1, got %v", msg2.Peers)
	}

	// c1 should get peer-joined from c2
	msgBytesJoined := <-c1.Send
	var msgJoined Message
	if err := json.Unmarshal(msgBytesJoined, &msgJoined); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if msgJoined.Type != TypePeerJoined || msgJoined.From != "peer-2" {
		t.Fatalf("expected c1 to receive peer-joined for peer-2, got %v", msgJoined)
	}

	// Test targeted offer from c2 to c1
	hub.HandleMessage(c2, Message{
		Type: TypeOffer,
		To:   "peer-1",
		SDP:  json.RawMessage(`{"type":"offer","sdp":"dummy"}`),
	})

	offerBytes := <-c1.Send
	var offerMsg Message
	if err := json.Unmarshal(offerBytes, &offerMsg); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if offerMsg.Type != TypeOffer || offerMsg.To != "peer-1" {
		t.Fatalf("expected offer to be delivered to c1, got %v", offerMsg)
	}

	// Test chat broadcast
	hub.HandleMessage(c1, Message{
		Type: TypeChat,
		Text: "Hello Bob!",
	})

	chat1 := <-c1.Send
	chat2 := <-c2.Send
	var mChat1, mChat2 Message
	_ = json.Unmarshal(chat1, &mChat1)
	_ = json.Unmarshal(chat2, &mChat2)
	if mChat1.Text != "Hello Bob!" || mChat2.Text != "Hello Bob!" {
		t.Fatalf("chat not broadcast to both peers: %v, %v", mChat1, mChat2)
	}

	// Unregister c1
	hub.Unregister(c1)
	leftBytes := <-c2.Send
	var leftMsg Message
	_ = json.Unmarshal(leftBytes, &leftMsg)
	if leftMsg.Type != TypePeerLeft || leftMsg.From != "peer-1" {
		t.Fatalf("expected c2 to receive peer-left for peer-1, got %v", leftMsg)
	}

	// Unregister c2 -> room should be deleted
	hub.Unregister(c2)
	hub.mu.RLock()
	_, roomExists := hub.Rooms["test-room"]
	hub.mu.RUnlock()
	if roomExists {
		t.Fatalf("expected room test-room to be cleaned up after all clients leave")
	}
}

func newTestClient(hub *Hub, room, id string) *Client {
	return &Client{Hub: hub, Send: make(chan []byte, 64), RoomID: room, ID: id, Name: id}
}

func TestRegisterRejectsDuplicateIDAndKeepsOriginal(t *testing.T) {
	hub := NewHub()
	orig := newTestClient(hub, "r", "same")
	if err := hub.Register(orig); err != nil {
		t.Fatalf("register orig: %v", err)
	}
	intruder := newTestClient(hub, "r", "same")
	if err := hub.Register(intruder); err != ErrDuplicateID {
		t.Fatalf("expected ErrDuplicateID, got %v", err)
	}

	// The rejected client was never registered, so its teardown must not evict the original.
	hub.Unregister(intruder)
	if !hub.Rooms["r"].Has("same") {
		t.Fatal("unregistering a rejected duplicate removed the original client")
	}
}

func TestRegisterEnforcesRoomCap(t *testing.T) {
	hub := NewHub()
	for i := 0; i < MaxRoomSize; i++ {
		if err := hub.Register(newTestClient(hub, "r", string(rune('a'+i)))); err != nil {
			t.Fatalf("register %d: %v", i, err)
		}
	}
	if err := hub.Register(newTestClient(hub, "r", "overflow")); err != ErrRoomFull {
		t.Fatalf("expected ErrRoomFull, got %v", err)
	}
}

func TestConcurrentJoinsSeeEachOther(t *testing.T) {
	// Each pair must be introduced to each other exactly once: the later joiner lists the earlier
	// one in existing-peers, and the earlier one gets peer-joined. Never both, never neither.
	for iter := 0; iter < 200; iter++ {
		hub := NewHub()
		a, b := newTestClient(hub, "r", "a"), newTestClient(hub, "r", "b")
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _ = hub.Register(a) }()
		go func() { defer wg.Done(); _ = hub.Register(b) }()
		wg.Wait()

		introductions := 0
		for _, c := range []*Client{a, b} {
			close(c.Send)
			for raw := range c.Send {
				var m Message
				_ = json.Unmarshal(raw, &m)
				if m.Type == TypePeerJoined || (m.Type == TypeExistingPeers && len(m.Peers) > 0) {
					introductions++
				}
			}
		}
		if introductions != 2 {
			t.Fatalf("iter %d: expected each side to learn of the other once (2 total), got %d", iter, introductions)
		}
	}
}

func TestUnregisterStaleClientDoesNotEvictReplacement(t *testing.T) {
	hub := NewHub()
	old := newTestClient(hub, "r", "x")
	_ = hub.Register(old)
	hub.Unregister(old)
	replacement := newTestClient(hub, "r", "x")
	if err := hub.Register(replacement); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	hub.Unregister(old) // late duplicate teardown of the old connection
	if !hub.Rooms["r"].Has("x") {
		t.Fatal("stale Unregister evicted the replacement client")
	}
}

func TestSlowConsumerIsDisconnectedNotSilentlyDropped(t *testing.T) {
	hub := NewHub()
	c := &Client{Hub: hub, Send: make(chan []byte, 1), RoomID: "r", ID: "slow", Name: "slow"}
	_ = c.SendJSON(Message{Type: TypeChat, Text: "1"})
	_ = c.SendJSON(Message{Type: TypeChat, Text: "2"}) // buffer full
	if !c.isClosed {
		t.Fatal("expected client with a full send buffer to be closed")
	}
}

func TestChatLimitsAndServerTimestamp(t *testing.T) {
	hub := NewHub()
	a := newTestClient(hub, "r", "a")
	_ = hub.Register(a)
	<-a.Send // existing-peers

	hub.HandleMessage(a, Message{Type: TypeChat, Text: strings.Repeat("x", maxChatRunes+1)})
	hub.HandleMessage(a, Message{Type: TypeChat, Text: "ok", Timestamp: "forged"})

	var m Message
	_ = json.Unmarshal(<-a.Send, &m)
	if m.Text != "ok" {
		t.Fatalf("oversized chat should be dropped; first delivered was %q", m.Text)
	}
	if m.Timestamp == "forged" || m.Timestamp == "" {
		t.Fatalf("timestamp must be server-assigned, got %q", m.Timestamp)
	}
}
