package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"webrtc-meeting/pkg/signaling"
)

func setupTestServer() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)
	mux.HandleFunc("/room/", handleRoom)
	mux.HandleFunc("/api/random-room", handleRandomRoom)
	mux.HandleFunc("/ws", handleWebSocket)

	return httptest.NewServer(mux)
}

func TestHTTPEndpoints(t *testing.T) {
	ts := setupTestServer()
	defer ts.Close()

	// 1. Test Index
	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("failed GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for /, got %d", resp.StatusCode)
	}

	// 2. Test Random Room API
	respRandom, err := http.Get(ts.URL + "/api/random-room")
	if err != nil {
		t.Fatalf("failed GET /api/random-room: %v", err)
	}
	defer respRandom.Body.Close()
	if respRandom.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for /api/random-room, got %d", respRandom.StatusCode)
	}

	// 3. Test Room View
	respRoom, err := http.Get(ts.URL + "/room/daily-sync?name=Alice")
	if err != nil {
		t.Fatalf("failed GET /room/daily-sync: %v", err)
	}
	defer respRoom.Body.Close()
	if respRoom.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for /room/daily-sync, got %d", respRoom.StatusCode)
	}
}

func TestWebSocketSignalingIntegration(t *testing.T) {
	ts := setupTestServer()
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	// Connect Client A (Alice)
	urlA := wsURL + "?room=dev-room&name=Alice&id=client_alice"
	connA, _, err := websocket.DefaultDialer.Dial(urlA, nil)
	if err != nil {
		t.Fatalf("failed to dial ws client A: %v", err)
	}
	defer connA.Close()

	// Alice should receive existing-peers (empty initially)
	var msgA signaling.Message
	if err := connA.ReadJSON(&msgA); err != nil {
		t.Fatalf("Alice failed to read initial message: %v", err)
	}
	if msgA.Type != signaling.TypeExistingPeers {
		t.Errorf("expected existing-peers, got %s", msgA.Type)
	}
	if len(msgA.Peers) != 0 {
		t.Errorf("expected 0 initial peers, got %d", len(msgA.Peers))
	}

	// Connect Client B (Bob)
	urlB := wsURL + "?room=dev-room&name=Bob&id=client_bob"
	connB, _, err := websocket.DefaultDialer.Dial(urlB, nil)
	if err != nil {
		t.Fatalf("failed to dial ws client B: %v", err)
	}
	defer connB.Close()

	// Bob should receive existing-peers containing Alice
	var msgB signaling.Message
	if err := connB.ReadJSON(&msgB); err != nil {
		t.Fatalf("Bob failed to read initial message: %v", err)
	}
	if msgB.Type != signaling.TypeExistingPeers || len(msgB.Peers) != 1 || msgB.Peers[0].ID != "client_alice" {
		t.Errorf("expected Bob to receive Alice in existing-peers, got %v", msgB)
	}

	// Alice should receive peer-joined for Bob
	var joinedMsg signaling.Message
	if err := connA.ReadJSON(&joinedMsg); err != nil {
		t.Fatalf("Alice failed to read peer-joined: %v", err)
	}
	if joinedMsg.Type != signaling.TypePeerJoined || joinedMsg.From != "client_bob" {
		t.Errorf("expected peer-joined for Bob, got %v", joinedMsg)
	}

	// Bob sends Offer to Alice
	offerPayload := json.RawMessage(`{"type":"offer","sdp":"mock-sdp-bob"}`)
	err = connB.WriteJSON(signaling.Message{
		Type: signaling.TypeOffer,
		To:   "client_alice",
		SDP:  offerPayload,
	})
	if err != nil {
		t.Fatalf("Bob failed to send offer: %v", err)
	}

	// Alice receives Offer
	var offerReceived signaling.Message
	if err := connA.ReadJSON(&offerReceived); err != nil {
		t.Fatalf("Alice failed to receive offer: %v", err)
	}
	if offerReceived.Type != signaling.TypeOffer || offerReceived.From != "client_bob" {
		t.Errorf("expected offer from Bob, got %v", offerReceived)
	}

	// Alice sends Answer to Bob
	answerPayload := json.RawMessage(`{"type":"answer","sdp":"mock-sdp-alice"}`)
	err = connA.WriteJSON(signaling.Message{
		Type: signaling.TypeAnswer,
		To:   "client_bob",
		SDP:  answerPayload,
	})
	if err != nil {
		t.Fatalf("Alice failed to send answer: %v", err)
	}

	// Bob receives Answer
	var answerReceived signaling.Message
	if err := connB.ReadJSON(&answerReceived); err != nil {
		t.Fatalf("Bob failed to receive answer: %v", err)
	}
	if answerReceived.Type != signaling.TypeAnswer || answerReceived.From != "client_alice" {
		t.Errorf("expected answer from Alice, got %v", answerReceived)
	}

	// Bob sends Chat message
	err = connB.WriteJSON(signaling.Message{
		Type: signaling.TypeChat,
		Text: "Hey Alice, can you hear me?",
	})
	if err != nil {
		t.Fatalf("Bob failed to send chat: %v", err)
	}

	// Alice receives Chat
	var chatAlice signaling.Message
	if err := connA.ReadJSON(&chatAlice); err != nil {
		t.Fatalf("Alice failed to receive chat: %v", err)
	}
	if chatAlice.Type != signaling.TypeChat || chatAlice.Text != "Hey Alice, can you hear me?" {
		t.Errorf("unexpected chat on Alice: %v", chatAlice)
	}

	// Bob also receives broadcast of his own chat (for display confirmation)
	var chatBob signaling.Message
	if err := connB.ReadJSON(&chatBob); err != nil {
		t.Fatalf("Bob failed to receive own chat: %v", err)
	}
	if chatBob.Type != signaling.TypeChat || chatBob.Text != "Hey Alice, can you hear me?" {
		t.Errorf("unexpected chat on Bob: %v", chatBob)
	}

	// Bob closes connection -> Alice should receive peer-left
	connB.Close()

	// Wait briefly for Alice to receive peer-left
	_ = connA.SetReadDeadline(time.Now().Add(2 * time.Second))
	var leftMsg signaling.Message
	if err := connA.ReadJSON(&leftMsg); err != nil {
		t.Fatalf("Alice failed to receive peer-left: %v", err)
	}
	if leftMsg.Type != signaling.TypePeerLeft || leftMsg.From != "client_bob" {
		t.Errorf("expected peer-left from Bob, got %v", leftMsg)
	}
}

func TestWebSocketRejectsCrossOriginAndBadInput(t *testing.T) {
	ts := setupTestServer()
	defer ts.Close()
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	// Cross-site origin must be refused.
	hdr := http.Header{"Origin": {"http://evil.example"}}
	if _, resp, err := websocket.DefaultDialer.Dial(wsURL+"?room=ok", hdr); err == nil {
		t.Fatal("expected cross-origin dial to fail")
	} else if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for cross-origin, got %v", resp)
	}

	// Same-origin browser header is allowed.
	hdr = http.Header{"Origin": {ts.URL}}
	c, _, err := websocket.DefaultDialer.Dial(wsURL+"?room=ok&id=same_origin", hdr)
	if err != nil {
		t.Fatalf("same-origin dial failed: %v", err)
	}
	c.Close()

	// Invalid room / id charset.
	for _, q := range []string{"?room=bad%20room", "?room=ok&id=a%20b", ""} {
		if _, resp, err := websocket.DefaultDialer.Dial(wsURL+q, nil); err == nil {
			t.Fatalf("expected 400 for %q", q)
		} else if resp == nil || resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 for %q, got %v", q, resp)
		}
	}
}

func TestWebSocketDuplicateIDClosedWithRetryCode(t *testing.T) {
	ts := setupTestServer()
	defer ts.Close()
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws?room=dup-room&id=dup_id"

	first, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	var m signaling.Message
	if err := first.ReadJSON(&m); err != nil {
		t.Fatal(err)
	}

	second, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = second.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseTryAgainLater) {
		t.Fatalf("expected close 1013, got %v", err)
	}

	// The original connection must be unaffected.
	if err := first.WriteJSON(signaling.Message{Type: signaling.TypeChat, Text: "still here"}); err != nil {
		t.Fatal(err)
	}
	if err := first.ReadJSON(&m); err != nil || m.Text != "still here" {
		t.Fatalf("original client broken after duplicate attempt: %v %v", m, err)
	}
}

// Browsers JSON.parse each frame, so a burst must never be packed into one frame.
func TestWebSocketBurstIsOneMessagePerFrame(t *testing.T) {
	ts := setupTestServer()
	defer ts.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/ws?room=burst&id=burst_a", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	const n = 50
	for range n {
		if err := conn.WriteJSON(signaling.Message{Type: signaling.TypeChat, Text: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for got := range n + 1 { // existing-peers + n chats
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("after %d messages: %v", got, err)
		}
		var m signaling.Message
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("frame is not a single JSON message: %q", raw)
		}
	}
}

func TestWebSocketFloodIsDisconnected(t *testing.T) {
	ts := setupTestServer()
	defer ts.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/ws?room=flood&id=flood_a", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for range 300 {
		if err := conn.WriteJSON(signaling.Message{Type: signaling.TypeChat, Text: "spam"}); err != nil {
			break // server already hung up
		}
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatal("flooding client was not disconnected")
			}
			return
		}
	}
}

func TestSanitizeName(t *testing.T) {
	if got := sanitizeName("  Al\x00ice\n "); got != "Alice" {
		t.Errorf("control chars not stripped: %q", got)
	}
	if got := sanitizeName(strings.Repeat("é", 100)); len([]rune(got)) != maxNameRunes {
		t.Errorf("name not capped to %d runes: %d", maxNameRunes, len([]rune(got)))
	}
}
