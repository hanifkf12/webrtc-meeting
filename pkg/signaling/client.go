package signaling

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512 * 1024 // 512 KB
	// Token bucket per client: absorbs a join/ICE-restart burst to a full room (offers plus
	// ~15 candidates per peer), stops floods that would overflow every peer's send buffer.
	// Burst stays below the 256-slot send buffer.
	msgBurst = 200
	msgRate  = 20 // messages per second
)

// Client represents a single connected peer over WebSocket
type Client struct {
	Hub      *Hub
	Conn     *websocket.Conn
	Send     chan []byte
	RoomID   string
	ID       string
	Name     string
	mu       sync.Mutex
	isClosed bool
}

// NewClient instantiates a new Client
func NewClient(hub *Hub, conn *websocket.Conn, roomID, clientID, clientName string) *Client {
	return &Client{
		Hub:    hub,
		Conn:   conn,
		Send:   make(chan []byte, 256),
		RoomID: roomID,
		ID:     clientID,
		Name:   clientName,
	}
}

// ReadPump continuously pumps messages from the websocket to the Hub
func (c *Client) ReadPump() {
	defer func() {
		c.Hub.Unregister(c)
		c.Close()
	}()

	c.Conn.SetReadLimit(maxMessageSize)
	_ = c.Conn.SetReadDeadline(time.Now().Add(pongWait))
	c.Conn.SetPongHandler(func(string) error {
		_ = c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	tokens, last := float64(msgBurst), time.Now()
	for {
		_, rawMessage, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure, websocket.CloseNoStatusReceived) {
				log.Printf("client %s [%s] read error: %v", c.ID, c.Name, err)
			}
			break
		}

		now := time.Now()
		tokens = min(msgBurst, tokens+now.Sub(last).Seconds()*msgRate)
		last = now
		if tokens < 1 {
			log.Printf("client %s [%s] exceeded message rate; disconnecting", c.ID, c.Name)
			break
		}
		tokens--

		var msg Message
		if err := json.Unmarshal(rawMessage, &msg); err != nil {
			log.Printf("invalid json from %s: %v", c.ID, err)
			continue
		}

		// Ensure identity invariants
		msg.From = c.ID
		msg.FromName = c.Name

		c.Hub.HandleMessage(c, msg)
	}
}

// WritePump continuously pumps messages from the send channel to the websocket
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			_ = c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Channel closed by hub
				_ = c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			// One JSON message per frame: the browser JSON.parses each frame whole.
			if _, err := w.Write(message); err != nil {
				return
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// SendJSON marshals and sends data to client safely
func (c *Client) SendJSON(v interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isClosed {
		return nil
	}

	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	select {
	case c.Send <- data:
		return nil
	default:
		// Dropping an offer/answer/candidate silently breaks a call; disconnect the slow
		// consumer instead so it can reconnect and renegotiate from a clean state.
		log.Printf("client %s send buffer full; disconnecting", c.ID)
		c.closeLocked()
		return nil
	}
}

// Close gracefully closes the client channel and connection
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeLocked()
}

// closeLocked requires c.mu to be held.
func (c *Client) closeLocked() {
	if c.isClosed {
		return
	}
	c.isClosed = true
	close(c.Send)
	if c.Conn != nil {
		_ = c.Conn.Close()
	}
}
