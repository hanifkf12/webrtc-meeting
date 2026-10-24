package signaling

import (
	"errors"
	"log"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	// MaxRoomSize caps participants per room; a full WebRTC mesh degrades quickly beyond this.
	MaxRoomSize = 8
	// MaxRooms caps concurrently open rooms so room creation cannot exhaust server memory.
	MaxRooms = 1000
	// maxChatRunes caps a single chat message.
	maxChatRunes = 2000
)

var (
	// ErrRoomFull is returned by Register when the room is at MaxRoomSize.
	ErrRoomFull = errors.New("room is full")
	// ErrTooManyRooms is returned by Register when creating a room would exceed MaxRooms.
	ErrTooManyRooms = errors.New("server has too many open rooms")
	// ErrDuplicateID is returned by Register when a live client already uses the ID in that room.
	ErrDuplicateID = errors.New("client id already in use in this room")
)

// Hub maintains the set of active rooms and manages peer signaling
type Hub struct {
	Rooms map[string]*Room
	mu    sync.RWMutex
}

// NewHub initializes a Hub
func NewHub() *Hub {
	return &Hub{
		Rooms: make(map[string]*Room),
	}
}

// Register adds a client to its room, sends existing peers to newcomer, and broadcasts peer-joined.
// Room lookup, capacity/duplicate checks, peer snapshot and insertion happen atomically under the
// hub lock, so concurrent joins and leaves cannot miss each other or land in a deleted room.
func (h *Hub) Register(c *Client) error {
	h.mu.Lock()
	room, exists := h.Rooms[c.RoomID]
	if !exists {
		if len(h.Rooms) >= MaxRooms {
			h.mu.Unlock()
			return ErrTooManyRooms
		}
		room = NewRoom(c.RoomID)
		h.Rooms[c.RoomID] = room
	}
	if room.Has(c.ID) {
		h.mu.Unlock()
		return ErrDuplicateID
	}
	if room.Count() >= MaxRoomSize {
		h.mu.Unlock()
		return ErrRoomFull
	}
	existingPeers := room.GetPeerList(c.ID)
	room.AddClient(c)

	// Notify under the hub lock (SendJSON never blocks) so concurrent joins are strictly ordered:
	// the later joiner sees the earlier in existing-peers, the earlier gets exactly one peer-joined.
	_ = c.SendJSON(Message{
		Type:  TypeExistingPeers,
		Peers: existingPeers,
	})
	room.Broadcast(Message{
		Type:     TypePeerJoined,
		From:     c.ID,
		FromName: c.Name,
	}, c.ID)
	h.mu.Unlock()

	log.Printf("client %s [%s] joined room %s (total: %d)", c.ID, c.Name, c.RoomID, len(existingPeers)+1)
	return nil
}

// Unregister removes a client and cleans up empty rooms. It is a no-op if c is no longer
// the registered client for its ID (e.g. it was never registered).
func (h *Hub) Unregister(c *Client) {
	h.mu.Lock()
	room, exists := h.Rooms[c.RoomID]
	if !exists || !room.RemoveClient(c) {
		h.mu.Unlock()
		return
	}
	if room.IsEmpty() {
		delete(h.Rooms, c.RoomID)
	}
	// Notify while still holding the hub lock so a concurrent join is ordered strictly before or after.
	room.Broadcast(Message{
		Type:     TypePeerLeft,
		From:     c.ID,
		FromName: c.Name,
	}, c.ID)
	h.mu.Unlock()

	log.Printf("client %s [%s] left room %s", c.ID, c.Name, c.RoomID)
}

// HandleMessage routes signaling, chat, and media events
func (h *Hub) HandleMessage(c *Client, msg Message) {
	h.mu.RLock()
	room, exists := h.Rooms[c.RoomID]
	h.mu.RUnlock()

	if !exists {
		return
	}

	switch msg.Type {
	case TypeOffer, TypeAnswer, TypeCandidate:
		if msg.To == "" {
			log.Printf("missing target peer 'to' in message %s from %s", msg.Type, c.ID)
			return
		}
		room.SendTo(msg.To, msg)

	case TypeChat:
		if msg.Text == "" || utf8.RuneCountInString(msg.Text) > maxChatRunes {
			return
		}
		// Server owns the timestamp; never trust the client's. UTC so each browser renders local time.
		msg.Timestamp = time.Now().UTC().Format(time.RFC3339)
		// Broadcast to all participants including sender
		room.Broadcast(msg, "")

	case TypeMediaToggle:
		if msg.Media == nil {
			return
		}
		// Broadcast media state change to all other peers
		room.Broadcast(msg, c.ID)

	default:
		log.Printf("unhandled message type: %s from %s", msg.Type, c.ID)
	}
}
