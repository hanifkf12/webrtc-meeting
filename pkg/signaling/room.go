package signaling

import "sync"

// Room holds all active participants in a meeting room
type Room struct {
	ID      string
	Clients map[string]*Client
	mu      sync.RWMutex
}

// NewRoom creates an empty Room instance
func NewRoom(id string) *Room {
	return &Room{
		ID:      id,
		Clients: make(map[string]*Client),
	}
}

// AddClient adds a client to the room
func (r *Room) AddClient(c *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Clients[c.ID] = c
}

// RemoveClient removes c from the room only if it is the currently registered client for its ID.
// It reports whether a removal happened.
func (r *Room) RemoveClient(c *Client) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.Clients[c.ID] != c {
		return false
	}
	delete(r.Clients, c.ID)
	return true
}

// Has reports whether a client with the given ID is in the room
func (r *Room) Has(clientID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.Clients[clientID]
	return ok
}

// IsEmpty returns whether the room has zero connected clients
func (r *Room) IsEmpty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.Clients) == 0
}

// Count returns the number of participants currently in the room
func (r *Room) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.Clients)
}

// GetPeerList returns a slice of PeerInfo for all participants except excludeID
func (r *Room) GetPeerList(excludeID string) []PeerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	peers := make([]PeerInfo, 0, len(r.Clients))
	for id, client := range r.Clients {
		if id != excludeID {
			peers = append(peers, PeerInfo{
				ID:   id,
				Name: client.Name,
			})
		}
	}
	return peers
}

// Broadcast sends a message to all connected clients in the room, optionally excluding excludeID
func (r *Room) Broadcast(msg Message, excludeID string) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for id, client := range r.Clients {
		if id != excludeID {
			_ = client.SendJSON(msg)
		}
	}
}

// SendTo delivers a message directly to a target client in the room
func (r *Room) SendTo(targetID string, msg Message) bool {
	r.mu.RLock()
	client, exists := r.Clients[targetID]
	r.mu.RUnlock()

	if !exists {
		return false
	}
	_ = client.SendJSON(msg)
	return true
}
