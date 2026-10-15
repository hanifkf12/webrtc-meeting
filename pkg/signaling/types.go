package signaling

import "encoding/json"

// Signal message types
const (
	TypeJoin          = "join"
	TypeExistingPeers = "existing-peers"
	TypePeerJoined    = "peer-joined"
	TypePeerLeft      = "peer-left"
	TypeOffer         = "offer"
	TypeAnswer        = "answer"
	TypeCandidate     = "candidate"
	TypeChat          = "chat"
	TypeMediaToggle   = "media-toggle"
)

// PeerInfo represents a connected participant in a room
type PeerInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// MediaTogglePayload conveys mic, video, or screen sharing state changes
type MediaTogglePayload struct {
	AudioEnabled  bool `json:"audioEnabled"`
	VideoEnabled  bool `json:"videoEnabled"`
	ScreenSharing bool `json:"screenSharing"`
}

// Message is the standard signaling and chat payload exchanged between clients and server
type Message struct {
	Type      string              `json:"type"`
	From      string              `json:"from,omitempty"`
	FromName  string              `json:"fromName,omitempty"`
	To        string              `json:"to,omitempty"`
	SDP       json.RawMessage     `json:"sdp,omitempty"`
	Candidate json.RawMessage     `json:"candidate,omitempty"`
	Peers     []PeerInfo          `json:"peers,omitempty"`
	Text      string              `json:"text,omitempty"`
	Media     *MediaTogglePayload `json:"media,omitempty"`
	Timestamp string              `json:"timestamp,omitempty"`
}
