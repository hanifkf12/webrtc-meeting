package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"html/template"
	"log"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"webrtc-meeting/pkg/signaling"
)

const maxNameRunes = 40

// idPattern restricts room and client IDs to the charset the UI already enforces.
var idPattern = regexp.MustCompile(`^[a-z0-9_-]{1,64}$`)

var (
	hub      *signaling.Hub
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     sameOrigin,
	}
	tmplIndex *template.Template
	tmplRoom  *template.Template
)

// sameOrigin allows non-browser clients (no Origin header) and browsers whose Origin host
// matches the request Host; this blocks cross-site WebSocket hijacking.
func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return strings.EqualFold(u.Host, r.Host)
}

// sanitizeName trims, strips control characters and caps the display name length.
func sanitizeName(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, strings.TrimSpace(s))
	if utf8.RuneCountInString(s) > maxNameRunes {
		s = string([]rune(s)[:maxNameRunes])
	}
	return strings.TrimSpace(s)
}

type RoomPageData struct {
	RoomID string
	Name   string
	// Optional TURN relay for peers behind symmetric NAT.
	// shortcut: static credentials are visible to every room visitor; switch to time-limited TURN REST credentials before public use.
	TURNURL, TURNUsername, TURNCredential string
}

func init() {
	var err error
	tmplIndex, err = template.ParseFiles("templates/index.html")
	if err != nil {
		log.Fatalf("failed to parse index template: %v", err)
	}

	tmplRoom, err = template.ParseFiles("templates/room.html")
	if err != nil {
		log.Fatalf("failed to parse room template: %v", err)
	}

	hub = signaling.NewHub()
}

// Generate human-friendly random room slugs
var (
	adjectives = []string{"cool", "bright", "swift", "calm", "bold", "wild", "cyber", "hyper", "neon", "solar"}
	nouns      = []string{"falcon", "river", "beacon", "summit", "orbit", "valley", "nexus", "aurora", "haven", "pulse"}
)

func randomRoomSlug() string {
	adjIdx, _ := rand.Int(rand.Reader, big.NewInt(int64(len(adjectives))))
	nounIdx, _ := rand.Int(rand.Reader, big.NewInt(int64(len(nouns))))
	num, _ := rand.Int(rand.Reader, big.NewInt(900))
	return fmt.Sprintf("%s-%s-%d", adjectives[adjIdx.Int64()], nouns[nounIdx.Int64()], num.Int64()+100)
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if err := tmplIndex.Execute(w, nil); err != nil {
		log.Printf("render index error: %v", err)
	}
}

func handleRoom(w http.ResponseWriter, r *http.Request) {
	// Room path: /room/{id}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 || parts[1] == "" {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	roomID := strings.ToLower(parts[1])
	if !idPattern.MatchString(roomID) {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	name := sanitizeName(r.URL.Query().Get("name"))
	if name == "" {
		name = "Guest-" + randomRoomSlug()[:4]
	}

	data := RoomPageData{
		RoomID:         roomID,
		Name:           name,
		TURNURL:        os.Getenv("TURN_URL"),
		TURNUsername:   os.Getenv("TURN_USERNAME"),
		TURNCredential: os.Getenv("TURN_CREDENTIAL"),
	}

	if err := tmplRoom.Execute(w, data); err != nil {
		log.Printf("render room error: %v", err)
	}
}

// HTMX endpoint: returns a fresh room ID input snippet
func handleRandomRoom(w http.ResponseWriter, r *http.Request) {
	slug := randomRoomSlug()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<input type="text" id="room-id" name="room" class="form-input" value="%s" required pattern="[a-zA-Z0-9_-]+" maxlength="64" title="Letters, numbers, hyphens, and underscores only">`, slug)
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	roomID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("room")))
	clientName := sanitizeName(r.URL.Query().Get("name"))
	clientID := strings.TrimSpace(r.URL.Query().Get("id"))

	if !idPattern.MatchString(roomID) {
		http.Error(w, "missing or invalid room query parameter", http.StatusBadRequest)
		return
	}
	if clientName == "" {
		clientName = "Participant"
	}
	if clientID == "" {
		clientID = fmt.Sprintf("peer_%d", time.Now().UnixNano())
	} else if !idPattern.MatchString(clientID) {
		http.Error(w, "invalid id query parameter", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("failed to upgrade websocket: %v", err)
		return
	}

	client := signaling.NewClient(hub, conn, roomID, clientID, clientName)
	if err := hub.Register(client); err != nil {
		// 1008: policy violation (room full, do not retry); 1013: try again later (stale duplicate ID).
		code := websocket.ClosePolicyViolation
		if errors.Is(err, signaling.ErrDuplicateID) {
			code = websocket.CloseTryAgainLater
		}
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, err.Error()), time.Now().Add(time.Second))
		_ = conn.Close()
		return
	}

	go client.WritePump()
	client.ReadPump()
}

func main() {
	mux := http.NewServeMux()

	// Static assets
	fs := http.FileServer(http.Dir("static"))
	mux.Handle("/static/", http.StripPrefix("/static/", fs))

	// Routes
	mux.HandleFunc("/", handleIndex)
	mux.HandleFunc("/room/", handleRoom)
	mux.HandleFunc("/api/random-room", handleRandomRoom)
	mux.HandleFunc("/ws", handleWebSocket)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	addr := ":" + port

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Server shutdown listener
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Browsers only expose camera/mic on secure origins, so LAN access needs TLS.
	certFile, keyFile := os.Getenv("TLS_CERT_FILE"), os.Getenv("TLS_KEY_FILE")

	go func() {
		var err error
		if certFile != "" || keyFile != "" { // one missing fails loudly instead of silently serving HTTP
			log.Printf("MeetStream server listening on https://localhost%s", addr)
			err = server.ListenAndServeTLS(certFile, keyFile)
		} else {
			log.Printf("MeetStream server listening on http://localhost%s", addr)
			err = server.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-stop
	log.Println("Shutting down MeetStream server gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("server shutdown failed: %v", err)
	}

	log.Println("Server stopped")
}
