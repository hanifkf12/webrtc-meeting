// MeetStream WebRTC Multi-Peer Mesh & Signaling Client

const rtcConfig = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    { urls: 'stun:stun2.l.google.com:19302' }
  ]
};

// Application State
let localStream = null;
let screenStream = null;
let ws = null;
const peerStates = {};      // peerId -> { pc, polite, makingOffer, ignoreOffer, videoSender, remoteStream, ... }
const peerNames = {};       // peerId -> string (displayName)
const pendingCandidates = {}; // peerId -> Array of RTCIceCandidateInit

const RECONNECT_BASE_MS = 500;
const RECONNECT_MAX_MS = 8000;
let reconnectAttempts = 0;
let reconnectTimer = null;
let isLeaving = false;
let signalQueue = Promise.resolve();

let isAudioEnabled = true;
let isVideoEnabled = true;
let isScreenSharing = false;
let isSidebarOpen = false;
let currentTab = 'chat';
let unreadChatCount = 0;

// Read config from DOM attributes
const appContainer = document.getElementById('room-app');
const roomId = appContainer.dataset.roomId;
const clientName = appContainer.dataset.clientName || 'Anonymous';
const clientId = 'peer_' + Math.random().toString(36).substring(2, 9);

// Init on DOM load
window.addEventListener('DOMContentLoaded', async () => {
  setupLocalAvatar();
  await initLocalMedia();
  initWebSocket();
});

// Setup local avatar initials
function setupLocalAvatar() {
  const initials = (clientName || 'U').trim().charAt(0).toUpperCase();
  const avatarEl = document.getElementById('local-avatar-initials');
  if (avatarEl) avatarEl.textContent = initials;
}

// 1. Local Media Initialization
async function initLocalMedia() {
  try {
    localStream = await navigator.mediaDevices.getUserMedia({
      audio: true,
      video: {
        width: { ideal: 1280 },
        height: { ideal: 720 },
        facingMode: 'user'
      }
    });
  } catch (err) {
    console.warn('Could not acquire audio/video stream with preferred constraints:', err);
    try {
      // Fallback: audio only or default video
      localStream = await navigator.mediaDevices.getUserMedia({ audio: true, video: false });
      isVideoEnabled = false;
      document.getElementById('tile-local').classList.add('cam-off');
      updateCamButtonUI(false);
    } catch (fallbackErr) {
      console.error('No media devices available:', fallbackErr);
      // Create empty stream so signaling still functions
      localStream = new MediaStream();
      isAudioEnabled = false;
      isVideoEnabled = false;
    }
  }

  const localVideo = document.getElementById('local-video');
  if (localVideo) {
    localVideo.srcObject = localStream;
  }
}

// 2. WebSocket Signaling Connection
function initWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/ws?room=${encodeURIComponent(roomId)}&name=${encodeURIComponent(clientName)}&id=${encodeURIComponent(clientId)}`;

  const socket = new WebSocket(wsUrl);
  ws = socket;

  socket.onopen = () => {
    console.log('[Signaling] WebSocket connected to room', roomId);
    reconnectAttempts = 0;
    updateParticipantsList();
  };

  socket.onmessage = (event) => {
    // Serialize handling: SDP/candidate order matters and the handlers are async.
    signalQueue = signalQueue.then(async () => {
      try {
        await handleSignalMessage(JSON.parse(event.data));
      } catch (e) {
        console.error('[Signaling] Failed to handle message:', event.data, e);
      }
    });
  };

  socket.onerror = (err) => {
    console.error('[Signaling] WebSocket error:', err);
  };

  socket.onclose = (event) => {
    if (socket !== ws || isLeaving) return;
    console.warn('[Signaling] WebSocket closed', event.code, event.reason);

    // The server drops us and tells peers peer-left; rejoin from a clean mesh state.
    for (const peerId of Object.keys(peerStates)) handlePeerLeft(peerId);

    if (event.code === 1008) {
      // Policy violation (e.g. room full): retrying will not help.
      showToast(event.reason || 'Unable to join this room');
      setTimeout(() => { window.location.href = '/'; }, 2500);
      return;
    }
    scheduleReconnect();
  };
}

function scheduleReconnect() {
  const delay = Math.min(RECONNECT_MAX_MS, RECONNECT_BASE_MS * 2 ** reconnectAttempts);
  reconnectAttempts++;
  showToast('Connection lost. Reconnecting...');
  reconnectTimer = setTimeout(initWebSocket, delay);
}

// Send helper
function sendSignal(msg) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}

// 3. Signal Message Dispatcher
async function handleSignalMessage(msg) {
  switch (msg.type) {
    case 'existing-peers':
      // We are the newcomer: create a connection to each existing peer; negotiationneeded sends the offers.
      if (Array.isArray(msg.peers)) {
        for (const peer of msg.peers) {
          peerNames[peer.id] = peer.name;
          if (!peerStates[peer.id]) createPeerState(peer.id, peer.name, true);
        }
      }
      updateParticipantsList();
      break;

    case 'peer-joined':
      // Another peer joined. Store name. The newcomer will send us an offer.
      peerNames[msg.from] = msg.fromName;
      updateParticipantsList();
      break;

    case 'offer':
    case 'answer':
      await handleDescription(msg.from, msg.fromName, msg.sdp);
      break;

    case 'candidate':
      await handleReceiveCandidate(msg.from, msg.candidate);
      break;

    case 'peer-left':
      handlePeerLeft(msg.from);
      break;

    case 'chat':
      renderChatMessage(msg.fromName, msg.text, msg.timestamp, msg.from === clientId);
      break;

    case 'media-toggle':
      handleRemoteMediaToggle(msg.from, msg.media);
      break;

    default:
      console.log('[Signaling] Unknown message:', msg);
  }
}

// 4. WebRTC Peer Connection (perfect negotiation pattern)
function currentVideoTrack() {
  if (isScreenSharing && screenStream) return screenStream.getVideoTracks()[0] || null;
  return localStream ? localStream.getVideoTracks()[0] || null : null;
}

// initiator: we create the offer (newcomer side) and own the m-lines, so add transceivers now.
// Otherwise the m-lines come from the remote offer and bindLocalMedia() attaches our tracks.
function createPeerState(peerId, peerName, initiator) {
  if (peerName) peerNames[peerId] = peerName;

  const pc = new RTCPeerConnection(rtcConfig);
  const st = {
    pc,
    // Deterministic tie-break for offer collisions: exactly one side is polite.
    polite: clientId > peerId,
    makingOffer: false,
    ignoreOffer: false,
    mediaBound: false,
    videoSender: null,
    remoteStream: new MediaStream(),
    disconnectTimer: null
  };
  peerStates[peerId] = st;

  pc.onnegotiationneeded = async () => {
    try {
      st.makingOffer = true;
      await pc.setLocalDescription();
      sendSignal({ type: 'offer', to: peerId, sdp: pc.localDescription });
    } catch (err) {
      console.error(`[WebRTC] Failed to create offer for ${peerId}:`, err);
    } finally {
      st.makingOffer = false;
    }
  };

  pc.onicecandidate = (event) => {
    if (event.candidate) {
      sendSignal({ type: 'candidate', to: peerId, candidate: event.candidate });
    }
  };

  // Audio and video tracks may arrive as separate streams (or none); merge into one per-peer stream.
  pc.ontrack = (event) => {
    if (!st.remoteStream.getTracks().includes(event.track)) {
      st.remoteStream.addTrack(event.track);
    }
    attachRemoteTrack(peerId, peerNames[peerId] || 'Participant', st.remoteStream);
  };

  pc.onconnectionstatechange = () => {
    console.log(`[WebRTC] Connection with ${peerId}:`, pc.connectionState);
    clearTimeout(st.disconnectTimer);
    if (pc.connectionState === 'failed') {
      pc.restartIce();
    } else if (pc.connectionState === 'disconnected') {
      // 'disconnected' is often transient; only restart ICE if it persists.
      st.disconnectTimer = setTimeout(() => {
        if (pc.connectionState === 'disconnected') pc.restartIce();
      }, 5000);
    }
    // Tile removal is driven by the server's peer-left, not by connection state.
  };

  if (initiator) {
    const audioTrack = localStream ? localStream.getAudioTracks()[0] : null;
    const videoTrack = currentVideoTrack();
    if (audioTrack) {
      pc.addTransceiver(audioTrack, { direction: 'sendrecv', streams: [localStream] });
    } else {
      pc.addTransceiver('audio', { direction: 'recvonly' });
    }
    // Always negotiate a sendrecv video m-line so screen sharing works even without a camera.
    const videoTransceiver = videoTrack
      ? pc.addTransceiver(videoTrack, { direction: 'sendrecv', streams: [localStream] })
      : pc.addTransceiver('video', { direction: 'sendrecv', streams: [localStream] });
    st.videoSender = videoTransceiver.sender;
    st.mediaBound = true;
  }

  return st;
}

async function bindLocalMedia(st) {
  if (st.mediaBound) return;
  st.mediaBound = true;
  for (const t of st.pc.getTransceivers()) {
    if (t.stopped || t.currentDirection === 'stopped') continue;
    const kind = t.receiver.track.kind;
    const track = kind === 'audio'
      ? (localStream ? localStream.getAudioTracks()[0] || null : null)
      : currentVideoTrack();
    if (kind === 'video') st.videoSender = t.sender;
    if (!track && kind === 'audio') continue; // stay recvonly/inactive for audio
    t.direction = 'sendrecv';
    if (t.sender.setStreams) t.sender.setStreams(localStream);
    if (track) await t.sender.replaceTrack(track);
  }
}

async function drainPendingCandidates(peerId, st) {
  const queued = pendingCandidates[peerId];
  if (!queued) return;
  delete pendingCandidates[peerId];
  for (const candidate of queued) {
    try {
      await st.pc.addIceCandidate(candidate);
    } catch (err) {
      if (!st.ignoreOffer) console.error(`[WebRTC] Error adding queued ICE candidate from ${peerId}:`, err);
    }
  }
}

// Handle an incoming offer or answer
async function handleDescription(fromPeerId, fromPeerName, description) {
  let st = peerStates[fromPeerId];
  if (!st) {
    if (description.type !== 'offer') return;
    st = createPeerState(fromPeerId, fromPeerName, false);
  }
  if (fromPeerName) peerNames[fromPeerId] = fromPeerName;
  const pc = st.pc;

  try {
    const offerCollision = description.type === 'offer' && (st.makingOffer || pc.signalingState !== 'stable');
    st.ignoreOffer = !st.polite && offerCollision;
    if (st.ignoreOffer) return;

    await pc.setRemoteDescription(description);
    if (description.type === 'offer') {
      await bindLocalMedia(st);
      await pc.setLocalDescription();
      sendSignal({ type: 'answer', to: fromPeerId, sdp: pc.localDescription });
    }
    await drainPendingCandidates(fromPeerId, st);
  } catch (err) {
    console.error(`[WebRTC] Failed handling ${description.type} from ${fromPeerId}:`, err);
  }
}

// Handle Incoming Candidate
async function handleReceiveCandidate(fromPeerId, candidate) {
  const st = peerStates[fromPeerId];
  if (!st || !st.pc.remoteDescription) {
    (pendingCandidates[fromPeerId] ||= []).push(candidate);
    return;
  }

  try {
    await st.pc.addIceCandidate(candidate);
  } catch (err) {
    if (!st.ignoreOffer) console.error(`[WebRTC] Error adding ICE candidate from ${fromPeerId}:`, err);
  }
}

// 5. Remote Video Card Rendering & Teardown
function attachRemoteTrack(peerId, peerName, stream) {
  let card = document.getElementById(`tile-${peerId}`);
  let videoEl = document.getElementById(`video-${peerId}`);

  if (!card) {
    const grid = document.getElementById('video-grid');
    card = document.createElement('div');
    card.id = `tile-${peerId}`;
    card.className = 'video-card';

    const initials = escapeHtml((peerName || 'P').trim().charAt(0).toUpperCase());
    const safeName = escapeHtml(peerName || 'Participant');

    card.innerHTML = `
      <video id="video-${peerId}" autoplay playsinline></video>
      <div class="avatar-placeholder" id="avatar-${peerId}">
        <div class="avatar-circle">${initials}</div>
      </div>
      <div class="video-overlay-badge">
        <span>${safeName}</span>
        <div class="media-state-icons">
          <span class="icon-badge" id="mic-badge-${peerId}" title="Audio Active">
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M12 2a3 3 0 0 0-3 3v7a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3Z"/>
              <path d="M19 10v2a7 7 0 0 1-14 0v-2"/>
            </svg>
          </span>
        </div>
      </div>
    `;

    grid.appendChild(card);
    videoEl = card.querySelector('video');
  }

  if (videoEl && videoEl.srcObject !== stream) {
    videoEl.srcObject = stream;
  }

  updateParticipantsList();
}

function handlePeerLeft(peerId) {
  const st = peerStates[peerId];
  if (st) {
    clearTimeout(st.disconnectTimer);
    st.pc.onnegotiationneeded = null;
    st.pc.onconnectionstatechange = null;
    st.pc.close();
    delete peerStates[peerId];
  }
  delete peerNames[peerId];
  delete pendingCandidates[peerId];

  const card = document.getElementById(`tile-${peerId}`);
  if (card && card.parentNode) {
    card.parentNode.removeChild(card);
  }

  updateParticipantsList();
}

function handleRemoteMediaToggle(peerId, media) {
  if (!media) return;
  const card = document.getElementById(`tile-${peerId}`);
  if (!card) return;

  if (media.videoEnabled === false) {
    card.classList.add('cam-off');
  } else {
    card.classList.remove('cam-off');
  }

  const micBadge = document.getElementById(`mic-badge-${peerId}`);
  if (micBadge) {
    if (media.audioEnabled === false) {
      micBadge.classList.add('off');
      micBadge.title = 'Audio Muted';
    } else {
      micBadge.classList.remove('off');
      micBadge.title = 'Audio Active';
    }
  }

  if (media.screenSharing) {
    card.classList.add('screen-active');
  } else {
    card.classList.remove('screen-active');
  }
}

// 6. Media Controls (Mic, Video, Screen Share)
function toggleAudio() {
  if (!localStream) return;
  const audioTracks = localStream.getAudioTracks();
  if (audioTracks.length === 0) return;

  isAudioEnabled = !isAudioEnabled;
  audioTracks.forEach(t => t.enabled = isAudioEnabled);

  updateMicButtonUI(isAudioEnabled);
  broadcastMediaState();
}

function updateMicButtonUI(enabled) {
  const btn = document.getElementById('btn-mic');
  const iconOn = document.getElementById('icon-mic-on');
  const iconOff = document.getElementById('icon-mic-off');
  const localBadge = document.getElementById('local-mic-badge');

  if (enabled) {
    btn.classList.remove('active-danger');
    iconOn.style.display = 'block';
    iconOff.style.display = 'none';
    if (localBadge) localBadge.classList.remove('off');
  } else {
    btn.classList.add('active-danger');
    iconOn.style.display = 'none';
    iconOff.style.display = 'block';
    if (localBadge) localBadge.classList.add('off');
  }
}

function toggleVideo() {
  if (!localStream) return;
  const videoTracks = localStream.getVideoTracks();
  if (videoTracks.length === 0) return;

  isVideoEnabled = !isVideoEnabled;
  videoTracks.forEach(t => t.enabled = isVideoEnabled);

  const localCard = document.getElementById('tile-local');
  if (isVideoEnabled) {
    localCard.classList.remove('cam-off');
  } else {
    localCard.classList.add('cam-off');
  }

  updateCamButtonUI(isVideoEnabled);
  broadcastMediaState();
}

function updateCamButtonUI(enabled) {
  const btn = document.getElementById('btn-cam');
  const iconOn = document.getElementById('icon-cam-on');
  const iconOff = document.getElementById('icon-cam-off');

  if (enabled) {
    btn.classList.remove('active-danger');
    iconOn.style.display = 'block';
    iconOff.style.display = 'none';
  } else {
    btn.classList.add('active-danger');
    iconOn.style.display = 'none';
    iconOff.style.display = 'block';
  }
}

async function toggleScreenShare() {
  const btn = document.getElementById('btn-screen');
  const localTile = document.getElementById('tile-local');
  const localVideo = document.getElementById('local-video');

  if (isScreenSharing) {
    // Stop sharing
    stopScreenSharing();
    return;
  }

  try {
    screenStream = await navigator.mediaDevices.getDisplayMedia({
      video: { cursor: 'always' },
      audio: false
    });

    const screenTrack = screenStream.getVideoTracks()[0];
    isScreenSharing = true;
    btn.classList.add('active-accent');
    localTile.classList.add('screen-active');
    localVideo.srcObject = screenStream;

    // Replace track on all active peer connections
    replaceVideoTrack(screenTrack);

    // Revert when user clicks browser native "Stop Sharing" bar
    screenTrack.onended = () => {
      stopScreenSharing();
    };

    broadcastMediaState();
  } catch (err) {
    console.warn('[ScreenShare] User canceled or denied screen share:', err);
  }
}

function stopScreenSharing() {
  if (screenStream) {
    screenStream.getTracks().forEach(t => t.stop());
    screenStream = null;
  }

  isScreenSharing = false;
  const btn = document.getElementById('btn-screen');
  const localTile = document.getElementById('tile-local');
  const localVideo = document.getElementById('local-video');

  btn.classList.remove('active-accent');
  localTile.classList.remove('screen-active');

  // Fall back to the camera track, or to no track at all if the user joined without a camera.
  const cameraTrack = localStream ? localStream.getVideoTracks()[0] || null : null;
  localVideo.srcObject = localStream;
  replaceVideoTrack(cameraTrack);

  broadcastMediaState();
}

function replaceVideoTrack(newTrack) {
  for (const peerId in peerStates) {
    const sender = peerStates[peerId].videoSender;
    if (sender) {
      sender.replaceTrack(newTrack).catch((err) => {
        console.error(`[WebRTC] replaceTrack failed for ${peerId}:`, err);
      });
    }
  }
}

function broadcastMediaState() {
  sendSignal({
    type: 'media-toggle',
    media: {
      audioEnabled: isAudioEnabled,
      videoEnabled: isVideoEnabled,
      screenSharing: isScreenSharing
    }
  });
}

// 7. In-Room Chat & Sidebar UI
function toggleSidebar(tab) {
  const sidebar = document.getElementById('room-sidebar');
  if (isSidebarOpen && currentTab === tab) {
    // Close
    sidebar.classList.remove('open');
    isSidebarOpen = false;
    document.getElementById('btn-chat').classList.remove('active-accent');
    document.getElementById('btn-participants').classList.remove('active-accent');
  } else {
    // Open or switch tab
    sidebar.classList.add('open');
    isSidebarOpen = true;
    switchSidebarTab(tab);
  }
}

function switchSidebarTab(tab) {
  currentTab = tab;
  const tabChat = document.getElementById('tab-btn-chat');
  const tabPart = document.getElementById('tab-btn-participants');
  const panelChat = document.getElementById('panel-chat');
  const panelPart = document.getElementById('panel-participants');
  const btnChat = document.getElementById('btn-chat');
  const btnPart = document.getElementById('btn-participants');

  if (tab === 'chat') {
    tabChat.classList.add('active');
    tabPart.classList.remove('active');
    panelChat.classList.add('active');
    panelPart.classList.remove('active');
    btnChat.classList.add('active-accent');
    btnPart.classList.remove('active-accent');

    // Reset unread badge
    unreadChatCount = 0;
    const badge = document.getElementById('chat-unread-badge');
    badge.textContent = '0';
    badge.style.display = 'none';

    document.getElementById('chat-input').focus();
  } else {
    tabPart.classList.add('active');
    tabChat.classList.remove('active');
    panelPart.classList.add('active');
    panelChat.classList.remove('active');
    btnPart.classList.add('active-accent');
    btnChat.classList.remove('active-accent');
  }
}

function sendChatMessage(event) {
  event.preventDefault();
  const input = document.getElementById('chat-input');
  const text = input.value.trim();
  if (!text) return;

  sendSignal({
    type: 'chat',
    text: text
  });

  input.value = '';
}

function renderChatMessage(sender, text, timestamp, isMine) {
  const chatMessages = document.getElementById('chat-messages');
  const bubble = document.createElement('div');
  bubble.className = `chat-bubble ${isMine ? 'mine' : ''}`;

  const header = document.createElement('div');
  header.className = 'chat-bubble-header';

  const nameSpan = document.createElement('span');
  nameSpan.className = 'chat-sender';
  nameSpan.textContent = isMine ? 'You' : sender;

  const timeSpan = document.createElement('span');
  timeSpan.className = 'chat-time';
  timeSpan.textContent = timestamp || '';

  header.appendChild(nameSpan);
  header.appendChild(timeSpan);

  const textDiv = document.createElement('div');
  textDiv.className = 'chat-text';
  textDiv.textContent = text;

  bubble.appendChild(header);
  bubble.appendChild(textDiv);

  chatMessages.appendChild(bubble);
  chatMessages.scrollTop = chatMessages.scrollHeight;

  // If chat sidebar is not open, increment unread counter
  if (!isSidebarOpen || currentTab !== 'chat') {
    unreadChatCount++;
    const badge = document.getElementById('chat-unread-badge');
    badge.textContent = unreadChatCount > 99 ? '99+' : unreadChatCount;
    badge.style.display = 'flex';
  }
}

// 8. Participants Roster and Counter
function updateParticipantsList() {
  const listEl = document.getElementById('participants-list');
  const countText = document.getElementById('participant-count-text');

  const remotePeerIds = Object.keys(peerNames);
  const totalCount = 1 + remotePeerIds.length;
  countText.textContent = totalCount === 1 ? '1 participant' : `${totalCount} participants`;

  let html = `
    <div class="participant-item">
      <div class="participant-info">
        <div class="participant-avatar-small">${(clientName || 'U').charAt(0).toUpperCase()}</div>
        <span class="participant-name">${escapeHtml(clientName)}</span>
      </div>
      <span class="participant-badge">You</span>
    </div>
  `;

  for (const peerId of remotePeerIds) {
    const name = peerNames[peerId] || 'Participant';
    html += `
      <div class="participant-item">
        <div class="participant-info">
          <div class="participant-avatar-small">${name.charAt(0).toUpperCase()}</div>
          <span class="participant-name">${escapeHtml(name)}</span>
        </div>
      </div>
    `;
  }

  listEl.innerHTML = html;
}

function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

// 9. Actions: Copy Link & Leave Call
function copyMeetingLink() {
  const url = window.location.origin + `/room/${roomId}`;
  navigator.clipboard.writeText(url).then(() => {
    showToast('Meeting link copied to clipboard!');
  }).catch(() => {
    prompt('Copy this link:', url);
  });
}

function showToast(text) {
  const toast = document.getElementById('room-toast');
  const toastText = document.getElementById('toast-text');
  toastText.textContent = text;
  toast.classList.add('show');
  setTimeout(() => {
    toast.classList.remove('show');
  }, 2500);
}

function leaveCall() {
  if (localStream) {
    localStream.getTracks().forEach(t => t.stop());
  }
  if (screenStream) {
    screenStream.getTracks().forEach(t => t.stop());
  }
  isLeaving = true;
  clearTimeout(reconnectTimer);
  for (const id of Object.keys(peerStates)) {
    peerStates[id].pc.close();
  }
  if (ws) {
    ws.close();
  }
  window.location.href = '/';
}
