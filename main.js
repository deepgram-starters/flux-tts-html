/**
 * Flux Text-to-Speech - Frontend Application
 *
 * Streaming WebSocket-based TTS against a backend bridge to Deepgram Flux
 * (v2 Speak, /api/tts). Sends { type: "Speak" } / { type: "Flush" } control
 * messages and plays the binary linear16 audio frames as they stream back.
 */

// ============================================================================
// SESSION MANAGEMENT
// ============================================================================

const SESSION_ENDPOINT = 'api/session';
let sessionToken = null;

async function getSessionToken() {
  if (sessionToken) return sessionToken;
  const response = await fetch(SESSION_ENDPOINT);
  if (!response.ok) throw new Error(`Session failed: ${response.status}`);
  const data = await response.json();
  sessionToken = data.token;
  return sessionToken;
}

// Flux streams raw linear16; keep the playback rate in sync with the connection
// query (sample_rate) so audio plays at the correct pitch.
const SAMPLE_RATE = 24000;
const ENCODING = 'linear16';
// Seconds of audio to pre-buffer before starting playback. Staging delivers
// audio slower than real-time (~0.68x), so a cushion lets the rest stream in
// behind smooth playback. Larger = smoother for long text but more latency.
const PREBUFFER_SEC = 2.5;

// Application state
let ws = null;
let audioContext = null;
let nextStartTime = 0; // continuous playback clock for scheduled clips (audioContext time)
let currentSources = []; // clips currently scheduled/playing
let streaming = false; // true once we've started playback for this turn
let pcmCarry = null; // trailing odd byte carried to the next frame (16-bit alignment)
let pcmChunks = []; // pre-buffer: Float32 audio accumulated before playback starts
let bufferedSamples = 0; // samples in the pre-buffer
let playIdleTimer = null; // fallback: start playback if audio stops arriving
let sessionStartTime = null;
let durationInterval = null;

// Stats
let chunksReceived = 0;
let bytesReceived = 0;

// DOM elements
let connectBtn, sendBtn, disconnectBtn;
let modelInput, textInput;
let connectionStatus, playbackStatus, currentModel;
let chunksReceivedEl, bytesReceivedEl, buffersQueuedEl, sessionDurationEl;
let playbackStateText, emptyState, playbackSection, transcriptContainer;
let connectOverlay, disconnectContainer;

// Metadata elements
let pageTitle, pageDescription, headerTitle, repoLink;

document.addEventListener('DOMContentLoaded', () => {
  console.log('Initializing Flux TTS application...');

  connectBtn = document.getElementById('connect-btn');
  sendBtn = document.getElementById('send-btn');
  disconnectBtn = document.getElementById('disconnect-btn');
  modelInput = document.getElementById('model-input');
  textInput = document.getElementById('text-input');

  connectionStatus = document.getElementById('connection-status');
  playbackStatus = document.getElementById('playback-status');
  currentModel = document.getElementById('current-model');
  chunksReceivedEl = document.getElementById('chunks-received');
  bytesReceivedEl = document.getElementById('bytes-received');
  buffersQueuedEl = document.getElementById('buffers-queued');
  sessionDurationEl = document.getElementById('session-duration');
  playbackStateText = document.getElementById('playback-state-text');

  emptyState = document.getElementById('empty-state');
  playbackSection = document.getElementById('playback-section');
  transcriptContainer = document.getElementById('transcript-container');
  connectOverlay = document.getElementById('connect-overlay');
  disconnectContainer = document.getElementById('disconnect-container');

  pageTitle = document.getElementById('pageTitle');
  pageDescription = document.getElementById('pageDescription');
  headerTitle = document.getElementById('headerTitle');
  repoLink = document.getElementById('repoLink');

  connectBtn.addEventListener('click', handleConnect);
  sendBtn.addEventListener('click', handleSend);
  disconnectBtn.addEventListener('click', handleDisconnect);

  loadMetadata();
  console.log('Application initialized');
});

function initAudioContext() {
  if (!audioContext) {
    const Ctor = window.AudioContext || window.webkitAudioContext;
    // Run the context at the audio's native rate so 24 kHz buffers play without
    // per-buffer resampling (which crackles at buffer boundaries). Fall back to
    // the default rate if the browser rejects an explicit rate.
    try {
      audioContext = new Ctor({ sampleRate: SAMPLE_RATE });
    } catch (e) {
      audioContext = new Ctor();
    }
    console.log('Audio context initialized at', audioContext.sampleRate, 'Hz');
  }
}

async function loadMetadata() {
  try {
    const response = await fetch('api/metadata');
    if (!response.ok) return;
    const metadata = await response.json();
    if (metadata.title && pageTitle) pageTitle.textContent = metadata.title;
    if (metadata.description && pageDescription) pageDescription.setAttribute('content', metadata.description);
    if (metadata.title && headerTitle) headerTitle.textContent = metadata.title;
    if (metadata.repository && repoLink) repoLink.href = metadata.repository;
    console.log('Metadata loaded:', metadata);
  } catch (error) {
    console.warn('Error loading metadata, using defaults:', error);
  }
}

async function handleConnect() {
  const model = (modelInput.value || 'flux-alexis-en').trim();
  const wsUrl = new URL(
    `api/tts?model=${encodeURIComponent(model)}&encoding=${ENCODING}&sample_rate=${SAMPLE_RATE}`,
    document.baseURI
  );
  wsUrl.protocol = wsUrl.protocol === 'https:' ? 'wss:' : 'ws:';

  console.log('Connecting to:', wsUrl.href);

  const token = await getSessionToken();

  // JWT auth via subprotocol: access_token.<jwt>
  ws = new WebSocket(wsUrl.href, [`access_token.${token}`]);

  ws.addEventListener('open', () => {
    console.log('✓ WebSocket connected');
    updateConnectionStatus('connected', 'Connected');

    connectOverlay.classList.add('hidden');
    disconnectContainer.classList.remove('hidden');

    sendBtn.disabled = false;
    currentModel.textContent = model;

    sessionStartTime = Date.now();
    durationInterval = setInterval(updateSessionDuration, 1000);

    resetStats();
    handleStopAudio();
  });

  ws.addEventListener('message', async (event) => {
    if (typeof event.data === 'string') {
      const msg = JSON.parse(event.data);
      // Backends forward control messages with a top-level `type`; some also
      // nest the payload under `data` — read either.
      const detail = msg.data && typeof msg.data === 'object' ? msg.data : msg;
      console.log('←', msg.type, detail);

      switch (msg.type) {
        case 'Connected':
          updateConnectionStatus('connected', 'Connected');
          break;
        case 'SpeechStarted':
          updatePlaybackState('Buffering');
          break;
        case 'Warning':
          console.warn('Warning from server:', detail);
          break;
        case 'Error':
          updateConnectionStatus('error', 'Error');
          console.error('Error from server:', detail);
          break;
        case 'SpeechMetadata':
          // End-of-turn marker. If a short turn finished before the pre-buffer
          // filled, start playing what we have now.
          startStreaming();
          break;
        case 'Flushed':
        case 'SessionMetadata':
        default:
          break;
      }
    } else {
      // Binary audio frame (linear16). Staging delivers audio slower than
      // real-time (~0.68x), so we accumulate the whole turn and play it as one
      // gapless clip rather than trying to stream-schedule (which underruns).
      const arrayBuffer = await event.data.arrayBuffer();
      const audioData = new Uint8Array(arrayBuffer);

      chunksReceived++;
      bytesReceived += arrayBuffer.byteLength;

      enqueuePcm(audioData);
      updateStats();
    }
  });

  ws.addEventListener('close', (event) => {
    console.log(`WebSocket closed: ${event.code} ${event.reason || '(no reason)'}`);

    if (event.code === 4401) {
      sessionToken = null;
      updateConnectionStatus('error', 'Session Expired');
      handleStopAudio();
      sendBtn.disabled = true;
      alert('Session expired, please refresh the page.');
      return;
    }

    updateConnectionStatus('disconnected', 'Disconnected');
    connectOverlay.classList.remove('hidden');
    disconnectContainer.classList.add('hidden');
    sendBtn.disabled = true;
    handleStopAudio();

    if (durationInterval) {
      clearInterval(durationInterval);
      durationInterval = null;
    }
  });

  ws.addEventListener('error', (error) => {
    console.error('WebSocket error:', error);
    updateConnectionStatus('error', 'Error');
  });
}

function handleSend() {
  const text = textInput.value.trim();
  if (!text) {
    console.error('No text to send');
    return;
  }

  handleStopAudio();
  resetStats();

  emptyState.classList.add('hidden');
  playbackSection.classList.remove('hidden');

  addTranscriptItem(text);

  console.log(`→ Speak: "${text.substring(0, 50)}${text.length > 50 ? '...' : ''}"`);
  ws.send(JSON.stringify({ type: 'Speak', text }));

  // Flush shortly after to finish the turn and flush audio
  setTimeout(() => {
    console.log('→ Flush');
    ws.send(JSON.stringify({ type: 'Flush' }));
  }, 100);
}

function handleStopAudio() {
  currentSources.forEach((source) => {
    try { source.stop(); } catch (e) { /* already stopped */ }
  });
  currentSources = [];
  nextStartTime = 0;
  streaming = false;
  pcmCarry = null;
  pcmChunks = [];
  bufferedSamples = 0;
  clearTimeout(playIdleTimer);
  updatePlaybackState('Idle');
  updateStats();
}

function handleDisconnect() {
  if (ws) {
    handleStopAudio();
    // Politely end the session server-side before closing.
    try { ws.send(JSON.stringify({ type: 'Close' })); } catch (e) { /* not open */ }
    ws.close();
  }
}

// Decode one linear16 frame to Float32, keeping 16-bit alignment across frames.
// Without the carry, a single odd-length frame shifts every subsequent sample by
// one byte and turns the rest of the stream into noise.
function decodePcm(audioData) {
  let bytes = audioData;
  if (pcmCarry) {
    const merged = new Uint8Array(pcmCarry.length + bytes.length);
    merged.set(pcmCarry, 0);
    merged.set(bytes, pcmCarry.length);
    bytes = merged;
    pcmCarry = null;
  }
  if (bytes.length % 2 === 1) {
    pcmCarry = bytes.slice(bytes.length - 1);
    bytes = bytes.subarray(0, bytes.length - 1);
  }
  const sampleCount = bytes.length >> 1;
  if (sampleCount === 0) return null;

  // Signed 16-bit little-endian PCM (Deepgram linear16).
  const view = new DataView(bytes.buffer, bytes.byteOffset, sampleCount * 2);
  const out = new Float32Array(sampleCount);
  for (let i = 0; i < sampleCount; i++) {
    out[i] = view.getInt16(i * 2, true) / 32768;
  }
  return out;
}

// Hybrid playback: pre-buffer PREBUFFER_SEC of audio, then start playing and
// stream the rest onto a continuous timeline. Low latency + smooth for typical
// sentences (which are usually fully buffered before the cushion fills).
function enqueuePcm(audioData) {
  initAudioContext();
  try {
    const f32 = decodePcm(audioData);
    if (!f32) return;

    if (streaming) {
      // Already playing — schedule this frame right after what's queued.
      scheduleClip(f32);
    } else {
      pcmChunks.push(f32);
      bufferedSamples += f32.length;
      updatePlaybackState('Buffering');
      if (bufferedSamples / SAMPLE_RATE >= PREBUFFER_SEC) startStreaming();
    }

    // Fallback: if audio stops arriving before the cushion fills (short turn),
    // start playing anyway. Inter-frame gaps are ~120ms, so 600ms == turn ended.
    clearTimeout(playIdleTimer);
    playIdleTimer = setTimeout(startStreaming, 600);
    updateStats();
  } catch (error) {
    console.error('Error handling audio:', error);
  }
}

// Begin playback: flush the pre-buffer as one clip, then switch to streaming mode
// so later frames are scheduled directly.
function startStreaming() {
  clearTimeout(playIdleTimer);
  if (streaming) return;
  streaming = true;
  if (bufferedSamples > 0) {
    const merged = new Float32Array(bufferedSamples);
    let offset = 0;
    for (const chunk of pcmChunks) {
      merged.set(chunk, offset);
      offset += chunk.length;
    }
    pcmChunks = [];
    bufferedSamples = 0;
    scheduleClip(merged);
  }
}

// Schedule a Float32 clip onto the continuous timeline. If we've fallen behind
// the audio clock (underrun near the end of long text), re-anchor slightly ahead
// rather than scheduling in the past.
function scheduleClip(float32Array) {
  initAudioContext();
  if (audioContext.state === 'suspended') audioContext.resume();

  const audioBuffer = audioContext.createBuffer(1, float32Array.length, SAMPLE_RATE);
  audioBuffer.getChannelData(0).set(float32Array);

  const now = audioContext.currentTime;
  if (nextStartTime < now + 0.02) nextStartTime = now + 0.05;

  const source = audioContext.createBufferSource();
  source.buffer = audioBuffer;
  source.connect(audioContext.destination);
  source.start(nextStartTime);
  nextStartTime += audioBuffer.duration;
  currentSources.push(source);
  updatePlaybackState('Playing');

  source.onended = () => {
    const i = currentSources.indexOf(source);
    if (i > -1) currentSources.splice(i, 1);
    if (currentSources.length === 0) updatePlaybackState('Idle');
    updateStats();
  };
}

function addTranscriptItem(text) {
  const timestamp = new Date().toLocaleTimeString();
  const item = document.createElement('div');
  item.className = 'transcript-item';

  const timestampDiv = document.createElement('div');
  timestampDiv.className = 'transcript-item__timestamp';
  timestampDiv.textContent = timestamp;

  const textDiv = document.createElement('div');
  textDiv.className = 'transcript-item__text';
  textDiv.textContent = text;

  item.appendChild(timestampDiv);
  item.appendChild(textDiv);
  transcriptContainer.appendChild(item);
  transcriptContainer.scrollTop = transcriptContainer.scrollHeight;
}

function updateConnectionStatus(status, text) {
  connectionStatus.className = `status-badge status-badge--${status}`;
  connectionStatus.innerHTML = '';
  const indicator = document.createElement('span');
  indicator.className = `status-indicator status-indicator--${status}`;
  connectionStatus.appendChild(indicator);
  connectionStatus.appendChild(document.createTextNode(text));
}

function updatePlaybackState(state) {
  playbackStatus.textContent = state;
  playbackStateText.textContent = state;
}

function updateStats() {
  chunksReceivedEl.textContent = chunksReceived;
  bytesReceivedEl.textContent = bytesReceived.toLocaleString();
  // Seconds of audio buffered but not yet played (playback state is owned by
  // updatePlaybackState, so don't overwrite it here).
  buffersQueuedEl.textContent = (bufferedSamples / SAMPLE_RATE).toFixed(1) + 's';
}

function resetStats() {
  chunksReceived = 0;
  bytesReceived = 0;
  updateStats();
}

function updateSessionDuration() {
  if (!sessionStartTime) return;
  const elapsed = Math.floor((Date.now() - sessionStartTime) / 1000);
  const minutes = Math.floor(elapsed / 60);
  const seconds = elapsed % 60;
  sessionDurationEl.textContent = `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
}
