# Flux Text-to-Speech — Frontend

Pure HTML/CSS/JS frontend for Deepgram **Flux streaming text-to-speech** (v2 Speak).

This is the shared frontend for the `*-flux-tts` starter backends (Node, Java, Flask, FastAPI, Django). It is framework-agnostic — it only speaks the backend's HTTP + WebSocket contract, so any of those backends can serve it. Built with [Vite](https://vitejs.dev/) and [`@deepgram/styles`](https://www.npmjs.com/package/@deepgram/styles).

## Backend contract

- `GET /api/session` → `{ "token": "<jwt>" }`
- `GET /api/metadata` → project metadata from `deepgram.toml` (title, description, repository)
- `WS /api/tts?model=<flux-voice>&encoding=linear16&sample_rate=24000` — authenticated with the `access_token.<jwt>` subprotocol. The client sends JSON control messages and receives binary `linear16` audio frames plus JSON control messages:

  ```jsonc
  // client → server
  { "type": "Speak", "text": "..." }
  { "type": "Flush" }
  { "type": "Close" }

  // server → client
  <binary linear16 audio frames>                 // played via Web Audio
  { "type": "Connected" | "SpeechStarted" | "SpeechMetadata" | "Flushed" | "SessionMetadata" | "Warning" | "Error", ... }
  ```

## Local Development

```bash
pnpm install
pnpm dev            # http://localhost:8080
```

`vite.config.js` proxies `/api` (HTTP + WS) to `http://localhost:8081` by default — start one of the `*-flux-tts` backends there. Override with `VITE_BACKEND_URL`.

As a submodule (how the starters consume it):

```bash
# inside a *-flux-tts starter
git submodule add https://github.com/deepgram-starters/flux-tts-html.git frontend
cd frontend && pnpm install && pnpm dev -- --port 8080
```

## Build

```bash
pnpm build          # outputs to dist/
```

## Notes

- Playback is `linear16` at **24 kHz** (`SAMPLE_RATE` in `main.js`), matching the `sample_rate` sent on connect. Change both together if you use a different rate.
- Flux voices use the format `flux-{voice}-{language}` (e.g. `flux-alexis-en`).

## License

MIT - See [LICENSE](./LICENSE)
