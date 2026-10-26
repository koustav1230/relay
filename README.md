# Void Survivors relay

Tiny WebSocket server that lets friends find each other by 4-letter room code for the itch.io web build.
The game itself runs in the host's browser; this server only forwards messages (no game logic, no accounts).

## Deploy on Render (free)
1. Put this folder in a GitHub repo (its own repo, or a subfolder).
2. render.com -> New -> Web Service -> pick the repo (set Root Directory if it's a subfolder).
3. Runtime: Node. Build command: `npm install`. Start command: `npm start`. Instance type: Free.
4. Environment variable `ALLOWED_ORIGINS` = comma list of allowed sites, e.g. `https://yourname.itch.io,https://html-classic.itch.zone`
   (leave empty while testing; set it before sharing widely so other sites can't use your relay).
5. Deploy. Open `https://YOUR-SERVICE.onrender.com/health` - you should see `{"ok":true,...}`.
6. Your relay URL is `wss://YOUR-SERVICE.onrender.com/ws`.

## Build the game with it
    VS_RELAY=wss://YOUR-SERVICE.onrender.com/ws node tools/build-web.js
or just edit `relay.js` inside the built folder / zip (one line: `window.VS_RELAY='wss://...'`) and re-zip.

## Notes
- Free tier sleeps after 15 min idle; the first player waits up to ~1 min. The game pre-warms it when the menu loads.
- Limits (env or defaults): 400 rooms, 10 players/room, 12 connections/IP, 12 room creations/min/IP.
- Players on the same room need the same game version.
