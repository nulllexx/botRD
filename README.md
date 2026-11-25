# botRD

A Discord forum gatekeeper written in Rust and built to use very little memory: it runs as one small native binary on a single thread and uses about 6 MB of RAM (the earlier Node version, still here as `bot.js`, used about 75 MB).

## Setup

1. Create an application at <https://discord.com/developers/applications>. Under **Bot**:
   - copy the token into `.env` (see `.env.example`)
   - turn on **Message Content Intent**, which the bot needs to read a new post's body. Without it, the bot exits with a 4014 error
2. Invite the bot. Replace `APP_ID` with your application ID:
   `https://discord.com/oauth2/authorize?client_id=APP_ID&scope=bot+applications.commands&permissions=292326280256`
   (View Channel, Send Messages, Send Messages in Threads, Add Reactions, Read Message History, Manage Roles, Manage Threads)
3. Build it with `cargo build --release` (needs [Rust](https://rustup.rs)), then run `./target/release/botrd` from the folder that holds `.env`.
   The binary is self-contained, so you can copy it to a server that doesn't have Rust installed.

Only members with **Manage Server** can see or use the commands. Server admins can change that under Server Settings → Integrations.

## Commands

| Command | What it does |
|---|---|
| `/setup forum role` | Sets the forum channel to manage and the default members role |
| `/open phase:1\|2` | Lets the role create posts and reply, and turns on that phase's blocklist |
| `/close` | Makes the forum read-only for the role. No blocklist checks run while it's closed |
| `/status` | Shows the current state and both blocklists |
| `/blocklist add\|remove phase type value` | `type` is `word` or `tag`. You can add several words at once, separated by commas. Tags autocomplete |

## How moderation works

- Only **new posts** are checked. Replies inside a post are never checked.
- A post is deleted right away if it has a blocked tag, or if its **title or body** contains a blocked word. Word matching ignores case and only matches whole words, so `ass` won't match `class`. Phrases match across any separator, so `free nitro` catches `free-nitro`. Entries with emoji or symbols, like `🍆` or `c++`, match anywhere in the text.
- The author gets a DM saying why, along with a copy of their text so they can repost. If their DMs are closed, the failure is only logged to the console.
- `/open` and `/close` only change View, Read History, Create Posts, Send in Posts and Reactions on the members role's permission overwrite. Everything else on it stays as it was.
  - Make sure `@everyone` (and any other role) doesn't grant Send in the forum, or members can still post through that role.

Settings are stored in `data.json` in the folder you run the bot from. Set `DATA_FILE` to keep it somewhere else. The file format is the same as the Node version's, so an existing `data.json` carries over.

## Performance notes

- **Tags and title** are checked as soon as Discord reports the new post, without contacting Discord again. Posts caught this way are deleted in a single request.
- **The post body** comes in over the bot's live connection to Discord. That's why it subscribes to guild messages: it only looks closely at the first message of a new post and drops everything else right away. If the body hasn't arrived within 1 second, the bot fetches it from Discord instead.
- **Requests to Discord** go over connections that stay open. While a forum is open, the bot sends a small request every 30 seconds so a deletion never has to open a new connection.
- **Blocklists** are compiled once and cached. Checking a post takes about the same time however many words are blocked.
- **Commands** are only re-registered with Discord when their definitions change.
- **Memory** stays low because each event from Discord is read straight into the handful of fields the bot uses. Ordinary chat messages are skipped without copying their text.

## Deploy

`.github/workflows/deploy.yml` runs the tests on every push. On `main` it also builds the Docker image, pushes it to GHCR, and runs it on the server as its own compose project in `/home/useradmin/botrd/` (see `deploy/docker-compose.yml`). Settings live in `/home/useradmin/botrd/data/data.json`. The deploy fails, and prints the bot's logs, if the bot hasn't logged in to Discord within 30 seconds.

It needs these repository secrets: `REMOTE_HOST`, `REMOTE_USER`, `REMOTE_SSH_KEY` (the same ones the other RainDrippy deploys use) and `DISCORD_TOKEN`.

## Tests

`cargo test` runs the blocklist and settings-file tests. `cargo test -- --ignored` also checks the connection handling against Discord's public API (no token needed).
