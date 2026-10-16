# WebDAV sync

WebDAV keeps Papr libraries in step across computers. Open **Settings → Sync → WebDAV**, enter a directory URL, username and password (or app password), and connect. Use the same directory on every device. The directory's parent must exist; Papr can create the final directory. The account needs read, create, update and delete permission.

Choose **Sync now** for the first merge. While the desktop app is running, Papr also syncs on its background refresh cycle, approximately every minute, even when no feeds are due. The CLI uses the same database and sync implementation:

```sh
papr sync connect --provider webdav --url https://example.com/dav/papr/ --user USER --password APP_PASSWORD
papr sync run
papr sync status
papr sync disconnect --yes
```

Connecting tests access with a temporary file, without immediately transferring library data. Disconnecting removes this device's WebDAV credentials; it leaves the remote library in place. Use one sync service at a time; disconnect FreshRSS/Miniflux before connecting WebDAV, or vice versa.

## Included data

- Nested folders and subscription metadata, including parent/child and feed folder assignment and per-feed reading/refresh preferences.
- Article content, extracted full text, enclosure URLs, nested tags and their article assignments, highlights and notes.
- Filter rules, read/unread, starred and read-later flags, and the reader's scroll position.
- Saved AI summary prompt, active AI provider/model and Codex reasoning effort.
- Deletions and retention tombstones, so an old device cannot simply restore removed articles or subscriptions.

Credentials, API keys, email newsletter sources and their articles, other local settings, custom AI endpoint URLs, ChatGPT/Codex authentication, AI/translation caches, search indexes, downloaded images and media are excluded. Article HTML is sanitized on import, and the local search index is updated. Reading position is a fraction of the reader's scroll range; the embedded original webpage and audio playback positions are not tracked.

## Storage and conflicts

Papr stores a versioned JSON library as `papr-library-v1.json` in the chosen directory. The current payload is version 5, including AI preferences and scoped tag paths; versions 1–4 are still readable. Update all devices before syncing. Older versions fail safely on newer snapshots. It merges records by feed URL and article GUID, resolving references to each device's own database IDs. Existing local articles are not replaced by another SQLite file. Read, star, read-later and scroll-position edits have independent revisions. One-sided changes propagate, including read-to-unread changes, removed stars and moving the reading position backwards. Independent fields merge automatically. Changes to the same record on both devices are compared against this device’s last observed server snapshot; conflicting state or hand-edited metadata pauses sync before import/upload. Both versions are stored locally and shown under **Settings → Sync → WebDAV**. Choose **Keep this device**, **Keep WebDAV** or **Keep newer revision**, then sync again. A choice receives a new revision, so an older snapshot cannot undo it. If the local item changes while a conflict is displayed, the stale choice is rejected; sync again to refresh it. Folder/tag/rule/highlight metadata uses record-level revisions. AI provider, active model and reasoning effort share one coherent revision; the summary prompt has its own revision, so independent prompt and model edits merge automatically. Conflicting AI settings appear in the same review list. Restoring defaults is also synced. API keys, ChatGPT sign-in and custom endpoint profiles stay on each device; sign in separately on each computer. Synced AI settings apply to future summary, article-chat and AI-translation requests; existing cached summaries remain unchanged. An open prompt editor preserves unsaved drafts when remote changes arrive, and an unsaved model/endpoint draft offers an explicit reload action. Concurrent moves that would create a hierarchy cycle are detected before applying remote data, with a **Move to top level** resolution. Derived article-fetch metadata reconciles automatically using revisions, rather than prompting for every feed refresh. Keep computers' clocks reasonably accurate for edits made offline.

Deletion records are retained for offline devices. Renames use the old natural key's deletion and a new record. Exact duplicate rules or highlights with identical anchors are coalesced. Libraries are uploaded as complete snapshots when changes exist; unchanged snapshots are not uploaded. The current transfer limit is **128 MiB** per snapshot, including retained deletion records. This format is designed for modest personal libraries rather than very large archives.

The server must implement conditional PUT (`If-Match` / `If-None-Match`) and return a **strong ETag** for the library file. Papr retries a concurrent write up to three times after downloading and merging the newer version. If the server lacks a strong ETag, the sync fails without an unconditional overwrite. This follows [WebDAV's ETag requirements](https://www.rfc-editor.org/rfc/rfc4918#section-8.6).

Malformed or unsupported snapshots fail validation, and a failed database merge rolls back atomically. If a download merges successfully but its upload fails, those remote changes remain locally and the next sync retries the transfer. The baseline tracks successfully observed downloads as well as completed uploads, so retries distinguish imported state from local edits. Settings show the last successful sync, pending conflicts and a failure indicator; choose **Sync now** to retry and see the error.

The remote JSON contains readable article content and notes. WebDAV credentials are stored only in the local settings database, as with Papr's other connection settings. Use a private WebDAV directory and HTTPS for remote servers.

## Creating tag paths

Use `/` for hierarchy and `;` or `；` between multiple tags. For example, `agent/code;reading/to-sort` creates two paths and, in the article picker, attaches only `code` and `to-sort`. Existing ancestors are reused. Choosing a parent makes paths relative to that tag. The whole creation batch rolls back on an invalid path or conflicting name. Shared prefixes under the same parent are reused. Identical names under different parents are independent tags, and a parent’s article count is the distinct union of its own and all descendants’ article assignments. Hierarchy and leaf article assignments are included in WebDAV sync.
