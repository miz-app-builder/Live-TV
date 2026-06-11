# MIZ Live TV — Feature Task List

## ✅ Completed
- [x] GitHub repo clone & project run
- [x] PiP (Picture-in-Picture) fix — tab switch bug fixed
- [x] Channel category filter tabs (Bangla, News, Movies, Music, Kids, Sports, International)
- [x] App name changed to "MIZ Live TV"
- [x] Custom TV logo added
- [x] "Open in Browser" button removed

---

## 📋 Pending Features

### T001 — ❤️ Favorites
- Channel card-এ heart icon যোগ করা
- Click করলে "My Favourites" category-তে save হবে
- Browser local storage-এ থাকবে (reload করলেও থাকবে)
- Category tab-এ ❤️ Favourites tab যোগ হবে

### T002 — 🕐 Recently Watched
- Channel দেখলে automatically track হবে
- শেষ ১০টা channel "Recently Watched" section-এ দেখাবে
- Local storage-এ save থাকবে

### T003 — 🔢 Category Tab-এ Channel Count
- প্রতিটা tab-এ channel সংখ্যা দেখাবে
- যেমন: 🇧🇩 Bangla (87) · 📰 News (120) · 🎬 Movies (95)

### T004 — 🏷️ HD Badge
- Channel name-এ "(HD)" বা "(1080p)" থাকলে card-এ ছোট HD badge দেখাবে
- Visual indicator হিসেবে card-এর corner-এ থাকবে

### T005 — 🔗 Channel Share Link
- Watch page-এ Share button যোগ
- Click করলে channel link clipboard-এ copy হবে
- "Link copied!" toast notification দেখাবে

### T006 — ⌨️ Keyboard Shortcuts
- Space = Play / Pause
- M = Mute / Unmute
- F = Fullscreen toggle
- Arrow Up / Down = Volume control
- Shortcut hints player-এ দেখাবে

### T007 — 🔴 Channel Online/Offline Status
- Channel card-এ ছোট dot indicator
- Online = সবুজ dot, Offline = লাল dot
- Background-এ stream check করবে

### T008 — 📺 Last Watched Resume
- App খুললেই আগে যেটা দেখছিলে সেটা auto-load হবে
- "Resume watching: [Channel Name]" prompt দেখাবে
- Local storage-এ last channel ID save থাকবে
