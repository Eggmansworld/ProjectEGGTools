# Project EGG Tools — by Eggman

A GUI toolkit for preserving [Project EGG](https://www.amusement-center.com) retro PC game downloads.

**GitHub:** https://github.com/Eggmansworld

---

## What Is Project EGG?

Project EGG is a Japanese digital distribution service run by D4 Enterprise that sells classic retro PC games for systems including PC-8801, PC-9801, MSX, MSX2, X1, FM-7, Mega Drive, PC Engine, and others. Games are sold as `.bin` container files hosted on the service's CDN and can be purchased and downloaded through a free registered account.

This toolkit automates the full preservation workflow: fetching the raw files, packaging them into named archives, extracting their contents into usable game folders, maintaining a romanization CSV, and generating No-Intro-compatible DAT files.

---

## Requirements

- **Python 3.10 or newer** — https://www.python.org
- **requests** and **pytz** — required for the Download tab only:
  ```
  pip install requests pytz
  ```
- **QuickBMS** — required for the Extract tab only. Download from:
  http://aluigi.altervista.org/papers.htm#quickbms
- **project_egg_extract_bins_working.bms** — the QuickBMS script for the Project EGG `.bin` format. Included in this repository.
- **7-Zip ZS** *(optional)* — required only if your ZIP archives use ZStandard compression (method 93). Standard deflate/store ZIPs do not need it. Download from: https://www.tc4shell.com/en/7zip/modern7z/
- A **Project EGG account** — free registration at https://www.amusement-center.com — required for downloading. No paid membership is needed.

---

## Running the Tool

```
python Eggmans_Project_EGG_Tools.py
```

The application opens as a tabbed window with six tabs: **Download**, **Package**, **Extract**, **Romanize**, **DAT**, and **About**.

---

## Configuration

All field values are saved automatically to `Eggmans_Project_EGG_Tools_config.json` in the same folder as the script whenever the application is closed. They are restored on the next launch, so you do not need to re-enter paths between sessions.

- **Save Config** button (toolbar) — saves immediately on demand at any time.
- **Clear Tab** button (toolbar) — resets all fields on the currently active tab to their defaults.
- Passwords are intentionally excluded from the config file for security.

---

## Tab: Download

Downloads the raw `.bin` game files directly from the Project EGG CDN.

### What it does

1. Logs in to the Project EGG API using your credentials and fetches the complete list of available content as a metadata JSON file (`data_YYYYMMDDHHMMSS.json`).
2. Builds a list of every `.bin` file referenced in that metadata.
3. Downloads each file to your specified download folder, skipping files that are already present and up to date (checked via HTTP `Last-Modified` header).
4. If an existing file has been updated on the server, the old copy is moved to a dated subfolder before the new version is downloaded.
5. Saves HTTP response headers alongside each `.bin` as `<filename>_headers.txt`. These serve as timestamped proof of download and are not required for any other operation.

### Fields

| Field | Description |
|---|---|
| Username | Your Project EGG account username |
| Password | Your Project EGG account password (not saved to config) |
| Download folder | Where the `.bin` files and headers will be saved |
| Use existing JSON | Tick this to skip server login and reuse a previously downloaded metadata JSON. Credentials are not required in this mode, but the metadata may be out of date. |

### Notes

- A full download of all available content takes **upwards of 10 hours**. The tool deliberately downloads slowly to avoid overloading the server. This is by design. Slow is steady, steady is fast.
- You can stop at any time with the Cancel button. Re-running with the same download folder and JSON will resume from where you left off, skipping already-downloaded files.
- As of 2026-04-02 there are **2,654** files available.
- It is recommended to use a VPN.
- The metadata JSON is saved as UTF-8-BOM so Japanese characters display correctly in Windows applications.

---

## Tab: Package

Groups downloaded `.bin` files into named ZIP archives using store compression (no recompression — the `.bin` files are already compressed internally where applicable).

### What it does

1. Reads the metadata JSON to determine which `.bin` files belong to each game entry (game file `a`, manual `m`, and music `d`).
2. For each entry that has at least one `.bin` file present in the download folder, creates a ZIP archive in the output folder.
3. The ZIP filename is constructed from the game's metadata:

   ```
   Title (Year) (Region) (Platform) [ProductId] [bin].zip
   ```

   Example:
   ```
   ロストパワー (1986) (Japan) (PC-8801) [24] [bin].zip
   ```

   The `[bin]` tag distinguishes these archives from extracted-content ZIPs if both sets are stored in the same location.

4. If some but not all expected files are present, the archive is created from what is available and tagged `[incomplete]` in the filename.
5. If no files for an entry are on disk, the entry is recorded in the end-of-log missing list.
6. Already-packaged ZIPs are skipped on re-runs — safe to run incrementally as new downloads arrive.

### Fields

| Field | Description |
|---|---|
| Download folder | The folder containing your downloaded `.bin` files (read only — never modified) |
| Metadata JSON | Path to the `data_*.json` file produced during download |
| Output folder | Where the ZIP archives will be written — must be different from the download folder |

### End-of-run log sections

- **No metadata** — JSON entries that list no filenames at all (shows productId and title)
- **Missing from disk** — entries whose files are not in the download folder, with the would-be ZIP name
- **Unprocessed .bin files** — `.bin` files on disk that were not matched to any JSON entry

### Special year handling

Some entries in the metadata have a year value of `複数` (meaning "multiple") or contain a `/`. These are written as `multi-year` in the archive filename.

---

## Tab: Extract

Extracts the contents of `.bin` files into named game folders using QuickBMS.

### What it does

1. Reads the metadata JSON to group each game's `.bin` files.
2. For each game, creates a named subfolder in the output folder:

   ```
   Title (Year) (Region) (Platform) [ProductId] [ext]
   ```

   The `[ext]` tag distinguishes these folders from Package ZIPs when both sets are stored together.

3. Runs QuickBMS on each `.bin` file, extracting its contents into that game folder. All files from multiple `.bin` parts (game, manual, music) go into the same folder, preserving the internal subdirectory structure from the archive.
4. After extraction, automatically renames all mojibake filenames to their correct Japanese names (see below).
5. Skips game folders that already exist and contain files — safe to re-run.

### Fields

| Field | Description |
|---|---|
| QuickBMS executable | Path to `quickbms.exe` |
| BMS script | Path to `project_egg_extract_bins_working.bms` |
| Download folder | The folder containing your downloaded `.bin` files (read only) |
| Metadata JSON | Path to the `data_*.json` file produced during download |
| Output folder | Where the extracted game folders will be written |

### Preview button

The **Preview .bin filenames…** button lets you select one or more `.bin` files and see the correct Japanese filenames stored inside their headers without extracting anything. Useful for verifying expected content before a full run, and for identifying the correct target name of a mojibake file.

### Mojibake filename correction

Project EGG `.bin` files store filenames in Shift-JIS (Japanese encoding). On a Western Windows system, QuickBMS passes those raw bytes to the Windows file system via the cp1252 ANSI codepage. This produces garbled (mojibake) filenames like:

```
ƒGƒŒƒx[ƒ^[ƒAƒNƒVƒ‡ƒ".exe   →   エレベーターアクション.exe
```

The tool corrects these automatically after each extraction:

1. Parses the correct filenames and exact decompressed sizes from the `.bin` headers and from QuickBMS stdout output.
2. Matches each mojibake disk file to its correct name using `(file extension, exact file size)` — these properties survive encoding damage intact.
3. As a tiebreaker for files with the same extension and size, the ASCII prefix of the filename stem (e.g. `5_` from `5_プリンセス・アン.wav`) is used, since ASCII characters are unaffected by the encoding mismatch.
4. Moves each file to its correct name and location. Some Shift-JIS characters have `0x5C` (backslash) as their second byte, which Windows interprets as a path separator, accidentally splitting filenames across new subdirectories. The tool detects and corrects this, removing any empty accidental directories afterward.

### Known non-correctable files

A small number of files cannot be automatically matched because two or more files in the same archive share an identical extension and file size with no ASCII prefix to distinguish them. These appear in the log as:

```
[INFO] Cannot uniquely match 'filename' — skipped
```

Use the Preview button alongside the log to identify the correct target names, then rename manually. The following are the known affected games:

| ProductId | Title | Files requiring manual rename |
|---|---|---|
| 639 | エレベータアクション＆ジャイロダイン／タイトーＭＳＸコレクション１ | `PTIT0003\エレベーターアクション.exe`, `PTIT0003\ジャイロダイン.exe` |
| 640 | スクランブルフォーメーション＆フロントライン／タイトーＭＳＸコレクション２ | `PTIT0004\スクランブルフォーメーション.exe`, `PTIT0004\フロントライン.exe` |
| 643 | ラスタンサーガ＆雀フレンド／タイトーＭＳＸコレクション３ | `PTIT0005\ラスタンサーガ.exe`, `PTIT0005\雀フレンド.exe` |

These three games each have exactly two `.exe` files with identical sizes. No automatic disambiguation is possible without further reverse engineering of the archive structure.

---

## Tab: Romanize

Maintains the romanization CSV that maps Japanese game titles to their English romanized equivalents. These romanized titles feed into the DAT generator.

### What it does

1. Loads the existing `romanizations.csv` (if one is provided).
2. Compares it against the metadata JSON and adds any entries not yet in the CSV, with the Japanese title in the `title` column and a blank `romanized` column ready to be filled in.
3. Sorts the full list by `productId` ascending.
4. Saves the result as a new timestamped file: `romanizations_YYYYMMDDHHMMSS.csv`

Entries without a romanization will use the Japanese title in all DAT output until a romanization is added and a new CSV is generated.

### Fields

| Field | Description |
|---|---|
| Metadata JSON | Path to the `data_*.json` file produced during download |
| Existing romanizations CSV | Path to your current `romanizations.csv` (optional — leave blank to build from scratch) |
| Output folder | Where the updated CSV will be saved (recommended: a `docs/` subfolder of your script folder) |

### Editing the CSV

The CSV uses UTF-8-BOM encoding. The safest way to edit it while preserving Japanese characters is to open it in **Google Sheets** (File → Import), fill in the `romanized` column, then download as CSV. Excel can mishandle the encoding on save.

---

## Tab: DAT

Generates No-Intro-compatible DAT files for use with RomVault or other DAT-based ROM management tools. Three DAT types can be generated independently or all at once in a single run.

### What it does

For each enabled DAT type, the generator:
1. Scans the relevant folder for ZIP files.
2. Hashes the contents of each ZIP (CRC32 + SHA1).
3. Builds `<game>` entries using the romanized title where available, falling back to the Japanese title.
4. Writes the `<description>` field using the original Japanese title.
5. Saves the DAT with a date-stamped filename.

### The three DAT types

#### Downloads (bin)
```
Project EGG Collection - Downloads (bin) (YYYY-MM-DD_RomVault).dat
```
Each raw `.bin` file is wrapped in its own individual ZIP (one `.bin` per ZIP), and the DAT hashes the `.bin` file inside. Game entries are named by the `.bin` stem (e.g. `AGL0001a`).

If the Downloads ZIPs folder does not yet contain ZIPs when you run the DAT generator, the tool will offer to create them automatically using store compression (no recompression of the original `.bin` data).

#### Games (bin)
```
Project EGG Collection - Games (bin) (YYYY-MM-DD_RomVault).dat
```
Reads the ZIPs produced by the **Package** tab. Each ZIP contains the `.bin` files for one game entry. The DAT hashes each `.bin` file inside the ZIP. Game entries are named using the full metadata title format with `[bin]` suffix.

#### Games (extracted)
```
Project EGG Collection - Games (extracted) (YYYY-MM-DD_RomVault).dat
```
Reads ZIPs containing the extracted game file contents. Each ZIP should contain the extracted folder for one game entry (created by zipping up the output of the **Extract** tab). Internal file paths are preserved with forward-slash separators. Game entries are named using the full metadata title format with `[ext]` suffix.

### Fields

| Field | Description |
|---|---|
| Metadata JSON | Path to the `data_*.json` file produced during download |
| Romanizations CSV | Path to the romanizations CSV (optional — Japanese titles used if omitted) |
| Output folder | Where the DAT files will be written |
| Author | Your name, written into the DAT header (default: Eggman) |
| 7-Zip ZS executable | Path to `7z.exe` from 7-Zip ZS — only needed if your ZIPs use ZStandard compression |
| Downloads (bin) checkbox | Enable/disable the Downloads (bin) DAT |
| Download folder | Folder containing the raw `.bin` files |
| Downloads ZIPs folder | Folder where per-file `.bin` ZIPs are stored or will be created |
| Games (bin) checkbox + folder | Enable/disable + folder containing Package tab ZIPs |
| Games (extracted) checkbox + folder | Enable/disable + folder containing extracted-content ZIPs |

### ZStandard compression

If your ZIP archives use ZStandard compression (method 93), Python's built-in ZIP handling cannot read them. The tool automatically falls back to 7-Zip ZS for those files if you have provided the path to its executable. ZIPs using standard deflate or store compression do not require 7-Zip.

---

## Log, Save Log, and Clear Log

All tabs share a single log window at the bottom of the application. The log scrolls automatically and persists until cleared.

- **Save Log** — saves the full log as a UTF-8-BOM `.txt` file with a timestamped name. The prefix reflects the active tab: `egg_download_log_...`, `egg_packager_log_...`, `egg_extract_log_...`, `egg_romanize_log_...`, or `egg_dat_log_...`
- **Clear Log** — clears the log window.

---

## Recommended Workflow

```
1. Download tab   →   fetch .bin files + metadata JSON to your download folder
2. Romanize tab   →   merge any new entries into romanizations.csv; fill in titles
3. Package tab    →   ZIP the .bin files into [bin]-tagged named archives
4. Extract tab    →   extract game contents into [ext]-tagged named folders
5. DAT tab        →   generate DAT files for RomVault / archival verification
```

The download folder is treated as read-only by all other tabs. Original `.bin` files are never modified or moved. All operations are safe to re-run — existing output is detected and skipped.

---

## File Naming Convention

All output filenames follow the same pattern:

```
Title (Year) (Region) (Platform) [ProductId] [type]
```

Where:
- **Title** — romanized title from the CSV if available; otherwise the Japanese title from the metadata
- **Year** — release year, or `multi-year` if the metadata lists multiple years (`複数` or contains `/`)
- **Region** — `Japan` for region 0, `World` for region 1
- **Platform** — system name; Japanese platform names are translated:
  - `アーケード` → `Arcade`
  - `メガドライブ` → `Mega Drive`
  - `PCエンジン` → `PC Engine`
  - `その他` → `Other`
- **[ProductId]** — the numeric product ID from the metadata
- **[bin]** — appended to Package tab ZIPs
- **[ext]** — appended to Extract tab output folders
- **[incomplete]** — appended when expected files are missing from disk

---

## Known Missing Files (404 on CDN)

These files no longer exist on the server and cannot be downloaded.

| Filename | ProductId | Title |
|---|---|---|
| BOT3008a.bin | 765 | T.N.T. |
| COM5032a.bin / COM5032m.bin | 1264, 1265 | Jino-Brodiaea's Wander Number; After Devil Force Gaiden |
| COM5033a.bin / COM5033m.bin | 1266 | Gensei Haiyuuki |
| COM5034a.bin / COM5034m.bin | 1267 | DEVIL FORCE III: Ken to Hanataba |
| COM5035a.bin / COM5035m.bin | 1268 | After Devil Force - Kyou-ou no Koukeisha |
| COM5036a.bin / COM5036m.bin | 1269 | Geo Conflict 3 - Hell's Gate Crusaders |
| COM5038a.bin / COM5038m.bin | 1270 | Daikaisen |
| COM5039a.bin / COM5039m.bin | 1271 | Quiz Tsunahiki Champ |
| COM5040a.bin / COM5040m.bin | 1272 | Poly Poly! Speed Daisakusen |
| HAM1010a.bin | 2095 | Fireball |
| SKP0011m.bin | 1044 | Ironclad |
| STW1003a.bin | 173 | Shiro to Kuro no Densetsu - Hyakki-hen (Ongaku Data nomi) |
| XTA0047a.bin / XTA0047m.bin | 2094 | Crimson II: The Revenge of the Evil God |

---

## Known Empty Entries

These metadata entries exist but have no downloadable files. They may be delisted or unreleased titles.

| Title | ProductId | Platform |
|---|---|---|
| 1chipMSXテストデータ_act (2004) | 509 | MSX |

---

## CDN Filename Anomalies

Two files have incorrect filenames in the metadata that differ from what the CDN actually serves. These are corrected automatically across all tabs.

| Metadata Filename | Actual CDN Filename |
|---|---|
| ECOM3005a.bin | COM3005a.bin |
| COM3008.bin | COM3008a.bin |

---

## Server Entry Count

For reference, the known count of available downloads at various points in time:

| Date | Count |
|---|---|
| 2023-07-28 | 2,456 |
| 2024-05-18 | 2,547 |
| 2025-08-02 | 2,631 |
| 2026-04-02 | 2,654 |

---

## Credits

This GUI was built by **Eggman**, extending and wrapping the original [deviled-eggs](https://github.com/Icyelut/deviled-eggs) command-line tools with a full graphical interface, automated download handling, ZIP packaging, QuickBMS-based extraction with automatic mojibake correction, romanization CSV management, and DAT file generation.

Original deviled-eggs project credits:

| Name | Contribution |
|---|---|
| **Eintei** | All original reverse engineering and format documentation |
| **obskyr** | Original data.json download script |
| **Bestest** | Coordination, leadership, reverse engineering |
| **Hiccup** | Datting support and advice |
| **proffrink** | Scraping, research |
| **Shadów** | Reverse engineering |
| **Icyelut** | Original script, romanization |
