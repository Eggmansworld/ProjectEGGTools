"""
package_gui.py - Project EGG Tools GUI
Part of the deviled-eggs toolset.

Combines the downloader (from main.py) and the packager into one GUI.
Select the operation using the tabs at the top.

Run with:
    python package_gui.py

Requires:
    pip install requests pytz
"""

import datetime
import json
import os
import pathlib
import queue
import shutil
import threading
import time
import tkinter as tk
from io import StringIO
from os import SEEK_END
from random import randint
from tkinter import filedialog, messagebox, scrolledtext, ttk
from urllib.parse import unquote_plus
import csv
import hashlib
import re
import struct
import sys
import webbrowser
import zipfile as _zipfile_mod
from zlib import crc32 as _zlib_crc32
import subprocess

try:
    import requests
    from requests.exceptions import HTTPError
    import pytz
    DOWNLOAD_AVAILABLE = True
except ImportError:
    DOWNLOAD_AVAILABLE = False


# ──────────────────────────────────────────────────────────────────────────────
# Shared constants
# ──────────────────────────────────────────────────────────────────────────────

# Config file sits next to the script, named <scriptname>_config.json
_SCRIPT_PATH = pathlib.Path(sys.argv[0]).resolve()
CONFIG_PATH  = _SCRIPT_PATH.parent / (_SCRIPT_PATH.stem + "_config.json")

# Variable names belonging to each tab (password excluded for security)
TAB_VARS = {
    "download": ["var_username", "var_dl_dest",
                 "var_use_existing_json", "var_existing_json"],
    "package":  ["var_pkg_download", "var_pkg_json", "var_pkg_output"],
    "extract":  ["var_qbms_exe", "var_bms_script", "var_ext_source",
                 "var_ext_json", "var_ext_output"],
    "romanize": ["var_rom_json", "var_rom_csv", "var_rom_out"],
    "dat":      ["var_dat_json", "var_dat_csv", "var_dat_out",
                 "var_dat_author", "var_dat_7z",
                 "var_dat_do_dl",  "var_dat_dl_folder", "var_dat_dl_zips_folder",
                 "var_dat_do_bin", "var_dat_bin_folder",
                 "var_dat_do_ext", "var_dat_ext_folder"],
}
# Default value for each var (anything not listed defaults to "")
VAR_DEFAULTS = {
    "var_dat_author":       "Eggman",
    "var_dat_do_dl":        True,
    "var_dat_do_bin":       True,
    "var_dat_do_ext":       True,
    "var_use_existing_json": False,
}
TAB_ORDER = ["download", "package", "extract", "romanize", "dat"]

USER_AGENT = 'c384da2W9f73dz20403d'

FILENAME_FIXES = {
    "ECOM3005a.bin": "COM3005a.bin",
    "COM3008.bin":   "COM3008a.bin",
}

PLATFORM_NAMES = {
    "\u30a2\u30fc\u30b1\u30fc\u30c9":   "Arcade",
    "\u30e1\u30ac\u30c9\u30e9\u30a4\u30d6": "Mega Drive",
    "PC\u30a8\u30f3\u30b8\u30f3":   "PC Engine",
    "\u305d\u306e\u4ed6":       "Other",
}

CONTENT_PROPERTIES = (
    ('egg',               str),
    ('version',           str),
    ('title',             str),
    ('productId',         str),
    ('publisher',         str),
    ('platform',          str),
    ('genre',             str),
    ('year',              str),
    ('mystery1',          str),
    ('gameFilename',      str),
    ('mystery2',          str),
    ('mystery3',          str),
    ('owned',             lambda s: bool(int(s))),
    ('thumbnailFilename', str),
    ('description',       str),
    ('mystery5',          str),
    ('manualFilename',    str),
    ('manualDate',        str),
    ('musicFilename',     str),
    ('musicDate',         str),
    ('lastUpdate',        str),
    ('mystery6',          str),
    ('mystery7',          str),
    ('mystery8',          str),
    ('mystery9',          str),
    ('region',            int),
    ('mystery10',         str),
)


# ──────────────────────────────────────────────────────────────────────────────
# Shared filename / naming helpers
# ──────────────────────────────────────────────────────────────────────────────

def resolve_filename(raw):
    return FILENAME_FIXES.get(raw, raw)


def get_platform(p):
    return PLATFORM_NAMES.get(p, p)


def get_region(code):
    return {0: "Japan", 1: "World"}.get(code, "Unknown")


def sanitize_for_filename(text):
    for ch in r'\/:*?"<>|':
        text = text.replace(ch, '_')
    text = (text.replace('\u3000', ' ')
                .replace('\u2013', '-')
                .replace('\u2014', '-')
                .strip())
    return text


def resolve_year(raw_year):
    y = raw_year.strip() if raw_year else ""
    if not y:
        return "Unknown"
    if "\u8907\u6570" in y or "/" in y:   # 複数 = multiple
        return "multi-year"
    return y


def build_zip_name(egg, incomplete):
    title    = sanitize_for_filename(egg["title"])
    year     = resolve_year(egg.get("year", ""))
    region   = get_region(egg["region"])
    platform = sanitize_for_filename(get_platform(egg["platform"]))
    pid      = egg["productId"]
    name = f"{title} ({year}) ({region}) ({platform}) [{pid}] [bin]"
    if incomplete:
        name += " [incomplete]"
    return name + ".zip"


def build_folder_name(egg, incomplete):
    """Same naming as build_zip_name but without the .zip extension."""
    title    = sanitize_for_filename(egg["title"])
    year     = resolve_year(egg.get("year", ""))
    region   = get_region(egg["region"])
    platform = sanitize_for_filename(get_platform(egg["platform"]))
    pid      = egg["productId"]
    name = f"{title} ({year}) ({region}) ({platform}) [{pid}] [ext]"
    if incomplete:
        name += " [incomplete]"
    return name


def collect_egg_files(egg):
    files = []
    for key in ("gameFilename", "manualFilename", "musicFilename"):
        raw = egg.get(key, "").strip()
        if raw:
            files.append(resolve_filename(raw))
    return files


# ──────────────────────────────────────────────────────────────────────────────
# Download logic  (ported from main.py)
# ──────────────────────────────────────────────────────────────────────────────

def parse_content_entry(data):
    return {key: transformer(unquote_plus(data.readline().rstrip('\n'),
                                          encoding='euc_jisx0213'))
            for key, transformer in CONTENT_PROPERTIES}


def parse_content_entries(data):
    data = data.replace(',', '\n')
    data = StringIO(data)
    data.seek(0, SEEK_END)
    end = data.tell()
    data.seek(0)
    while data.tell() < end:
        yield parse_content_entry(data)


def get_purchased(username, password):
    r = requests.post(
        'http://api.amusement-center.com/api/dcp/v1/getcontentslist',
        headers={'User-Agent': USER_AGENT},
        data={'userid': username, 'passwd': password}
    )
    if r.status_code != 200:
        raise ConnectionError("Could not get list of purchased content.")

    data = r.content.decode("euc_jisx0213")
    first_comma = data.find(',')
    if first_comma == -1:
        status, data = data, ""
    else:
        status, data = data[:first_comma], data[first_comma + 1:]

    if status != 'ok':
        raise ValueError("Login failed — check username and password.")

    return tuple(parse_content_entries(data))


def generate_file_list(server_json):
    file_list = {}
    for egg in server_json:
        for key in ("gameFilename", "manualFilename", "musicFilename"):
            fn = egg.get(key, "").strip()
            if fn:
                file_list[fn] = fn

    for idx, x in list(file_list.items()):
        if x == "ECOM3005a.bin":
            file_list[idx] = "COM3005a.bin"
        elif x == "COM3008.bin":
            file_list[idx] = "COM3008a.bin"

    return sorted(set(file_list.values()))


def convert_last_modified_str(timestr):
    d = datetime.datetime.strptime(timestr, '%a, %d %b %Y %H:%M:%S %Z')
    local = pytz.timezone("GMT")
    local_dt = local.localize(d, is_dst=None)
    return int(local_dt.timestamp())


def get_dict_from_http1(header_filepath):
    headers = requests.structures.CaseInsensitiveDict()
    with open(header_filepath, mode="r", newline="\r\n", encoding="utf8") as f:
        for line in f:
            if ":" in line:
                parts = line.split(":", maxsplit=1)
                headers[parts[0].strip()] = parts[1].strip()
    return headers


def get_last_modified_as_epoch(header_filepath):
    headers = get_dict_from_http1(header_filepath)
    return convert_last_modified_str(headers["last-modified"])


def get_access_date_as_epoch(header_filepath):
    headers = get_dict_from_http1(header_filepath)
    return convert_last_modified_str(headers["date"])


def check_for_older_headers(new_headers, header_filepath):
    try:
        last_modified_epoch = get_last_modified_as_epoch(header_filepath)
    except (KeyError, FileNotFoundError):
        return True
    if "last-modified" in new_headers:
        return last_modified_epoch < convert_last_modified_str(new_headers["last-modified"])
    return True


def move_older_file(filepath, header_filepath):
    if not os.path.exists(header_filepath):
        return None
    try:
        epoch = get_last_modified_as_epoch(header_filepath)
        suffix = str(epoch)
    except KeyError:
        epoch = get_access_date_as_epoch(header_filepath)
        suffix = f"{epoch}a"

    new_folder = os.path.join(
        pathlib.Path(filepath).parent,
        f"{pathlib.Path(filepath).name}_{suffix}")
    os.makedirs(new_folder, exist_ok=True)
    shutil.move(filepath, os.path.join(new_folder, pathlib.Path(filepath).name))
    shutil.move(header_filepath, os.path.join(new_folder, pathlib.Path(header_filepath).name))
    return new_folder


def download_file(url, dest_dir, index, total, log):
    """Download one file with skip-if-current logic. Returns (wrote_file)."""
    headers_req    = {'user-agent': USER_AGENT}
    local_filename = url.split('/')[-1]

    if local_filename == "COM3008.bin":
        local_filename = "COM3008a.bin"
        url = url.replace("COM3008.bin", "COM3008a.bin")
    elif local_filename == "ECOM3005a.bin":
        local_filename = "COM3005a.bin"
        url = url.replace("ECOM3005a.bin", "COM3005a.bin")

    target_path  = os.path.join(dest_dir, local_filename)
    headers_path = os.path.join(dest_dir, f"{local_filename}_headers.txt")
    wrote_file   = False
    success      = False

    log(f"({index}/{total}): {local_filename}")

    for attempt in range(3):
        time.sleep(randint(1, 5))
        with requests.get(url, stream=True, headers=headers_req) as r:
            resp_headers = r.headers
            try:
                r.raise_for_status()
                success = True
            except HTTPError as exc:
                code = exc.response.status_code
                if code == 404:
                    log("  [WARNING] 404 — file not available on server.")
                    return False
                log(f"  [WARNING] HTTP {code}, retrying ({attempt + 1}/3)...")
                time.sleep(attempt + 1)
                continue

            if check_for_older_headers(resp_headers, headers_path):
                try:
                    new_folder = move_older_file(target_path, headers_path)
                    if new_folder:
                        log(f"  Old version moved to: {new_folder}")
                except FileNotFoundError:
                    pass

                with open(target_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                wrote_file = True

                with open(headers_path, newline="\r\n", mode="w") as hf:
                    hf.writelines(f"{k}: {v}\n" for k, v in resp_headers.items())
                    hf.write("\n")

                log("  Downloaded.")
            else:
                log("  Already up to date, skipping.")
            break

    if not success:
        log("  [WARNING] Failed after 3 attempts, skipping.")

    return wrote_file


def run_downloader(username, password, dest_dir, existing_json,
                   log, progress_cb, done_cb, cancel_flag):
    """Worker thread for the Download tab."""
    os.makedirs(dest_dir, exist_ok=True)

    # Step 1: metadata
    if existing_json:
        log(f"Using existing JSON: {existing_json}")
        try:
            with open(existing_json, encoding="utf-8-sig") as f:
                server_json = json.load(f)
            json_path = existing_json
        except Exception as e:
            log(f"ERROR reading JSON: {e}")
            done_cb(None)
            return
    else:
        log("Fetching purchased content list from server...")
        try:
            entries = get_purchased(username, password)
        except Exception as e:
            log(f"ERROR: {e}")
            done_cb(None)
            return

        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        json_path = os.path.join(dest_dir, f"data_{timestamp}.json")
        with open(json_path, "w", encoding="utf-8-sig") as f:
            json.dump(list(entries), f, ensure_ascii=False, indent=4)
        server_json = list(entries)
        log(f"Metadata saved: {pathlib.Path(json_path).name}  ({len(server_json)} entries)")

    log("")

    if cancel_flag.is_set():
        log("── Cancelled by user ──")
        done_cb(None)
        return

    # Step 2: download
    file_list  = generate_file_list(server_json)
    total      = len(file_list)
    log(f"Files to check: {total}")
    log("")

    downloaded = skipped = failed = 0

    for idx, filename in enumerate(file_list, start=1):
        if cancel_flag.is_set():
            log("── Cancelled by user ──")
            break

        progress_cb(idx, total)
        url = f"http://www.amusement-center.com/productfiles/EGGFILES/{filename}"

        try:
            wrote = download_file(url, dest_dir, idx, total, log)
            if wrote:
                downloaded += 1
            else:
                skipped += 1
        except Exception as e:
            log(f"  ERROR: {e}")
            failed += 1

    log("")
    log("── Summary ──────────────────────────────")
    log(f"  Downloaded (new/updated) : {downloaded}")
    log(f"  Already current (skipped): {skipped}")
    log(f"  Failed                   : {failed}")
    log(f"  Metadata JSON            : {pathlib.Path(json_path).name}")
    log("─────────────────────────────────────────")

    done_cb({
        "downloaded": downloaded,
        "skipped":    skipped,
        "failed":     failed,
        "json_path":  json_path,
    })


# ──────────────────────────────────────────────────────────────────────────────
# Extractor logic  (orchestrates QuickBMS externally)
# ──────────────────────────────────────────────────────────────────────────────

def read_bin_filenames(bin_path):
    """
    Convenience wrapper — returns just the filenames for the Preview button.
    """
    return [fname for fname, _size in read_bin_file_info(bin_path)]


def read_bin_file_info(bin_path):
    """
    Parse a Project EGG .bin and return a list of
    (correct_unicode_filename, decompressed_size) tuples.

    Header layout:
        char[6]  magic       "CNPFVR"
        short    version
        uint32   filesize
        uint32   dataoff     offset to the file-data block
        uint32   num_files
    Then (dataoff - 0x14) bytes: Shift-JIS CSV  name,type,name,type,...
    Then at dataoff: per-file structs
        byte  comp           1 = compressed, 0 = raw
        byte  type
        uint32 size          compressed size (or raw size if comp==0)
        [if comp==1] uint32 decsize  decompressed size
        [data bytes]
    """
    try:
        with open(bin_path, "rb") as f:
            if f.read(6) != b"CNPFVR":
                return []
            f.read(2)                                      # version
            _fsize    = struct.unpack("<I", f.read(4))[0]
            dataoff   = struct.unpack("<I", f.read(4))[0]
            num_files = struct.unpack("<I", f.read(4))[0]

            csv_len = dataoff - 0x14
            if csv_len <= 0:
                return []
            csv_str = f.read(csv_len).decode("cp932", errors="replace")
            parts = [p for p in csv_str.split(",") if p.strip()]
            filenames = [parts[i] for i in range(0, len(parts), 2)]

            # Read decompressed sizes from data block
            f.seek(dataoff)
            sizes = []
            for _ in range(num_files):
                comp = struct.unpack("B", f.read(1))[0]
                f.read(1)                                  # type byte
                size = struct.unpack("<I", f.read(4))[0]
                if comp == 1:
                    decsize = struct.unpack("<I", f.read(4))[0]
                    sizes.append(decsize)
                    f.read(size - 4)
                else:
                    sizes.append(size)
                    f.read(size)

            return [(filenames[i], sizes[i] if i < len(sizes) else 0)
                    for i in range(len(filenames))]
    except Exception:
        return []


def parse_quickbms_stdout(stdout_text):
    """
    Extract (correct_filepath, decompressed_size) pairs from QuickBMS stdout.

    QuickBMS prints one line per extracted file in this format:
        hex_offset  decimal_size    relative/path/to/file

    The decimal_size is the actual decompressed/stored size — guaranteed to
    match the file QuickBMS just wrote to disk.  Using these sizes (rather
    than sizes parsed from the .bin header) avoids mismatches caused by
    compression overhead, padding, or format ambiguities.
    """
    results = []
    # Match lines like: "  00000094 5947392    PTAE0006\file.exe"
    pattern = re.compile(r"^\s*[0-9a-fA-F]{4,}\s+(\d+)\s+(\S.*?)\s*$")
    for line in stdout_text.splitlines():
        m = pattern.match(line)
        if m:
            try:
                size     = int(m.group(1))
                filepath = m.group(2).strip()
                if filepath and not filepath.startswith("-"):
                    results.append((filepath, size))
            except ValueError:
                pass
    return results


def _ascii_stem_prefix(name):
    """Return the leading all-ASCII portion of the filename stem."""
    stem = pathlib.Path(name).stem
    prefix = []
    for ch in stem:
        if ord(ch) < 128:
            prefix.append(ch)
        else:
            break
    return "".join(prefix)


def rename_extracted_files(game_folder, file_info, log):
    """
    Rename and relocate mojibake-named files after QuickBMS extraction.

    Two bugs affect all files on a Western Windows system:

    Bug 1 — Directory mismatch: QuickBMS stdout uses single backslashes
    (PTIT0003\file.exe). A naive double-backslash replace leaves the path
    unsplit, making the expected key ("", ".exe") while the disk key is
    ("ptit0003", ".exe") — they never match.

    Bug 2 — 0x5C in Shift-JIS second bytes: Characters like \u30bd (\u30bd=ソ,
    Shift-JIS 0x83 0x5C) and \u80FD (能, Shift-JIS 0x94 0x5C) contain 0x5C
    (backslash) as their second byte. Windows treats this as a path separator,
    accidentally splitting the filename across a new subdirectory:
    e.g. game_folder/\u0192/[rest-of-name.pdf  instead of  \u30bd\u30fc\u30b5\u30ea\u30a2\u30f3.pdf

    Solution: match ONLY by (extension, exact size), ignoring directory entirely.
    If the file is in the wrong directory, MOVE it to the correct location.
    Clean up any empty accidental directories afterward.

    Tiebreaker for same-ext/same-size: ASCII stem prefix (survives cp1252 intact).
    """
    game_folder = pathlib.Path(game_folder)

    if not file_info:
        return

    # Build lookup ignoring directory:
    # {(ext_lower, size): [correct_relative_path, ...]}
    # correct_relative_path uses forward slashes, e.g. "PTIT0003/\u30a8\u30ec..."
    by_size = {}   # {(ext, size): [rel_path, ...]}

    for filepath, size in file_info:
        # Fix Bug 1: replace ALL backslashes (single or double) with forward slash
        rel_path = filepath.replace("\\", "/")
        ext      = pathlib.PurePosixPath(rel_path).suffix.lower()
        key      = (ext, size)
        if key not in by_size:
            by_size[key] = []
        by_size[key].append(rel_path)

    renamed = skipped = 0

    for dirpath, _dirs, filenames in os.walk(game_folder, topdown=False):
        dirpath = pathlib.Path(dirpath)

        for filename in filenames:
            disk_file = dirpath / filename
            ext       = pathlib.Path(filename).suffix.lower()

            try:
                actual_size = disk_file.stat().st_size
            except OSError:
                continue

            key      = (ext, actual_size)
            cands    = by_size.get(key, [])

            correct_rel = None

            if len(cands) == 1:
                correct_rel = cands[0]
            elif len(cands) > 1:
                # Tiebreak: ASCII stem prefix (survives cp1252 mangling intact)
                disk_prefix = _ascii_stem_prefix(filename)
                pre_cands   = [c for c in cands
                               if _ascii_stem_prefix(
                                   pathlib.PurePosixPath(c).name) == disk_prefix]
                if len(pre_cands) == 1:
                    correct_rel = pre_cands[0]
                else:
                    log(f"         [INFO] Cannot uniquely match {filename!r} — skipped")
                    skipped += 1
                    continue

            if not correct_rel:
                continue

            correct_dest = game_folder / correct_rel

            if disk_file == correct_dest:
                continue   # already in the right place with the right name

            # Create parent directory if it doesn't exist yet
            correct_dest.parent.mkdir(parents=True, exist_ok=True)

            if not correct_dest.exists():
                try:
                    shutil.move(str(disk_file), str(correct_dest))
                    renamed += 1
                except Exception as e:
                    log(f"         [WARNING] Could not move {filename!r}: {e}")

    # Remove any empty directories left behind by accidental 0x5C splits
    for dirpath, dirnames, filenames in os.walk(game_folder, topdown=False):
        dirpath = pathlib.Path(dirpath)
        if dirpath == game_folder:
            continue
        try:
            dirpath.rmdir()   # only succeeds if the directory is empty
        except OSError:
            pass

    msg = f"         Encoding fixed: {renamed} file(s) renamed/moved"
    if skipped:
        msg += f", {skipped} could not be matched"
    log(msg)


def run_extractor(quickbms_path, bms_path, source_dir, json_path, output_dir,
                  log, progress_cb, done_cb, cancel_flag):
    """
    Worker thread for the Extract tab.
    For each game in the JSON, finds its .bin files in source_dir, creates a
    named game folder in output_dir, then calls QuickBMS on each .bin file.
    """
    try:
        with open(json_path, encoding="utf-8-sig") as f:
            server_json = json.load(f)
    except Exception as e:
        log(f"ERROR: Could not read JSON: {e}")
        done_cb(None)
        return

    server_json = sorted(server_json, key=lambda e: int(e["productId"]))
    total = len(server_json)

    log(f"Loaded {total} entries from {pathlib.Path(json_path).name}")
    log(f"Source .bin dir : {source_dir}")
    log(f"Output dir      : {output_dir}")
    log(f"QuickBMS        : {quickbms_path}")
    log(f"BMS script      : {bms_path}")
    log("")

    extracted = skipped_exists = skipped_no_files = failed = 0
    no_metadata_entries = []

    for idx, egg in enumerate(server_json, start=1):
        if cancel_flag.is_set():
            log("── Cancelled by user ──")
            break

        progress_cb(idx, total)
        pid      = egg["productId"]
        expected = collect_egg_files(egg)

        if not expected:
            no_metadata_entries.append({"productId": pid, "title": egg.get("title", "")})
            continue

        found   = [pathlib.Path(source_dir) / fn for fn in expected
                   if (pathlib.Path(source_dir) / fn).exists()]
        missing = [fn for fn in expected
                   if not (pathlib.Path(source_dir) / fn).exists()]

        if not found:
            skipped_no_files += 1
            continue

        is_incomplete  = len(missing) > 0
        folder_name    = build_folder_name(egg, is_incomplete)
        game_folder    = pathlib.Path(output_dir) / folder_name

        if game_folder.exists() and any(game_folder.iterdir()):
            log(f"[{pid:>5}] EXISTS, skipping  : {folder_name}")
            skipped_exists += 1
            continue

        game_folder.mkdir(parents=True, exist_ok=True)
        log(f"[{pid:>5}] Extracting to    : {folder_name}")
        if is_incomplete:
            log(f"         Missing files  : {', '.join(missing)}")

        game_ok   = True
        all_file_info = []   # (correct_path, size) from QuickBMS stdout

        for bin_file in found:
            log(f"         Processing    : {bin_file.name}")
            try:
                result = subprocess.run(
                    [quickbms_path, "-o", bms_path,
                     str(bin_file), str(game_folder)],
                    capture_output=True,
                    text=False,          # capture raw bytes
                    timeout=120
                )
                # QuickBMS output contains Shift-JIS filenames; decode as cp932
                stdout = result.stdout.decode("cp932", errors="replace")
                stderr = result.stderr.decode("cp932", errors="replace")
                if result.returncode != 0:
                    log(f"         [WARNING] QuickBMS returned code {result.returncode}")
                    if stderr.strip():
                        for line in stderr.strip().splitlines():
                            log(f"           {line}")
                    game_ok = False
                else:
                    # Collect file info for renaming, and log for visibility
                    parsed = parse_quickbms_stdout(stdout)
                    all_file_info.extend(parsed)
                    for line in stdout.strip().splitlines():
                        stripped = line.strip()
                        if stripped and not stripped.startswith("-"):
                            log(f"           {stripped}")
            except subprocess.TimeoutExpired:
                log(f"         [ERROR] QuickBMS timed out on {bin_file.name}")
                game_ok = False
            except Exception as e:
                log(f"         [ERROR] {e}")
                game_ok = False

        # Rename mojibake files using exact sizes from QuickBMS stdout
        rename_extracted_files(game_folder, all_file_info, log)

        if game_ok:
            extracted += 1
        else:
            failed += 1

    log("")
    log("── Summary ──────────────────────────────")
    log(f"  Games extracted         : {extracted}")
    log(f"  Already exist (skipped) : {skipped_exists}")
    log(f"  No files on disk        : {skipped_no_files}")
    log(f"  Failed / partial errors : {failed}")
    log(f"  No metadata (skipped)   : {len(no_metadata_entries)}")
    log("─────────────────────────────────────────")

    if no_metadata_entries:
        log("")
        log("── No metadata (no filenames in JSON) ───")
        for entry in no_metadata_entries:
            log(f"  [{entry['productId']:>5}] {entry['title']}")
        log("─────────────────────────────────────────")

    done_cb({
        "extracted":       extracted,
        "skipped_exists":  skipped_exists,
        "skipped_no_files":skipped_no_files,
        "failed":          failed,
        "no_metadata":     len(no_metadata_entries),
    })


# ──────────────────────────────────────────────────────────────────────────────
# Packager logic
# ──────────────────────────────────────────────────────────────────────────────

def run_packager(download_dir, json_path, output_dir, log, progress_cb, done_cb, cancel_flag):
    """Worker thread for the Package tab."""
    try:
        with open(json_path, encoding="utf-8-sig") as f:
            server_json = json.load(f)
    except Exception as e:
        log(f"ERROR: Could not read JSON: {e}")
        done_cb(None)
        return

    server_json = sorted(server_json, key=lambda e: int(e["productId"]))
    total = len(server_json)

    log(f"Loaded {total} entries from {pathlib.Path(json_path).name}")
    log(f"Download dir : {download_dir}")
    log(f"Output dir   : {output_dir}")
    log("")

    created = skipped = incomplete_count = 0
    all_bins_on_disk    = {f.name for f in pathlib.Path(download_dir).iterdir()
                           if f.suffix.lower() == ".bin"}
    processed_bins      = set()
    missing_from_disk   = []
    no_metadata_entries = []

    for idx, egg in enumerate(server_json, start=1):
        if cancel_flag.is_set():
            log("── Cancelled by user ──")
            break

        progress_cb(idx, total)
        pid      = egg["productId"]
        expected = collect_egg_files(egg)

        if not expected:
            no_metadata_entries.append({"productId": pid, "title": egg.get("title", "")})
            continue

        found   = [pathlib.Path(download_dir) / fn for fn in expected
                   if (pathlib.Path(download_dir) / fn).exists()]
        missing = [fn for fn in expected
                   if not (pathlib.Path(download_dir) / fn).exists()]

        if not found:
            missing_from_disk.append({
                "bins":     expected,
                "zip_name": build_zip_name(egg, False),
            })
            continue

        is_incomplete = len(missing) > 0
        zip_name      = build_zip_name(egg, is_incomplete)
        zip_path      = pathlib.Path(output_dir) / zip_name

        if zip_path.exists():
            log(f"[{pid:>5}] EXISTS, skipping  : {zip_name}")
            for fp in found:
                processed_bins.add(fp.name)
            skipped += 1
            continue

        log(f"[{pid:>5}] Packaging        : {zip_name}")
        if is_incomplete:
            log(f"         Missing files  : {', '.join(missing)}")

        try:
            with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_STORED) as zf:
                for fp in found:
                    zf.write(fp, fp.name)
                    log(f"         Added         : {fp.name}")
                    processed_bins.add(fp.name)
            created += 1
            if is_incomplete:
                incomplete_count += 1
        except Exception as e:
            log(f"         ERROR: {e}")
            if zip_path.exists():
                zip_path.unlink()

    unprocessed_bins = sorted(all_bins_on_disk - processed_bins)

    log("")
    log("── Summary ──────────────────────────────")
    log(f"  ZIPs created            : {created}  ({incomplete_count} marked [incomplete])")
    log(f"  Already exist           : {skipped}")
    log(f"  No metadata (skipped)   : {len(no_metadata_entries)}")
    log(f"  Missing from disk       : {len(missing_from_disk)}")
    log(f"  Unprocessed .bin files  : {len(unprocessed_bins)}")
    log("─────────────────────────────────────────")

    if no_metadata_entries:
        log("")
        log("── No metadata (no filenames in JSON) ───")
        for entry in no_metadata_entries:
            log(f"  [{entry['productId']:>5}] {entry['title']}")
        log("─────────────────────────────────────────")

    if missing_from_disk:
        log("")
        log("── Missing from disk (not downloaded) ───")
        for entry in missing_from_disk:
            log(f"  Would-be ZIP : {entry['zip_name']}")
            for bn in entry["bins"]:
                log(f"    {bn}")
        log("─────────────────────────────────────────")

    if unprocessed_bins:
        log("")
        log("── Unprocessed .bin files ───────────────")
        for fn in unprocessed_bins:
            log(f"  {fn}")
        log("─────────────────────────────────────────")

    done_cb({
        "created":     created,
        "incomplete":  incomplete_count,
        "skipped":     skipped,
        "no_metadata": len(no_metadata_entries),
        "missing":     len(missing_from_disk),
        "unprocessed": len(unprocessed_bins),
    })


# ──────────────────────────────────────────────────────────────────────────────
# DAT / Romanize helpers
# ──────────────────────────────────────────────────────────────────────────────

def xml_e(s):
    """XML-escape a string for use in attributes and text."""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _hash_file_dat(path):
    """Return (size, crc32_lower_hex, sha1_lower_hex) for a file on disk."""
    crc = 0
    h   = hashlib.sha1()
    sz  = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            crc = _zlib_crc32(chunk, crc)
            h.update(chunk)
            sz += len(chunk)
    return sz, "%08x" % (crc & 0xffffffff), h.hexdigest()


def _hash_bytes_dat(data):
    """Return (size, crc32_lower_hex, sha1_lower_hex) for in-memory bytes."""
    crc = _zlib_crc32(data) & 0xffffffff
    h   = hashlib.sha1(data).hexdigest()
    return len(data), "%08x" % crc, h


def _pid_from_zip_stem(stem):
    """Find the last [number] in a ZIP stem — the ProductId.
    Works with or without [bin]/[ext]/[incomplete] suffixes.
    """
    matches = re.findall(r"\[(\d+)\]", stem)
    return matches[-1] if matches else None


def _dat_header(name, description, author, date_str, fileonly=False):
    rv = '\t\t<romvault forcepacking="fileonly"/>' if fileonly else "\t\t<romvault/>"
    return (
        f'''<?xml version="1.0"?>\r\n''' +
        f'''<datafile>\r\n''' +
        f'''\t<header>\r\n''' +
        f'''\t\t<n>{xml_e(name)}</n>\r\n''' +
        f'''\t\t<description>{xml_e(description)}</description>\r\n''' +
        f'''\t\t<category>Computer</category>\r\n''' +
        f'''\t\t<date>{xml_e(date_str)}</date>\r\n''' +
        f'''\t\t<author>{xml_e(author)}</author>\r\n''' +
        f'''\t\t<homepage>https://github.com/Eggmansworld/Datfiles</homepage>\r\n''' +
        f'''\t\t<url>https://www.amusement-center.com/project/egg/</url>\r\n''' +
        f'''\t\t<comment></comment>\r\n''' +
        f'''\t\t<version></version>\r\n''' +
        f'''{rv}\r\n''' +
        f'''\t</header>\r\n'''
    )


def _game_block(game_name, description, rom_entries):
    """
    rom_entries: list of (name, size, crc, sha1)
    Returns the XML block for one <game>.
    """
    roms = "".join(
        f'''\t\t<rom name="{xml_e(n)}" size="{sz}" crc="{crc}" sha1="{sha1}"/>\r\n'''
        for n, sz, crc, sha1 in rom_entries
    )
    return (
        f'''\t<game name="{xml_e(game_name)}">\r\n''' +
        f'''\t\t<description>{xml_e(description)}</description>\r\n''' +
        roms +
        f'''\t</game>\r\n'''
    )


def _build_game_title(egg, rom_map, suffix, romanized=True):
    """
    Build the DAT game name string.
    suffix: "[bin]" or "[ext]"
    romanized: if True, use romanized title (fall back to Japanese if blank)
    """
    pid      = egg["productId"]
    year     = resolve_year(egg.get("year", ""))
    region   = get_region(egg["region"])
    platform = sanitize_for_filename(get_platform(egg["platform"]))

    if romanized:
        title = (rom_map.get(pid) or "").strip()
        if not title:
            title = sanitize_for_filename(egg["title"])
    else:
        title = sanitize_for_filename(egg["title"])

    return f"{title} ({year}) ({region}) ({platform}) [{pid}] {suffix}"


# ── Romanize worker ───────────────────────────────────────────────────────────

def run_romanize(json_path, csv_path, output_dir, log, progress_cb, done_cb, cancel_flag):
    """
    Merge JSON entries missing from the romanizations CSV,
    sort by productId, and save a new timestamped CSV.
    """
    log("Loading metadata JSON...")
    try:
        with open(json_path, encoding="utf-8-sig") as f:
            server_json = json.load(f)
    except Exception as e:
        log(f"ERROR reading JSON: {e}")
        done_cb(None)
        return

    log(f"  {len(server_json)} entries in JSON.")

    # Load existing CSV
    existing_rows = []
    fieldnames    = ["title", "romanized", "productId", "platform",
                     "gameFilename", "thumbnailFilename", "manualFilename",
                     "musicFilename", "Store Page"]
    if pathlib.Path(csv_path).is_file():
        try:
            with open(csv_path, encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                existing_rows = list(reader)
                if reader.fieldnames:
                    fieldnames = reader.fieldnames
            log(f"  {len(existing_rows)} rows in existing CSV.")
        except Exception as e:
            log(f"ERROR reading CSV: {e}")
            done_cb(None)
            return
    else:
        log("  No existing CSV found — building from scratch.")

    existing_pids = {r["productId"] for r in existing_rows}
    added = 0

    for egg in server_json:
        pid = egg["productId"]
        if pid in existing_pids:
            continue
        fn_game  = resolve_filename(egg.get("gameFilename",  "") or "")
        fn_man   = resolve_filename(egg.get("manualFilename","") or "")
        fn_music = resolve_filename(egg.get("musicFilename", "") or "")
        store_url = f"https://www.amusement-center.com/project/egg/game/?product_id={pid}"
        new_row = {
            "title":             egg.get("title",             ""),
            "romanized":         "",
            "productId":         pid,
            "platform":          egg.get("platform",          ""),
            "gameFilename":      fn_game,
            "thumbnailFilename": egg.get("thumbnailFilename", ""),
            "manualFilename":    fn_man,
            "musicFilename":     fn_music,
            "Store Page":        store_url,
        }
        existing_rows.append(new_row)
        existing_pids.add(pid)
        added += 1

    log(f"  {added} new entries added.")

    # Sort by productId ascending
    try:
        existing_rows.sort(key=lambda r: int(r.get("productId") or 0))
    except Exception:
        pass

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    out_path  = pathlib.Path(output_dir) / f"romanizations_{timestamp}.csv"

    try:
        with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(existing_rows)
    except Exception as e:
        log(f"ERROR writing CSV: {e}")
        done_cb(None)
        return

    log("")
    log("── Summary ──────────────────────────────")
    log(f"  Total rows   : {len(existing_rows)}")
    log(f"  New entries  : {added}")
    log(f"  Saved to     : {out_path.name}")
    log("─────────────────────────────────────────")

    done_cb({"total": len(existing_rows), "added": added, "path": str(out_path)})


# ── ZIP hashing helpers ──────────────────────────────────────────────────────

def _hash_zip_via_7z(sevenz_path, zip_path, log):
    """
    Extract a ZIP using 7-Zip ZS (handles ZStandard and other non-native
    compression methods) into a temp dir, hash every file, return results.
    """
    import tempfile
    results = []
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            proc = subprocess.run(
                [sevenz_path, "x", str(zip_path), f"-o{tmpdir}", "-y"],
                capture_output=True, timeout=300
            )
            if proc.returncode not in (0, 1):   # 7z uses 1 for warnings
                stderr = proc.stderr.decode("utf-8", errors="replace")
                raise RuntimeError(f"7-Zip exited {proc.returncode}: {stderr[:200]}")
        except FileNotFoundError:
            raise RuntimeError(f"7-Zip executable not found: {sevenz_path}")

        for dirpath, _, files in os.walk(tmpdir):
            for fname in sorted(files):
                fp  = pathlib.Path(dirpath) / fname
                rel = fp.relative_to(tmpdir).as_posix()   # forward slashes
                sz, crc, sha1 = _hash_file_dat(fp)
                results.append((rel, sz, crc, sha1))

    return sorted(results, key=lambda x: x[0].upper())


def _hash_zip_contents(zip_path, sevenz_path, log, keep_full_path=False):
    """
    Hash every file inside a ZIP archive.
    Returns list of (name, size, crc, sha1).

    keep_full_path: if True, use the full internal ZIP path (for extracted DAT).
                    if False, use only the bare filename (for bin DAT).

    Tries Python zipfile first. Falls back to 7-Zip when the compression
    method is unsupported (e.g. ZStandard / method 93).
    """
    try:
        results = []
        with _zipfile_mod.ZipFile(zip_path, "r") as zf:
            members = sorted(
                [m for m in zf.infolist() if not m.is_dir()],
                key=lambda m: m.filename.upper()
            )
            for m in members:
                data = zf.read(m.filename)
                sz, crc, sha1 = _hash_bytes_dat(data)
                if keep_full_path:
                    name = m.filename.replace("\\", "/")
                else:
                    name = pathlib.Path(m.filename).name
                results.append((name, sz, crc, sha1))
        return results

    except NotImplementedError:
        # Compression method not supported by Python zipfile (e.g. ZStandard)
        if not sevenz_path or not pathlib.Path(sevenz_path).is_file():
            raise RuntimeError(
                "ZIP uses unsupported compression (likely ZStandard). "
                "Set the 7-Zip ZS executable path in the DAT tab to handle these."
            )
        log(f"    (using 7-Zip for ZStandard compression)")
        raw = _hash_zip_via_7z(sevenz_path, zip_path, log)
        if not keep_full_path:
            # Strip path — only keep bare filename
            raw = [(pathlib.Path(r).name, sz, crc, sha1) for r, sz, crc, sha1 in raw]
        return raw


# ── DAT generation worker ─────────────────────────────────────────────────────

def run_dat_generator(json_path, csv_path, author, output_dir,
                      gen_dl, dl_folder, dl_zips_folder, zip_dl_first,
                      gen_bin, bin_folder,
                      gen_ext, ext_folder,
                      sevenz_path,
                      log, progress_cb, done_cb, cancel_flag):
    """Worker thread for the DAT tab."""

    date_str  = datetime.date.today().isoformat()   # YYYY-MM-DD
    out_dir   = pathlib.Path(output_dir)
    dat_count = 0

    # Load JSON
    log("Loading metadata JSON...")
    try:
        with open(json_path, encoding="utf-8-sig") as f:
            server_json = json.load(f)
        json_map = {e["productId"]: e for e in server_json}  # pid → egg
        log(f"  {len(server_json)} entries.")
    except Exception as e:
        log(f"ERROR reading JSON: {e}")
        done_cb(None)
        return

    # Load romanizations
    rom_map = {}   # productId → romanized_title (blank = not yet done)
    if pathlib.Path(csv_path).is_file():
        try:
            with open(csv_path, encoding="utf-8-sig", newline="") as f:
                for row in csv.DictReader(f):
                    rom_map[row["productId"]] = row.get("romanized", "").strip()
            log(f"  Romanizations loaded: {len(rom_map)} entries.")
        except Exception as e:
            log(f"WARNING: could not read romanizations CSV: {e}")
    else:
        log("  No romanizations CSV — Japanese titles will be used throughout.")

    log("")

    # ── Downloads (bin) DAT ──────────────────────────────────────────────────
    if gen_dl and not cancel_flag.is_set():
        log("── Generating: Downloads (bin) DAT ──────")
        dl_src   = pathlib.Path(dl_folder)
        zips_dir = pathlib.Path(dl_zips_folder)

        # Optionally create one ZIP per .bin file (Store compression)
        if zip_dl_first:
            bin_files = sorted(
                [f for f in dl_src.iterdir()
                 if f.is_file() and f.suffix.lower() == ".bin"],
                key=lambda f: f.name.upper()
            )
            log(f"  Creating ZIPs for {len(bin_files)} .bin files...")
            zips_dir.mkdir(parents=True, exist_ok=True)
            zipped = 0
            for idx, bf in enumerate(bin_files, 1):
                progress_cb(idx, len(bin_files))
                if cancel_flag.is_set():
                    log("── Cancelled by user ──")
                    break
                zp = zips_dir / (bf.stem + ".zip")
                if zp.exists():
                    continue
                try:
                    with _zipfile_mod.ZipFile(zp, "w", compression=_zipfile_mod.ZIP_STORED) as zf:
                        zf.write(bf, bf.name)
                    zipped += 1
                except Exception as e:
                    log(f"  WARNING: could not ZIP {bf.name}: {e}")
            log(f"  {zipped} ZIPs created ({len(bin_files) - zipped} already existed).")
            log("")

        # Hash the ZIPs (one game entry per ZIP)
        zips = sorted(
            [f for f in zips_dir.iterdir()
             if f.is_file() and f.suffix.lower() == ".zip"],
            key=lambda f: f.name.upper()
        )
        log(f"  {len(zips)} ZIP files found in Downloads ZIPs folder.")
        blocks = []
        for idx, zf_path in enumerate(zips, 1):
            if cancel_flag.is_set():
                break
            progress_cb(idx, len(zips))
            stem = zf_path.stem
            try:
                roms = _hash_zip_contents(zf_path, sevenz_path, log, keep_full_path=False)
            except Exception as e:
                log(f"  WARNING: error reading {zf_path.name}: {e}")
                continue
            blocks.append(_game_block(stem, stem, roms))

        dat_name = f"Project EGG Collection - Downloads (bin) ({date_str}_RomVault).dat"
        dat_path = out_dir / dat_name
        header = _dat_header("Project EGG Collection - Downloads (bin)", "", author, date_str)
        with open(dat_path, "w", encoding="utf-8-sig", newline="") as f:
            f.write(header + "".join(blocks) + "</datafile>\r\n")
        log(f"  Written: {dat_name}")
        log("")
        dat_count += 1

    # ── Games (bin) DAT ──────────────────────────────────────────────────────
    if gen_bin and not cancel_flag.is_set():
        log("── Generating: Games (bin) DAT ──────────")
        bin_zip_path = pathlib.Path(bin_folder)
        zips = sorted(
            [f for f in bin_zip_path.iterdir()
             if f.is_file() and f.suffix.lower() == ".zip"],
            key=lambda f: f.name.upper()
        )
        log(f"  {len(zips)} ZIP files found.")
        blocks = []
        for idx, zf_path in enumerate(zips, 1):
            if cancel_flag.is_set():
                break
            progress_cb(idx, len(zips))
            pid = _pid_from_zip_stem(zf_path.stem)
            if not pid or pid not in json_map:
                log(f"  WARNING: no JSON entry for {zf_path.name} (pid={pid})")
                continue
            egg = json_map[pid]
            try:
                roms = _hash_zip_contents(zf_path, sevenz_path, log, keep_full_path=False)
            except Exception as e:
                log(f"  WARNING: error reading {zf_path.name}: {e}")
                continue

            g_name    = _build_game_title(egg, rom_map, "[bin]", romanized=True)
            g_desc    = _build_game_title(egg, rom_map, "[bin]", romanized=False)
            blocks.append(_game_block(g_name, g_desc, roms))

        # Sort blocks by game name (first line contains the name)
        blocks.sort(key=lambda b: b.split('"')[1].upper())
        dat_name = f"Project EGG Collection - Games (bin) ({date_str}_RomVault).dat"
        dat_path = out_dir / dat_name
        header = _dat_header("Project EGG Collection - Games (bin)", "", author, date_str)
        with open(dat_path, "w", encoding="utf-8-sig", newline="") as f:
            f.write(header + "".join(blocks) + "</datafile>\r\n")
        log(f"  Written: {dat_name}")
        log("")
        dat_count += 1

    # ── Games (extracted) DAT ────────────────────────────────────────────────
    if gen_ext and not cancel_flag.is_set():
        log("── Generating: Games (extracted) DAT ────")
        ext_zip_path = pathlib.Path(ext_folder)
        zips = sorted(
            [f for f in ext_zip_path.iterdir()
             if f.is_file() and f.suffix.lower() == ".zip"],
            key=lambda f: f.name.upper()
        )
        log(f"  {len(zips)} ZIP files found.")
        blocks = []
        for idx, zf_path in enumerate(zips, 1):
            if cancel_flag.is_set():
                break
            progress_cb(idx, len(zips))
            pid = _pid_from_zip_stem(zf_path.stem)
            if not pid or pid not in json_map:
                log(f"  WARNING: no JSON entry for {zf_path.name} (pid={pid})")
                continue
            egg = json_map[pid]
            try:
                roms = _hash_zip_contents(zf_path, sevenz_path, log, keep_full_path=True)
            except Exception as e:
                log(f"  WARNING: error reading {zf_path.name}: {e}")
                continue

            g_name = _build_game_title(egg, rom_map, "[ext]", romanized=True)
            g_desc = _build_game_title(egg, rom_map, "[ext]", romanized=False)
            blocks.append(_game_block(g_name, g_desc, roms))

        blocks.sort(key=lambda b: b.split('"')[1].upper())
        dat_name = f"Project EGG Collection - Games (extracted) ({date_str}_RomVault).dat"
        dat_path = out_dir / dat_name
        header = _dat_header("Project EGG Collection - Games (extracted)", "", author, date_str)
        with open(dat_path, "w", encoding="utf-8-sig", newline="") as f:
            f.write(header + "".join(blocks) + "</datafile>\r\n")
        log(f"  Written: {dat_name}")
        log("")
        dat_count += 1

    log("── Summary ──────────────────────────────")
    log(f"  DAT files written: {dat_count}")
    log("─────────────────────────────────────────")

    done_cb({"dats": dat_count})


# ──────────────────────────────────────────────────────────────────────────────
# GUI
# ──────────────────────────────────────────────────────────────────────────────

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Project EGG Tools  —  by Eggman")
        self.resizable(True, True)
        self.minsize(720, 600)
        self.geometry("900x750")

        self._cancel_flag = threading.Event()
        self._log_queue   = queue.Queue()
        self._worker      = None

        self._build_ui()
        self._poll_log()
        self._on_tab_change()
        self._load_config()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # ── UI construction ───────────────────────────────────────────────────────

    def _build_ui(self):
        PAD = 10

        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="x", padx=PAD, pady=(PAD, 0))
        self.notebook.bind("<<NotebookTabChanged>>", lambda e: self._on_tab_change())

        self._build_download_tab()
        self._build_package_tab()
        self._build_extract_tab()
        self._build_romanize_tab()
        self._build_dat_tab()
        self._build_about_tab()

        # Progress
        frame_prog = ttk.Frame(self, padding=(PAD, 6))
        frame_prog.pack(fill="x", padx=PAD)
        self.progress = ttk.Progressbar(frame_prog, mode="determinate")
        self.progress.pack(fill="x", side="left", expand=True)
        self.lbl_progress = ttk.Label(frame_prog, text="", width=14, anchor="e")
        self.lbl_progress.pack(side="left", padx=(8, 0))

        # Buttons
        frame_btns = ttk.Frame(self, padding=(PAD, 0))
        frame_btns.pack(fill="x", padx=PAD)
        self.btn_run = ttk.Button(frame_btns, text="▶  Run",
                                  command=self._start, width=22)
        self.btn_run.pack(side="left")
        self.btn_cancel = ttk.Button(frame_btns, text="⏹  Cancel",
                                     command=self._cancel, width=12, state="disabled")
        self.btn_cancel.pack(side="left", padx=(8, 0))
        ttk.Button(frame_btns, text="Save Log", command=self._save_log,
                   width=10).pack(side="right", padx=(6, 0))
        ttk.Button(frame_btns, text="Clear Log", command=self._clear_log,
                   width=10).pack(side="right")
        ttk.Separator(frame_btns, orient="vertical").pack(side="right", fill="y", padx=8)
        ttk.Button(frame_btns, text="Save Config", command=self._save_config,
                   width=11).pack(side="right", padx=(0, 4))
        ttk.Button(frame_btns, text="Clear Tab", command=self._clear_current_tab,
                   width=10).pack(side="right")

        # Log
        frame_log = ttk.LabelFrame(self, text="Log", padding=PAD)
        frame_log.pack(fill="both", expand=True, padx=PAD, pady=(6, PAD))
        self.log_box = scrolledtext.ScrolledText(
            frame_log, state="disabled", wrap="none",
            font=("Consolas", 9), bg="#1e1e1e", fg="#d4d4d4",
            insertbackground="white"
        )
        self.log_box.pack(fill="both", expand=True)

    # ── Config persistence ───────────────────────────────────────────────────────

    def _on_close(self):
        """Auto-save config then exit."""
        self._save_config()
        self.destroy()

    def _save_config(self):
        """Save all field values to the config JSON file."""
        config = {}
        for varnames in TAB_VARS.values():
            for name in varnames:
                if hasattr(self, name):
                    config[name] = getattr(self, name).get()
        try:
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            messagebox.showwarning("Config save failed", str(e))

    def _load_config(self):
        """Load field values from the config JSON file on startup."""
        if not CONFIG_PATH.is_file():
            return
        try:
            with open(CONFIG_PATH, encoding="utf-8") as f:
                config = json.load(f)
            for name, value in config.items():
                if not hasattr(self, name):
                    continue
                var = getattr(self, name)
                if isinstance(var, tk.BooleanVar):
                    var.set(bool(value))
                else:
                    var.set(str(value))
            # Re-apply toggle states after loading
            self._toggle_existing_json()
            self._dat_toggle_dl()
            self._dat_toggle_bin()
            self._dat_toggle_ext()
        except Exception:
            pass  # silently ignore corrupt/missing config

    def _clear_current_tab(self):
        """Reset all fields on the currently visible tab to defaults."""
        idx = self._active_tab()
        if idx is None or idx >= len(TAB_ORDER):
            return
        tab_name = TAB_ORDER[idx]
        for name in TAB_VARS.get(tab_name, []):
            if not hasattr(self, name):
                continue
            var     = getattr(self, name)
            default = VAR_DEFAULTS.get(name, "" if isinstance(var, tk.StringVar) else False)
            var.set(default)
        # Re-apply any toggle states
        if tab_name == "download":
            self._toggle_existing_json()
        elif tab_name == "dat":
            self._dat_toggle_dl()
            self._dat_toggle_bin()
            self._dat_toggle_ext()

    def _build_download_tab(self):
        tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(tab, text="  Download  ")
        tab.columnconfigure(1, weight=1)

        # Row 0 is always reserved for the warning label so input rows
        # are stable regardless of whether the warning is shown.
        if not DOWNLOAD_AVAILABLE:
            ttk.Label(
                tab,
                text="⚠  'requests' and/or 'pytz' are not installed.\n"
                     "Run:  pip install requests pytz",
                foreground="red"
            ).grid(row=0, column=0, columnspan=3, pady=(0, 6), sticky="w")

        ttk.Label(tab, text="Username:").grid(
            row=1, column=0, sticky="w", pady=3, padx=(0, 6))
        self.var_username = tk.StringVar()
        ttk.Entry(tab, textvariable=self.var_username, width=40).grid(
            row=1, column=1, sticky="ew", pady=3)

        ttk.Label(tab, text="Password:").grid(
            row=2, column=0, sticky="w", pady=3, padx=(0, 6))
        self.var_password = tk.StringVar()
        ttk.Entry(tab, textvariable=self.var_password, show="*", width=40).grid(
            row=2, column=1, sticky="ew", pady=3)

        ttk.Label(tab, text="Download folder:").grid(
            row=3, column=0, sticky="w", pady=3, padx=(0, 6))
        self.var_dl_dest = tk.StringVar()
        ttk.Entry(tab, textvariable=self.var_dl_dest, width=40).grid(
            row=3, column=1, sticky="ew", pady=3)
        ttk.Button(tab, text="Browse…",
                   command=self._browse_dl_dest, width=9).grid(
            row=3, column=2, padx=(6, 0), pady=3)

        ttk.Separator(tab, orient="horizontal").grid(
            row=4, column=0, columnspan=3, sticky="ew", pady=(8, 4))

        self.var_use_existing_json = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            tab,
            text="Use existing metadata JSON instead of fetching from server "
                 "(credentials not required)",
            variable=self.var_use_existing_json,
            command=self._toggle_existing_json
        ).grid(row=5, column=0, columnspan=3, sticky="w")

        ttk.Label(tab, text="Existing JSON:").grid(
            row=6, column=0, sticky="w", pady=3, padx=(0, 6))
        self.var_existing_json = tk.StringVar()
        self.entry_existing_json = ttk.Entry(
            tab, textvariable=self.var_existing_json, width=40, state="disabled")
        self.entry_existing_json.grid(row=6, column=1, sticky="ew", pady=3)
        self.btn_browse_existing_json = ttk.Button(
            tab, text="Browse…", command=self._browse_existing_json,
            width=9, state="disabled")
        self.btn_browse_existing_json.grid(row=6, column=2, padx=(6, 0), pady=3)

    def _build_package_tab(self):
        tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(tab, text="  Package  ")
        tab.columnconfigure(1, weight=1)

        fields = [
            ("Download folder (.bin files):", "var_pkg_download", self._browse_pkg_download),
            ("Metadata JSON (data_*.json):",  "var_pkg_json",     self._browse_pkg_json),
            ("Output folder (ZIPs go here):", "var_pkg_output",   self._browse_pkg_output),
        ]
        for row_idx, (label, varname, cmd) in enumerate(fields):
            setattr(self, varname, tk.StringVar())
            ttk.Label(tab, text=label).grid(
                row=row_idx, column=0, sticky="w", pady=3, padx=(0, 6))
            ttk.Entry(tab, textvariable=getattr(self, varname), width=55).grid(
                row=row_idx, column=1, sticky="ew", pady=3)
            ttk.Button(tab, text="Browse…", command=cmd, width=9).grid(
                row=row_idx, column=2, padx=(6, 0), pady=3)

    def _build_extract_tab(self):
        tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(tab, text="  Extract  ")
        tab.columnconfigure(1, weight=1)

        fields = [
            ("QuickBMS executable:",          "var_qbms_exe",    self._browse_qbms_exe),
            ("BMS script (.bms):",            "var_bms_script",  self._browse_bms_script),
            ("Download folder (.bin files):", "var_ext_source",  self._browse_ext_source),
            ("Metadata JSON (data_*.json):",  "var_ext_json",    self._browse_ext_json),
            ("Output folder (games go here):","var_ext_output",  self._browse_ext_output),
        ]
        for row_idx, (label, varname, cmd) in enumerate(fields):
            setattr(self, varname, tk.StringVar())
            ttk.Label(tab, text=label).grid(
                row=row_idx, column=0, sticky="w", pady=3, padx=(0, 6))
            ttk.Entry(tab, textvariable=getattr(self, varname), width=52).grid(
                row=row_idx, column=1, sticky="ew", pady=3)
            ttk.Button(tab, text="Browse…", command=cmd, width=9).grid(
                row=row_idx, column=2, padx=(6, 0), pady=3)

        ttk.Label(
            tab,
            text="Each game's .bin files will be extracted into its own named subfolder\n"
                 "inside the output folder.  Folders that already contain files are skipped.",
            foreground="gray"
        ).grid(row=5, column=0, columnspan=3, sticky="w", pady=(6, 0))

        ttk.Button(
            tab, text="Preview .bin filenames…",
            command=self._preview_bin, width=24
        ).grid(row=6, column=0, columnspan=2, sticky="w", pady=(8, 0))

    def _build_romanize_tab(self):
        tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(tab, text="  Romanize  ")
        tab.columnconfigure(1, weight=1)

        fields = [
            ("Metadata JSON (data_*.json):",     "var_rom_json",   self._browse_rom_json),
            ("Existing romanizations CSV:",       "var_rom_csv",    self._browse_rom_csv),
            ("Output folder (docs/):",            "var_rom_out",    self._browse_rom_out),
        ]
        for row_idx, (label, varname, cmd) in enumerate(fields):
            setattr(self, varname, tk.StringVar())
            ttk.Label(tab, text=label).grid(
                row=row_idx, column=0, sticky="w", pady=3, padx=(0, 6))
            ttk.Entry(tab, textvariable=getattr(self, varname), width=55).grid(
                row=row_idx, column=1, sticky="ew", pady=3)
            ttk.Button(tab, text="Browse…", command=cmd, width=9).grid(
                row=row_idx, column=2, padx=(6, 0), pady=3)

        ttk.Label(
            tab,
            text="Adds any JSON entries not yet in the CSV (with blank romanized field),\n"
                 "sorts by productId, and saves a new romanizations_YYYYMMDDHHMMSS.csv.",
            foreground="gray"
        ).grid(row=3, column=0, columnspan=3, sticky="w", pady=(8, 0))

    def _build_dat_tab(self):
        """Scrollable DAT generation tab."""
        outer = ttk.Frame(self.notebook)
        self.notebook.add(outer, text="  DAT  ")
        outer.rowconfigure(0, weight=1)
        outer.columnconfigure(0, weight=1)

        canvas = tk.Canvas(outer, highlightthickness=0)
        canvas.grid(row=0, column=0, sticky="nsew")
        sb = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
        sb.grid(row=0, column=1, sticky="ns")
        canvas.configure(yscrollcommand=sb.set)
        tab = ttk.Frame(canvas, padding=10)
        wid = canvas.create_window((0, 0), window=tab, anchor="nw")
        tab.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>", lambda e: canvas.itemconfig(wid, width=e.width))
        canvas.bind_all("<MouseWheel>", lambda e: canvas.yview_scroll(int(-1*(e.delta/120)), "units"))
        tab.columnconfigure(1, weight=1)

        row = 0
        # Common fields
        ttk.Label(tab, text="Common settings", font=("Segoe UI", 9, "bold")).grid(
            row=row, column=0, columnspan=3, sticky="w", pady=(0, 4))
        row += 1

        common_fields = [
            ("Metadata JSON (data_*.json):", "var_dat_json",   self._browse_dat_json),
            ("Romanizations CSV:",           "var_dat_csv",    self._browse_dat_csv),
            ("Output folder:",               "var_dat_out",    self._browse_dat_out),
        ]
        for label, varname, cmd in common_fields:
            setattr(self, varname, tk.StringVar())
            ttk.Label(tab, text=label).grid(row=row, column=0, sticky="w", pady=3, padx=(0, 6))
            ttk.Entry(tab, textvariable=getattr(self, varname), width=48).grid(
                row=row, column=1, sticky="ew", pady=3)
            ttk.Button(tab, text="Browse…", command=cmd, width=9).grid(
                row=row, column=2, padx=(6, 0), pady=3)
            row += 1

        ttk.Label(tab, text="Author:").grid(row=row, column=0, sticky="w", pady=3, padx=(0, 6))
        self.var_dat_author = tk.StringVar(value="Eggman")
        ttk.Entry(tab, textvariable=self.var_dat_author, width=30).grid(
            row=row, column=1, sticky="w", pady=3)
        row += 1

        ttk.Label(tab, text="7-Zip ZS executable (for ZStandard ZIPs):").grid(
            row=row, column=0, sticky="w", pady=3, padx=(0, 6))
        self.var_dat_7z = tk.StringVar()
        ttk.Entry(tab, textvariable=self.var_dat_7z, width=48).grid(
            row=row, column=1, sticky="ew", pady=3)
        ttk.Button(tab, text="Browse…", command=self._browse_dat_7z, width=9).grid(
            row=row, column=2, padx=(6, 0), pady=3)
        ttk.Label(tab, text="Optional. Only needed if your ZIPs use ZStandard compression.",
                  foreground="gray").grid(row=row+1, column=1, columnspan=2, sticky="w")
        row += 2

        ttk.Separator(tab, orient="horizontal").grid(
            row=row, column=0, columnspan=3, sticky="ew", pady=(8, 8))
        row += 1

        # Downloads (bin)
        self.var_dat_do_dl = tk.BooleanVar(value=True)
        ttk.Checkbutton(tab, text="Downloads (bin) — .bin files inside per-file ZIPs",
                        variable=self.var_dat_do_dl,
                        command=self._dat_toggle_dl).grid(
            row=row, column=0, columnspan=3, sticky="w")
        row += 1
        ttk.Label(tab, text="Download folder (.bin files):").grid(
            row=row, column=0, sticky="w", pady=3, padx=(16, 6))
        self.var_dat_dl_folder = tk.StringVar()
        self.entry_dat_dl = ttk.Entry(tab, textvariable=self.var_dat_dl_folder, width=48)
        self.entry_dat_dl.grid(row=row, column=1, sticky="ew", pady=3)
        self.btn_dat_dl = ttk.Button(tab, text="Browse…",
                                     command=self._browse_dat_dl, width=9)
        self.btn_dat_dl.grid(row=row, column=2, padx=(6, 0), pady=3)
        row += 1
        ttk.Label(tab, text="Downloads ZIPs folder:").grid(
            row=row, column=0, sticky="w", pady=3, padx=(16, 6))
        self.var_dat_dl_zips_folder = tk.StringVar()
        self.entry_dat_dl_zips = ttk.Entry(tab, textvariable=self.var_dat_dl_zips_folder, width=48)
        self.entry_dat_dl_zips.grid(row=row, column=1, sticky="ew", pady=3)
        self.btn_dat_dl_zips = ttk.Button(tab, text="Browse…",
                                          command=self._browse_dat_dl_zips, width=9)
        self.btn_dat_dl_zips.grid(row=row, column=2, padx=(6, 0), pady=3)
        row += 1

        ttk.Separator(tab, orient="horizontal").grid(
            row=row, column=0, columnspan=3, sticky="ew", pady=(8, 8))
        row += 1

        # Games (bin)
        self.var_dat_do_bin = tk.BooleanVar(value=True)
        ttk.Checkbutton(tab, text="Games (bin) — hash .bin files inside Package ZIPs",
                        variable=self.var_dat_do_bin,
                        command=self._dat_toggle_bin).grid(
            row=row, column=0, columnspan=3, sticky="w")
        row += 1
        ttk.Label(tab, text="Packaged ZIPs folder:").grid(
            row=row, column=0, sticky="w", pady=3, padx=(16, 6))
        self.var_dat_bin_folder = tk.StringVar()
        self.entry_dat_bin = ttk.Entry(tab, textvariable=self.var_dat_bin_folder, width=48)
        self.entry_dat_bin.grid(row=row, column=1, sticky="ew", pady=3)
        self.btn_dat_bin = ttk.Button(tab, text="Browse…",
                                      command=self._browse_dat_bin, width=9)
        self.btn_dat_bin.grid(row=row, column=2, padx=(6, 0), pady=3)
        row += 1

        ttk.Separator(tab, orient="horizontal").grid(
            row=row, column=0, columnspan=3, sticky="ew", pady=(8, 8))
        row += 1

        # Games (extracted)
        self.var_dat_do_ext = tk.BooleanVar(value=True)
        ttk.Checkbutton(tab, text="Games (extracted) — hash files inside extracted-content ZIPs",
                        variable=self.var_dat_do_ext,
                        command=self._dat_toggle_ext).grid(
            row=row, column=0, columnspan=3, sticky="w")
        row += 1
        ttk.Label(tab, text="Extracted ZIPs folder:").grid(
            row=row, column=0, sticky="w", pady=3, padx=(16, 6))
        self.var_dat_ext_folder = tk.StringVar()
        self.entry_dat_ext = ttk.Entry(tab, textvariable=self.var_dat_ext_folder, width=48)
        self.entry_dat_ext.grid(row=row, column=1, sticky="ew", pady=3)
        self.btn_dat_ext = ttk.Button(tab, text="Browse…",
                                      command=self._browse_dat_ext, width=9)
        self.btn_dat_ext.grid(row=row, column=2, padx=(6, 0), pady=3)
        row += 1

        ttk.Label(
            tab,
            text="Note: Extracted ZIPs must be created manually by zipping each extracted\n"
                 "game folder. The rom names inside will use the internal ZIP paths.",
            foreground="gray"
        ).grid(row=row, column=0, columnspan=3, sticky="w", pady=(6, 0))

    # ── Romanize browse handlers ──────────────────────────────────────────────

    def _browse_rom_json(self):
        f = filedialog.askopenfilename(title="Select metadata JSON",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")])
        if f: self.var_rom_json.set(f)

    def _browse_rom_csv(self):
        f = filedialog.askopenfilename(title="Select existing romanizations CSV",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
        if f: self.var_rom_csv.set(f)

    def _browse_rom_out(self):
        d = filedialog.askdirectory(title="Select output folder for updated CSV")
        if d: self.var_rom_out.set(d)

    # ── DAT browse + toggle handlers ──────────────────────────────────────────

    def _browse_dat_json(self):
        f = filedialog.askopenfilename(title="Select metadata JSON",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")])
        if f: self.var_dat_json.set(f)

    def _browse_dat_csv(self):
        f = filedialog.askopenfilename(title="Select romanizations CSV",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
        if f: self.var_dat_csv.set(f)

    def _browse_dat_out(self):
        d = filedialog.askdirectory(title="Select DAT output folder")
        if d: self.var_dat_out.set(d)

    def _browse_dat_dl(self):
        d = filedialog.askdirectory(title="Select download folder containing .bin files")
        if d: self.var_dat_dl_folder.set(d)

    def _browse_dat_dl_zips(self):
        d = filedialog.askdirectory(title="Select folder for per-.bin ZIPs")
        if d: self.var_dat_dl_zips_folder.set(d)

    def _browse_dat_bin(self):
        d = filedialog.askdirectory(title="Select folder containing packaged ZIPs")
        if d: self.var_dat_bin_folder.set(d)

    def _browse_dat_ext(self):
        d = filedialog.askdirectory(title="Select folder containing extracted-content ZIPs")
        if d: self.var_dat_ext_folder.set(d)

    def _browse_dat_7z(self):
        f = filedialog.askopenfilename(
            title="Select 7-Zip ZS executable",
            filetypes=[("Executable files", "*.exe"), ("All files", "*.*")]
        )
        if f: self.var_dat_7z.set(f)

    def _dat_toggle_dl(self):
        s = "normal" if self.var_dat_do_dl.get() else "disabled"
        self.entry_dat_dl.config(state=s); self.btn_dat_dl.config(state=s)
        self.entry_dat_dl_zips.config(state=s); self.btn_dat_dl_zips.config(state=s)

    def _dat_toggle_bin(self):
        s = "normal" if self.var_dat_do_bin.get() else "disabled"
        self.entry_dat_bin.config(state=s); self.btn_dat_bin.config(state=s)

    def _dat_toggle_ext(self):
        s = "normal" if self.var_dat_do_ext.get() else "disabled"
        self.entry_dat_ext.config(state=s); self.btn_dat_ext.config(state=s)

    def _build_about_tab(self):
        # Outer frame holds canvas + scrollbar; no padding so they fill edge-to-edge
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="  About  ")
        tab.rowconfigure(0, weight=1)
        tab.columnconfigure(0, weight=1)

        canvas = tk.Canvas(tab, highlightthickness=0)
        canvas.grid(row=0, column=0, sticky="nsew")
        sb = ttk.Scrollbar(tab, orient="vertical", command=canvas.yview)
        sb.grid(row=0, column=1, sticky="ns")
        canvas.configure(yscrollcommand=sb.set)

        # Inner frame carries all the actual content
        inner = ttk.Frame(canvas, padding=20)
        window_id = canvas.create_window((0, 0), window=inner, anchor="nw")

        # Resize the scroll region whenever inner content changes
        def _on_inner_configure(event):
            canvas.configure(scrollregion=canvas.bbox("all"))
        inner.bind("<Configure>", _on_inner_configure)

        # Keep inner frame width matched to canvas width
        def _on_canvas_configure(event):
            canvas.itemconfig(window_id, width=event.width)
        canvas.bind("<Configure>", _on_canvas_configure)

        # Mouse-wheel scrolling
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)

        inner.columnconfigure(0, weight=1)
        row = 0

        ttk.Label(inner, text="Project EGG Tools",
                  font=("Segoe UI", 14, "bold")).grid(
            row=row, column=0, sticky="w", pady=(0, 2))
        row += 1
        ttk.Label(inner, text="by Eggman",
                  font=("Segoe UI", 10, "italic")).grid(
            row=row, column=0, sticky="w", pady=(0, 12))
        row += 1

        ttk.Separator(inner, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=(0, 12))
        row += 1

        about_text = (
            "A GUI toolkit for preserving Project EGG (https://www.amusement-center.com)\n"
            "retro PC game downloads.\n\n"
            "Project EGG is a Japanese digital distribution service for classic PC-8801,\n"
            "PC-9801, MSX, X1, FM-7 and other retro platform games.\n\n"
            "This tool automates the three main preservation workflow steps:\n"
            "  •  Download  —  fetch .bin files from the CDN using your account credentials\n"
            "  •  Package   —  group and ZIP the .bin files into named archives\n"
            "  •  Extract   —  unpack .bin contents via QuickBMS into named game folders"
        )
        ttk.Label(inner, text=about_text, justify="left").grid(
            row=row, column=0, sticky="w", pady=(0, 16))
        row += 1

        ttk.Separator(inner, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=(0, 12))
        row += 1

        link_frame = ttk.Frame(inner)
        link_frame.grid(row=row, column=0, sticky="w")
        ttk.Label(link_frame, text="GitHub: ").pack(side="left")
        link = tk.Label(link_frame,
                        text="https://github.com/Eggmansworld",
                        foreground="#0078d7", cursor="hand2")
        link.pack(side="left")
        link.bind("<Button-1>", lambda e: self._open_url("https://github.com/Eggmansworld"))
        row += 1

        ttk.Separator(inner, orient="horizontal").grid(
            row=row, column=0, sticky="ew", pady=(12, 12))
        row += 1

        credits = (
            "Original deviled-eggs project credits:\n"
            "  Eintei    —  reverse engineering and format documentation\n"
            "  obskyr    —  original data.json download script\n"
            "  Bestest   —  coordination, leadership, reverse engineering\n"
            "  Hiccup    —  datting support and advice\n"
            "  proffrink —  scraping, research\n"
            "  Shadów    —  reverse engineering\n"
            "  Icyelut   —  original script and romanization\n\n"
            "Original source: https://github.com/Icyelut/deviled-eggs"
        )
        ttk.Label(inner, text=credits, justify="left",
                  foreground="gray").grid(row=row, column=0, sticky="w")

    def _open_url(self, url):
        import webbrowser
        webbrowser.open(url)

    # ── Browse handlers ───────────────────────────────────────────────────────

    def _browse_dl_dest(self):
        d = filedialog.askdirectory(title="Select download destination folder")
        if d:
            self.var_dl_dest.set(d)

    def _browse_existing_json(self):
        f = filedialog.askopenfilename(
            title="Select existing metadata JSON",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if f:
            self.var_existing_json.set(f)

    def _browse_pkg_download(self):
        d = filedialog.askdirectory(title="Select folder containing .bin files")
        if d:
            self.var_pkg_download.set(d)

    def _browse_pkg_json(self):
        f = filedialog.askopenfilename(
            title="Select metadata JSON",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if f:
            self.var_pkg_json.set(f)

    def _browse_pkg_output(self):
        d = filedialog.askdirectory(title="Select output folder for ZIPs")
        if d:
            self.var_pkg_output.set(d)

    def _browse_qbms_exe(self):
        f = filedialog.askopenfilename(
            title="Select QuickBMS executable",
            filetypes=[("Executable files", "*.exe"), ("All files", "*.*")]
        )
        if f:
            self.var_qbms_exe.set(f)

    def _browse_bms_script(self):
        f = filedialog.askopenfilename(
            title="Select BMS script",
            filetypes=[("BMS scripts", "*.bms"), ("All files", "*.*")]
        )
        if f:
            self.var_bms_script.set(f)

    def _browse_ext_source(self):
        d = filedialog.askdirectory(title="Select folder containing .bin files")
        if d:
            self.var_ext_source.set(d)

    def _browse_ext_json(self):
        f = filedialog.askopenfilename(
            title="Select metadata JSON",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if f:
            self.var_ext_json.set(f)

    def _browse_ext_output(self):
        d = filedialog.askdirectory(title="Select output folder for extracted games")
        if d:
            self.var_ext_output.set(d)

    def _preview_bin(self):
        """
        Let the user pick one or more .bin files and show the correct
        (Shift-JIS decoded) filenames from each archive header in the log.
        """
        files = filedialog.askopenfilenames(
            title="Select .bin file(s) to preview",
            filetypes=[("BIN files", "*.bin"), ("All files", "*.*")]
        )
        if not files:
            return

        self._append_log("")
        self._append_log("── Preview: filenames inside .bin ────────")
        for bin_path in files:
            self._append_log(f"  {pathlib.Path(bin_path).name}")
            names = read_bin_filenames(bin_path)
            if names:
                for n in names:
                    self._append_log(f"    {n}")
            else:
                self._append_log("    (not a recognised Project EGG .bin, or no filenames)")
        self._append_log("─────────────────────────────────────────")

    def _toggle_existing_json(self):
        state = "normal" if self.var_use_existing_json.get() else "disabled"
        self.entry_existing_json.config(state=state)
        self.btn_browse_existing_json.config(state=state)

    # ── Tab switching ─────────────────────────────────────────────────────────

    def _on_tab_change(self):
        labels = ["▶  Start Download", "▶  Run Packager", "▶  Extract Games", "▶  Update CSV", "▶  Generate DATs", ""]
        tab = self._active_tab()
        if tab is not None:
            self.btn_run.config(text=labels[tab])

    def _active_tab(self):
        selected = self.notebook.select()
        if not selected:
            return None
        return self.notebook.index(selected)

    # ── Validation ────────────────────────────────────────────────────────────

    def _validate_download(self):
        errors = []
        use_existing = self.var_use_existing_json.get()

        if not use_existing:
            if not DOWNLOAD_AVAILABLE:
                errors.append("Required libraries missing.\nRun:  pip install requests pytz")
            if not self.var_username.get().strip():
                errors.append("Username is required.")
            if not self.var_password.get().strip():
                errors.append("Password is required.")
        else:
            js = self.var_existing_json.get().strip()
            if not js:
                errors.append("Select an existing JSON file, or uncheck that option.")
            elif not pathlib.Path(js).is_file():
                errors.append(f"JSON file not found:\n  {js}")

        if not self.var_dl_dest.get().strip():
            errors.append("Download folder is required.")

        return errors

    def _validate_romanize(self):
        errors = []
        js  = self.var_rom_json.get().strip()
        out = self.var_rom_out.get().strip()
        if not js:
            errors.append("Metadata JSON is required.")
        elif not pathlib.Path(js).is_file():
            errors.append(f"JSON file not found:\n  {js}")
        if not out:
            errors.append("Output folder is required.")
        return errors

    def _validate_dat(self):
        errors = []
        js  = self.var_dat_json.get().strip()
        out = self.var_dat_out.get().strip()
        if not js:
            errors.append("Metadata JSON is required.")
        elif not pathlib.Path(js).is_file():
            errors.append(f"JSON file not found:\n  {js}")
        if not out:
            errors.append("Output folder is required.")

        do_dl  = self.var_dat_do_dl.get()
        do_bin = self.var_dat_do_bin.get()
        do_ext = self.var_dat_do_ext.get()
        if not (do_dl or do_bin or do_ext):
            errors.append("Select at least one DAT type to generate.")
        if do_dl:
            d = self.var_dat_dl_folder.get().strip()
            if not d: errors.append("Download folder is required for Downloads (bin) DAT.")
            elif not pathlib.Path(d).is_dir(): errors.append(f"Download folder not found:\n  {d}")
            z = self.var_dat_dl_zips_folder.get().strip()
            if not z: errors.append("Downloads ZIPs folder is required for Downloads (bin) DAT.")
        if do_bin:
            d = self.var_dat_bin_folder.get().strip()
            if not d: errors.append("Packaged ZIPs folder is required for Games (bin) DAT.")
            elif not pathlib.Path(d).is_dir(): errors.append(f"Packaged ZIPs folder not found:\n  {d}")
        if do_ext:
            d = self.var_dat_ext_folder.get().strip()
            if not d: errors.append("Extracted ZIPs folder is required for Games (extracted) DAT.")
            elif not pathlib.Path(d).is_dir(): errors.append(f"Extracted ZIPs folder not found:\n  {d}")
        return errors

    def _validate_extract(self):
        errors = []
        exe  = self.var_qbms_exe.get().strip()
        bms  = self.var_bms_script.get().strip()
        src  = self.var_ext_source.get().strip()
        js   = self.var_ext_json.get().strip()
        out  = self.var_ext_output.get().strip()

        if not exe:
            errors.append("QuickBMS executable is required.")
        elif not pathlib.Path(exe).is_file():
            errors.append(f"QuickBMS executable not found:\n  {exe}")

        if not bms:
            errors.append("BMS script is required.")
        elif not pathlib.Path(bms).is_file():
            errors.append(f"BMS script not found:\n  {bms}")

        if not src:
            errors.append("Source .bin folder is required.")
        elif not pathlib.Path(src).is_dir():
            errors.append(f"Source folder does not exist:\n  {src}")

        if not js:
            errors.append("Metadata JSON is required.")
        elif not pathlib.Path(js).is_file():
            errors.append(f"JSON file not found:\n  {js}")

        if not out:
            errors.append("Output folder is required.")
        elif src and pathlib.Path(out).resolve() == pathlib.Path(src).resolve():
            errors.append("Output folder must be different from the source .bin folder.")

        return errors

    def _validate_package(self):
        errors = []
        dl = self.var_pkg_download.get().strip()
        js = self.var_pkg_json.get().strip()
        op = self.var_pkg_output.get().strip()

        if not dl:
            errors.append("Download folder is required.")
        elif not pathlib.Path(dl).is_dir():
            errors.append(f"Download folder does not exist:\n  {dl}")

        if not js:
            errors.append("Metadata JSON is required.")
        elif not pathlib.Path(js).is_file():
            errors.append(f"JSON file not found:\n  {js}")

        if not op:
            errors.append("Output folder is required.")
        elif dl and pathlib.Path(op).resolve() == pathlib.Path(dl).resolve():
            errors.append("Output folder must be different from the download folder.")

        return errors

    # ── Run / Cancel ──────────────────────────────────────────────────────────

    def _start(self):
        tab    = self._active_tab()
        if tab == 0:
            errors = self._validate_download()
        elif tab == 1:
            errors = self._validate_package()
        elif tab == 2:
            errors = self._validate_extract()
        elif tab == 3:
            errors = self._validate_romanize()
        elif tab == 4:
            errors = self._validate_dat()
        else:
            errors = []
        if errors:
            messagebox.showerror("Invalid input", "\n\n".join(errors))
            return

        self._cancel_flag.clear()
        self.btn_run.config(state="disabled")
        self.btn_cancel.config(state="normal")
        self.progress["value"] = 0
        self.lbl_progress.config(text="")

        if tab == 0:
            use_existing = self.var_use_existing_json.get()
            self._worker = threading.Thread(
                target=run_downloader,
                args=(
                    self.var_username.get().strip(),
                    self.var_password.get().strip(),
                    self.var_dl_dest.get().strip(),
                    self.var_existing_json.get().strip() if use_existing else None,
                    self._enqueue_log, self._update_progress,
                    self._on_done, self._cancel_flag
                ),
                daemon=True
            )
        elif tab == 1:
            op = self.var_pkg_output.get().strip()
            pathlib.Path(op).mkdir(parents=True, exist_ok=True)
            self._worker = threading.Thread(
                target=run_packager,
                args=(
                    self.var_pkg_download.get().strip(),
                    self.var_pkg_json.get().strip(),
                    op,
                    self._enqueue_log, self._update_progress,
                    self._on_done, self._cancel_flag
                ),
                daemon=True
            )
        elif tab == 2:
            ext_out = self.var_ext_output.get().strip()
            pathlib.Path(ext_out).mkdir(parents=True, exist_ok=True)
            self._worker = threading.Thread(
                target=run_extractor,
                args=(
                    self.var_qbms_exe.get().strip(),
                    self.var_bms_script.get().strip(),
                    self.var_ext_source.get().strip(),
                    self.var_ext_json.get().strip(),
                    ext_out,
                    self._enqueue_log, self._update_progress,
                    self._on_done, self._cancel_flag
                ),
                daemon=True
            )
        elif tab == 3:
            rom_out = self.var_rom_out.get().strip()
            pathlib.Path(rom_out).mkdir(parents=True, exist_ok=True)
            self._worker = threading.Thread(
                target=run_romanize,
                args=(
                    self.var_rom_json.get().strip(),
                    self.var_rom_csv.get().strip(),
                    rom_out,
                    self._enqueue_log, self._update_progress,
                    self._on_done, self._cancel_flag
                ),
                daemon=True
            )
        else:
            dat_out = self.var_dat_out.get().strip()
            pathlib.Path(dat_out).mkdir(parents=True, exist_ok=True)
            csv_p = self.var_dat_csv.get().strip()

            # Check if Downloads ZIPs need to be created
            zip_dl_first = False
            if self.var_dat_do_dl.get():
                zips_dir = pathlib.Path(self.var_dat_dl_zips_folder.get().strip())
                has_zips = zips_dir.is_dir() and any(
                    f.suffix.lower() == ".zip" for f in zips_dir.iterdir()
                    if f.is_file()
                ) if zips_dir.is_dir() else False
                if not has_zips:
                    zip_dl_first = messagebox.askyesno(
                        "No ZIPs found",
                        f"No ZIP files were found in the Downloads ZIPs folder:\n"
                        f"  {zips_dir}\n\n"
                        f"Would you like to create a ZIP for each .bin file now?\n"
                        f"(Store compression — no re-compression of the .bin data)"
                    )
                    if not zip_dl_first:
                        # User said No — abort rather than generate an empty DAT
                        self.btn_run.config(state="normal")
                        self.btn_cancel.config(state="disabled")
                        return

            self._worker = threading.Thread(
                target=run_dat_generator,
                args=(
                    self.var_dat_json.get().strip(),
                    csv_p,
                    self.var_dat_author.get().strip() or "Eggman",
                    dat_out,
                    self.var_dat_do_dl.get(),  self.var_dat_dl_folder.get().strip(),
                    self.var_dat_dl_zips_folder.get().strip(), zip_dl_first,
                    self.var_dat_do_bin.get(), self.var_dat_bin_folder.get().strip(),
                    self.var_dat_do_ext.get(), self.var_dat_ext_folder.get().strip(),
                    self.var_dat_7z.get().strip(),
                    self._enqueue_log, self._update_progress,
                    self._on_done, self._cancel_flag
                ),
                daemon=True
            )

        self._worker.start()

    def _cancel(self):
        self._cancel_flag.set()
        self.btn_cancel.config(state="disabled")

    # ── Log / Progress ────────────────────────────────────────────────────────

    def _enqueue_log(self, msg):
        self._log_queue.put(msg)

    def _poll_log(self):
        try:
            while True:
                self._append_log(self._log_queue.get_nowait())
        except queue.Empty:
            pass
        self.after(100, self._poll_log)

    def _append_log(self, msg):
        self.log_box.config(state="normal")
        self.log_box.insert("end", msg + "\n")
        self.log_box.see("end")
        self.log_box.config(state="disabled")

    def _clear_log(self):
        self.log_box.config(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.config(state="disabled")

    def _update_progress(self, current, total):
        pct = int((current / total) * 100) if total else 0
        self.after(0, lambda: (
            self.progress.configure(value=pct),
            self.lbl_progress.configure(text=f"{current} / {total}")
        ))

    def _on_done(self, summary):
        self.after(0, lambda: self._finish(summary))

    def _finish(self, summary):
        self.btn_run.config(state="normal")
        self.btn_cancel.config(state="disabled")
        self.progress["value"] = 100 if summary else 0

        if summary is None:
            self._append_log("── Finished with errors. ──")
            return

        if "dats" in summary:
            messagebox.showinfo(
                "DAT generation complete",
                f"DAT files written: {summary['dats']}"
            )
        elif "total" in summary:
            messagebox.showinfo(
                "Romanize complete",
                f"Total rows  : {summary['total']}\n"
                f"New entries : {summary['added']}\n"
                f"Saved to    : {pathlib.Path(summary['path']).name}"
            )
        elif "downloaded" in summary:
            messagebox.showinfo(
                "Download complete",
                f"Downloaded (new/updated) : {summary['downloaded']}\n"
                f"Already current (skipped): {summary['skipped']}\n"
                f"Failed                   : {summary['failed']}\n\n"
                f"Metadata JSON: {pathlib.Path(summary['json_path']).name}"
            )
        elif "extracted" in summary:
            messagebox.showinfo(
                "Extraction complete",
                f"Games extracted         : {summary['extracted']}\n"
                f"Already exist (skipped) : {summary['skipped_exists']}\n"
                f"No files on disk        : {summary['skipped_no_files']}\n"
                f"Failed / partial errors : {summary['failed']}\n"
                f"No metadata (skipped)   : {summary['no_metadata']}"
            )
        else:
            messagebox.showinfo(
                "Packaging complete",
                f"ZIPs created          : {summary['created']}\n"
                f"  (incomplete)        : {summary['incomplete']}\n"
                f"Already exist         : {summary['skipped']}\n"
                f"No metadata (skipped) : {summary['no_metadata']}\n"
                f"Missing from disk     : {summary['missing']}\n"
                f"Unprocessed .bin files: {summary['unprocessed']}"
            )

    def _save_log(self):
        prefixes = ["egg_download_log_", "egg_packager_log_", "egg_extract_log_", "egg_romanize_log_", "egg_dat_log_"]
        tab      = self._active_tab()
        prefix   = prefixes[tab] if tab < len(prefixes) else "egg_log_"
        default  = prefix + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".txt"
        filepath = filedialog.asksaveasfilename(
            title="Save log",
            initialfile=default,
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        if not filepath:
            return
        try:
            with open(filepath, "w", encoding="utf-8-sig") as f:
                f.write(self.log_box.get("1.0", "end"))
        except Exception as e:
            messagebox.showerror("Save failed", str(e))


# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = App()
    app.mainloop()
