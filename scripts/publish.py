#!/usr/bin/env python3
"""
Pushes the digest to GitHub as ONE commit, through the Git Data API.
Standard library only; git does not need to be installed on the PC.

    python scripts/publish.py               push data/*
    python scripts/publish.py --bootstrap   also push scripts/, the workflow
                                            and the PC docs (first run)

Environment:
    GITHUB_TOKEN        fine-grained PAT for the feed repo with
                        Contents: Read and write  (and Workflows: Read and
                        write if you use --bootstrap, because it touches
                        .github/workflows/universe.yml)
    HTF_FEED_REPO       default tyim87/htf-feed
    HTF_FEED_BRANCH     default main

feed-config.txt itself is never uploaded.
"""
from __future__ import annotations

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = os.environ.get("HTF_FEED_REPO", "tyim87/htf-feed")
BRANCH = os.environ.get("HTF_FEED_BRANCH", "main")
TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
API = "https://api.github.com"

DATA_FILES = ["data/universe.csv", "data/bars.csv.gz", "data/weekly.csv.gz", "data/scan.csv",
              "data/insiders.csv", "data/profiles.csv", "data/STATUS.txt"]
BOOTSTRAP_FILES = ["scripts/kiwoom_source.py", "scripts/build_universe_kiwoom.py", "scripts/publish.py",
                   "scripts/build_insiders.py", "workflow/universe.yml",
                   "run_feed.bat", "bootstrap_first_run.bat", "install_task.bat", "feed-config.example.txt", "README-PC.md"]
# local path -> path in the repo (the PC keeps the workflow outside .github/,
# which desktop tooling treats as a protected folder)
REPO_PATH = {"workflow/universe.yml": ".github/workflows/universe.yml"}


def api(method, path, body=None):
    req = urllib.request.Request(API + path, method=method,
                                 headers={"Authorization": "Bearer " + TOKEN, "Accept": "application/vnd.github+json",
                                          "User-Agent": "htf-feed-publish", "X-GitHub-Api-Version": "2022-11-28"},
                                 data=json.dumps(body).encode() if body is not None else None)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    last = None
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                return json.loads(r.read().decode() or "{}")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:300]
            if exc.code in (500, 502, 503, 504) and attempt < 4:
                time.sleep(5 * (attempt + 1))
                continue
            raise SystemExit("GitHub %s %s -> HTTP %d: %s" % (method, path, exc.code, detail))
        except (TimeoutError, OSError, urllib.error.URLError) as exc:   # timeouts, resets, DNS blips
            last = exc
            print("  GitHub %s %s: %r - retrying (%d/5)" % (method, path, exc, attempt + 1), flush=True)
            time.sleep(10 * (attempt + 1))
    raise SystemExit("GitHub API unreachable after 5 attempts: %r" % (last,))


def main():
    if not TOKEN:
        raise SystemExit("GITHUB_TOKEN not set (see feed-config.example.txt)")
    files = list(DATA_FILES) + (BOOTSTRAP_FILES if "--bootstrap" in sys.argv else [])
    files = [f for f in files if os.path.exists(os.path.join(ROOT, f))]
    if not any(f.startswith("data/") for f in files):
        raise SystemExit("nothing to publish - data/ is empty")

    ref = api("GET", "/repos/%s/git/ref/heads/%s" % (REPO, BRANCH))
    head = ref["object"]["sha"]
    base_tree = api("GET", "/repos/%s/git/commits/%s" % (REPO, head))["tree"]["sha"]

    entries = []
    for f in files:
        with open(os.path.join(ROOT, f), "rb") as fh:
            raw = fh.read()
        blob = api("POST", "/repos/%s/git/blobs" % REPO, {"content": base64.b64encode(raw).decode(), "encoding": "base64"})
        entries.append({"path": REPO_PATH.get(f, f).replace("\\", "/"), "mode": "100644", "type": "blob", "sha": blob["sha"]})
        print("  %-40s %8d bytes" % (f, len(raw)), flush=True)

    tree = api("POST", "/repos/%s/git/trees" % REPO, {"base_tree": base_tree, "tree": entries})
    status = {}
    try:
        for line in open(os.path.join(ROOT, "data", "STATUS.txt")):
            if "=" in line:
                k, v = line.strip().split("=", 1)
                status[k] = v
    except OSError:
        pass
    msg = "universe: %s (kiwoom, %s survivors, latest bar %s)" % (
        time.strftime("%Y-%m-%d"), status.get("survivors", "?"), status.get("latest_bar", "?"))
    if "--bootstrap" in sys.argv:
        msg = "PC feed runner: Kiwoom source + full scan; Action schedule off\n\n" + msg
    commit = api("POST", "/repos/%s/git/commits" % REPO, {"message": msg, "tree": tree["sha"], "parents": [head]})
    api("PATCH", "/repos/%s/git/refs/heads/%s" % (REPO, BRANCH), {"sha": commit["sha"], "force": False})
    print("pushed %s -> %s@%s" % (commit["sha"][:10], REPO, BRANCH), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
