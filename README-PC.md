# HTF feed — PC runner (Kiwoom source, full US scan)

The nightly universe digest now builds on **this PC** instead of GitHub Actions,
because Kiwoom's REST API only answers the PC IP registered on
openapi.kiwoom.com (any other IP gets error 8010). The screen in the cloud
keeps reading `data/*` from `github.com/tyim87/htf-feed` exactly as before —
this runner just pushes those files.

What changes versus the old Action feed:

| | old (Action, yfinance) | new (PC, Kiwoom) |
|---|---|---|
| symbols scanned | ~6,900, 800 shipped | ~7,800 non-ETF, **every survivor shipped** (`MAX_ROWS=0`) |
| missing-day problem | frequent (yfinance lag) | none — bars come straight from Kiwoom after the close |
| `scan.csv` | — | every symbol with the pre-filter it failed |
| extra packages | pandas, numpy, yfinance | **none** (standard library only) |

## One-time setup (≈10 minutes)

1. Unzip this folder somewhere permanent, e.g. `C:\htf-feed\`. Keep the folder
   layout (`scripts\`, `run_feed.bat`, …).
2. Copy `feed-config.example.txt` → `feed-config.txt` and fill in:
   - `KIWOOM_APP_KEY`, `KIWOOM_APP_SECRET` — same values the smoke test accepted.
   - `GITHUB_TOKEN` — GitHub → Settings → Developer settings → Fine-grained
     tokens → *Generate new token*: repository access **only `htf-feed`**,
     permissions **Contents: Read and write** and **Workflows: Read and write**
     (Workflows is only needed for the first `--bootstrap` push).
   - `SEC_CONTACT` — the contact email the SEC requires on EDGAR requests
     (same address you put in the repo secret).
3. **First run, by hand** — open a Command Prompt in the folder and run:

       run_feed.bat --bootstrap

   `--bootstrap` also pushes these scripts and turns the GitHub Action's
   schedule off (otherwise the Action would overwrite the Kiwoom data every
   morning with the old 800-name yfinance digest). Watch `logs\feed_YYYYMMDD.log`;
   a full run takes roughly **60–90 minutes** at Kiwoom's ~3 requests/second.
   A crash or Ctrl+C can be resumed by re-running: progress is cached in `cache\`.
4. Register the schedule: double-click `install_task.bat` (06:20 KST, Tue–Sat,
   i.e. after each US close). The PC must be on or waking at that time —
   Settings → Power → *allow wake timers* if it sleeps overnight.

## Daily flow

    06:20 KST  run_feed.bat  →  build_universe_kiwoom.py   (Kiwoom bars, full scan)
                              →  build_insiders.py         (SEC EDGAR Form 4s)
                              →  publish.py                (one commit to htf-feed)
    09:00 KST  the screen (cloud) reads data/* from the repo and republishes the report

Check `data\STATUS.txt` after a run: `latest_bar` should be the last US session,
`latest_bar_coverage` close to `survivors/survivors`, and `kiwoom_errors` small.

## Tuning (`feed-config.txt`)

- `PAGES_FULL=3` → ~300 daily bars (≈14 months, 60+ weekly bars for Stine's
  laws) for survivors. `5` gives 2 years at the cost of ~30 min more.
- `MAX_ROWS=0` ships every survivor; set e.g. `1500` to cap the digest size.
- `KIWOOM_RPS=3.0` — raise cautiously; the smoke test throttled at ~3.8/s.
- `ENRICH=1` uses yfinance for sector/industry/summary **if** it is installed
  (`pip install yfinance` — optional; needs pandas/numpy wheels for your
  Python). Without it, Kiwoom's industry label and the SEC SIC description from
  `profiles.csv` drive the theme layer.

## Manual commands

    python scripts\kiwoom_source.py NVDA ND 2        fetch 200 bars of one name (sanity check)
    set SMOKE_TEST=1 && run_feed.bat                 150-symbol quick run
    schtasks /Run /TN "HTF feed (Kiwoom)"            trigger the scheduled task now
    schtasks /Delete /TN "HTF feed (Kiwoom)" /F      remove it

`feed-config.txt` holds secrets — never upload it or paste it into chat. `publish.py`
never sends it.
