#!/usr/bin/env python3
"""Fetch every RSS feed in feeds.txt, and the GOV.UK watch list, and merge them into one CSV.

The GOV.UK part reads the topic names in govuk_topics.txt, rebuilds GOV.UK's topic tree
live from the Content API, and pulls the newest content across those topics from the
Search API. It is treated as one more source, so it shares the date window, the saved
history and the status panel with the news feeds.

Writes three files to the output folder (default: site/):
  news_feeds.csv      one row per article, with duplicates across feeds merged
  articles.json       the same rows without descriptions, for the preview page
  status.json         what happened to each source, for the status panel

It also keeps a saved copy of the CSV (default: data/news_feeds.csv). Each run
starts from that copy, adds what the feeds hold today, and drops anything older than
the date window. So an article stays in the CSV for the whole window even after the
feed it came from has moved on, and a feed that fails one morning does not lose the
articles it supplied earlier.

Uses only the Python standard library, so there is nothing to install.
"""

import argparse
import csv
import gzip
import html
import json
import re
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlencode, urlparse

ROOT = Path(__file__).resolve().parent.parent
USER_AGENT = "Mozilla/5.0 (compatible; feed-collector/1.0)"
TIMEOUT = 30          # seconds per request
ATTEMPTS = 3          # tries per feed before giving up
WORKERS = 2           # feeds fetched at once; kept low so the Guardian does not rate-limit us
INCLUDE_DESCRIPTION = True   # set to False to leave article summaries out
KEEP_HISTORY = True          # set to False to start from nothing on every run
CSV_NAME = "news_feeds.csv"                        # the file published on the site
ARCHIVE = ROOT / "data" / "news_feeds.csv"         # the saved copy that each run builds on
# The saved copy used to be called guardian_feeds.csv. If the new file does not exist yet,
# the first run starts from the old one, so nothing gathered so far is lost.
LEGACY_ARCHIVE = ROOT / "data" / "guardian_feeds.csv"
# Longest summary kept, in characters. Yahoo News puts whole articles in its summaries, and
# a spreadsheet cell cannot hold more than about 32,000 characters. Use None for no limit.
MAX_DESCRIPTION = 1000

# GOV.UK watch list. The topic names to watch are in govuk_topics.txt, one per line, written
# exactly as GOV.UK titles them. Delete that file (or run with --no-govuk) to leave GOV.UK out.
GOVUK_TOPICS = ROOT / "govuk_topics.txt"
GOVUK_CONTENT = "https://www.gov.uk/api/content"       # Content API: the topic tree
GOVUK_SEARCH = "https://www.gov.uk/api/search.json"    # Search API: the content itself
GOVUK_MAX_RESULTS = 1500                               # newest first, across all the topics
# The 20 top-level branches of GOV.UK's topic tree. The Content API returns each one's whole
# subtree in a single call, so this is 20 requests, not one per topic.
GOVUK_BRANCHES = [
    "health-and-social-care", "money", "crime-justice-and-law", "defence-and-armed-forces",
    "work", "life-circumstances", "welfare", "housing-local-and-community",
    "society-and-culture", "international", "regional-and-local-government", "government/all",
    "environment", "business-and-industry", "transport", "education", "going-and-being-abroad",
    "entering-staying-uk", "childcare-parenting", "corporate-information",
]
GOVUK_FIELDS = [
    "title", "description", "link", "public_timestamp", "format", "organisations", "taxons",
    "content_id", "content_store_document_type",
]

# Date filter. Articles published before the cut-off are left out of the CSV.
# MAX_AGE_DAYS is a rolling window that moves with the day you run the script.
# With 5, a run on 14 October keeps everything from 9 October (00:00 UTC) onwards.
# Use None to switch it off.
MAX_AGE_DAYS = 5
# START_DATE is a fixed day (YYYY-MM-DD) that never moves, for example "2026-10-01".
# Leave it as "" to use only the rolling window. If both are set, the later cut-off applies.
START_DATE = ""

NS = {
    "dc": "http://purl.org/dc/elements/1.1/",
    "media": "http://search.yahoo.com/mrss/",
}

COLUMNS = [
    "title", "link", "source", "published", "author", "categories", "description",
    "image_url", "source_feeds", "feed_count", "guid",
]


def read_feeds(path):
    """Read feed URLs, one per line. Blank lines and lines starting # are skipped."""
    urls = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and line not in urls:
            urls.append(line)
    return urls


def unpack(body):
    """Undo gzip compression, which some sites send whether or not it was asked for,
    and drop any blank space or byte order mark ahead of the XML."""
    if body[:2] == b"\x1f\x8b":
        try:
            body = gzip.decompress(body)
        except (OSError, EOFError):
            pass
    return body.lstrip(b"\xef\xbb\xbf \t\r\n")


RSS_ACCEPT = "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5"


def fetch(url, accept=RSS_ACCEPT):
    """Return (bytes, None) on success or (None, message) on failure."""
    last = "unknown error"
    for attempt in range(1, ATTEMPTS + 1):
        wait = 2 * attempt  # seconds to pause before the next try
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": USER_AGENT,
                "Accept": accept,
                "Accept-Encoding": "gzip",
            })
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                return unpack(resp.read()), None
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
            if e.code not in (429, 500, 502, 503, 504):
                break  # 403, 404 and the like will not improve on a retry
            if e.code == 429:
                # "Too many requests": wait as long as the server asks, else back off.
                try:
                    wait = min(int(e.headers.get("Retry-After", "")), 60)
                except ValueError:
                    wait = 15 * attempt
        except OSError as e:  # includes URLError, timeouts and dropped connections
            last = str(getattr(e, "reason", e))
        if attempt < ATTEMPTS:
            time.sleep(wait)
    return None, last


def clean(text):
    """Strip HTML tags, decode entities and collapse whitespace."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def to_iso(value):
    """Turn an RSS date (RFC 822) or ISO date into UTC ISO 8601, or '' if unreadable."""
    value = (value or "").strip()
    if not value:
        return ""
    try:
        moment = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        try:
            moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return ""
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def child_text(element, tag):
    found = element.find(tag, NS)
    return (found.text or "").strip() if found is not None else ""


def shorten(text, limit):
    """Cut text to about `limit` characters at a word boundary, adding '...' when it is cut."""
    if not limit or len(text) <= limit:
        return text
    return text[:limit].rsplit(" ", 1)[0].rstrip(" ,;:.") + "..."


def tidy_description(raw_html):
    """Turn a summary into plain text, minus the stock endings sites add to every one."""
    # Google News fills the summary with a list of related headlines, so it is left blank.
    if raw_html.lstrip().startswith("<ol>"):
        return ""
    # FE News: a last paragraph reading "<title> was published on FE News by <author>".
    raw_html = re.sub(r"<p>(?:(?!</p>).)*? was published on (?:(?!</p>).)*</p>\s*$", "", raw_html, flags=re.S)
    text = clean(raw_html)
    text = re.sub(r"\s*Continue reading\.\.\.$", "", text)                                   # the Guardian
    text = re.sub(r"\s*The post .*? appeared first on [^.]*\.?\s*$", "", text, flags=re.S)  # many WordPress sites
    return text.strip()


def clean_category(text):
    """Tidy one category label. The Telegraph's internal tags ('structure:business',
    'storytype:standard') are dropped, and its topics ('topics:things/uk-economy')
    become plain words ('uk economy'). Other labels are left as they are."""
    text = html.unescape(text).strip()   # the Independent's CDATA labels arrive as 'News &amp; Advice'
    # Yahoo News adds housekeeping tags such as 'site|news' and 'region|US'; these are dropped.
    # Any other label with a '|' in it has it swapped for '/', because ' | ' separates the
    # categories in the CSV.
    if "|" in text:
        if text.partition("|")[0].strip() in ("site", "region", "language", "provider_name", "author_name"):
            return ""
        text = text.replace("|", "/")
    prefix, colon, rest = text.partition(":")
    if colon and prefix in ("structure", "storytype"):
        return ""
    if colon and prefix == "topics":
        return rest.rsplit("/", 1)[-1].replace("-", " ").strip()
    return text


def author_of(item):
    """Names from <dc:creator> (one or more), else from <author>.
    An <author> that holds only an email address is ignored; 'a@b.com (Name)' gives 'Name'."""
    names = [e.text.strip() for e in item.findall("dc:creator", NS) if e.text and e.text.strip()]
    if not names:
        raw = child_text(item, "author")
        match = re.match(r"^\S+@\S+\s*\((.+)\)$", raw)
        if match:
            names = [match.group(1).strip()]
        elif raw and "@" not in raw:
            names = [raw]
    return "; ".join(names)


def best_image(item, summary_html=""):
    """Pick the widest media:content or media:thumbnail image, else an enclosure,
    else the first picture inside the summary (WordPress sites put it there)."""
    best_url, best_width = "", -1
    for node in item.findall("media:content", NS) + item.findall("media:thumbnail", NS):
        url = node.get("url", "")
        try:
            width = int(node.get("width", 0))
        except ValueError:
            width = 0
        if url and width > best_width:
            best_url, best_width = url, width
    if not best_url:
        enclosure = item.find("enclosure")
        if enclosure is not None:
            best_url = enclosure.get("url", "")
    if not best_url:
        found = re.search(r"""<img\b[^>]*?\ssrc=["']([^"']+)["']""", summary_html)
        if found:
            best_url = html.unescape(found.group(1))
    return best_url


def parse_feed(data):
    """Return (feed title, list of item dicts) from RSS 2.0 bytes."""
    root = ET.fromstring(data)
    channel = root.find("channel")
    if channel is None:
        raise ValueError("no <channel> element, so this is not an RSS feed")
    items = []
    for node in channel.findall("item"):
        link = child_text(node, "link")
        guid = child_text(node, "guid")
        if not (link or guid):
            continue
        title = clean(child_text(node, "title"))
        author = author_of(node)
        # News aggregators (Google News, Yahoo News) name the original publisher in <source>.
        # It becomes the author if there is none, or goes in brackets after the author's name.
        publisher = clean(child_text(node, "source"))
        if publisher:
            if title.endswith(" - " + publisher):   # Google News adds the publisher to every headline
                title = title[: -len(publisher) - 3].rstrip()
            if not author:
                author = publisher
            elif publisher.lower() not in author.lower():
                author = f"{author} ({publisher})"
        items.append({
            "title": title,
            "link": link,
            "guid": guid or link,
            "published": to_iso(child_text(node, "pubDate")) or to_iso(child_text(node, "dc:date")),
            "author": author,
            # Category labels, plus the Express's comma-separated <media:keywords> tags.
            "categories": [c for c in (clean_category(e.text) for e in node.findall("category") if e.text) if c]
                          + [k.strip() for k in child_text(node, "media:keywords").split(",") if k.strip()],
            "description": shorten(tidy_description(child_text(node, "description")), MAX_DESCRIPTION),
            "image_url": best_image(node, child_text(node, "description")),
        })
    return clean(child_text(channel, "title")), items


def process(url):
    data, error = fetch(url)
    if error:
        return {"url": url, "title": "", "items": [], "error": error}
    try:
        title, items = parse_feed(data)
    except (ET.ParseError, ValueError) as e:
        # Show how the response began, so a block page or other non-RSS reply is easy to spot.
        start = re.sub(r"\s+", " ", data[:100].decode("utf-8", "replace")).strip()
        return {"url": url, "title": "", "items": [], "error": f"could not read as RSS: {e}. The response began: {start!r}"}
    return {"url": url, "title": title, "items": items, "error": ""}


# ---- GOV.UK ----------------------------------------------------------------------------

def get_json(url):
    """Fetch a GOV.UK API address and return the decoded JSON. Raises RuntimeError on failure."""
    data, error = fetch(url, accept="application/json")
    if error:
        raise RuntimeError(error)
    try:
        return json.loads(data)
    except ValueError as e:
        raise RuntimeError(f"not valid JSON ({e})")


def walk_taxons(node, names):
    """Record the title of this topic, and of every topic beneath it (id -> title)."""
    if not isinstance(node, dict) or not node.get("content_id"):
        return
    names[node["content_id"]] = node.get("title") or "(untitled)"
    for child in (node.get("links") or {}).get("child_taxons") or []:
        walk_taxons(child, names)


def load_taxonomy():
    """Rebuild GOV.UK's topic tree. Returns (id -> title, the branches that failed to load)."""
    names, failed = {}, []

    def one(branch):
        try:
            return branch, get_json(f"{GOVUK_CONTENT}/{branch}"), ""
        except RuntimeError as e:
            return branch, None, str(e)

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for branch, tree, error in pool.map(one, GOVUK_BRANCHES):
            if error:
                failed.append(f"{branch} ({error})")
            else:
                walk_taxons(tree, names)
    return names, failed


def govuk_item(r, names):
    """One Search API result in the same shape as an RSS item, or None if it has no link.
    The department(s) go in the author column; the format (such as 'news story') and the
    topic names go in categories."""
    link = r.get("link") or ""
    if link.startswith("/"):
        link = "https://www.gov.uk" + link
    if not link:
        return None
    departments = [o.get("title", "") if isinstance(o, dict) else str(o)
                   for o in r.get("organisations") or []]
    topics = []
    for t in r.get("taxons") or []:
        if isinstance(t, dict):
            topics.append(t.get("title") or names.get(t.get("content_id"), ""))
        else:
            topics.append(names.get(t, ""))   # a plain topic ID; unknown ones are left out
    kind = (r.get("format") or r.get("content_store_document_type") or "").replace("_", " ")
    categories = [c.replace("|", "/").strip() for c in [kind] + topics if c and c.strip()]
    return {
        "title": clean(r.get("title")),
        "link": link,
        "guid": r.get("content_id") or link,
        "published": to_iso(r.get("public_timestamp")),
        "author": "; ".join(d for d in departments if d),
        "categories": list(dict.fromkeys(categories)),
        "description": shorten(clean(r.get("description")), MAX_DESCRIPTION),
        "image_url": "",
    }


def process_govuk(titles):
    """Pull the newest GOV.UK content across the watch-list topics. Returns a result in the
    same shape as process(), plus a list of warnings about anything that went partly wrong."""
    result = {"url": GOVUK_SEARCH, "title": "GOV.UK watch list", "items": [], "error": "",
              "name": "gov.uk/watch-list", "source": "gov.uk", "warnings": []}
    names, failed = load_taxonomy()
    if failed:
        result["warnings"].append(
            f"Could not load {len(failed)} of {len(GOVUK_BRANCHES)} GOV.UK topic branches: " + ", ".join(failed))
    if not names:
        result["error"] = "could not load the GOV.UK topic tree"
        return result
    by_title = {}
    for topic_id, title in names.items():
        by_title.setdefault(title, []).append(topic_id)
    ids, missing = [], []
    for title in titles:
        if title in by_title:
            ids.extend(by_title[title])
        else:
            missing.append(title)
    if missing:
        result["warnings"].append(
            f"{len(missing)} watch-list topic(s) not found on GOV.UK, so skipped (renamed, or their "
            "branch failed to load): " + "; ".join(missing))
    if not ids:
        result["error"] = "none of the watch-list topics were found on GOV.UK"
        return result
    query = ([("filter_part_of_taxonomy_tree[]", i) for i in dict.fromkeys(ids)]
             + [("order", "-public_timestamp"), ("count", str(GOVUK_MAX_RESULTS))]
             + [("fields", f) for f in GOVUK_FIELDS])
    try:
        data = get_json(GOVUK_SEARCH + "?" + urlencode(query))
    except RuntimeError as e:
        result["error"] = str(e)
        return result
    for r in data.get("results") or []:
        item = govuk_item(r, names)
        if item:
            result["items"].append(item)
    return result


def host_of(url):
    """Website name without 'www.', e.g. 'fenews.co.uk'."""
    host = urlparse(url).netloc.lower()
    return host[4:] if host.startswith("www.") else host


def feed_name(url):
    """Short label for a feed. Guardian feeds show just the path, e.g.
    'education/higher-education'; other sites show the domain first, e.g. 'fenews.co.uk'."""
    path = urlparse(url).path.strip("/")
    for tail in ("/rss.xml", "/rss", "/feed"):
        if path.endswith(tail):
            path = path[: -len(tail)]
    if path in ("rss.xml", "rss", "feed"):
        path = ""
    host = host_of(url)
    if not host or host == "theguardian.com":
        return path or url
    return f"{host}/{path}" if path else host


def load_archive(path):
    """Rows saved by earlier runs, in the shape merge() works with. Empty if there are none."""
    if not path or not path.exists():
        return []
    rows = []
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                if not (r.get("link") or r.get("guid")):
                    continue
                rows.append({
                    "title": r.get("title", ""), "link": r.get("link", ""),
                    "guid": r.get("guid") or r.get("link", ""), "source": r.get("source", ""),
                    # Back to the internal form, whichever way the saved copy was written.
                    "published": to_iso(r.get("published", "")), "author": r.get("author", ""),
                    "categories": [c for c in r.get("categories", "").split(" | ") if c],
                    "description": r.get("description", ""), "image_url": r.get("image_url", ""),
                    "feeds": [x for x in r.get("source_feeds", "").split(" | ") if x],
                })
    except (OSError, csv.Error, UnicodeDecodeError) as e:
        print(f"Warning: could not read the saved copy {path} ({e}), so this run starts without it.")
        return []
    return rows


def merge(results, cutoff="", previous=()):
    """Combine items from all feeds into one row per article.
    Rows saved by earlier runs (previous) are the starting point. Items published before
    the cut-off are counted but left out; items with no date are kept.
    Returns (rows, items fetched, older items left out, rows carried over from earlier runs)."""
    rows, seen = [], {}
    total = old = 0
    for r in previous:
        if cutoff and r["published"] and r["published"] < cutoff:
            continue
        key = r["link"] or r["guid"]
        if key in seen:
            continue
        row = {**r, "categories": list(dict.fromkeys(r["categories"])),
               "feeds": list(dict.fromkeys(r["feeds"])), "fresh": False}
        seen[key] = row
        rows.append(row)
    for result in results:
        if result["error"]:
            continue
        name = result.get("name") or feed_name(result["url"])
        source = result.get("source") or host_of(result["url"])
        for item in result["items"]:
            total += 1
            if cutoff and item["published"] and item["published"] < cutoff:
                old += 1
                continue
            key = item["link"] or item["guid"]  # the link is the same wherever the article appears
            row = seen.get(key)
            if row is None:
                row = {**item, "source": source, "fresh": True,
                       "categories": list(dict.fromkeys(item["categories"])), "feeds": [name]}
                seen[key] = row
                rows.append(row)
            else:
                row["fresh"] = True
                for field in ("title", "description", "image_url", "author"):
                    if item[field]:
                        row[field] = item[field]  # today's wording wins over the saved copy
                if name not in row["feeds"]:
                    row["feeds"].append(name)
                for cat in item["categories"]:
                    if cat not in row["categories"]:
                        row["categories"].append(cat)
    rows.sort(key=lambda r: r["published"], reverse=True)
    carried = sum(1 for r in rows if not r["fresh"])
    return rows, total, old, carried


def cutoff_from(start_date, max_age_days):
    """Earliest publication time to keep, as an ISO string, or '' for no limit."""
    limits = []
    if start_date:
        try:
            limits.append(datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00Z"))
        except ValueError:
            sys.exit(f'The start date must look like 2026-10-01, not "{start_date}".')
    if max_age_days:
        # Count back whole days from today's midnight (UTC), so the window does not
        # shift by the hour: every run on the same day gives the same cut-off.
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        limits.append((today - timedelta(days=int(max_age_days))).strftime("%Y-%m-%dT%H:%M:%SZ"))
    return max(limits) if limits else ""


def excel_date(iso):
    """'2026-10-21T15:08:12Z' becomes '2026-10-21 15:08:12'. Excel treats the first form as
    text, so it cannot sort or filter on it; the second it reads as a date and time (UTC)."""
    return iso.replace("T", " ").rstrip("Z")


def flatten(row):
    return {
        "title": row["title"],
        "link": row["link"],
        "source": row["source"],
        "published": excel_date(row["published"]),
        "author": row["author"],
        "categories": " | ".join(row["categories"]),
        "description": row["description"] if INCLUDE_DESCRIPTION else "",
        "image_url": row["image_url"],
        "source_feeds": " | ".join(row["feeds"]),
        "feed_count": len(row["feeds"]),
        "guid": row["guid"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--feeds", type=Path, default=ROOT / "feeds.txt", help="file listing feed URLs")
    parser.add_argument("--out", type=Path, default=ROOT / "site", help="output folder")
    parser.add_argument("--from-date", default=START_DATE, metavar="YYYY-MM-DD",
                        help="leave out articles published before this day (default: START_DATE in the script)")
    parser.add_argument("--max-age-days", type=int, default=MAX_AGE_DAYS, metavar="N",
                        help="leave out articles older than N days (default: MAX_AGE_DAYS in the script)")
    parser.add_argument("--archive", type=Path, default=ARCHIVE if KEEP_HISTORY else None, metavar="FILE",
                        help="saved copy of the CSV that each run builds on (default: data/news_feeds.csv)")
    parser.add_argument("--no-history", action="store_true", help="ignore and do not update the saved copy")
    parser.add_argument("--govuk-topics", type=Path, default=GOVUK_TOPICS, metavar="FILE",
                        help="file listing the GOV.UK topics to watch (default: govuk_topics.txt)")
    parser.add_argument("--no-govuk", action="store_true", help="leave the GOV.UK watch list out of this run")
    args = parser.parse_args()
    if args.no_history:
        args.archive = None
    cutoff = cutoff_from(args.from_date, args.max_age_days)

    urls = read_feeds(args.feeds)
    topics = read_feeds(args.govuk_topics) if (not args.no_govuk and args.govuk_topics.exists()) else []
    if not urls and not topics:
        sys.exit(f"No feed URLs found in {args.feeds}, and no GOV.UK topics to watch.")

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        results = list(pool.map(process, urls))

    # Feeds that were rate-limited (429) or refused (403, which busy sites sometimes
    # send to servers like GitHub's for a while) get one more go, one at a time, after a pause.
    limited = [i for i, r in enumerate(results) if r["error"] in ("HTTP 429", "HTTP 403")]
    if limited:
        print(f"{len(limited)} feed(s) were refused or rate-limited; waiting 30 seconds, then trying them again.")
        time.sleep(30)
        for i in limited:
            results[i] = process(urls[i])
            time.sleep(3)

    # GOV.UK counts as one more source, with its own list of topics to watch.
    if topics:
        results.append(process_govuk(topics))

    failures = [{"url": r["url"], "error": r["error"]} for r in results if r["error"]]
    warnings = [w for r in results for w in r.get("warnings", [])]
    if len(failures) == len(results):
        for fail in failures:
            print(f"  FAILED {fail['url']}: {fail['error']}")
        sys.exit("No sources could be fetched, so nothing has been changed.")

    previous = load_archive(args.archive)
    if args.archive == ARCHIVE and not ARCHIVE.exists() and LEGACY_ARCHIVE.exists():
        print(f"Starting from the old saved copy, {LEGACY_ARCHIVE.name}; it will be saved as {ARCHIVE.name} from now on.")
        previous = load_archive(LEGACY_ARCHIVE)
    rows, total, old, carried = merge(results, cutoff, previous)
    flat = [flatten(r) for r in rows]

    args.out.mkdir(parents=True, exist_ok=True)
    # utf-8-sig adds a byte order mark so Excel reads accents and symbols correctly.
    with open(args.out / CSV_NAME, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(flat)
    if args.archive:
        args.archive.parent.mkdir(parents=True, exist_ok=True)
        with open(args.archive, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(flat)

    preview_cols = ["title", "link", "source", "published", "author", "categories", "feed_count"]
    with open(args.out / "articles.json", "w", encoding="utf-8") as f:
        # The page reads these dates as UTC, so they keep the 'T' and 'Z' form.
        json.dump([{**{k: out[k] for k in preview_cols}, "published": row["published"]}
                   for row, out in zip(rows, flat)], f, ensure_ascii=False)

    status = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "feeds_total": len(results),
        "feeds_ok": len(results) - len(failures),
        "items_total": total,
        "items_unique": len(rows),
        "date_from": cutoff[:10],
        "items_older_left_out": old,
        "items_carried_over": carried,
        "failures": failures,
        "warnings": warnings,
        "feeds": [{"url": r["url"], "title": r["title"], "items": len(r["items"]), "error": r["error"]}
                  for r in results],
    }
    with open(args.out / "status.json", "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)

    print(f"{status['feeds_ok']} of {status['feeds_total']} feeds fetched; "
          f"{status['items_unique']} unique articles ({total} items fetched before merging duplicates).")
    if args.archive:
        print(f"History: {carried} article(s) kept from earlier runs that today's feeds no longer hold.")
    if cutoff:
        print(f"Date filter: kept articles from {cutoff[:10]} onwards; {old} older item(s) left out.")
        if not rows:
            print("  Warning: no articles are newer than the cut-off, so the CSV is empty.")
    for fail in failures:
        print(f"  FAILED {fail['url']}: {fail['error']}")
    for warning in warnings:
        print(f"  NOTE {warning}")
    print(f"CSV saved to: {(args.out / CSV_NAME).resolve()}")


if __name__ == "__main__":
    main()
