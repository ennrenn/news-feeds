# News feeds to CSV

This repository fetches a list of RSS feeds (the Guardian, FE News, the Daily Express, the Telegraph, Google News and Yahoo News, plus any other site you add), merges the articles into one CSV, and publishes the CSV on a GitHub Pages site. A GitHub Action does the fetching on GitHub's servers, automatically every three hours, or whenever you start it by hand.

## Setting it up

Create a new repository on GitHub and upload everything in this folder, keeping the folder structure (including the hidden `.github` folder; if the upload skips it, use Add file, then Create new file, and type `.github/workflows/fetch-feeds.yml` as the name so the folders are made for you). Replace the example in `feeds.txt` with your full list, one URL per line.

In the repository, go to Settings, then Pages, and set the source to GitHub Actions. Then open the Actions tab, choose Fetch feeds, and press Run workflow to make the first run. After a minute or so the site is live at `https://YOUR-USERNAME.github.io/REPOSITORY-NAME/`. It shows how many feeds worked, lists any that failed, previews the articles, and has a Download CSV button.

## The schedule

The workflow runs every three hours, at 17 minutes past (UTC) from 00:17, so 01:17, 04:17, 07:17 and so on in UK summer time, and an hour earlier on the clock in winter. GitHub often starts scheduled runs late, sometimes by an hour or two, and now and then skips one, especially on quiet repositories. That is why articles are saved between runs (see below): a late or missed run loses nothing as long as one lands within a day or so. The Run workflow button on the Actions tab still works whenever you want a run straight away.

To change the timing, open `.github/workflows/fetch-feeds.yml`, find the line `- cron: "17 */3 * * *"`, and change the `*/3` to the number of hours you want. Edit the file on GitHub (the pencil icon) rather than uploading over it, or the change can be undone by an older copy. Each run is stopped automatically if it is still going after 15 minutes, so a stuck run cannot hold up the next one. If the runs ever stop altogether, open the Actions tab; GitHub switches scheduled workflows off after 60 days with no activity in a public repository, and there is an Enable workflow button on the workflow's page.

## Running it on your own computer

You need Python 3.9 or later and nothing else.

```
python scripts/fetch_feeds.py
```

The CSV appears in the `site` folder, as `site/news_feeds.csv`, and the last line the script prints gives the full path. This is also the fallback if GitHub's servers are ever refused by a site but your own connection is not.

The Guardian sometimes answers "HTTP 429", which means too many requests, and other sites sometimes answer "HTTP 403", which means the site has refused the request (busy sites do this to servers like GitHub's for a while). The script fetches two feeds at a time, waits as long as the site asks, and tries any refused or rate-limited feeds again, one at a time, after a 30 second pause. If a site keeps refusing GitHub, running the script on your own computer is the fallback.

## Adding a feed from another site

Add its URL to `feeds.txt` on a new line and run the workflow again. Any standard RSS feed works. The script reads the usual fields (title, link, date, author, categories, summary and image), copes with the different ways sites present them, and removes the stock endings some sites add to every summary, such as "Continue reading..." or "was published on FE News by...". It also copes with some site habits: the Express's separate author tag and keyword tags (the keywords are added to `categories`), and the Telegraph's internal tags, which are dropped (`structure:` and `storytype:`) or turned into plain words (`topics:things/uk-economy` becomes `uk economy`). The `source` column shows which site each article came from, and the site's page has a filter box that searches it.

Google News and Yahoo News are news aggregators, so a few things are different for them. The `source` column shows the aggregator (news.google.com or news.yahoo.com), and the original publisher goes in the `author` column: on its own for Google News, and in brackets after the writer's name for Yahoo, such as "Jane Smith (BBC News)". The publisher's name is taken off the end of Google News headlines. Google News summaries are only a list of related headlines, so that column is left blank. Yahoo News puts whole articles in its summaries, so summaries are cut to 1,000 characters, which is the `MAX_DESCRIPTION` setting near the top of the script. Google News links go through a Google redirect, so the same story from the Guardian and from Google will not be merged into one row.

A site you add might need a small tweak if its summaries are laid out unusually. If a feed fails or looks wrong, the status panel on the page names it and gives the reason.

## Limiting the CSV by date

By default the CSV holds the last five days of articles. The window moves with the day the workflow runs: a run on 14 October keeps everything published from 9 October (00:00 UTC) onwards, and a run on 3 November keeps everything from 29 October onwards. Nothing needs changing as the months pass. The site's summary line says which date the CSV starts from and how many older items were left out. Articles with no publication date are kept.

Near the top of `scripts/fetch_feeds.py` there are two settings. `MAX_AGE_DAYS = 5` is the rolling window; change the number to widen or narrow it, or set it to `None` to switch it off. `START_DATE` is a fixed day, written as `"2026-10-01"`, that never moves; it is empty by default, and if you set both, the later cut-off applies. To change a setting, open the file in GitHub, choose the pencil icon, edit the line, and commit. To try a different window on your own computer without editing the file, add `--max-age-days 10` to the command, or `--from-date 2026-10-01`, or `--max-age-days 0 --from-date ""` for no limit.

## What is in the CSV

There is one row per article. The same article often appears in several feeds, so it is merged into one row, with `source_feeds` listing every feed it appeared in and `feed_count` giving the number. The columns are title, link, source (the website, such as theguardian.com or fenews.co.uk), published (UTC), author, categories, description (plain text), image_url, source_feeds, feed_count and guid. Rows are sorted newest first. The file is saved so Excel shows accents and symbols correctly.

Each feed only holds its most recent items (about 20 for the Guardian, 50 for FE News, 10 for the Express and 120 for the Telegraph, going by the samples, although the Telegraph's go back about a year, so only two or three fall inside five days). A busy feed turns over in a day or two, so an article can be gone from its feed by the next morning.

## How articles are kept between runs

To stop articles disappearing, each run saves its CSV in the repository as `data/news_feeds.csv`, and the next run starts from that saved copy. It adds whatever the feeds hold today, updates any article whose headline or summary has changed, and drops anything older than the date window. The result is that an article stays in the CSV for the whole window (five days by default) even after its feed has moved on, and a feed that fails one morning does not lose the articles it supplied earlier. The site's summary line says how many articles were kept from earlier runs.

The `source_feeds` and `feed_count` columns add up across runs, so an article that appeared in one feed on Monday and another on Tuesday lists both. The saved copy is an ordinary CSV, so you can open it in GitHub. To seed it with a CSV you already have, upload that file to `data/news_feeds.csv` before running the workflow. To start again from nothing, delete that file. To switch the feature off, set `KEEP_HISTORY = False` near the top of the script.

The saved copy used to be called `data/guardian_feeds.csv`. If the new file does not exist yet, the first run starts from the old one and saves it under the new name, so nothing is lost. After that first run you can delete `data/guardian_feeds.csv`; it is no longer used.

The workflow needs permission to save the file. If a run fails at the step "Save the CSV in the repository", open Settings, then Actions, then General, and check that workflow permissions are not set to read-only, and that no branch protection rule is blocking the push.

## Things to know

Pages sites are public, even when the repository is private, unless your GitHub plan includes private Pages. The CSV contains article summaries from the sites you fetch. If you would rather not publish them, set `INCLUDE_DESCRIPTION = False` near the top of `scripts/fetch_feeds.py`.

If a feed fails, the run carries on with the rest and the site lists the failures with the reason. The run only stops, and leaves the previous site in place, if no feed could be fetched at all.
