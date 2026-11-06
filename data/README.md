# Replay data

`replay/` holds one gzipped JSON file per UTC day, from 2021-07-15 to 2021-10-15. Each file is the last state that day of CAL FIRE's public incident feed, as recorded by [simonw/ca-fires-history](https://github.com/simonw/ca-fires-history), a Git-scraping project that stored each change to the feed as a commit.

There are 90 files in a 93-day window. No commits exist for October 4 to 6, 2021, so those days are a real source outage and were not removed for this project.

Narrative HTML fields were dropped to keep the fixture small: `ConditionStatement`, `ControlStatement`, `SearchDescription`, `SearchKeywords`, and `ExternalIncidentLink`. All other fields are as published. `scripts/build_replay_fixture.py` rebuilds these files from a clone of the source repository.

Figures here reflect what the feed published during this window. They are not official season totals.
