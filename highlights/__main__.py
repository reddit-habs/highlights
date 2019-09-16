import argparse
import sqlite3
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests
import jinja2
from attr import attrib, attrs

import yaml
import nhlapi
import pendulum
from nhlapi.endpoints import NHLAPI

PACKAGE_PATH = Path(__file__).parent
DATA_PATH = PACKAGE_PATH / "data"
TEMPLATES_PATH = PACKAGE_PATH / "templates"

_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS highlights (
    game_id INTEGER PRIMARY KEY NOT NULL,
    date TEXT NOT NULL,
    home TEXT NOT NULL,
    away TEXT NOT NULL,
    recap TEXT,
    extended TEXT
);

CREATE TABLE IF NOT EXISTS seasons (
    name TEXT PRIMARY KEY NOT NULL,
    begin INTEGER NOT NULL,
    end INTEGER NOT NULL
);
"""


@attrs(slots=True)
class Highlight:
    game_id = attrib()
    date = attrib()
    home = attrib()
    away = attrib()
    recap = attrib(default=None)
    extended = attrib(default=None)


@attrs(slots=True)
class Season:
    name = attrib()
    begin = attrib()
    end = attrib()


class Database:
    def __init__(self):
        self._con = sqlite3.connect("highlights.db")
        self._con.executescript(_TABLES_SQL)

        seasons = [(2019, 2020), (2018, 2019)]

        for (begin, end) in seasons:
            try:
                self._con.execute(
                    "INSERT INTO seasons (name, begin, end) VALUES (?, ?, ?)", ["{}-{}".format(begin, end), begin, end]
                )
                self._con.commit()
            except sqlite3.Error as e:
                print(e)
                self._con.rollback()

    def get_by_id(self, game_id):
        cur = self._con.execute("SELECT * FROM highlights WHERE game_id = ?", [game_id])
        row = cur.fetchone()
        if row is not None:
            return Highlight(*row)
        return None

    def get_seasons(self):
        cur = self._con.execute("SELECT * FROM seasons ORDER BY end DESC")
        return [Season(*row) for row in cur]

    def update(self, h: Highlight):
        self._con.execute(
            "UPDATE highlights SET date = ?, home = ?, away = ?, recap = ?, extended = ? WHERE game_id = ?",
            [h.date, h.home, h.away, h.recap, h.extended, h.game_id],
        )
        self._con.commit()

    def insert(self, h: Highlight):
        self._con.execute(
            "INSERT INTO highlights VALUES (?, ?, ?, ?, ?, ?)", [h.game_id, h.date, h.home, h.away, h.recap, h.extended]
        )
        self._con.commit()

    def select_missing(self):
        # Some highlights might simply never show up. Give up trying to find them after 3 days.
        cutoff = pendulum.today().add(days=-3).format("YYYY-MM-DD")
        cur = self._con.execute(
            "SELECT * FROM highlights WHERE date >= ? AND (recap IS NULL OR extended IS NULL)", [cutoff]
        )
        return [Highlight(*row) for row in cur.fetchall()]

    def select_all(self):
        cur = self._con.execute("SELECT * FROM highlights ORDER BY date DESC")
        return [Highlight(*row) for row in cur.fetchall()]

    def select_team(self, team):
        cur = self._con.execute("SELECT * FROM highlights WHERE home = ? OR away = ?", [team, team])
        return [Highlight(*row) for row in cur.fetchall()]


def highlights_to_days(hs):
    days = defaultdict(list)

    for h in hs:
        days[h.date].append(h)

    days = [dict(date=key, games=val) for key, val in days.items()]
    days.sort(key=lambda d: d["date"], reverse=True)

    return days


def date_pretty(s):
    return pendulum.parse(s).format("dddd MMMM Do YYYY")


def maybe(val, func):
    if val is None:
        return None
    return func(val)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path_html")
    parser.add_argument("--date", default=None)
    parser.add_argument("--start", default=None)
    args = parser.parse_args()

    date = maybe(args.date, pendulum.parse)
    start = maybe(args.start, pendulum.parse)

    db = Database()
    api = NHLAPI(nhlapi.SyncClient())

    print("Fetching schedule...")
    print(date)
    if start is not None:
        s = api.schedule(start_date=start, end_date=pendulum.today())
    else:
        s = api.schedule(date=date)

    for gameDay in s.dates:
        for g in gameDay.games:
            game_id = g.gamePk
            date = gameDay.date
            try:
                home = _TEAMS[str(g.teams.home.team.id)]
                away = _TEAMS[str(g.teams.away.team.id)]
            except KeyError:
                continue

            h = Highlight(game_id, date, home, away)
            if db.get_by_id(h.game_id) is None:
                print("Inserting new game into database", h.game_id, ":", h.away, "at", h.home)
                db.insert(h)

    for h in db.select_missing():
        print("Getting content for game", h.game_id, ":", h.away, "at", h.home)
        try:
            g = api.content(h.game_id)
        except requests.HTTPError:
            continue
        for media in g.media.epg:
            if media.title == "Recap" and len(media["items"]) > 0:
                h.recap = media["items"][0].playbacks[-1].url
            if media.title == "Extended Highlights" and len(media["items"]) > 0:
                h.extended = media["items"][0].playbacks[-1].url
        db.update(h)

    divisions = yaml.safe_load(DATA_PATH.joinpath("teams.yaml").read_text())["divisions"]

    date = datetime.now().strftime("%Y-%m-%d %H:%M")

    loader = jinja2.FileSystemLoader(TEMPLATES_PATH)
    env = jinja2.Environment(loader=loader)
    env.filters["date_pretty"] = date_pretty

    tpl = env.get_template("index.jinja")

    seasons = db.get_seasons()

    tpl_data = dict(
        days=highlights_to_days(db.select_all()), date=date, team=None, divisions=divisions, seasons=seasons
    )

    Path(args.path_html, "index.html").write_text(tpl.render(**tpl_data))

    for division in divisions:
        for team in division["teams"]:
            hs = db.select_team(team["code"])
            days = highlights_to_days(hs)
            tpl_data["days"] = days
            tpl_data["team"] = team
            text = tpl.render(**tpl_data)
            Path(args.path_html, team + ".html").write_text(text)
