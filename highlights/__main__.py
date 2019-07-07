import argparse
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import jinja2
from attr import attrib, attrs

import nhlapi
import pendulum
from nhlapi.endpoints import NHLAPI

_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS highlights (
    game_id INTEGER PRIMARY KEY NOT NULL,
    date TEXT NOT NULL,
    home TEXT NOT NULL,
    away TEXT NOT NULL,
    recap TEXT,
    extended TEXT
)
"""

_TEMPLATE = """\
<!doctype html>
<html>
<head>
    <meta charset="utf-8"/>
    <title>NHL game recaps</title>
</head>
<body>
    <div>
        <a href="index.html">Home</a> |
        {% for team in teams %}
        <a href="{{team}}.html">{{team}}</a>
        {% endfor %}
    <div>
    <h1>NHL game recaps | <small>direct links to videos</small></h1>
    <hr/>
    {% for day in days %}
    {{ day.date | date_pretty }}
    <table border="0" cellpadding="5">
        <tr>
            <th>Home</th>
            <th>Away</th>
            <th>Short</th>
            <th>Extended</th>
        </tr>
        {% for game in day.games %}
        <tr>
            <th>{{ game.home | upper }}</th>
            <th>{{ game.away | upper }}</th>
            {% if game.recap %}
                <th><a href="{{ game.recap }}" target="_blank">link</a></th>
            {% else %}
                <th>-</th>
            {% endif %}
            {% if game.extended %}
                <th><a href="{{ game.extended }}" target="_blank">link</a></th>
            {% else %}
                <th>-</th>
            {% endif %}
        </tr>
        {% endfor %}
    </table>
    <hr/>
    {% endfor %}
    <p>This is a non-profit page created to bring NHL game recaps to users that
    cannot access them, such as Linux users.
    Last update: {{ date }}
</body>
</html>
"""

_TEAMS = {
    "1": "NJD",
    "2": "NYI",
    "3": "NYR",
    "4": "PHI",
    "5": "PIT",
    "6": "BOS",
    "7": "BUF",
    "8": "MTL",
    "9": "OTT",
    "10": "TOR",
    "12": "CAR",
    "13": "FLA",
    "14": "TBL",
    "15": "WSH",
    "16": "CHI",
    "17": "DET",
    "18": "NSH",
    "19": "STL",
    "20": "CGY",
    "21": "COL",
    "22": "EDM",
    "23": "VAN",
    "24": "ANA",
    "25": "DAL",
    "26": "LAK",
    "28": "SJS",
    "29": "CBJ",
    "30": "MIN",
    "52": "WPG",
    "53": "ARI",
    "54": "VGK",
}


@attrs(slots=True)
class Highlight:
    game_id = attrib()
    date = attrib()
    home = attrib()
    away = attrib()
    recap = attrib(default=None)
    extended = attrib(default=None)


class Database:
    def __init__(self):
        self._con = sqlite3.connect("highlights.db")
        self._con.execute(_TABLES_SQL)

    def get_by_id(self, game_id):
        cur = self._con.execute("SELECT * FROM highlights WHERE game_id = ?", [game_id])
        row = cur.fetchone()
        if row is not None:
            return Highlight(*row)
        return None

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
        g = api.content(h.game_id)
        for media in g.media.epg:
            if media.title == "Recap" and len(media["items"]) > 0:
                h.recap = media["items"][0].playbacks[-1].url
            if media.title == "Extended Highlights" and len(media["items"]) > 0:
                h.extended = media["items"][0].playbacks[-1].url
        db.update(h)

    teams = list(sorted(_TEAMS.values()))

    date = datetime.now().strftime("%Y-%m-%d %H:%M")
    env = jinja2.Environment()
    env.filters["date_pretty"] = date_pretty
    tpl = env.from_string(_TEMPLATE)

    text = tpl.render(days=highlights_to_days(db.select_all()), date=date, teams=teams)
    Path(args.path_html, "index.html").write_text(text)

    for team in _TEAMS.values():
        hs = db.select_team(team)
        days = highlights_to_days(hs)
        text = tpl.render(days=days, date=date, teams=teams)
        Path(args.path_html, team + ".html").write_text(text)
