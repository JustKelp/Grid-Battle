# ══════════════════════════════════════════════════════════════════════════
#  StatCheck — Competitive Sports Trivia Tic-Tac-Toe
# ══════════════════════════════════════════════════════════════════════════
#
# Requirements (pip install):
#   flask
#   flask-socketio
#   werkzeug
#   rapidfuzz  (optional — improves fuzzy name matching)
#
# ── DATABASE-BACKED STATE MIGRATION PATH ─────────────────────────────────
# All game state is currently held in-memory in the ROOMS dict.
# Access is funnelled through get_room() / save_room() / delete_room().
# To move to Redis or Postgres later:
#   1. Replace ROOMS dict with a Redis/Postgres connection.
#   2. In get_room():  deserialise JSON from DB; convert used_players list→set.
#   3. In save_room(): convert used_players set→list; serialise to JSON; write.
#   4. In delete_room(): issue a DELETE.
#   5. Update cleanup_stale_rooms() to query by last_active timestamp.
# No game-logic functions access ROOMS directly, so no other changes needed.
# ─────────────────────────────────────────────────────────────────────────

import math
import random
import re
import sqlite3
import os
import sys
import unicodedata
import time
import string
import json as _json
import threading
import flask
from flask import Flask, jsonify, request
from flask_socketio import SocketIO, join_room as sio_join_room, emit
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "grid-game-secret-change-me")
socketio = SocketIO(app, cors_allowed_origins="*")

DB_PATH = os.path.join(os.path.dirname(__file__), "users.db")
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")

# ── ROOM STORAGE ─────────────────────────────────────────────────────────
ROOMS = {}  # {room_id: state_dict}
_rooms_lock = threading.Lock()

ROOM_ID_LENGTH = 6
ROOM_INACTIVE_TIMEOUT = 2 * 60 * 60    # 2 hours in seconds
ROOM_GAMEOVER_TIMEOUT = 30 * 60         # 30 minutes in seconds

def _generate_room_id():
    chars = string.ascii_uppercase + string.digits
    while True:
        rid = ''.join(random.choices(chars, k=ROOM_ID_LENGTH))
        if rid not in ROOMS:
            return rid

def get_room(room_id):
    """Retrieve a game room's state. Returns None if not found."""
    return ROOMS.get(room_id)

def save_room(room_id, state):
    """Persist a game room's state (in-memory for now)."""
    state["_last_active"] = time.time()
    ROOMS[room_id] = state

def delete_room(room_id):
    """Remove a game room."""
    ROOMS.pop(room_id, None)

def cleanup_stale_rooms():
    """Prune rooms that are inactive 2+ hours or game_over for 30+ minutes.
    Called lazily on new room creation."""
    now = time.time()
    stale = []
    for rid, s in list(ROOMS.items()):
        last = s.get("_last_active", 0)
        if now - last > ROOM_INACTIVE_TIMEOUT:
            stale.append(rid)
        elif s.get("game_over") and now - last > ROOM_GAMEOVER_TIMEOUT:
            stale.append(rid)
    for rid in stale:
        ROOMS.pop(rid, None)

# ── RATE LIMITING (simple in-memory) ─────────────────────────────────────
_rate_limit_store = {}  # {ip: [timestamp, ...]}
_RATE_LIMIT_MAX = 10
_RATE_LIMIT_WINDOW = 60  # seconds

def _check_rate_limit(ip):
    """Returns True if rate-limited (too many requests)."""
    now = time.time()
    timestamps = _rate_limit_store.get(ip, [])
    timestamps = [t for t in timestamps if now - t < _RATE_LIMIT_WINDOW]
    if len(timestamps) >= _RATE_LIMIT_MAX:
        _rate_limit_store[ip] = timestamps
        return True
    timestamps.append(now)
    _rate_limit_store[ip] = timestamps
    return False

# ── CONSTANTS ──────────────────────────────────────────────────────────────
NFL_TEAMS = [
    "ARI","ATL","BAL","BUF","CAR","CHI","CIN","CLE",
    "DAL","DEN","DET","GB","HOU","IND","JAX","KC",
    "LV","LAC","LAR","MIA","MIN","NE","NO","NYG",
    "NYJ","PHI","PIT","SF","SEA","TB","TEN","WAS"
]
TEAM_NAMES = {
    "ARI":"Arizona Cardinals","ATL":"Atlanta Falcons","BAL":"Baltimore Ravens",
    "BUF":"Buffalo Bills","CAR":"Carolina Panthers","CHI":"Chicago Bears",
    "CIN":"Cincinnati Bengals","CLE":"Cleveland Browns","DAL":"Dallas Cowboys",
    "DEN":"Denver Broncos","DET":"Detroit Lions","GB":"Green Bay Packers",
    "HOU":"Houston Texans","IND":"Indianapolis Colts","JAX":"Jacksonville Jaguars",
    "KC":"Kansas City Chiefs","LV":"Las Vegas Raiders","LAC":"Los Angeles Chargers",
    "LAR":"Los Angeles Rams","MIA":"Miami Dolphins","MIN":"Minnesota Vikings",
    "NE":"New England Patriots","NO":"New Orleans Saints","NYG":"New York Giants",
    "NYJ":"New York Jets","PHI":"Philadelphia Eagles","PIT":"Pittsburgh Steelers",
    "SF":"San Francisco 49ers","SEA":"Seattle Seahawks","TB":"Tampa Bay Buccaneers",
    "TEN":"Tennessee Titans","WAS":"Washington Commanders"
}
TEAM_MASCOTS = {
    "ARI":"Cardinals","ATL":"Falcons","BAL":"Ravens","BUF":"Bills","CAR":"Panthers",
    "CHI":"Bears","CIN":"Bengals","CLE":"Browns","DAL":"Cowboys","DEN":"Broncos",
    "DET":"Lions","GB":"Packers","HOU":"Texans","IND":"Colts","JAX":"Jaguars",
    "KC":"Chiefs","LV":"Raiders","LAC":"Chargers","LAR":"Rams","MIA":"Dolphins",
    "MIN":"Vikings","NE":"Patriots","NO":"Saints","NYG":"Giants","NYJ":"Jets",
    "PHI":"Eagles","PIT":"Steelers","SF":"49ers","SEA":"Seahawks","TB":"Buccaneers",
    "TEN":"Titans","WAS":"Commanders"
}
TEAM_ALIAS = {
    "OAK":"LV","RAI":"LV","SD":"LAC","SDC":"LAC","SDG":"LAC","STL":"LAR",
    "RAM":"LAR","LA":"LAR","SL":"LAR","PHL":"PHI","BLT":"BAL","CLV":"CLE",
    "HST":"HOU","ARZ":"ARI","WSH":"WAS","WFT":"WAS","JAC":"JAX","GNB":"GB",
    "NWE":"NE","SFO":"SF","KAN":"KC","TAM":"TB","NOR":"NO","HTX":"HOU",
    "PHX":"ARI","LVR":"LV","BOS":"NE","NYT":"NYJ",
}

# ── NBA CONSTANTS ──────────────────────────────────────────────────────────
NBA_TEAMS = [
    "ATL","BOS","BKN","CHA","CHI","CLE","DAL","DEN","DET","GSW",
    "HOU","IND","LAC","LAL","MEM","MIA","MIL","MIN","NOP","NYK",
    "OKC","ORL","PHI","PHX","POR","SAC","SAS","TOR","UTA","WAS"
]
NBA_TEAM_NAMES = {
    "ATL":"Atlanta Hawks","BOS":"Boston Celtics","BKN":"Brooklyn Nets",
    "CHA":"Charlotte Hornets","CHI":"Chicago Bulls","CLE":"Cleveland Cavaliers",
    "DAL":"Dallas Mavericks","DEN":"Denver Nuggets","DET":"Detroit Pistons",
    "GSW":"Golden State Warriors","HOU":"Houston Rockets","IND":"Indiana Pacers",
    "LAC":"Los Angeles Clippers","LAL":"Los Angeles Lakers","MEM":"Memphis Grizzlies",
    "MIA":"Miami Heat","MIL":"Milwaukee Bucks","MIN":"Minnesota Timberwolves",
    "NOP":"New Orleans Pelicans","NYK":"New York Knicks","OKC":"Oklahoma City Thunder",
    "ORL":"Orlando Magic","PHI":"Philadelphia 76ers","PHX":"Phoenix Suns",
    "POR":"Portland Trail Blazers","SAC":"Sacramento Kings","SAS":"San Antonio Spurs",
    "TOR":"Toronto Raptors","UTA":"Utah Jazz","WAS":"Washington Wizards"
}
NBA_TEAM_MASCOTS = {
    "ATL":"Hawks","BOS":"Celtics","BKN":"Nets","CHA":"Hornets","CHI":"Bulls",
    "CLE":"Cavaliers","DAL":"Mavericks","DEN":"Nuggets","DET":"Pistons",
    "GSW":"Warriors","HOU":"Rockets","IND":"Pacers","LAC":"Clippers","LAL":"Lakers",
    "MEM":"Grizzlies","MIA":"Heat","MIL":"Bucks","MIN":"Timberwolves",
    "NOP":"Pelicans","NYK":"Knicks","OKC":"Thunder","ORL":"Magic","PHI":"76ers",
    "PHX":"Suns","POR":"Trail Blazers","SAC":"Kings","SAS":"Spurs","TOR":"Raptors",
    "UTA":"Jazz","WAS":"Wizards"
}
NBA_TEAM_ALIAS = {
    "BRK":"BKN","CHO":"CHA","NJN":"BKN","PHO":"PHX","SEA":"OKC",
    "NOH":"NOP","NOK":"NOP","NOP":"NOP","VAN":"MEM","CHH":"CHA",
    "CHB":"CHI","WSB":"WAS","SDC":"LAC","PHL":"PHI","SA":"SAS",
    "GS":"GSW","GSW":"GSW","NY":"NYK","NO":"NOP","LA":"LAL",
    "ORL":"ORL","KCK":"SAC","CIN":"SAC","ROC":"SAC","NOJ":"UTA",
    "SDR":"HOU","SFW":"GSW","STB":"ATL","MLH":"ATL","TRI":"ATL",
    "SYR":"PHI","FTW":"DET","MNL":"LAL","CAP":"WAS","BAL":"WAS",
    "CHZ":"WAS","CHI2":"WAS","AND":"IND","DLC":"DAL","KEN":"IND",
    "WAS":"WAS","SAS":"SAS",
}

# ── NHL CONSTANTS ──────────────────────────────────────────────────────────
NHL_TEAMS = [
    "ANA","BOS","BUF","CAR","CBJ","CGY","CHI","COL","DAL","DET",
    "EDM","FLA","LAK","MIN","MTL","NJD","NSH","NYI","NYR","OTT",
    "PHI","PIT","SEA","SJS","STL","TBL","TOR","UTA","VAN","VGK","WPG","WSH"
]
NHL_TEAM_NAMES = {
    "ANA":"Anaheim Ducks","BOS":"Boston Bruins","BUF":"Buffalo Sabres",
    "CAR":"Carolina Hurricanes","CBJ":"Columbus Blue Jackets","CGY":"Calgary Flames",
    "CHI":"Chicago Blackhawks","COL":"Colorado Avalanche","DAL":"Dallas Stars",
    "DET":"Detroit Red Wings","EDM":"Edmonton Oilers","FLA":"Florida Panthers",
    "LAK":"Los Angeles Kings","MIN":"Minnesota Wild","MTL":"Montreal Canadiens",
    "NJD":"New Jersey Devils","NSH":"Nashville Predators","NYI":"New York Islanders",
    "NYR":"New York Rangers","OTT":"Ottawa Senators","PHI":"Philadelphia Flyers",
    "PIT":"Pittsburgh Penguins","SEA":"Seattle Kraken","SJS":"San Jose Sharks",
    "STL":"St. Louis Blues","TBL":"Tampa Bay Lightning","TOR":"Toronto Maple Leafs",
    "UTA":"Utah Hockey Club","VAN":"Vancouver Canucks","VGK":"Vegas Golden Knights",
    "WPG":"Winnipeg Jets","WSH":"Washington Capitals"
}
NHL_TEAM_MASCOTS = {
    "ANA":"Ducks","BOS":"Bruins","BUF":"Sabres","CAR":"Hurricanes","CBJ":"Blue Jackets",
    "CGY":"Flames","CHI":"Blackhawks","COL":"Avalanche","DAL":"Stars","DET":"Red Wings",
    "EDM":"Oilers","FLA":"Panthers","LAK":"Kings","MIN":"Wild","MTL":"Canadiens",
    "NJD":"Devils","NSH":"Predators","NYI":"Islanders","NYR":"Rangers","OTT":"Senators",
    "PHI":"Flyers","PIT":"Penguins","SEA":"Kraken","SJS":"Sharks","STL":"Blues",
    "TBL":"Lightning","TOR":"Maple Leafs","UTA":"Hockey Club","VAN":"Canucks",
    "VGK":"Golden Knights","WPG":"Jets","WSH":"Capitals"
}
NHL_TEAM_ALIAS = {
    "S.J":"SJS","N.J":"NJD","T.B":"TBL","L.A":"LAK",
    "QUE":"COL","HFD":"CAR","ATL":"WPG","MNS":"DAL",
    "WIN":"UTA","PHX":"UTA","ARI":"UTA","ARI_OLD":"UTA",
    "CLR":"NJD","KCS":"NJD","CGS":"DAL","CAL":"CGY",
    "CLE":"MIN","MIN2":"DAL","ATF":"CGY","KC":"NJD",
    "TB":"TBL","NJ":"NJD","SJ":"SJS","LA":"LAK","WAS":"WSH",
    "ATL2":"WPG","CBJ2":"CBJ",
}

# ── MLB CONSTANTS ──────────────────────────────────────────────────────────
MLB_TEAMS = [
    "ARI","ATL","BAL","BOS","CHC","CWS","CIN","CLE","COL","DET",
    "HOU","KC","LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK",
    "PHI","PIT","SD","SF","SEA","STL","TB","TEX","TOR","WSH"
]
MLB_TEAM_NAMES = {
    "ARI":"Arizona Diamondbacks","ATL":"Atlanta Braves","BAL":"Baltimore Orioles",
    "BOS":"Boston Red Sox","CHC":"Chicago Cubs","CWS":"Chicago White Sox",
    "CIN":"Cincinnati Reds","CLE":"Cleveland Guardians","COL":"Colorado Rockies",
    "DET":"Detroit Tigers","HOU":"Houston Astros","KC":"Kansas City Royals",
    "LAA":"Los Angeles Angels","LAD":"Los Angeles Dodgers","MIA":"Miami Marlins",
    "MIL":"Milwaukee Brewers","MIN":"Minnesota Twins","NYM":"New York Mets",
    "NYY":"New York Yankees","OAK":"Oakland Athletics","PHI":"Philadelphia Phillies",
    "PIT":"Pittsburgh Pirates","SD":"San Diego Padres","SF":"San Francisco Giants",
    "SEA":"Seattle Mariners","STL":"St. Louis Cardinals","TB":"Tampa Bay Rays",
    "TEX":"Texas Rangers","TOR":"Toronto Blue Jays","WSH":"Washington Nationals"
}
MLB_TEAM_MASCOTS = {
    "ARI":"Diamondbacks","ATL":"Braves","BAL":"Orioles","BOS":"Red Sox",
    "CHC":"Cubs","CWS":"White Sox","CIN":"Reds","CLE":"Guardians","COL":"Rockies",
    "DET":"Tigers","HOU":"Astros","KC":"Royals","LAA":"Angels","LAD":"Dodgers",
    "MIA":"Marlins","MIL":"Brewers","MIN":"Twins","NYM":"Mets","NYY":"Yankees",
    "OAK":"Athletics","PHI":"Phillies","PIT":"Pirates","SD":"Padres","SF":"Giants",
    "SEA":"Mariners","STL":"Cardinals","TB":"Rays","TEX":"Rangers","TOR":"Blue Jays","WSH":"Nationals"
}
MLB_TEAM_ALIAS = {
    "FLA":"MIA","MON":"WSH","ANA":"LAA","CAL":"LAA","TBD":"TB","TBR":"TB",
    "CHW":"CWS","SDP":"SD","SFG":"SF","KCR":"KC","WSN":"WSH","AZ":"ARI",
    "ATH":"OAK","NYA":"NYY","NYN":"NYM","CHA":"CWS","CHN":"CHC","SFN":"SF",
    "SLN":"STL","LAN":"LAD","SDN":"SD","KCA":"KC","TBA":"TB","WAS":"WSH",
    "FLO":"MIA","ML4":"MIL",
}

# ── LOGOS ──────────────────────────────────────────────────────────────────
TEAM_LOGOS = {
    "ARI":"https://a.espncdn.com/i/teamlogos/nfl/500/ari.png","ATL":"https://a.espncdn.com/i/teamlogos/nfl/500/atl.png",
    "BAL":"https://a.espncdn.com/i/teamlogos/nfl/500/bal.png","BUF":"https://a.espncdn.com/i/teamlogos/nfl/500/buf.png",
    "CAR":"https://a.espncdn.com/i/teamlogos/nfl/500/car.png","CHI":"https://a.espncdn.com/i/teamlogos/nfl/500/chi.png",
    "CIN":"https://a.espncdn.com/i/teamlogos/nfl/500/cin.png","CLE":"https://a.espncdn.com/i/teamlogos/nfl/500/cle.png",
    "DAL":"https://a.espncdn.com/i/teamlogos/nfl/500/dal.png","DEN":"https://a.espncdn.com/i/teamlogos/nfl/500/den.png",
    "DET":"https://a.espncdn.com/i/teamlogos/nfl/500/det.png","GB":"https://a.espncdn.com/i/teamlogos/nfl/500/gb.png",
    "HOU":"https://a.espncdn.com/i/teamlogos/nfl/500/hou.png","IND":"https://a.espncdn.com/i/teamlogos/nfl/500/ind.png",
    "JAX":"https://a.espncdn.com/i/teamlogos/nfl/500/jax.png","KC":"https://a.espncdn.com/i/teamlogos/nfl/500/kc.png",
    "LV":"https://a.espncdn.com/i/teamlogos/nfl/500/lv.png","LAC":"https://a.espncdn.com/i/teamlogos/nfl/500/lac.png",
    "LAR":"https://a.espncdn.com/i/teamlogos/nfl/500/lar.png","MIA":"https://a.espncdn.com/i/teamlogos/nfl/500/mia.png",
    "MIN":"https://a.espncdn.com/i/teamlogos/nfl/500/min.png","NE":"https://a.espncdn.com/i/teamlogos/nfl/500/ne.png",
    "NO":"https://a.espncdn.com/i/teamlogos/nfl/500/no.png","NYG":"https://a.espncdn.com/i/teamlogos/nfl/500/nyg.png",
    "NYJ":"https://a.espncdn.com/i/teamlogos/nfl/500/nyj.png","PHI":"https://a.espncdn.com/i/teamlogos/nfl/500/phi.png",
    "PIT":"https://a.espncdn.com/i/teamlogos/nfl/500/pit.png","SF":"https://a.espncdn.com/i/teamlogos/nfl/500/sf.png",
    "SEA":"https://a.espncdn.com/i/teamlogos/nfl/500/sea.png","TB":"https://a.espncdn.com/i/teamlogos/nfl/500/tb.png",
    "TEN":"https://a.espncdn.com/i/teamlogos/nfl/500/ten.png","WAS":"https://a.espncdn.com/i/teamlogos/nfl/500/wsh.png",
}
NBA_LOGOS = {
    "ATL":"https://a.espncdn.com/i/teamlogos/nba/500/atl.png",
    "BOS":"https://a.espncdn.com/i/teamlogos/nba/500/bos.png",
    "BKN":"https://a.espncdn.com/i/teamlogos/nba/500/bkn.png",
    "CHA":"https://a.espncdn.com/i/teamlogos/nba/500/cha.png",
    "CHI":"https://a.espncdn.com/i/teamlogos/nba/500/chi.png",
    "CLE":"https://a.espncdn.com/i/teamlogos/nba/500/cle.png",
    "DAL":"https://a.espncdn.com/i/teamlogos/nba/500/dal.png",
    "DEN":"https://a.espncdn.com/i/teamlogos/nba/500/den.png",
    "DET":"https://a.espncdn.com/i/teamlogos/nba/500/det.png",
    "GSW":"https://a.espncdn.com/i/teamlogos/nba/500/gs.png",
    "HOU":"https://a.espncdn.com/i/teamlogos/nba/500/hou.png",
    "IND":"https://a.espncdn.com/i/teamlogos/nba/500/ind.png",
    "LAC":"https://a.espncdn.com/i/teamlogos/nba/500/lac.png",
    "LAL":"https://a.espncdn.com/i/teamlogos/nba/500/lal.png",
    "MEM":"https://a.espncdn.com/i/teamlogos/nba/500/mem.png",
    "MIA":"https://a.espncdn.com/i/teamlogos/nba/500/mia.png",
    "MIL":"https://a.espncdn.com/i/teamlogos/nba/500/mil.png",
    "MIN":"https://a.espncdn.com/i/teamlogos/nba/500/min.png",
    "NOP":"https://a.espncdn.com/i/teamlogos/nba/500/no.png",
    "NYK":"https://a.espncdn.com/i/teamlogos/nba/500/ny.png",
    "OKC":"https://a.espncdn.com/i/teamlogos/nba/500/okc.png",
    "ORL":"https://a.espncdn.com/i/teamlogos/nba/500/orl.png",
    "PHI":"https://a.espncdn.com/i/teamlogos/nba/500/phi.png",
    "PHX":"https://a.espncdn.com/i/teamlogos/nba/500/phx.png",
    "POR":"https://a.espncdn.com/i/teamlogos/nba/500/por.png",
    "SAC":"https://a.espncdn.com/i/teamlogos/nba/500/sac.png",
    "SAS":"https://a.espncdn.com/i/teamlogos/nba/500/sa.png",
    "TOR":"https://a.espncdn.com/i/teamlogos/nba/500/tor.png",
    "UTA":"https://a.espncdn.com/i/teamlogos/nba/500/utah.png",
    "WAS":"https://a.espncdn.com/i/teamlogos/nba/500/wsh.png",
}
NHL_LOGOS = {
    "ANA":"https://a.espncdn.com/i/teamlogos/nhl/500/ana.png",
    "BOS":"https://a.espncdn.com/i/teamlogos/nhl/500/bos.png",
    "BUF":"https://a.espncdn.com/i/teamlogos/nhl/500/buf.png",
    "CAR":"https://a.espncdn.com/i/teamlogos/nhl/500/car.png",
    "CBJ":"https://a.espncdn.com/i/teamlogos/nhl/500/cbj.png",
    "CGY":"https://a.espncdn.com/i/teamlogos/nhl/500/cgy.png",
    "CHI":"https://a.espncdn.com/i/teamlogos/nhl/500/chi.png",
    "COL":"https://a.espncdn.com/i/teamlogos/nhl/500/col.png",
    "DAL":"https://a.espncdn.com/i/teamlogos/nhl/500/dal.png",
    "DET":"https://a.espncdn.com/i/teamlogos/nhl/500/det.png",
    "EDM":"https://a.espncdn.com/i/teamlogos/nhl/500/edm.png",
    "FLA":"https://a.espncdn.com/i/teamlogos/nhl/500/fla.png",
    "LAK":"https://a.espncdn.com/i/teamlogos/nhl/500/la.png",
    "MIN":"https://a.espncdn.com/i/teamlogos/nhl/500/min.png",
    "MTL":"https://a.espncdn.com/i/teamlogos/nhl/500/mon.png",
    "NJD":"https://a.espncdn.com/i/teamlogos/nhl/500/njd.png",
    "NSH":"https://a.espncdn.com/i/teamlogos/nhl/500/nsh.png",
    "NYI":"https://a.espncdn.com/i/teamlogos/nhl/500/nyi.png",
    "NYR":"https://a.espncdn.com/i/teamlogos/nhl/500/nyr.png",
    "OTT":"https://a.espncdn.com/i/teamlogos/nhl/500/ott.png",
    "PHI":"https://a.espncdn.com/i/teamlogos/nhl/500/phi.png",
    "PIT":"https://a.espncdn.com/i/teamlogos/nhl/500/pit.png",
    "SEA":"https://a.espncdn.com/i/teamlogos/nhl/500/sea.png",
    "SJS":"https://a.espncdn.com/i/teamlogos/nhl/500/sj.png",
    "STL":"https://a.espncdn.com/i/teamlogos/nhl/500/stl.png",
    "TBL":"https://a.espncdn.com/i/teamlogos/nhl/500/tb.png",
    "TOR":"https://a.espncdn.com/i/teamlogos/nhl/500/tor.png",
    "UTA":"https://a.espncdn.com/i/teamlogos/nhl/500/utah.png",
    "VAN":"https://a.espncdn.com/i/teamlogos/nhl/500/van.png",
    "VGK":"https://a.espncdn.com/i/teamlogos/nhl/500/vgk.png",
    "WPG":"https://a.espncdn.com/i/teamlogos/nhl/500/wpg.png",
    "WSH":"https://a.espncdn.com/i/teamlogos/nhl/500/wsh.png",
}
MLB_LOGOS = {
    "ARI":"https://a.espncdn.com/i/teamlogos/mlb/500/ari.png","ATL":"https://a.espncdn.com/i/teamlogos/mlb/500/atl.png",
    "BAL":"https://a.espncdn.com/i/teamlogos/mlb/500/bal.png","BOS":"https://a.espncdn.com/i/teamlogos/mlb/500/bos.png",
    "CHC":"https://a.espncdn.com/i/teamlogos/mlb/500/chc.png","CWS":"https://a.espncdn.com/i/teamlogos/mlb/500/chw.png",
    "CIN":"https://a.espncdn.com/i/teamlogos/mlb/500/cin.png","CLE":"https://a.espncdn.com/i/teamlogos/mlb/500/cle.png",
    "COL":"https://a.espncdn.com/i/teamlogos/mlb/500/col.png","DET":"https://a.espncdn.com/i/teamlogos/mlb/500/det.png",
    "HOU":"https://a.espncdn.com/i/teamlogos/mlb/500/hou.png","KC":"https://a.espncdn.com/i/teamlogos/mlb/500/kc.png",
    "LAA":"https://a.espncdn.com/i/teamlogos/mlb/500/laa.png","LAD":"https://a.espncdn.com/i/teamlogos/mlb/500/lad.png",
    "MIA":"https://a.espncdn.com/i/teamlogos/mlb/500/mia.png","MIL":"https://a.espncdn.com/i/teamlogos/mlb/500/mil.png",
    "MIN":"https://a.espncdn.com/i/teamlogos/mlb/500/min.png","NYM":"https://a.espncdn.com/i/teamlogos/mlb/500/nym.png",
    "NYY":"https://a.espncdn.com/i/teamlogos/mlb/500/nyy.png","OAK":"https://a.espncdn.com/i/teamlogos/mlb/500/oak.png",
    "PHI":"https://a.espncdn.com/i/teamlogos/mlb/500/phi.png","PIT":"https://a.espncdn.com/i/teamlogos/mlb/500/pit.png",
    "SD":"https://a.espncdn.com/i/teamlogos/mlb/500/sd.png","SF":"https://a.espncdn.com/i/teamlogos/mlb/500/sf.png",
    "SEA":"https://a.espncdn.com/i/teamlogos/mlb/500/sea.png","STL":"https://a.espncdn.com/i/teamlogos/mlb/500/stl.png",
    "TB":"https://a.espncdn.com/i/teamlogos/mlb/500/tb.png","TEX":"https://a.espncdn.com/i/teamlogos/mlb/500/tex.png",
    "TOR":"https://a.espncdn.com/i/teamlogos/mlb/500/tor.png","WSH":"https://a.espncdn.com/i/teamlogos/mlb/500/wsh.png",
}

# ── GAME TUNING ────────────────────────────────────────────────────────────
NFL_START_YEAR   = 1920
MLB_START_YEAR   = 1900
NBA_START_YEAR   = 1946
NHL_START_YEAR   = 1917
MAX_MISS_STREAK  = 5   # Full rounds (both players) before alternate-win
WIN_HOLD_TURNS   = 3   # Full rounds to hold three-in-a-row
SUDDEN_DEATH_ROUNDS = 5  # Full rounds of sudden death
HINTS_PER_PLAYER = 3
EXCLUDED_POSITIONS = {"K","P","LS","PK","PT"}

# ── STAT CATEGORIES ───────────────────────────────────────────────────────
NFL_STAT_CATEGORIES = [
    {"key":"rush_1000","label":"1000+ Rush Yds","desc":"1,000+ rushing yards in a season"},
    {"key":"rec_1000","label":"1000+ Rec Yds","desc":"1,000+ receiving yards in a season"},
    {"key":"pass_4000","label":"4000+ Pass Yds","desc":"4,000+ passing yards in a season"},
    {"key":"pass_td_30","label":"30+ Pass TDs","desc":"30+ passing touchdowns in a season"},
    {"key":"rush_td_10","label":"10+ Rush TDs","desc":"10+ rushing touchdowns in a season"},
    {"key":"rec_td_10","label":"10+ Rec TDs","desc":"10+ receiving touchdowns in a season"},
    {"key":"rec_100","label":"100+ Receptions","desc":"100+ receptions in a season"},
    {"key":"sack_10","label":"10+ Sacks","desc":"10+ sacks in a season"},
    {"key":"int_5","label":"5+ INTs","desc":"5+ interceptions in a season"},
]
MLB_STAT_CATEGORIES = [
    {"key":"hr_30","label":"30+ HRs","desc":"30+ home runs in a season"},
    {"key":"hr_40","label":"40+ HRs","desc":"40+ home runs in a season"},
    {"key":"hr_50","label":"50+ HRs","desc":"50+ home runs in a season"},
    {"key":"rbi_100","label":"100+ RBIs","desc":"100+ RBIs in a season"},
    {"key":"rbi_130","label":"130+ RBIs","desc":"130+ RBIs in a season"},
    {"key":"avg_300","label":".300+ AVG","desc":".300+ batting average (min 400 AB)"},
    {"key":"avg_350","label":".350+ AVG","desc":".350+ batting average (min 400 AB)"},
    {"key":"hits_200","label":"200+ Hits","desc":"200+ hits in a season"},
    {"key":"hits_220","label":"220+ Hits","desc":"220+ hits in a season"},
    {"key":"sb_30","label":"30+ SBs","desc":"30+ stolen bases in a season"},
    {"key":"sb_40","label":"40+ SBs","desc":"40+ stolen bases in a season"},
    {"key":"runs_120","label":"120+ Runs","desc":"120+ runs scored in a season"},
    {"key":"doubles_40","label":"40+ Doubles","desc":"40+ doubles in a season"},
    {"key":"triples_10","label":"10+ Triples","desc":"10+ triples in a season"},
    {"key":"slg_500","label":".500+ SLG","desc":".500+ slugging percentage (min 400 AB)"},
    {"key":"slg_600","label":".600+ SLG","desc":".600+ slugging percentage (min 400 AB)"},
    {"key":"wins_15","label":"15+ Wins (P)","desc":"15+ wins as a pitcher"},
    {"key":"wins_20","label":"20+ Wins (P)","desc":"20+ wins as a pitcher"},
    {"key":"wins_25","label":"25+ Wins (P)","desc":"25+ wins as a pitcher"},
    {"key":"k_200","label":"200+ Ks (P)","desc":"200+ strikeouts as a pitcher"},
    {"key":"k_250","label":"250+ Ks (P)","desc":"250+ strikeouts as a pitcher"},
    {"key":"k_300","label":"300+ Ks (P)","desc":"300+ strikeouts as a pitcher"},
    {"key":"era_sub3","label":"Sub-3.00 ERA (P)","desc":"ERA under 3.00 (min 100 IP)"},
    {"key":"era_sub2","label":"Sub-2.00 ERA (P)","desc":"ERA under 2.00 (min 100 IP)"},
    {"key":"win_pct_700","label":"70%+ Win Pct","desc":"70%+ win percentage (min 25 decisions)"},
    {"key":"cg_30","label":"30+ CGs","desc":"30+ complete games in a season"},
    {"key":"sho_5","label":"5+ Shutouts","desc":"5+ shutouts in a season"},
]
NBA_STAT_CATEGORIES = [
    {"key":"pts_25","label":"25+ PPG","desc":"25+ points per game in a season (min 40 GP)"},
    {"key":"pts_30","label":"30+ PPG","desc":"30+ points per game in a season (min 40 GP)"},
    {"key":"reb_10","label":"10+ RPG","desc":"10+ rebounds per game in a season (min 40 GP)"},
    {"key":"ast_10","label":"10+ APG","desc":"10+ assists per game in a season (min 40 GP)"},
    {"key":"pts_2000","label":"2000+ Pts","desc":"2000+ total points in a season"},
    {"key":"blk_150","label":"150+ Blocks","desc":"150+ blocks in a season"},
    {"key":"stl_150","label":"150+ Steals","desc":"150+ steals in a season"},
]
NHL_STAT_CATEGORIES = [
    {"key":"goals_30","label":"30+ Goals","desc":"30+ goals in a season"},
    {"key":"goals_50","label":"50+ Goals","desc":"50+ goals in a season"},
    {"key":"points_80","label":"80+ Points","desc":"80+ points in a season"},
    {"key":"points_100","label":"100+ Points","desc":"100+ points in a season"},
    {"key":"goalie_30w","label":"30+ Wins (G)","desc":"30+ wins as a goalie in a season"},
]

# ── NAME HANDLING ──────────────────────────────────────────────────────────
def sanitize_name(name: str) -> str:
    if not name: return ""
    name = name.replace("\u2019","'").replace("\u2018","'").replace("\u02bc","'")
    name = unicodedata.normalize("NFC", name)
    return name.strip()

def _strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn').lower()

NFL_PLAYER_ALIASES = {
    "Nickell Robey":"Nickell Robey-Coleman","Chad Johnson":"Chad Ochocinco",
    "Robert Griffin":"Robert Griffin III","Melvin Gordon III":"Melvin Gordon",
    "Odell Beckham":"Odell Beckham Jr.","Patrick Mahomes":"Patrick Mahomes II",
    "Will Fuller":"Will Fuller V","Kenneth Walker":"Kenneth Walker III",
    "Brian Robinson":"Brian Robinson Jr.",
}
MLB_PLAYER_ALIASES = {
    "Mike Stanton":"Giancarlo Stanton","Jake deGrom":"Jacob deGrom",
    "Dee Gordon":"Dee Strange-Gordon","Hank Aaron":"Henry Aaron",
    "Nap Lajoie":"Napoleon Lajoie","Yogi Berra":"Lawrence Berra",
}
NBA_PLAYER_ALIASES = {
    "Ron Artest":"Metta World Peace","Metta World Peace":"Metta World Peace",
    "Stephen Curry":"Stephen Curry","Steph Curry":"Stephen Curry",
}
NHL_PLAYER_ALIASES = {}

def _normalise_player_name(name, aliases):
    name = sanitize_name(name)
    return aliases.get(name, name)

# ── USERNAME VALIDATION ───────────────────────────────────────────────────
_USERNAME_RE = re.compile(r'^[A-Za-z0-9_]{2,20}$')

def _validate_username(username):
    """Usernames must be alphanumeric + underscores only, 2-20 chars."""
    return bool(_USERNAME_RE.match(username))

# ── USER DATABASE ──────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        username         TEXT    UNIQUE NOT NULL,
        password_hash    TEXT    NOT NULL,
        nfl_mascot       TEXT    NOT NULL DEFAULT 'KC',
        mlb_mascot       TEXT    NOT NULL DEFAULT 'NYY',
        nba_mascot       TEXT    NOT NULL DEFAULT 'LAL',
        nhl_mascot       TEXT    NOT NULL DEFAULT 'BOS',
        lifetime_correct INTEGER NOT NULL DEFAULT 0,
        lifetime_total   INTEGER NOT NULL DEFAULT 0,
        wins             INTEGER NOT NULL DEFAULT 0,
        losses           INTEGER NOT NULL DEFAULT 0,
        draws            INTEGER NOT NULL DEFAULT 0,
        win_streak       INTEGER NOT NULL DEFAULT 0,
        best_streak      INTEGER NOT NULL DEFAULT 0,
        created_at       DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")
    for col, defval in [
        ("nfl_mascot","'KC'"),("mlb_mascot","'NYY'"),
        ("nba_mascot","'LAL'"),("nhl_mascot","'BOS'"),
        ("wins","0"),("losses","0"),("draws","0"),
        ("win_streak","0"),("best_streak","0"),
    ]:
        try: con.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT NOT NULL DEFAULT {defval}")
        except Exception: pass
    con.commit(); con.close()

def get_user(username):
    con = sqlite3.connect(DB_PATH); con.row_factory = sqlite3.Row
    row = con.execute("SELECT * FROM users WHERE LOWER(username)=LOWER(?)",(username,)).fetchone()
    con.close(); return dict(row) if row else None

def create_user(username, password, nfl_mascot, mlb_mascot="NYY", nba_mascot="LAL", nhl_mascot="BOS"):
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute("INSERT INTO users (username,password_hash,nfl_mascot,mlb_mascot,nba_mascot,nhl_mascot) VALUES (?,?,?,?,?,?)",
                    (username, generate_password_hash(password), nfl_mascot, mlb_mascot, nba_mascot, nhl_mascot))
        con.commit(); con.close(); return get_user(username)
    except sqlite3.IntegrityError: return None

def update_lifetime_stats(user_id, correct, total, won=None, draw=False):
    con = sqlite3.connect(DB_PATH)
    if won is True:
        con.execute("UPDATE users SET lifetime_correct=lifetime_correct+?,lifetime_total=lifetime_total+?,wins=wins+1,win_streak=win_streak+1,best_streak=MAX(best_streak,win_streak+1) WHERE id=?",(correct,total,user_id))
    elif won is False:
        con.execute("UPDATE users SET lifetime_correct=lifetime_correct+?,lifetime_total=lifetime_total+?,losses=losses+1,win_streak=0 WHERE id=?",(correct,total,user_id))
    elif draw:
        con.execute("UPDATE users SET lifetime_correct=lifetime_correct+?,lifetime_total=lifetime_total+?,draws=draws+1 WHERE id=?",(correct,total,user_id))
    else:
        con.execute("UPDATE users SET lifetime_correct=lifetime_correct+?,lifetime_total=lifetime_total+? WHERE id=?",(correct,total,user_id))
    con.commit(); con.close()

def update_mascot(user_id, sport, mascot):
    col = {"nfl":"nfl_mascot","mlb":"mlb_mascot","nba":"nba_mascot","nhl":"nhl_mascot"}.get(sport,"nfl_mascot")
    con = sqlite3.connect(DB_PATH)
    con.execute(f"UPDATE users SET {col}=? WHERE id=?",(mascot,user_id))
    con.commit(); con.close()

init_db()

# ── LOAD PLAYER DATA FROM PRE-BUILT CACHE ─────────────────────────────────
def _load_cache(sport):
    cache_file = os.path.join(CACHE_DIR, f"{sport}_players.json")
    if not os.path.exists(cache_file):
        print(f"  WARNING: {cache_file} not found — {sport.upper()} will have no players.")
        print(f"  Run the appropriate build script to generate this file.")
        return []
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            data = _json.load(f)
        print(f"  {sport.upper()} loaded: {len(data)} players")
        return data
    except Exception as e:
        print(f"  ERROR loading {sport.upper()} cache: {e}")
        return []

def _build_team_index(players_db, key="teams"):
    index = {}
    for p in players_db:
        for t in p.get(key, []):
            index.setdefault(t, []).append(p)
    return index

def _build_stat_cache(players_db):
    """Return (totals, counts) where totals[(team,stat)] = total qualifying seasons,
    counts[(team,stat)] = number of unique qualifying players."""
    totals = {}
    counts = {}
    for p in players_db:
        for stat_key, team_dict in p.get("achievements", {}).items():
            if isinstance(team_dict, dict):
                for team, count in team_dict.items():
                    totals[(team, stat_key)] = totals.get((team, stat_key), 0) + count
                    counts[(team, stat_key)] = counts.get((team, stat_key), 0) + 1
    return totals, counts

def _build_name_index(players_db):
    """Build a lowercase name -> player lookup for O(1) guess resolution."""
    index = {}
    for p in players_db:
        key = p["name"].lower()
        index[key] = p
        # Also index accent-stripped variant
        stripped = _strip_accents(p["name"])
        if stripped != key:
            index.setdefault(stripped, p)
    return index

print("Loading player databases from cache...")
PLAYERS_DB       = _load_cache("nfl")
MLB_PLAYERS_DB   = _load_cache("mlb")
NBA_PLAYERS_DB   = _load_cache("nba")
NHL_PLAYERS_DB   = _load_cache("nhl")

TEAM_INDEX       = _build_team_index(PLAYERS_DB)
MLB_TEAM_INDEX   = _build_team_index(MLB_PLAYERS_DB)
NBA_TEAM_INDEX   = _build_team_index(NBA_PLAYERS_DB)
NHL_TEAM_INDEX   = _build_team_index(NHL_PLAYERS_DB)

PLAYER_NAMES_SORTED      = sorted(p["name"] for p in PLAYERS_DB)
MLB_PLAYER_NAMES_SORTED  = sorted(p["name"] for p in MLB_PLAYERS_DB)
NBA_PLAYER_NAMES_SORTED  = sorted(p["name"] for p in NBA_PLAYERS_DB)
NHL_PLAYER_NAMES_SORTED  = sorted(p["name"] for p in NHL_PLAYERS_DB)

_NFL_STAT_CACHE, _NFL_STAT_COUNT = _build_stat_cache(PLAYERS_DB)
_MLB_STAT_CACHE, _MLB_STAT_COUNT = _build_stat_cache(MLB_PLAYERS_DB)
_NBA_STAT_CACHE, _NBA_STAT_COUNT = _build_stat_cache(NBA_PLAYERS_DB)
_NHL_STAT_CACHE, _NHL_STAT_COUNT = _build_stat_cache(NHL_PLAYERS_DB)

NFL_NAME_INDEX = _build_name_index(PLAYERS_DB)
MLB_NAME_INDEX = _build_name_index(MLB_PLAYERS_DB)
NBA_NAME_INDEX = _build_name_index(NBA_PLAYERS_DB)
NHL_NAME_INDEX = _build_name_index(NHL_PLAYERS_DB)

print("All databases loaded.")

# ── RARITY CALCULATIONS ───────────────────────────────────────────────────
# Pre-compute crossover totals at startup to avoid linear scans per guess.
def _precompute_crossover(players_db, games_key):
    """Build {(teamA,teamB): total_games} for all team pairs with shared players."""
    cache = {}
    for p in players_db:
        teams = p.get("teams", [])
        games = p.get(games_key, {})
        for i, ta in enumerate(teams):
            for tb in teams[i:]:
                key = (min(ta, tb), max(ta, tb))
                cache[key] = cache.get(key, 0) + games.get(ta, 0) + games.get(tb, 0)
    return cache

def _precompute_crossover_counts(players_db):
    """Build {(teamA,teamB): player_count} for all team pairs."""
    cache = {}
    for p in players_db:
        teams = p.get("teams", [])
        for i, ta in enumerate(teams):
            for tb in teams[i:]:
                key = (min(ta, tb), max(ta, tb))
                cache[key] = cache.get(key, 0) + 1
    return cache

_NFL_CROSS_TOTAL  = _precompute_crossover(PLAYERS_DB, "weeks_by_team")
_MLB_CROSS_TOTAL  = _precompute_crossover(MLB_PLAYERS_DB, "games_by_team")
_NBA_CROSS_TOTAL  = _precompute_crossover(NBA_PLAYERS_DB, "games_by_team")
_NHL_CROSS_TOTAL  = _precompute_crossover(NHL_PLAYERS_DB, "games_by_team")

_NFL_CROSS_COUNT  = _precompute_crossover_counts(PLAYERS_DB)
_MLB_CROSS_COUNT  = _precompute_crossover_counts(MLB_PLAYERS_DB)
_NBA_CROSS_COUNT  = _precompute_crossover_counts(NBA_PLAYERS_DB)
_NHL_CROSS_COUNT  = _precompute_crossover_counts(NHL_PLAYERS_DB)

_CURRENT_YEAR_RARITY = 2026  # Used for era adjustment in rarity calc

def _calc_rarity_common(player, team_a, team_b, games_key, cross_total, cross_count, stat_total, stat_count):
    """
    Rarity formula:
      Lower rarity = more obvious answer = better score for the guesser.
      Higher rarity = deep cut = rare find (harder to steal).

    Crossover cells: factors in games played + era (older = lower rarity).
    Stat cells: factors in seasons completed + pool size + era.
    """
    debut = player.get("debut_year", 0)
    try:
        years_ago = max(0, _CURRENT_YEAR_RARITY - int(debut)) if debut else 0
    except (TypeError, ValueError):
        years_ago = 0

    # ── STAT CATEGORY RARITY ───────────────────────────────
    if team_a.startswith("STAT:"):
        stat_key = team_a.split(":", 1)[1]
        p_seasons = player.get("achievements", {}).get(stat_key, {}).get(team_b, 0)
        if p_seasons == 0: return 0.5
        pool_total = stat_total.get((team_b, stat_key), 1)
        pool_count = stat_count.get((team_b, stat_key), 1)
        avg_seasons = pool_total / pool_count if pool_count > 0 else 1
        # More seasons = more obvious = higher rarity
        ratio = p_seasons / avg_seasons
        base = 99 - 98 / (1 + max(0.01, ratio) ** 1.3)
        # Bump rarity slightly for very small pools (hard to recall anyone)
        if pool_count < 3:
            base *= 1.1
        elif pool_count < 6:
            base *= 1.05
        # Era reduction: older = less obvious
        era_reduction = min(30, (years_ago / 3) ** 0.85) if years_ago > 0 else 0
        rarity = base - era_reduction
        return round(max(1, min(99, rarity)) / 100, 4)

    # ── SAME-TEAM FALLBACK (shouldn't trigger with valid board) ──
    if team_a == team_b:
        g = player.get(games_key, {}).get(team_a, 0)
        key = (team_a, team_a)
        total = cross_total.get(key, 0)
        n = cross_count.get(key, 0)
        if total <= 0 or n <= 1: return 0.5
        ratio = (g / total) * n if total > 0 else 0
        rarity = 99 - 98 / (1 + max(0.01, ratio) ** 1.2)
        era_reduction = min(25, (years_ago / 4) ** 0.85) if years_ago > 0 else 0
        rarity -= era_reduction
        return round(max(1, min(99, rarity)) / 100, 4)

    # ── TEAM-TEAM CROSSOVER RARITY ─────────────────────────
    g_a = player.get(games_key, {}).get(team_a, 0)
    g_b = player.get(games_key, {}).get(team_b, 0)
    key = (min(team_a, team_b), max(team_a, team_b))
    total = cross_total.get(key, 0)
    n = cross_count.get(key, 0)
    if total <= 0 or n <= 1: return 0.5
    # Share of total crossover games, scaled by pool size
    share = (g_a + g_b) / total
    ratio = share * n  # 1.0 = average amount of playing time
    # Smooth sigmoid: ratio=0→1, ratio=1→50, ratio=3→82, ratio=10→97
    base = 99 - 98 / (1 + max(0.01, ratio) ** 1.2)
    # Era reduction: older players score lower
    era_reduction = min(25, (years_ago / 4) ** 0.85) if years_ago > 0 else 0
    rarity = base - era_reduction
    return round(max(1, min(99, rarity)) / 100, 4)

def calc_rarity(player, team_a, team_b):
    return _calc_rarity_common(player, team_a, team_b, "weeks_by_team", _NFL_CROSS_TOTAL, _NFL_CROSS_COUNT, _NFL_STAT_CACHE, _NFL_STAT_COUNT)

def calc_mlb_rarity(player, team_a, team_b):
    return _calc_rarity_common(player, team_a, team_b, "games_by_team", _MLB_CROSS_TOTAL, _MLB_CROSS_COUNT, _MLB_STAT_CACHE, _MLB_STAT_COUNT)

def calc_nba_rarity(player, team_a, team_b):
    return _calc_rarity_common(player, team_a, team_b, "games_by_team", _NBA_CROSS_TOTAL, _NBA_CROSS_COUNT, _NBA_STAT_CACHE, _NBA_STAT_COUNT)

def calc_nhl_rarity(player, team_a, team_b):
    return _calc_rarity_common(player, team_a, team_b, "games_by_team", _NHL_CROSS_TOTAL, _NHL_CROSS_COUNT, _NHL_STAT_CACHE, _NHL_STAT_COUNT)

# ── BOARD GENERATION ──────────────────────────────────────────────────────
WIN_LINES = [(0,1,2),(3,4,5),(6,7,8),(0,3,6),(1,4,7),(2,5,8),(0,4,8),(2,4,6)]

def pick_teams_with_shared_players(pool, index, needed=3, attempts=100):
    for _ in range(attempts):
        sample = random.sample(pool, needed)
        valid = all(
            any(b in p.get("teams", []) for p in index.get(a, []))
            for i, a in enumerate(sample) for j, b in enumerate(sample) if i != j
        )
        if valid: return sample
    return random.sample(pool, needed)

def _stat_row_has_valid_cells(stat_key, cols, db):
    for team_b in cols:
        if not any(team_b in p.get("teams", []) and team_b in p.get("achievements", {}).get(stat_key, []) for p in db):
            return False
    return True

def _new_board_common(teams, team_index, stat_categories, players_db):
    rows = pick_teams_with_shared_players(teams, team_index)
    cols = pick_teams_with_shared_players(teams, team_index)
    att = 0
    while set(rows) & set(cols) and att < 50:
        cols = pick_teams_with_shared_players(teams, team_index); att += 1
    stat_metas = {}  # {stat_key: {key, label, desc}}
    # Randomly decide 0, 1, or 2 stat rows
    num_stats = random.choices([0, 1, 2], weights=[20, 55, 25], k=1)[0]
    if num_stats == 0 or not stat_categories:
        return rows, cols, stat_metas
    # Choose random row positions for stat rows
    positions = random.sample([0, 1, 2], num_stats)
    cats = list(stat_categories); random.shuffle(cats)
    for pos in positions:
        for cat in cats:
            if cat["key"] in stat_metas:
                continue
            if sum(1 for p in players_db if cat["key"] in p.get("achievements", {})) < 10:
                continue
            if _stat_row_has_valid_cells(cat["key"], cols, players_db):
                rows[pos] = f"STAT:{cat['key']}"
                stat_metas[cat["key"]] = {"key": cat["key"], "label": cat["label"], "desc": cat["desc"]}
                break
    return rows, cols, stat_metas

def new_board():       return _new_board_common(NFL_TEAMS, TEAM_INDEX, NFL_STAT_CATEGORIES, PLAYERS_DB)
def mlb_new_board():   return _new_board_common(MLB_TEAMS, MLB_TEAM_INDEX, MLB_STAT_CATEGORIES, MLB_PLAYERS_DB)
def nba_new_board():   return _new_board_common(NBA_TEAMS, NBA_TEAM_INDEX, NBA_STAT_CATEGORIES, NBA_PLAYERS_DB)
def nhl_new_board():   return _new_board_common(NHL_TEAMS, NHL_TEAM_INDEX, NHL_STAT_CATEGORIES, NHL_PLAYERS_DB)

# ── WIN & STATE LOGIC ────────────────────────────────────────────────────
def _get_active_lines(board, uid):
    return [line for line in WIN_LINES if all(board.get(str(i), {}).get("owner") == uid for i in line)]

def _avg_rarity_of_lines(board, lines):
    if not lines: return 1.0
    total, count = 0.0, 0
    for line in lines:
        for i in line:
            cell = board.get(str(i))
            if cell and "rarity" in cell: total += cell["rarity"]; count += 1
    return (total / count) if count > 0 else 1.0

def count_squares(board, pid): return sum(1 for c in board.values() if c and c.get("owner") == pid)
def total_rarity(board, pid): return sum(c["rarity"] for c in board.values() if c and c.get("owner") == pid)

def _resolve_win(s, phrases):
    if s["game_over"]: return
    uid1, uid2, board = s["players"][1]["user_id"], s["players"][2]["user_id"], s["board"]
    lines_p1, lines_p2 = _get_active_lines(board, uid1), _get_active_lines(board, uid2)
    if lines_p1 and lines_p2:
        dtt = s.get("double_ttt")
        sd_half = SUDDEN_DEATH_ROUNDS * 2
        if dtt is None:
            s["double_ttt"] = {"turns_held": 0, "turns_remaining": sd_half}
        else:
            dtt["turns_held"] += 1
            dtt["turns_remaining"] = max(0, sd_half - dtt["turns_held"])
            if dtt["turns_held"] >= sd_half:
                r1, r2 = _avg_rarity_of_lines(board, lines_p1), _avg_rarity_of_lines(board, lines_p2)
                winner_turn = 1 if r1 <= r2 else 2
                wname = s["players"][winner_turn]["username"]
                wr = round(r1 * 100) if winner_turn == 1 else round(r2 * 100)
                lr = round(r2 * 100) if winner_turn == 1 else round(r1 * 100)
                s["game_over"] = True; s["winner"] = winner_turn
                s["win_reason"] = f"Sudden death! {wname} wins by lower rarity — {wr} vs {lr}"
                _flush_stats(s, winner_turn); return
        # Store live rarity averages for frontend display
        dtt_ref = s["double_ttt"]
        dtt_ref["p1_rarity"] = round(_avg_rarity_of_lines(board, lines_p1) * 100)
        dtt_ref["p2_rarity"] = round(_avg_rarity_of_lines(board, lines_p2) * 100)
        s["hold_line"] = None
        return
    else:
        s["double_ttt"] = None
    three_owner = uid1 if lines_p1 else (uid2 if lines_p2 else None)
    if three_owner:
        owner_turn = next((t for t, sl in s["players"].items() if sl["user_id"] == three_owner), None)
        if s["hold_line"] and s["hold_line"]["owner"] == three_owner:
            s["hold_line"]["turns_held"] += 1
        else:
            s["hold_line"] = {"owner": three_owner, "owner_turn": owner_turn, "turns_held": 0}
        hold_threshold = WIN_HOLD_TURNS * 2 - 1  # 3 full rounds = 5 half-turns (0-indexed)
        if s["hold_line"]["turns_held"] >= hold_threshold:
            owner_turn = s["hold_line"].get("owner_turn") or next(
                (t for t, sl in s["players"].items() if sl["user_id"] == three_owner), None)
            if owner_turn is None: return
            wname = s["players"][owner_turn]["username"]
            s["game_over"] = True; s["winner"] = owner_turn
            s["win_reason"] = random.choice(phrases).format(winner=wname) + f" (held for {WIN_HOLD_TURNS} rounds)"
            _flush_stats(s, owner_turn); return
    else:
        s["hold_line"] = None
    _check_alternate_win(s, phrases)

def _check_alternate_win(s, phrases):
    if s["game_over"] or s["miss_streak"] < MAX_MISS_STREAK * 2: return
    uid1, uid2 = s["players"][1]["user_id"], s["players"][2]["user_id"]
    p1, p2 = count_squares(s["board"], uid1), count_squares(s["board"], uid2)
    s["game_over"] = True
    if p1 != p2:
        winner_turn = 1 if p1 > p2 else 2
        s["winner"] = winner_turn
        s["win_reason"] = random.choice(phrases).format(winner=s["players"][winner_turn]["username"]) + f" ({max(p1, p2)} vs {min(p1, p2)} squares)"
    else:
        r1, r2 = total_rarity(s["board"], uid1), total_rarity(s["board"], uid2)
        if abs(r1 - r2) < 1e-6:
            s["winner"] = 0
            s["win_reason"] = f"It is a draw! {s['players'][1]['username']} and {s['players'][2]['username']} are perfectly matched."
            _flush_stats(s, None); return
        winner_turn = 1 if r1 <= r2 else 2
        wname = s["players"][winner_turn]["username"]
        wr = round(r1 * 100) if winner_turn == 1 else round(r2 * 100)
        lr = round(r2 * 100) if winner_turn == 1 else round(r1 * 100)
        s["winner"] = winner_turn
        s["win_reason"] = f"Rarity tiebreak! {wname} wins — {wr} vs {lr}"
    _flush_stats(s, s.get("winner"))

def _flush_stats(s, winner_turn=None):
    if s.get("_stats_flushed"): return
    s["_stats_flushed"] = True
    is_draw = winner_turn == 0
    for turn, slot in s["players"].items():
        if slot.get("is_guest") or slot.get("is_bot"): continue
        if is_draw:
            if slot["session_total"] > 0: update_lifetime_stats(slot["user_id"], slot["session_correct"], slot["session_total"], draw=True)
        else:
            won = (turn == winner_turn) if winner_turn is not None else None
            if slot["session_total"] > 0 or won is not None: update_lifetime_stats(slot["user_id"], slot["session_correct"], slot["session_total"], won=won)

# ── PLAYER SLOTS & STATE ─────────────────────────────────────────────────
def make_player_slot(user, sport="nfl"):
    is_guest = str(user.get("id", "")).startswith("guest_")
    is_bot = str(user.get("id", "")).startswith("bot_")
    fresh = is_guest or is_bot
    mascot_key = {"nfl":"nfl_mascot","mlb":"mlb_mascot","nba":"nba_mascot","nhl":"nhl_mascot"}.get(sport, "nfl_mascot")
    default_mascot = {"nfl":"KC","mlb":"NYY","nba":"LAL","nhl":"BOS"}.get(sport, "KC")
    logos = {"nfl":TEAM_LOGOS,"mlb":MLB_LOGOS,"nba":NBA_LOGOS,"nhl":NHL_LOGOS}.get(sport, TEAM_LOGOS)
    mascot = user.get(mascot_key, default_mascot)
    return {
        "user_id": user["id"], "username": user["username"],
        "mascot": mascot, "mascot_logo": logos.get(mascot, ""),
        "hints_remaining": HINTS_PER_PLAYER, "session_correct": 0, "session_total": 0,
        "lifetime_correct": 0 if fresh else user.get("lifetime_correct", 0),
        "lifetime_total": 0 if fresh else user.get("lifetime_total", 0),
        "wins": 0 if fresh else user.get("wins", 0),
        "losses": 0 if fresh else user.get("losses", 0),
        "draws": 0 if fresh else user.get("draws", 0),
        "win_streak": 0 if fresh else user.get("win_streak", 0),
        "best_streak": 0 if fresh else user.get("best_streak", 0),
        "is_guest": is_guest, "is_bot": is_bot,
        "bot_difficulty": user.get("bot_difficulty", ""),
    }

def _base_state(rows, cols, stat_metas, p1_user, p2_user, sport, data_years):
    return {
        "rows": rows, "cols": cols, "stat_categories": stat_metas, "board": {},
        "turn": 1, "used_players": set(), "miss_streak": 0,
        "hold_line": None, "double_ttt": None,
        "game_over": False, "winner": None, "win_reason": None,
        "turn_number": 0, "data_years": data_years, "sport": sport,
        "_stats_flushed": False, "_last_active": time.time(),
        "players": {1: make_player_slot(p1_user, sport), 2: make_player_slot(p2_user, sport)}
    }

def empty_state(p1, p2):     return _base_state(*new_board(), p1, p2, "nfl", f"{NFL_START_YEAR}\u20132026")
def mlb_empty_state(p1, p2): return _base_state(*mlb_new_board(), p1, p2, "mlb", f"{MLB_START_YEAR}\u20132026")
def nba_empty_state(p1, p2): return _base_state(*nba_new_board(), p1, p2, "nba", f"{NBA_START_YEAR}\u20132026")
def nhl_empty_state(p1, p2): return _base_state(*nhl_new_board(), p1, p2, "nhl", f"{NHL_START_YEAR}\u20132026")

# ── SERIALISATION ─────────────────────────────────────────────────────────
def _slot_json(p, board):
    lt, lc, st, sc = p["lifetime_total"], p["lifetime_correct"], p["session_total"], p["session_correct"]
    sq = count_squares(board, p["user_id"])
    wins, losses = int(p.get("wins", 0) or 0), int(p.get("losses", 0) or 0)
    return {
        "username": p["username"], "mascot": p["mascot"], "mascot_logo": p["mascot_logo"],
        "hints_remaining": p["hints_remaining"],
        "session_correct": sc, "session_total": st,
        "session_pct": round(sc / st * 100, 1) if st else 0.0,
        "lifetime_correct": lc, "lifetime_total": lt,
        "lifetime_pct": round(lc / lt * 100, 1) if lt else 0.0,
        "wins": wins, "losses": losses,
        "draws": int(p.get("draws", 0) or 0),
        "win_streak": int(p.get("win_streak", 0) or 0),
        "best_streak": int(p.get("best_streak", 0) or 0),
        "win_pct": round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0.0,
        "squares": sq, "rarity_total": round(total_rarity(board, p["user_id"]), 3),
        "is_bot": p.get("is_bot", False), "is_guest": p.get("is_guest", False),
        "bot_difficulty": p.get("bot_difficulty", "")
    }

def _serialise(s, team_names_map, team_logos_map, team_mascots_map, data_years_default):
    def _axis_name(t):
        if t.startswith("STAT:"):
            stat_key = t.split(":", 1)[1]
            cats = s.get("stat_categories", {})
            cat = cats.get(stat_key, {})
            return cat.get("label", stat_key)
        return team_names_map.get(t, t)
    def _axis_logo(t):
        # Stats have no logo — frontend will render the label as text instead
        if t.startswith("STAT:"): return ""
        return team_logos_map.get(t, "")
    def _axis_mascot(t):
        if t.startswith("STAT:"): return ""
        return team_mascots_map.get(t, "")
    # Convert used_players set to list for JSON safety
    serialised = {
        "room_id": s.get("room_id"),
        "sport": s.get("sport", "nfl"),
        "rows": s["rows"], "cols": s["cols"],
        "row_names": [_axis_name(t) for t in s["rows"]],
        "col_names": [_axis_name(t) for t in s["cols"]],
        "row_logos": [_axis_logo(t) for t in s["rows"]],
        "col_logos": [_axis_logo(t) for t in s["cols"]],
        "row_mascots": [_axis_mascot(t) for t in s["rows"]],
        "col_mascots": [_axis_mascot(t) for t in s["cols"]],
        "stat_categories": s.get("stat_categories", {}), "board": dict(s["board"]),
        "turn": s["turn"], "miss_streak": s["miss_streak"], "hold_line": s["hold_line"],
        "double_ttt": s.get("double_ttt"), "game_over": s["game_over"],
        "winner": s["winner"], "win_reason": s["win_reason"], "turn_number": s["turn_number"],
        "data_years": s.get("data_years", data_years_default),
        "player1": _slot_json(s["players"][1], s["board"]),
        "player2": _slot_json(s["players"][2], s["board"])
    }
    # Attach best-possible answers when the game ends (for the end-screen reveal board)
    if s.get("game_over") and s.get("_best_answers"):
        serialised["best_answers"] = s["_best_answers"]
    return serialised

def _serialise_with_best(s, team_names_map, team_logos_map, team_mascots_map, data_years_default, db, calc_fn):
    """Serialise + compute best answers on first game-over call (cached after)."""
    if s.get("game_over") and not s.get("_best_answers"):
        try:
            s["_best_answers"] = _find_best_answers_all_cells(s, db, calc_fn)
        except Exception:
            s["_best_answers"] = {}
    return _serialise(s, team_names_map, team_logos_map, team_mascots_map, data_years_default)

def serialise_state(s):     return _serialise_with_best(s, TEAM_NAMES, TEAM_LOGOS, TEAM_MASCOTS, f"{NFL_START_YEAR}\u20132026", PLAYERS_DB, calc_rarity)
def mlb_serialise_state(s):  return _serialise_with_best(s, MLB_TEAM_NAMES, MLB_LOGOS, MLB_TEAM_MASCOTS, f"{MLB_START_YEAR}\u20132026", MLB_PLAYERS_DB, calc_mlb_rarity)
def nba_serialise_state(s):  return _serialise_with_best(s, NBA_TEAM_NAMES, NBA_LOGOS, NBA_TEAM_MASCOTS, f"{NBA_START_YEAR}\u20132026", NBA_PLAYERS_DB, calc_nba_rarity)
def nhl_serialise_state(s):  return _serialise_with_best(s, NHL_TEAM_NAMES, NHL_LOGOS, NHL_TEAM_MASCOTS, f"{NHL_START_YEAR}\u20132026", NHL_PLAYERS_DB, calc_nhl_rarity)

# ── HELPERS ───────────────────────────────────────────────────────────────
def _switch_turn(s): s["turn"] = 2 if s["turn"] == 1 else 1

def _cell_entry(owner_uid, owner_turn, player, rarity):
    return {"owner": owner_uid, "owner_turn": owner_turn, "player_name": player["name"], "rarity": rarity, "headshot": player.get("headshot", "")}

def _resolve_user(player_data):
    if not player_data: return None
    uid = player_data.get("id", "")
    if str(uid).startswith("guest_") or str(uid).startswith("bot_"):
        return {
            "id": uid, "username": player_data.get("username", "Guest"),
            "nfl_mascot": player_data.get("nfl_mascot", "KC"), "mlb_mascot": player_data.get("mlb_mascot", "NYY"),
            "nba_mascot": player_data.get("nba_mascot", "LAL"), "nhl_mascot": player_data.get("nhl_mascot", "BOS"),
            "lifetime_correct": 0, "lifetime_total": 0,
            "wins": 0, "losses": 0, "draws": 0, "win_streak": 0, "best_streak": 0,
            "bot_difficulty": player_data.get("bot_difficulty", "medium")
        }
    return get_user(player_data.get("username", ""))

def _emit_update(room_id, serialised_state):
    """Emit game state update to all clients in the socket room."""
    try:
        socketio.emit('game_update', serialised_state, room=room_id)
    except Exception:
        pass  # Socket emit is best-effort; HTTP response is the primary channel

def _get_room_id_from_request():
    """Extract room_id from POST JSON body or GET query param."""
    if request.method == "POST":
        data = request.json or {}
        return data.get("room_id") or request.args.get("room_id")
    return request.args.get("room_id")

# ── PHRASES ──────────────────────────────────────────────────────────────
CORRECT_PHRASES = ["First down!", "Great connection!", "That's a completion!", "Right on target!"]
STEAL_PHRASES   = ["Intercepted! Pick six!", "Stripped and returned!", "Turnover on the field!"]
MISS_PHRASES    = ["Incomplete pass.", "Flag on the play.", "False start.", "Delay of game."]
WIN_PHRASES     = ["Final whistle — {winner} wins!", "Game over! {winner} takes it!", "Clock hits zero — {winner} wins!"]

MLB_CORRECT_PHRASES = ["Base hit!", "Great swing!", "That's a hit!", "Right down the line!"]
MLB_STEAL_PHRASES   = ["Stolen base! Safe!", "What a steal!", "He took that base!"]
MLB_MISS_PHRASES    = ["Strikeout.", "Foul ball.", "Swung and missed.", "Called strike three."]
MLB_WIN_PHRASES     = ["Final out — {winner} wins!", "Game over! {winner} takes the pennant!"]

NBA_CORRECT_PHRASES = ["Bucket!", "Nothing but net!", "That's a bucket!", "Two points!", "And it counts!"]
NBA_STEAL_PHRASES   = ["And-one steal!", "Stripped! Fast break!", "Pick-pocketed!", "Turnover converted!"]
NBA_MISS_PHRASES    = ["Brick.", "Off the rim.", "Air ball.", "No good.", "Rejected!"]
NBA_WIN_PHRASES     = ["Buzzer beater — {winner} wins!", "Game over! {winner} takes the championship!", "Final horn — {winner} wins!"]

NHL_CORRECT_PHRASES = ["Goal!", "Score!", "Top shelf!", "Bar down!", "He scores!"]
NHL_STEAL_PHRASES   = ["Icing called off — steal!", "Puck stolen!", "Cleared and taken!", "Counter-attack goal!"]
NHL_MISS_PHRASES    = ["Wide right.", "Saved by the goalie.", "Hit the post.", "Iced.", "Off the iron."]
NHL_WIN_PHRASES     = ["Final buzzer — {winner} wins!", "Game over! {winner} lifts the cup!", "Three stars: {winner}!"]

# ── CORE GAME ACTIONS ────────────────────────────────────────────────────
def _make_miss(s, result, message, serialise_fn, win_phrases):
    slot = s["players"][s["turn"]]
    slot["session_total"] += 1
    s["miss_streak"] += 1
    s["turn_number"] += 1
    _resolve_win(s, win_phrases)
    if not s["game_over"]:
        _check_alternate_win(s, win_phrases)
    if s["game_over"]:
        _flush_stats(s, s.get("winner"))
    _switch_turn(s)
    room_id = s.get("room_id")
    serialised = serialise_fn(s)
    if room_id:
        save_room(room_id, s)
        _emit_update(room_id, serialised)
    return jsonify({
        "result": result, "message": message, "miss_streak": s["miss_streak"],
        "game_over": s["game_over"], "winner": s["winner"], "win_reason": s["win_reason"],
        "state": serialised
    })

def _do_guess(s, db, calc_fn, serialise_fn, team_names_map, aliases, correct_phrases, steal_phrases, miss_phrases, wrong_team_phrases, win_phrases, name_index=None):
    if s["game_over"]: return jsonify({"result": "game_over", "message": "Game is already over."}), 400
    data = request.json or {}
    # Enforce turn in online games — reject guesses from wrong player
    req_user = data.get("username")
    if req_user:
        active_slot = s["players"][s["turn"]]
        if active_slot["username"] != req_user and not active_slot.get("is_guest"):
            return jsonify({"result": "error", "message": "Not your turn."}), 400

    cell = int(data.get("cell", -1))
    player_name = sanitize_name(str(data.get("player", "")).strip())
    if not (0 <= cell <= 8): return jsonify({"result": "error", "message": "Invalid cell."}), 400
    if not player_name: return jsonify({"result": "error", "message": "No player name given."}), 400
    canonical = _normalise_player_name(player_name, aliases)

    # 1. O(1) lookup via name index
    player = None
    if name_index:
        player = name_index.get(canonical.lower()) or name_index.get(player_name.lower())
        if not player:
            player = name_index.get(_strip_accents(canonical))

    # 2. Fallback linear scan (should rarely trigger)
    if not player:
        player = (
            next((p for p in db if p["name"].lower() == canonical.lower()), None) or
            next((p for p in db if p["name"].lower() == player_name.lower()), None)
        )
    # 3. Accent-stripped fallback
    if not player:
        cs = _strip_accents(canonical)
        player = next((p for p in db if _strip_accents(p["name"]) == cs), None)
    # 4. Fuzzy match
    if not player:
        try:
            from rapidfuzz import process, fuzz
            best = process.extractOne(canonical, [p["name"] for p in db], scorer=fuzz.token_sort_ratio, score_cutoff=92)
            if best: player = next((p for p in db if p["name"] == best[0]), None)
        except ImportError: pass

    if not player:
        return _make_miss(s, "not_found", random.choice(miss_phrases) + f" '{player_name}' is not in our system.", serialise_fn, win_phrases)

    ri, ci = cell // 3, cell % 3
    team_a, team_b = s["rows"][ri], s["cols"][ci]
    current_turn = s["turn"]
    slot = s["players"][current_turn]
    uid = slot["user_id"]

    if player["name"].lower() in {n.lower() for n in s["used_players"]}:
        return _make_miss(s, "already_used", f"{player['name']} has already been used this game.", serialise_fn, win_phrases)

    if team_a.startswith("STAT:"):
        stat_key = team_a.split(":", 1)[1]
        if team_b not in player.get("teams", []):
            return _make_miss(s, "wrong_team", f"{player['name']} didn't play for {team_names_map.get(team_b, team_b)}.", serialise_fn, win_phrases)
        if team_b not in player.get("achievements", {}).get(stat_key, []):
            cats = s.get("stat_categories", {})
            cat = cats.get(stat_key, {})
            return _make_miss(s, "wrong_team", f"{player['name']} didn't achieve {cat.get('label', stat_key)} with {team_names_map.get(team_b, team_b)}.", serialise_fn, win_phrases)
    elif team_a not in player.get("teams", []) or team_b not in player.get("teams", []):
        return _make_miss(s, "wrong_team", f"{player['name']} didn't play for both {team_names_map.get(team_a, team_a)} and {team_names_map.get(team_b, team_b)}.", serialise_fn, win_phrases)

    # BUG FIX: stat rows were passing (team_b, team_b) — now correctly pass (team_a, team_b)
    # so the STAT: prefix is preserved and _calc_rarity_common triggers stat logic.
    rarity = calc_fn(player, team_a, team_b)
    cell_key = str(cell)
    existing = s["board"].get(cell_key)

    if existing and existing["owner"] != uid:
        # ── STEAL ATTEMPT (opponent's cell) ──
        slot["session_total"] += 1
        if rarity < existing["rarity"]:
            s["board"][cell_key] = _cell_entry(uid, current_turn, player, rarity)
            s["used_players"].add(player["name"])
            slot["session_correct"] += 1; s["miss_streak"] = 0
            result_label, phrase = "steal", random.choice(steal_phrases)
        else:
            # BUG FIX: steal_failed now increments turn_number and miss_streak
            s["miss_streak"] += 1
            s["turn_number"] += 1
            _resolve_win(s, win_phrases)
            if not s["game_over"]:
                _check_alternate_win(s, win_phrases)
            if s["game_over"]:
                _flush_stats(s, s.get("winner"))
            _switch_turn(s)
            room_id = s.get("room_id")
            serialised = serialise_fn(s)
            if room_id:
                save_room(room_id, s)
                _emit_update(room_id, serialised)
            return jsonify({
                "result": "steal_failed",
                "message": f"{player['name']} ({round(rarity*100)}) couldn't beat {existing['player_name']} ({round(existing['rarity']*100)}).",
                "miss_streak": s["miss_streak"], "game_over": s["game_over"],
                "winner": s["winner"], "win_reason": s["win_reason"],
                "state": serialised
            })
    elif existing and existing["owner"] == uid:
        # ── SELF-UPGRADE ATTEMPT (own cell) ──
        if rarity < existing["rarity"]:
            # Upgrade succeeds — swap player in
            s["board"][cell_key] = _cell_entry(uid, current_turn, player, rarity)
            s["used_players"].add(player["name"])
            slot["session_correct"] += 1; s["miss_streak"] = 0
            result_label, phrase = "improved", "Upgraded!"
        else:
            # BUG FIX: No improvement — do NOT count as a miss, do NOT switch turn.
            # Player can try another cell or pass.
            room_id = s.get("room_id")
            serialised = serialise_fn(s)
            if room_id:
                save_room(room_id, s)
                _emit_update(room_id, serialised)
            return jsonify({
                "result": "no_improvement",
                "message": f"{player['name']} ({round(rarity*100)}) doesn't improve on {existing['player_name']} ({round(existing['rarity']*100)}). Try another cell.",
                "miss_streak": s["miss_streak"], "game_over": False,
                "winner": None, "win_reason": None,
                "state": serialised
            })
    else:
        # ── EMPTY CELL ──
        s["board"][cell_key] = _cell_entry(uid, current_turn, player, rarity)
        s["used_players"].add(player["name"])
        slot["session_total"] += 1
        slot["session_correct"] += 1; s["miss_streak"] = 0
        result_label, phrase = "correct", random.choice(correct_phrases)

    s["turn_number"] += 1
    _resolve_win(s, win_phrases)
    _switch_turn(s)
    room_id = s.get("room_id")
    serialised = serialise_fn(s)
    if room_id:
        save_room(room_id, s)
        _emit_update(room_id, serialised)
    return jsonify({
        "result": result_label,
        "message": f"{phrase} {player['name']} — Rarity: {round(rarity*100)}",
        "rarity": rarity, "rarity_pct": round(rarity * 100),
        "cell": cell, "owner": uid, "owner_turn": current_turn,
        "player_name": player["name"], "headshot": player.get("headshot", ""),
        "miss_streak": s["miss_streak"], "hold_line": s["hold_line"],
        "game_over": s["game_over"], "winner": s["winner"], "win_reason": s["win_reason"],
        "state": serialised
    })

def _find_best_hint(s, db, calc_fn):
    candidates = []
    for cell_idx in range(9):
        ri, ci = cell_idx // 3, cell_idx % 3
        team_a, team_b = s["rows"][ri], s["cols"][ci]
        existing = s["board"].get(str(cell_idx))
        for p in db:
            if p["name"] in s["used_players"] or p.get("position", "") in EXCLUDED_POSITIONS:
                continue
            if team_a.startswith("STAT:"):
                stat_key = team_a.split(":", 1)[1]
                if team_b not in p.get("teams", []) or team_b not in p.get("achievements", {}).get(stat_key, []):
                    continue
                # BUG FIX: pass team_a (with STAT: prefix), team_b — not team_b, team_b
                r = calc_fn(p, team_a, team_b)
            else:
                if team_a not in p.get("teams", []) or team_b not in p.get("teams", []):
                    continue
                r = calc_fn(p, team_a, team_b)
            if existing and r >= existing["rarity"]:
                continue
            candidates.append((r, p, cell_idx))
    if not candidates: return None, None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1], candidates[0][2]

def _find_best_answers_all_cells(s, db, calc_fn):
    """
    For each cell on the board, find the single LOWEST-rarity (most obvious)
    player that satisfies the row+col constraint. Used on the end screen
    to show players what the 'easy' answer was for every cell.
    Returns: {cell_idx_str: {"name": str, "rarity": float, "headshot": str, "position": str}}
    """
    result = {}
    for cell_idx in range(9):
        ri, ci = cell_idx // 3, cell_idx % 3
        team_a, team_b = s["rows"][ri], s["cols"][ci]
        best = None
        best_rarity = 1.1  # higher than max
        for p in db:
            if p.get("position", "") in EXCLUDED_POSITIONS:
                continue
            if team_a.startswith("STAT:"):
                stat_key = team_a.split(":", 1)[1]
                if team_b not in p.get("teams", []):
                    continue
                if team_b not in p.get("achievements", {}).get(stat_key, []):
                    continue
                r = calc_fn(p, team_a, team_b)
            else:
                if team_a not in p.get("teams", []) or team_b not in p.get("teams", []):
                    continue
                r = calc_fn(p, team_a, team_b)
            if r < best_rarity:
                best_rarity = r
                best = p
        if best is not None:
            result[str(cell_idx)] = {
                "name": best["name"],
                "rarity": round(best_rarity, 4),
                "headshot": best.get("headshot", ""),
                "position": best.get("position", ""),
            }
    return result

def _do_hint(s, db, calc_fn, serialise_fn, sport):
    """Provide a hint to the current player — reveals a valid player for an open/stealable cell."""
    if s["game_over"]:
        return jsonify({"error": "Game is already over."}), 400
    slot = s["players"][s["turn"]]
    if slot["hints_remaining"] <= 0:
        return jsonify({"error": "No hints remaining.", "state": serialise_fn(s)}), 400
    player, cell_idx = _find_best_hint(s, db, calc_fn)
    if player is None:
        return jsonify({"error": "No valid hint available.", "state": serialise_fn(s)}), 400
    slot["hints_remaining"] -= 1
    ri, ci = cell_idx // 3, cell_idx % 3
    team_a, team_b = s["rows"][ri], s["cols"][ci]
    # Build hint — position, debut year, jersey, number of teams played for
    position = player.get("position", "")
    jersey = player.get("jersey", "")
    debut = player.get("debut_year", 0)
    num_teams = len(player.get("teams", []))
    parts = []
    if position: parts.append(f"Position: {position}")
    if debut and isinstance(debut, int) and debut > 0: parts.append(f"Debut: {debut}")
    if jersey and str(jersey).strip(): parts.append(f"Jersey: #{jersey}")
    if num_teams > 0: parts.append(f"Played for {num_teams} team{'s' if num_teams != 1 else ''}")
    hint_msg = f"Cell {cell_idx + 1} — " + ", ".join(parts) if parts else "No additional info available"
    room_id = s.get("room_id")
    serialised = serialise_fn(s)
    if room_id:
        save_room(room_id, s)
        _emit_update(room_id, serialised)
    return jsonify({
        "message": hint_msg,
        "hint": {
            "cell": cell_idx,
            "position": position,
            "jersey": jersey if jersey and str(jersey).strip() else "",
            "debut_year": debut if isinstance(debut, int) and debut > 0 else "",
            "num_teams": num_teams,
        },
        "hints_remaining": slot["hints_remaining"],
        "state": serialised
    })

def _do_pass(s, serialise_fn, win_phrases):
    """Current player passes their turn without guessing."""
    if s["game_over"]:
        return jsonify({"error": "Game is already over."}), 400
    s["miss_streak"] += 1
    s["turn_number"] += 1
    _resolve_win(s, win_phrases)
    if not s["game_over"]:
        _check_alternate_win(s, win_phrases)
    if s["game_over"]:
        _flush_stats(s, s.get("winner"))
    _switch_turn(s)
    room_id = s.get("room_id")
    serialised = serialise_fn(s)
    if room_id:
        save_room(room_id, s)
        _emit_update(room_id, serialised)
    return jsonify({
        "result": "pass",
        "message": f"{s['players'][2 if s['turn'] == 2 else 1]['username']} passed their turn.",
        "miss_streak": s["miss_streak"],
        "game_over": s["game_over"],
        "winner": s["winner"],
        "win_reason": s["win_reason"],
        "state": serialised
    })

def _do_forfeit(s, serialise_fn, win_phrases):
    """Current player forfeits the game — opponent wins immediately."""
    if s["game_over"]:
        return jsonify({"error": "Game is already over."}), 400
    forfeiter_turn = s["turn"]
    winner_turn = 2 if forfeiter_turn == 1 else 1
    wname = s["players"][winner_turn]["username"]
    fname = s["players"][forfeiter_turn]["username"]
    s["game_over"] = True
    s["winner"] = winner_turn
    s["win_reason"] = f"{fname} forfeited — {wname} wins!"
    _flush_stats(s, winner_turn)
    room_id = s.get("room_id")
    serialised = serialise_fn(s)
    if room_id:
        save_room(room_id, s)
        _emit_update(room_id, serialised)
    return jsonify({
        "result": "forfeit",
        "message": s["win_reason"],
        "game_over": True,
        "winner": winner_turn,
        "win_reason": s["win_reason"],
        "state": serialised
    })

def _start_game_common(new_state_fn, serialise_fn):
    data = request.json or {}
    p1, p2 = data.get("player1"), data.get("player2")
    if not p1 or not p2: return jsonify({"error": "Both player accounts required."}), 400
    u1, u2 = _resolve_user(p1), _resolve_user(p2)
    if not u1 or not u2: return jsonify({"error": "One or both accounts not found."}), 404
    real_ids = [x["id"] for x in [u1, u2] if not str(x["id"]).startswith("guest_") and not str(x["id"]).startswith("bot_")]
    if len(real_ids) == 2 and real_ids[0] == real_ids[1]:
        return jsonify({"error": "Both players must be different accounts."}), 400
    # Cleanup stale rooms before creating a new one
    cleanup_stale_rooms()
    room_id = _generate_room_id()
    s = new_state_fn(u1, u2)
    s["room_id"] = room_id
    save_room(room_id, s)
    serialised = serialise_fn(s)
    serialised["room_id"] = room_id
    return s, serialised

# ── BOT AI ───────────────────────────────────────────────────────────────
def _bot_pick_move(s, db, calc_fn, difficulty):
    board = s["board"]
    bot_uid = s["players"][s["turn"]]["user_id"]
    opp_turn = 2 if s["turn"] == 1 else 1
    opp_uid = s["players"][opp_turn]["user_id"]

    valid = {}
    for ci in range(9):
        ri, col = ci // 3, ci % 3
        ta, tb = s["rows"][ri], s["cols"][col]
        cands = []
        for p in db:
            if p["name"] in s["used_players"]: continue
            if p.get("position", "") in EXCLUDED_POSITIONS: continue
            if ta.startswith("STAT:"):
                sk = ta.split(":", 1)[1]
                if tb not in p.get("teams", []): continue
                if tb not in p.get("achievements", {}).get(sk, []): continue
                # BUG FIX: pass ta (with STAT: prefix), tb — not tb, tb
                r = calc_fn(p, ta, tb)
            else:
                if ta not in p.get("teams", []) or tb not in p.get("teams", []): continue
                r = calc_fn(p, ta, tb)
            cands.append((r, p))
        if cands:
            cands.sort(key=lambda x: x[0])
            valid[ci] = cands

    if not valid: return None, None

    if difficulty == "easy":
        ci = random.choice(list(valid.keys()))
        cands = valid[ci]
        cands.sort(key=lambda x: x[0], reverse=True)
        return ci, random.choice(cands[:max(1, len(cands) // 2)])[1]

    elif difficulty == "medium":
        for line in WIN_LINES:
            bot_count = sum(1 for i in line if board.get(str(i), {}).get("owner") == bot_uid)
            empty = [i for i in line if not board.get(str(i))]
            if bot_count == 2 and len(empty) == 1 and empty[0] in valid:
                return empty[0], valid[empty[0]][0][1]
        for line in WIN_LINES:
            opp_count = sum(1 for i in line if board.get(str(i), {}).get("owner") == opp_uid)
            empty = [i for i in line if not board.get(str(i))]
            if opp_count == 2 and len(empty) == 1 and empty[0] in valid:
                return empty[0], valid[empty[0]][len(valid[empty[0]]) // 2][1]
        ci = random.choice(list(valid.keys()))
        return ci, valid[ci][len(valid[ci]) // 2][1]

    else:  # hard
        for line in WIN_LINES:
            bot_count = sum(1 for i in line if board.get(str(i), {}).get("owner") == bot_uid)
            empty = [i for i in line if not board.get(str(i))]
            if bot_count == 2 and len(empty) == 1 and empty[0] in valid:
                return empty[0], valid[empty[0]][0][1]
        for line in WIN_LINES:
            opp_count = sum(1 for i in line if board.get(str(i), {}).get("owner") == opp_uid)
            empty = [i for i in line if not board.get(str(i))]
            if opp_count == 2 and len(empty) == 1 and empty[0] in valid:
                return empty[0], valid[empty[0]][0][1]
        best_steal = None
        for ci, cands in valid.items():
            ex = board.get(str(ci))
            if ex and ex.get("owner") == opp_uid and cands[0][0] < ex["rarity"]:
                if not best_steal or cands[0][0] < best_steal[0]:
                    best_steal = (cands[0][0], cands[0][1], ci)
        if best_steal: return best_steal[2], best_steal[1]
        best_ci, best_score = None, -1
        for ci in valid:
            score = sum(1 for l in WIN_LINES if ci in l and
                        any(board.get(str(i), {}).get("owner") == bot_uid for i in l if i != ci))
            if score > best_score: best_score = score; best_ci = ci
        if best_ci is None: best_ci = random.choice(list(valid.keys()))
        return best_ci, valid[best_ci][0][1]

def _do_bot_turn(s, db, calc_fn, serialise_fn, win_phrases):
    if s["game_over"]: return jsonify({"error": "Game is over."}), 400
    bot_slot = s["players"][s["turn"]]
    if not bot_slot.get("is_bot"): return jsonify({"error": "Not bot's turn."}), 400
    difficulty = bot_slot.get("bot_difficulty", "medium")
    cell_idx, player = _bot_pick_move(s, db, calc_fn, difficulty)
    if player is None:
        return _do_pass(s, serialise_fn, win_phrases)
    ta, tb = s["rows"][cell_idx // 3], s["cols"][cell_idx % 3]
    uid = bot_slot["user_id"]; ct = s["turn"]
    # BUG FIX: pass ta (with STAT: prefix), tb — not tb, tb
    rarity = calc_fn(player, ta, tb)
    ck = str(cell_idx); existing = s["board"].get(ck)
    bot_slot["session_total"] += 1
    s["used_players"].add(player["name"])  # Always mark as used before branching
    if existing and existing["owner"] != uid:
        if rarity < existing["rarity"]:
            s["board"][ck] = _cell_entry(uid, ct, player, rarity)
            bot_slot["session_correct"] += 1; s["miss_streak"] = 0
        else:
            # BUG FIX: bot steal_failed increments turn_number and miss_streak
            s["miss_streak"] += 1
            s["turn_number"] += 1
            _resolve_win(s, win_phrases)
            if not s["game_over"]:
                _check_alternate_win(s, win_phrases)
            if s["game_over"]:
                _flush_stats(s, s.get("winner"))
            _switch_turn(s)
            room_id = s.get("room_id")
            serialised = serialise_fn(s)
            if room_id:
                save_room(room_id, s)
                _emit_update(room_id, serialised)
            return jsonify({"result": "bot_steal_failed", "message": "Bot tried to steal but failed.", "state": serialised})
    elif existing and existing["owner"] == uid:
        if rarity < existing["rarity"]:
            s["board"][ck] = _cell_entry(uid, ct, player, rarity)
            bot_slot["session_correct"] += 1
        s["miss_streak"] = 0
    else:
        s["board"][ck] = _cell_entry(uid, ct, player, rarity)
        bot_slot["session_correct"] += 1; s["miss_streak"] = 0
    s["turn_number"] += 1
    _resolve_win(s, win_phrases)
    # BUG FIX: add _check_alternate_win after _resolve_win (was missing)
    if not s["game_over"]:
        _check_alternate_win(s, win_phrases)
    if s["game_over"]:
        _flush_stats(s, s.get("winner"))
    _switch_turn(s)
    room_id = s.get("room_id")
    serialised = serialise_fn(s)
    if room_id:
        save_room(room_id, s)
        _emit_update(room_id, serialised)
    return jsonify({
        "result": "bot_move", "message": f"Bot played {player['name']} — Rarity: {round(rarity*100)}",
        "cell": cell_idx, "player_name": player["name"], "headshot": player.get("headshot", ""),
        "rarity": rarity, "game_over": s["game_over"], "winner": s["winner"],
        "win_reason": s["win_reason"], "state": serialised
    })

# ══════════════════════════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════════════════════════

# ── AUTH ROUTES ───────────────────────────────────────────────────────────
def _user_json(user, guest=False):
    return {
        "id": user["id"], "username": user["username"],
        "nfl_mascot": user.get("nfl_mascot", user.get("mascot", "KC")),
        "mlb_mascot": user.get("mlb_mascot", "NYY"),
        "nba_mascot": user.get("nba_mascot", "LAL"),
        "nhl_mascot": user.get("nhl_mascot", "BOS"),
        "nfl_mascot_logo": TEAM_LOGOS.get(user.get("nfl_mascot", "KC"), ""),
        "mlb_mascot_logo": MLB_LOGOS.get(user.get("mlb_mascot", "NYY"), ""),
        "nba_mascot_logo": NBA_LOGOS.get(user.get("nba_mascot", "LAL"), ""),
        "nhl_mascot_logo": NHL_LOGOS.get(user.get("nhl_mascot", "BOS"), ""),
        "lifetime_correct": user.get("lifetime_correct", 0), "lifetime_total": user.get("lifetime_total", 0),
        "wins": user.get("wins", 0), "losses": user.get("losses", 0), "draws": user.get("draws", 0),
        "win_streak": user.get("win_streak", 0), "best_streak": user.get("best_streak", 0), "is_guest": guest
    }

@app.route("/api/auth/register", methods=["POST"])
def register():
    if _check_rate_limit(request.remote_addr):
        return jsonify({"error": "Too many requests. Please wait a minute."}), 429
    data = request.json or {}
    username, password = str(data.get("username", "")).strip(), str(data.get("password", "")).strip()
    nfl_mascot = str(data.get("nfl_mascot", "KC")).strip().upper()
    mlb_mascot = str(data.get("mlb_mascot", "NYY")).strip().upper()
    nba_mascot = str(data.get("nba_mascot", "LAL")).strip().upper()
    nhl_mascot = str(data.get("nhl_mascot", "BOS")).strip().upper()
    if nfl_mascot not in NFL_TEAMS: nfl_mascot = "KC"
    if mlb_mascot not in MLB_TEAMS: mlb_mascot = "NYY"
    if nba_mascot not in NBA_TEAMS: nba_mascot = "LAL"
    if nhl_mascot not in NHL_TEAMS: nhl_mascot = "BOS"
    if not username or not password: return jsonify({"error": "Username and password required."}), 400
    if not _validate_username(username):
        return jsonify({"error": "Username must be 2\u201320 characters, alphanumeric and underscores only."}), 400
    user = create_user(username, password, nfl_mascot, mlb_mascot, nba_mascot, nhl_mascot)
    if not user: return jsonify({"error": "Username already taken."}), 409
    return jsonify({"ok": True, "user": _user_json(user)})

@app.route("/api/auth/login", methods=["POST"])
def login():
    if _check_rate_limit(request.remote_addr):
        return jsonify({"error": "Too many requests. Please wait a minute."}), 429
    data = request.json or {}
    username, password = str(data.get("username", "")).strip(), str(data.get("password", "")).strip()
    if not username: return jsonify({"error": "Username required."}), 400
    if not password: return jsonify({"error": "Password required."}), 400
    user = get_user(username)
    if not user: return jsonify({"error": "No account found with that username."}), 401
    # Support both new werkzeug hashes and legacy SHA-256 for backward compat
    stored = user["password_hash"]
    if stored.startswith("pbkdf2:") or stored.startswith("scrypt:"):
        valid = check_password_hash(stored, password)
    else:
        import hashlib
        valid = stored == hashlib.sha256(password.encode()).hexdigest()
        if valid:
            # Silently upgrade to new hash on successful legacy login
            con = sqlite3.connect(DB_PATH)
            con.execute("UPDATE users SET password_hash=? WHERE id=?",
                        (generate_password_hash(password), user["id"]))
            con.commit(); con.close()
    if not valid: return jsonify({"error": "Incorrect password."}), 401
    return jsonify({"ok": True, "user": _user_json(user)})

@app.route("/api/auth/guest", methods=["POST"])
def guest_login():
    guest_id = f"guest_{int(time.time()*1000)%999999}"
    user = {
        "id": guest_id, "username": f"Guest_{guest_id[-4:]}",
        "nfl_mascot": random.choice(NFL_TEAMS), "mlb_mascot": random.choice(MLB_TEAMS),
        "nba_mascot": random.choice(NBA_TEAMS), "nhl_mascot": random.choice(NHL_TEAMS),
        "lifetime_correct": 0, "lifetime_total": 0,
        "wins": 0, "losses": 0, "draws": 0, "win_streak": 0, "best_streak": 0
    }
    return jsonify({"ok": True, "user": _user_json(user, guest=True)})

@app.route("/api/auth/bot", methods=["POST"])
def create_bot():
    data = request.json or {}
    difficulty = data.get("difficulty", "medium")
    if difficulty not in ("easy", "medium", "hard"): difficulty = "medium"
    name = data.get("name", "") or {"easy": "Rookie Bot", "medium": "Pro Bot", "hard": "Legend Bot"}[difficulty]
    bot_id = f"bot_{difficulty}_{int(time.time()*1000)%999999}"
    user = {
        "id": bot_id, "username": name,
        "nfl_mascot": random.choice(NFL_TEAMS), "mlb_mascot": random.choice(MLB_TEAMS),
        "nba_mascot": random.choice(NBA_TEAMS), "nhl_mascot": random.choice(NHL_TEAMS),
        "lifetime_correct": 0, "lifetime_total": 0, "wins": 0, "losses": 0, "draws": 0,
        "win_streak": 0, "best_streak": 0, "bot_difficulty": difficulty, "is_bot": True
    }
    return jsonify({"ok": True, "user": {**_user_json(user), "is_bot": True, "bot_difficulty": difficulty}})

@app.route("/api/auth/mascot", methods=["POST"])
def update_mascot_route():
    data = request.json or {}
    username = str(data.get("username", "")).strip()
    sport = str(data.get("sport", "nfl")).strip().lower()
    mascot = str(data.get("mascot", "")).strip().upper()
    valid_sets = {"nfl": NFL_TEAMS, "mlb": MLB_TEAMS, "nba": NBA_TEAMS, "nhl": NHL_TEAMS}
    if sport not in valid_sets: return jsonify({"error": "Invalid sport."}), 400
    if mascot not in valid_sets[sport]: return jsonify({"error": "Invalid team."}), 400
    user = get_user(username)
    if not user: return jsonify({"error": "User not found."}), 404
    update_mascot(user["id"], sport, mascot)
    updated = get_user(username)
    return jsonify({"ok": True, "user": _user_json(updated)})

@app.route("/api/leaderboard", methods=["GET"])
def leaderboard_route():
    """Return top 20 players by wins, then best streak as tiebreaker."""
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("""
        SELECT username, wins, losses, draws, lifetime_correct, lifetime_total,
               win_streak, best_streak
        FROM users
        WHERE (wins + losses + draws) > 0
        ORDER BY wins DESC, best_streak DESC, lifetime_correct DESC
        LIMIT 20
    """).fetchall()
    con.close()
    result = []
    for r in rows:
        total_games = r["wins"] + r["losses"] + r["draws"]
        win_pct = round(r["wins"] / total_games * 100) if total_games > 0 else 0
        guess_pct = round(r["lifetime_correct"] / r["lifetime_total"] * 100) if r["lifetime_total"] > 0 else 0
        result.append({
            "username": r["username"],
            "wins": r["wins"],
            "losses": r["losses"],
            "draws": r["draws"],
            "win_pct": win_pct,
            "guess_pct": guess_pct,
            "win_streak": r["win_streak"],
            "best_streak": r["best_streak"],
        })
    return jsonify({"ok": True, "leaderboard": result})

# ── GENERIC SPORT ROUTE HELPERS ──────────────────────────────────────────
def _route_start(new_state_fn, serialise_fn):
    result = _start_game_common(new_state_fn, serialise_fn)
    if isinstance(result, tuple) and len(result) == 2 and not isinstance(result[0], dict):
        return result
    s, json_state = result
    room_id = s.get("room_id")
    if room_id:
        _emit_update(room_id, json_state)
    return jsonify(json_state)

def _route_get_game(serialise_fn):
    room_id = request.args.get("room_id")
    if not room_id: return jsonify({"error": "room_id required."}), 400
    s = get_room(room_id)
    if s is None: return jsonify({"error": "No active game for this room."}), 404
    return jsonify(serialise_fn(s))

def _route_guess(db, calc_fn, serialise_fn, team_names_map, aliases, correct_phrases, steal_phrases, miss_phrases, win_phrases, name_index):
    room_id = _get_room_id_from_request()
    if not room_id: return jsonify({"result": "error", "message": "room_id required."}), 400
    s = get_room(room_id)
    if s is None: return jsonify({"result": "error", "message": "No active game for this room."}), 400
    return _do_guess(s, db, calc_fn, serialise_fn, team_names_map, aliases, correct_phrases, steal_phrases, miss_phrases, [], win_phrases, name_index)

def _route_hint(db, calc_fn, serialise_fn, sport):
    room_id = _get_room_id_from_request()
    if not room_id: return jsonify({"error": "room_id required."}), 400
    s = get_room(room_id)
    if s is None: return jsonify({"error": "No active game for this room."}), 400
    return _do_hint(s, db, calc_fn, serialise_fn, sport)

def _route_pass(serialise_fn, win_phrases):
    room_id = _get_room_id_from_request()
    if not room_id: return jsonify({"error": "room_id required."}), 400
    s = get_room(room_id)
    if s is None: return jsonify({"error": "No active game for this room."}), 400
    return _do_pass(s, serialise_fn, win_phrases)

def _route_forfeit(serialise_fn, win_phrases):
    room_id = _get_room_id_from_request()
    if not room_id: return jsonify({"error": "room_id required."}), 400
    s = get_room(room_id)
    if s is None: return jsonify({"error": "No active game for this room."}), 400
    return _do_forfeit(s, serialise_fn, win_phrases)

def _route_bot_turn(db, calc_fn, serialise_fn, win_phrases):
    room_id = _get_room_id_from_request()
    if not room_id: return jsonify({"error": "room_id required."}), 400
    s = get_room(room_id)
    if s is None: return jsonify({"error": "No active game for this room."}), 400
    return _do_bot_turn(s, db, calc_fn, serialise_fn, win_phrases)

def _route_reset(new_state_fn, serialise_fn):
    """Reset creates a NEW room (preserving old room until cleanup)."""
    data = request.json or {}
    old_room_id = data.get("room_id")
    if old_room_id:
        old_s = get_room(old_room_id)
        if old_s:
            _flush_stats(old_s, old_s.get("winner"))
            save_room(old_room_id, old_s)
    p1, p2 = data.get("player1"), data.get("player2")
    if p1 and p2:
        u1, u2 = _resolve_user(p1), _resolve_user(p2)
        if u1 and u2:
            cleanup_stale_rooms()
            room_id = _generate_room_id()
            s = new_state_fn(u1, u2)
            s["room_id"] = room_id
            save_room(room_id, s)
            serialised = serialise_fn(s)
            serialised["room_id"] = room_id
            _emit_update(room_id, serialised)
            return jsonify({"status": "reset", "room_id": room_id, "state": serialised})
    return jsonify({"status": "cleared"})

# ── NFL ROUTES ────────────────────────────────────────────────────────────
@app.route("/api/game/start", methods=["POST"])
def start_game():
    return _route_start(empty_state, serialise_state)

@app.route("/api/game")
def get_game():
    return _route_get_game(serialise_state)

@app.route("/api/guess", methods=["POST"])
def guess():
    return _route_guess(PLAYERS_DB, calc_rarity, serialise_state, TEAM_NAMES, NFL_PLAYER_ALIASES, CORRECT_PHRASES, STEAL_PHRASES, MISS_PHRASES, WIN_PHRASES, NFL_NAME_INDEX)

@app.route("/api/hint", methods=["POST"])
def hint():
    return _route_hint(PLAYERS_DB, calc_rarity, serialise_state, "nfl")

@app.route("/api/pass", methods=["POST"])
def pass_turn():
    return _route_pass(serialise_state, WIN_PHRASES)

@app.route("/api/forfeit", methods=["POST"])
def forfeit():
    return _route_forfeit(serialise_state, WIN_PHRASES)

@app.route("/api/bot/turn", methods=["POST"])
def nfl_bot_turn():
    return _route_bot_turn(PLAYERS_DB, calc_rarity, serialise_state, WIN_PHRASES)

@app.route("/api/reset", methods=["POST"])
def reset():
    return _route_reset(empty_state, serialise_state)

# ── MLB ROUTES ────────────────────────────────────────────────────────────
@app.route("/api/mlb/game/start", methods=["POST"])
def mlb_start_game():
    return _route_start(mlb_empty_state, mlb_serialise_state)

@app.route("/api/mlb/game")
def mlb_get_game():
    return _route_get_game(mlb_serialise_state)

@app.route("/api/mlb/guess", methods=["POST"])
def mlb_guess():
    return _route_guess(MLB_PLAYERS_DB, calc_mlb_rarity, mlb_serialise_state, MLB_TEAM_NAMES, MLB_PLAYER_ALIASES, MLB_CORRECT_PHRASES, MLB_STEAL_PHRASES, MLB_MISS_PHRASES, MLB_WIN_PHRASES, MLB_NAME_INDEX)

@app.route("/api/mlb/hint", methods=["POST"])
def mlb_hint():
    return _route_hint(MLB_PLAYERS_DB, calc_mlb_rarity, mlb_serialise_state, "mlb")

@app.route("/api/mlb/pass", methods=["POST"])
def mlb_pass_turn():
    return _route_pass(mlb_serialise_state, MLB_WIN_PHRASES)

@app.route("/api/mlb/forfeit", methods=["POST"])
def mlb_forfeit():
    return _route_forfeit(mlb_serialise_state, MLB_WIN_PHRASES)

@app.route("/api/mlb/bot/turn", methods=["POST"])
def mlb_bot_turn():
    return _route_bot_turn(MLB_PLAYERS_DB, calc_mlb_rarity, mlb_serialise_state, MLB_WIN_PHRASES)

@app.route("/api/mlb/reset", methods=["POST"])
def mlb_reset():
    return _route_reset(mlb_empty_state, mlb_serialise_state)

# ── NBA ROUTES ────────────────────────────────────────────────────────────
@app.route("/api/nba/game/start", methods=["POST"])
def nba_start_game():
    return _route_start(nba_empty_state, nba_serialise_state)

@app.route("/api/nba/game")
def nba_get_game():
    return _route_get_game(nba_serialise_state)

@app.route("/api/nba/guess", methods=["POST"])
def nba_guess():
    return _route_guess(NBA_PLAYERS_DB, calc_nba_rarity, nba_serialise_state, NBA_TEAM_NAMES, NBA_PLAYER_ALIASES, NBA_CORRECT_PHRASES, NBA_STEAL_PHRASES, NBA_MISS_PHRASES, NBA_WIN_PHRASES, NBA_NAME_INDEX)

@app.route("/api/nba/hint", methods=["POST"])
def nba_hint():
    return _route_hint(NBA_PLAYERS_DB, calc_nba_rarity, nba_serialise_state, "nba")

@app.route("/api/nba/pass", methods=["POST"])
def nba_pass_turn():
    return _route_pass(nba_serialise_state, NBA_WIN_PHRASES)

@app.route("/api/nba/forfeit", methods=["POST"])
def nba_forfeit():
    return _route_forfeit(nba_serialise_state, NBA_WIN_PHRASES)

@app.route("/api/nba/bot/turn", methods=["POST"])
def nba_bot_turn():
    return _route_bot_turn(NBA_PLAYERS_DB, calc_nba_rarity, nba_serialise_state, NBA_WIN_PHRASES)

@app.route("/api/nba/reset", methods=["POST"])
def nba_reset():
    return _route_reset(nba_empty_state, nba_serialise_state)

# ── NHL ROUTES ────────────────────────────────────────────────────────────
@app.route("/api/nhl/game/start", methods=["POST"])
def nhl_start_game():
    return _route_start(nhl_empty_state, nhl_serialise_state)

@app.route("/api/nhl/game")
def nhl_get_game():
    return _route_get_game(nhl_serialise_state)

@app.route("/api/nhl/guess", methods=["POST"])
def nhl_guess():
    return _route_guess(NHL_PLAYERS_DB, calc_nhl_rarity, nhl_serialise_state, NHL_TEAM_NAMES, NHL_PLAYER_ALIASES, NHL_CORRECT_PHRASES, NHL_STEAL_PHRASES, NHL_MISS_PHRASES, NHL_WIN_PHRASES, NHL_NAME_INDEX)

@app.route("/api/nhl/hint", methods=["POST"])
def nhl_hint():
    return _route_hint(NHL_PLAYERS_DB, calc_nhl_rarity, nhl_serialise_state, "nhl")

@app.route("/api/nhl/pass", methods=["POST"])
def nhl_pass_turn():
    return _route_pass(nhl_serialise_state, NHL_WIN_PHRASES)

@app.route("/api/nhl/forfeit", methods=["POST"])
def nhl_forfeit():
    return _route_forfeit(nhl_serialise_state, NHL_WIN_PHRASES)

@app.route("/api/nhl/bot/turn", methods=["POST"])
def nhl_bot_turn():
    return _route_bot_turn(NHL_PLAYERS_DB, calc_nhl_rarity, nhl_serialise_state, NHL_WIN_PHRASES)

@app.route("/api/nhl/reset", methods=["POST"])
def nhl_reset():
    return _route_reset(nhl_empty_state, nhl_serialise_state)

# ── UTILITY ROUTES ───────────────────────────────────────────────────────
@app.route("/api/teams")
def teams():
    return jsonify({t: {"name": TEAM_NAMES[t], "mascot": TEAM_MASCOTS[t], "logo": TEAM_LOGOS.get(t, "")} for t in NFL_TEAMS})

@app.route("/api/mlb/teams")
def mlb_teams():
    return jsonify({t: {"name": MLB_TEAM_NAMES[t], "mascot": MLB_TEAM_MASCOTS[t], "logo": MLB_LOGOS.get(t, "")} for t in MLB_TEAMS})

@app.route("/api/nba/teams")
def nba_teams():
    return jsonify({t: {"name": NBA_TEAM_NAMES[t], "mascot": NBA_TEAM_MASCOTS[t], "logo": NBA_LOGOS.get(t, "")} for t in NBA_TEAMS})

@app.route("/api/nhl/teams")
def nhl_teams():
    return jsonify({t: {"name": NHL_TEAM_NAMES[t], "mascot": NHL_TEAM_MASCOTS[t], "logo": NHL_LOGOS.get(t, "")} for t in NHL_TEAMS})

@app.route("/api/search")
def search_players():
    q = sanitize_name(request.args.get("q", "")).strip().lower()
    sport = request.args.get("sport", "nfl")
    limit = min(int(request.args.get("limit", 10)), 25)
    if len(q) < 2: return jsonify([])
    names_map = {"nfl": PLAYER_NAMES_SORTED, "mlb": MLB_PLAYER_NAMES_SORTED, "nba": NBA_PLAYER_NAMES_SORTED, "nhl": NHL_PLAYER_NAMES_SORTED}
    names = names_map.get(sport, PLAYER_NAMES_SORTED)
    qs = _strip_accents(q)
    starts = [n for n in names if _strip_accents(n).startswith(qs)]
    last_name = [n for n in names if not _strip_accents(n).startswith(qs) and _strip_accents(n.split()[-1]).startswith(qs)]
    contains = [n for n in names if qs in _strip_accents(n) and not _strip_accents(n).startswith(qs) and not _strip_accents(n.split()[-1]).startswith(qs)]
    return jsonify((starts + last_name + contains)[:limit])

@app.route("/api/player_counts")
def player_counts():
    return jsonify({
        "nfl": len(PLAYERS_DB),
        "mlb": len(MLB_PLAYERS_DB),
        "nba": len(NBA_PLAYERS_DB),
        "nhl": len(NHL_PLAYERS_DB),
    })

@app.route("/")
def home():
    base_dir = os.path.dirname(__file__)
    possible_paths = [
        os.path.join(base_dir, "index.html"),
        os.path.join(base_dir, "user_input_files", "index.html"),
        os.path.join(os.path.dirname(base_dir), "user_input_files", "index.html"),
    ]
    for path in possible_paths:
        if os.path.exists(path):
            resp = flask.make_response(open(path, encoding='utf-8').read())
            resp.headers['Content-Type'] = 'text/html'
            resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
            resp.headers['Pragma'] = 'no-cache'
            return resp
    return "index.html not found", 404

# ── WEBSOCKET EVENTS ─────────────────────────────────────────────────────
@socketio.on('join_room')
def handle_join_room(data):
    """Player joins a socket room matching their game room_id."""
    room_id = data.get('room_id') if isinstance(data, dict) else data
    if not room_id:
        emit('error', {'message': 'room_id required.'})
        return
    sio_join_room(room_id)
    s = get_room(room_id)
    if s:
        # Determine sport from state for correct serialisation
        serialise_fn = _get_serialise_fn_for_room(s)
        emit('game_update', serialise_fn(s))
    else:
        emit('error', {'message': 'Room not found.'})

@socketio.on('create_room')
def handle_create_room(data):
    """Player 1 creates a game room and gets a room code.
    Expects: {sport, player1: {id, username, ...}}
    """
    if not isinstance(data, dict):
        emit('error', {'message': 'Invalid data.'}); return
    sport = data.get('sport', 'nfl')
    p1_data = data.get('player1')
    if not p1_data:
        emit('error', {'message': 'player1 data required.'}); return
    u1 = _resolve_user(p1_data)
    if not u1:
        emit('error', {'message': 'Player 1 account not found.'}); return
    cleanup_stale_rooms()
    room_id = _generate_room_id()
    # Create a state with a placeholder for player 2 — game won't start yet
    new_state_fn = {'nfl': empty_state, 'mlb': mlb_empty_state, 'nba': nba_empty_state, 'nhl': nhl_empty_state}.get(sport, empty_state)
    # Use a placeholder bot for P2 slot — will be replaced when P2 joins
    placeholder = {"id": "waiting_for_player", "username": "Waiting...", "nfl_mascot": "KC", "mlb_mascot": "NYY", "nba_mascot": "LAL", "nhl_mascot": "BOS",
                    "lifetime_correct": 0, "lifetime_total": 0, "wins": 0, "losses": 0, "draws": 0, "win_streak": 0, "best_streak": 0}
    s = new_state_fn(u1, placeholder)
    s["room_id"] = room_id
    s["sport"] = sport
    s["_waiting_for_p2"] = True
    save_room(room_id, s)
    sio_join_room(room_id)
    emit('room_created', {'room_id': room_id, 'sport': sport})

@socketio.on('join_game')
def handle_join_game(data):
    """Player 2 enters a room code to join an existing game.
    Expects: {room_id, player2: {id, username, ...}}
    """
    if not isinstance(data, dict):
        emit('error', {'message': 'Invalid request data.'}); return
    room_id = data.get('room_id')
    p2_data = data.get('player2')
    if not room_id:
        emit('error', {'message': 'Enter a room code to join.'}); return
    if not p2_data:
        emit('error', {'message': 'Sign in before joining a room.'}); return
    s = get_room(room_id)
    if not s:
        emit('error', {'message': f'Room "{room_id}" not found. Check the code and try again.'}); return
    if not s.get('_waiting_for_p2'):
        emit('error', {'message': 'That game already has two players.'}); return
    u2 = _resolve_user(p2_data)
    if not u2:
        emit('error', {'message': 'Your account could not be found. Try logging in again.'}); return
    # Replace the placeholder P2 slot with the real player
    sport = s.get('sport', 'nfl')
    s["players"][2] = make_player_slot(u2, sport)
    s["_waiting_for_p2"] = False
    save_room(room_id, s)
    sio_join_room(room_id)
    serialise_fn = _get_serialise_fn_for_room(s)
    serialised = serialise_fn(s)
    serialised["room_id"] = room_id
    socketio.emit('game_update', serialised, room=room_id)

def _get_serialise_fn_for_room(s):
    """Return the correct serialise function based on sport stored in state."""
    sport = s.get("sport", "nfl")
    return {"nfl": serialise_state, "mlb": mlb_serialise_state, "nba": nba_serialise_state, "nhl": nhl_serialise_state}.get(sport, serialise_state)

@socketio.on('rematch_request')
def handle_rematch_request(data):
    """Player requests a rematch — relay to opponent."""
    room_id = data.get('room_id') if isinstance(data, dict) else None
    from_user = data.get('from', 'Opponent') if isinstance(data, dict) else 'Opponent'
    if room_id:
        emit('rematch_requested', {'from': from_user}, room=room_id, include_self=False)

@socketio.on('rematch_accept')
def handle_rematch_accept(data):
    """Opponent accepts rematch — create new game and notify both players."""
    if not isinstance(data, dict): return
    room_id = data.get('room_id')
    if not room_id: return
    s = get_room(room_id)
    if not s: return
    sport = s.get('sport', 'nfl')
    new_state_fn = {'nfl': empty_state, 'mlb': mlb_empty_state, 'nba': nba_empty_state, 'nhl': nhl_empty_state}.get(sport, empty_state)
    serialise_fn = _get_serialise_fn_for_room(s)
    # Rebuild users from existing player slots
    u1 = _resolve_user({"username": s["players"][1]["username"], "id": s["players"][1]["user_id"]})
    u2 = _resolve_user({"username": s["players"][2]["username"], "id": s["players"][2]["user_id"]})
    if not u1 or not u2: return
    cleanup_stale_rooms()
    new_room_id = _generate_room_id()
    new_s = new_state_fn(u1, u2)
    new_s["room_id"] = new_room_id
    new_s["sport"] = sport
    save_room(new_room_id, new_s)
    serialised = serialise_fn(new_s)
    serialised["room_id"] = new_room_id
    sio_join_room(new_room_id)
    socketio.emit('rematch_accepted', {'room_id': new_room_id, 'state': serialised}, room=room_id)

@socketio.on('rematch_decline')
def handle_rematch_decline(data):
    """Opponent declines rematch — notify requester."""
    room_id = data.get('room_id') if isinstance(data, dict) else None
    if room_id:
        emit('rematch_declined', {}, room=room_id, include_self=False)

@socketio.on('sport_switch_request')
def handle_sport_switch_request(data):
    """Player proposes switching to a different sport."""
    if not isinstance(data, dict): return
    room_id = data.get('room_id')
    sport = data.get('sport', 'nfl')
    from_user = data.get('from', 'Opponent')
    if room_id:
        emit('sport_switch_requested', {'sport': sport, 'from': from_user}, room=room_id, include_self=False)

@socketio.on('sport_switch_accept')
def handle_sport_switch_accept(data):
    """Opponent accepts sport switch — create new game with new sport."""
    if not isinstance(data, dict): return
    room_id = data.get('room_id')
    sport = data.get('sport', 'nfl')
    if not room_id: return
    s = get_room(room_id)
    if not s: return
    new_state_fn = {'nfl': empty_state, 'mlb': mlb_empty_state, 'nba': nba_empty_state, 'nhl': nhl_empty_state}.get(sport, empty_state)
    serialise_fn = {'nfl': serialise_state, 'mlb': mlb_serialise_state, 'nba': nba_serialise_state, 'nhl': nhl_serialise_state}.get(sport, serialise_state)
    u1 = _resolve_user({"username": s["players"][1]["username"], "id": s["players"][1]["user_id"]})
    u2 = _resolve_user({"username": s["players"][2]["username"], "id": s["players"][2]["user_id"]})
    if not u1 or not u2: return
    cleanup_stale_rooms()
    new_room_id = _generate_room_id()
    new_s = new_state_fn(u1, u2)
    new_s["room_id"] = new_room_id
    new_s["sport"] = sport
    save_room(new_room_id, new_s)
    serialised = serialise_fn(new_s)
    serialised["room_id"] = new_room_id
    sio_join_room(new_room_id)
    socketio.emit('sport_switch_accepted', {'room_id': new_room_id, 'sport': sport, 'state': serialised}, room=room_id)

@socketio.on('sport_switch_decline')
def handle_sport_switch_decline(data):
    """Opponent declines sport switch."""
    room_id = data.get('room_id') if isinstance(data, dict) else None
    if room_id:
        emit('sport_switch_declined', {}, room=room_id, include_self=False)

@socketio.on('leave_room')
def handle_leave_room(data):
    """Player leaves the room — notify opponent."""
    if not isinstance(data, dict): return
    room_id = data.get('room_id')
    username = data.get('username', 'Opponent')
    if room_id:
        emit('opponent_left', {'username': username}, room=room_id, include_self=False)

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)
