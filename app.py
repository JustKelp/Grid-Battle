import math
import random
import hashlib
import sqlite3
import os
import sys
import unicodedata
import time
import json as _json
import urllib.request
import flask

# ── DATA LIBRARIES ──────────────────────────────────────────────────────────
# NFL - Using nflreadpy (preferred library for NFL data)
try:
    import nflreadpy as nfl
    HAS_NFL_LIB = True
except ImportError:
    HAS_NFL_LIB = False

# pybaseball for MLB data
try:
    from pybaseball import lahman
    HAS_PYBASEBALL = True
except ImportError:
    HAS_PYBASEBALL = False

from flask import Flask, jsonify, request

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "grid-game-secret-change-me")
DB_PATH = os.path.join(os.path.dirname(__file__), "users.db")

# ── CACHE SYSTEM ──────────────────────────────────────────────────────────
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
CACHE_DAYS = 7  # Refresh cache every 7 days

def ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)

def get_cache_path(sport):
    return os.path.join(CACHE_DIR, f"{sport}_players.json")

def load_from_cache(sport):
    """Load player data from cache if valid"""
    cache_file = get_cache_path(sport)
    if not os.path.exists(cache_file):
        return None
    try:
        # Check if cache is too old
        mtime = os.path.getmtime(cache_file)
        age_days = (time.time() - mtime) / (24*3600)
        if age_days > CACHE_DAYS:
            return None

        with open(cache_file, 'r', encoding='utf-8') as f:
            data = _json.load(f)
        return data
    except Exception:
        return None

def save_to_cache(sport, players):
    """Save player data to cache"""
    ensure_cache_dir()
    cache_file = get_cache_path(sport)
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            _json.dump(players, f, ensure_ascii=False, indent=2)
    except Exception: pass

def clear_cache(sport=None):
    """Clear cache for a sport or all sports"""
    if sport:
        cache_file = get_cache_path(sport)
        if os.path.exists(cache_file):
            os.remove(cache_file)
    else:
        for s in ['nfl', 'mlb', 'nba', 'nhl']:
            cache_file = get_cache_path(s)
            if os.path.exists(cache_file):
                os.remove(cache_file)

# Check for --clear-cache flag
if '--clear-cache' in sys.argv:
    clear_cache()
    sys.exit(0)

ensure_cache_dir()
# Clear NHL cache to rebuild with new API approach
clear_cache('nhl')

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
    # ── Basketball Reference CSV abbreviations → game abbreviations ────────
    "BRK":"BKN",    # Brooklyn Nets (BR uses BRK, game uses BKN)
    "CHO":"CHA",    # Charlotte Hornets 2014+ (BR uses CHO)
    "NJN":"BKN",    # New Jersey Nets → Brooklyn
    "PHO":"PHX",    # Phoenix Suns (BR uses PHO)
    "SEA":"OKC",    # Seattle SuperSonics → OKC Thunder
    "NOH":"NOP",    # New Orleans Hornets
    "NOK":"NOP",    # New Orleans/OKC Hornets
    "NOP":"NOP",    # New Orleans Pelicans (passthrough)
    "VAN":"MEM",    # Vancouver Grizzlies → Memphis
    "CHH":"CHA",    # Charlotte Hornets original
    "CHB":"CHI",    # Chicago Bulls variant
    "WSB":"WAS",    # Washington Bullets → Wizards
    "SDC":"LAC",    # San Diego Clippers → LA
    "PHL":"PHI",    # Philadelphia variant
    "SA":"SAS",     # San Antonio shorthand
    "GS":"GSW",     # Golden State shorthand
    "GSW":"GSW",    # passthrough
    "NY":"NYK",     # New York shorthand
    "NO":"NOP",     # New Orleans shorthand
    "LA":"LAL",     # Los Angeles shorthand
    "ORL":"ORL",    # passthrough
    # ── Historical franchises → successor teams ────────────────────────────
    "KCK":"SAC",    # Kansas City Kings → Sacramento
    "CIN":"SAC",    # Cincinnati Royals → Sacramento
    "ROC":"SAC",    # Rochester Royals → Sacramento
    "NOJ":"UTA",    # New Orleans Jazz → Utah
    "SDR":"HOU",    # San Diego Rockets → Houston
    "SFW":"GSW",    # San Francisco Warriors → GSW
    "STB":"ATL",    # St. Louis Hawks → Atlanta
    "MLH":"ATL",    # Milwaukee Hawks → Atlanta
    "TRI":"ATL",    # Tri-Cities Blackhawks → Atlanta
    "SYR":"PHI",    # Syracuse Nationals → Philadelphia
    "FTW":"DET",    # Fort Wayne Pistons → Detroit
    "MNL":"LAL",    # Minneapolis Lakers → LA
    "CAP":"WAS",    # Capital Bullets → Washington
    "BAL":"WAS",    # Baltimore Bullets → Washington
    "CHZ":"WAS",    # Chicago Zephyrs → Washington
    "CHI2":"WAS",   # Chicago Packers → Washington
    "AND":"IND",
    "DLC":"DAL",
    "KEN":"IND",
    "WAS":"WAS",
    "SAS":"SAS",
}
_ESPN_NBA_LOGOS = {
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
NBA_LOGOS = dict(_ESPN_NBA_LOGOS)

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
    # ── API dot-notation abbreviations ────────────────────────────────────
    "S.J":"SJS","N.J":"NJD","T.B":"TBL","L.A":"LAK",
    # ── Relocated franchises → successor current team ──────────────────────
    "QUE":"COL",   # Quebec Nordiques → Colorado Avalanche
    "HFD":"CAR",   # Hartford Whalers → Carolina Hurricanes
    "ATL":"WPG",   # Atlanta Thrashers → Winnipeg Jets (2.0)
    "MNS":"DAL",   # Minnesota North Stars → Dallas Stars
    "WIN":"UTA",   # Winnipeg Jets (1.0) → PHX/ARI → Utah Hockey Club
    "PHX":"UTA",   # Phoenix Coyotes → Utah Hockey Club
    "ARI":"UTA",   # Arizona Coyotes → Utah Hockey Club
    "ARI_OLD":"UTA",
    # ── Very old / early NHL franchises ───────────────────────────────────
    "CLR":"NJD",   # Colorado Rockies (NHL) → New Jersey Devils
    "KCS":"NJD",   # Kansas City Scouts → CLR → NJD
    "CGS":"DAL",   # California Golden Seals → Cleveland → MIN → DAL
    "CAL":"CGY",   # Atlanta Flames / early Calgary → Calgary Flames
    "CLE":"MIN",   # Cleveland Barons → merged with Minnesota North Stars
    "MIN2":"DAL",  # Minnesota North Stars (pre-split) → Dallas Stars
    "ATF":"CGY",   # Atlanta Flames → Calgary Flames
    "KC":"NJD",    # Kansas City Scouts → NJD
    # ── Misc abbreviation variants the API may return ─────────────────────
    "TB":"TBL","NJ":"NJD","SJ":"SJS","LA":"LAK","WAS":"WSH",
    "ATL2":"WPG","CBJ2":"CBJ",
}

_ESPN_NHL_LOGOS = {
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
NHL_LOGOS = dict(_ESPN_NHL_LOGOS)

NFL_START_YEAR   = 1999
NFL_WEEKLY_START = 2002
DATA_YEARS       = list(range(NFL_START_YEAR, 2026))
WEEKLY_YEARS     = list(range(NFL_WEEKLY_START, 2026))
MAX_MISS_STREAK  = 5
WIN_HOLD_TURNS   = 3
HINTS_PER_PLAYER = 3
EXCLUDED_POSITIONS = {"K","P","LS","PK","PT"}
MLB_START_YEAR   = 1900
NBA_START_YEAR   = 1946
NHL_START_YEAR   = 1917

# ── NAME SANITIZATION ──────────────────────────────────────────────────────
def sanitize_name(name: str) -> str:
    if not name: return ""
    name = name.replace("\u2019","'").replace("\u2018","'").replace("\u02bc","'")
    name = unicodedata.normalize("NFC", name)
    return name.strip()

NFL_PLAYER_ALIASES = {
    "Nickell Robey":"Nickell Robey-Coleman","Chad Johnson":"Chad Ochocinco",
    "Robert Griffin":"Robert Griffin III","Melvin Gordon III":"Melvin Gordon",
    "Odell Beckham":"Odell Beckham Jr.","Patrick Mahomes":"Patrick Mahomes II",
    "Will Fuller":"Will Fuller V","Kenneth Walker":"Kenneth Walker III",
    "Brian Robinson":"Brian Robinson Jr.",
}
MLB_PLAYER_ALIASES = {
    "Mike Stanton":"Giancarlo Stanton","Jake deGrom":"Jacob deGrom",
    "Dee Gordon":"Dee Strange-Gordon",
}
NBA_PLAYER_ALIASES = {
    "Ron Artest":"Metta World Peace","Metta World Peace":"Metta World Peace",
    "Stephen Curry":"Stephen Curry","Steph Curry":"Stephen Curry",
}
NHL_PLAYER_ALIASES = {}

def _normalise_player_name(name, aliases):
    name = sanitize_name(name)
    return aliases.get(name, name)
def calculate_rarity(player_games, total_games):
    if total_games == 0:
        return 0
    return player_games / total_games


def build_square_index(players):
    square_totals = {}

    for p in players:
        for team in p.get("teams", []):
            square_totals[team] = square_totals.get(team, 0) + p.get("games", 0)

    return square_totals


def apply_rarity(players):
    square_totals = build_square_index(players)

    for p in players:
        p["rarity"] = {}
        for team in p.get("teams", []):
            total = square_totals.get(team, 1)
            p["rarity"][team] = calculate_rarity(p.get("games", 0), total)

    return players

# ── DATABASE ────────────────────────────────────────────────────────────────
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

def hash_password(pw): return hashlib.sha256(pw.encode()).hexdigest()

def get_user(username):
    con = sqlite3.connect(DB_PATH); con.row_factory = sqlite3.Row
    row = con.execute("SELECT * FROM users WHERE LOWER(username)=LOWER(?)",(username,)).fetchone()
    con.close(); return dict(row) if row else None

def create_user(username, password, nfl_mascot, mlb_mascot="NYY", nba_mascot="LAL", nhl_mascot="BOS"):
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute("INSERT INTO users (username,password_hash,nfl_mascot,mlb_mascot,nba_mascot,nhl_mascot) VALUES (?,?,?,?,?,?)",
                    (username, hash_password(password), nfl_mascot, mlb_mascot, nba_mascot, nhl_mascot))
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

# ── NFL BUILD ───────────────────────────────────────────────────────────────
def normalise_team(t):
    if not isinstance(t, str): return ""
    t = t.strip().upper(); return TEAM_ALIAS.get(t, t)

def build_player_db():
    """Build NFL player database using nflreadpy"""
    # Try cache first
    cached = load_from_cache('nfl')
    if cached:
        return cached

    players = {}

    if not HAS_NFL_LIB:
        return []

    # Load rosters from nflreadpy (multiple seasons for coverage)
    seasons = list(range(1970, 2026))

    try:
        # Load rosters - this gives us player names, teams, positions
        rosters_df = nfl.load_rosters(seasons=seasons)

        # Convert to pandas if needed (nflreadpy returns Polars by default)
        if hasattr(rosters_df, 'to_pandas'):
            rosters_df = rosters_df.to_pandas()

        for _, row in rosters_df.iterrows():
            try:
                # Get player name
                first_name = str(row.get("first_name", "")).strip()
                last_name = str(row.get("last_name", "")).strip()
                full_name = f"{first_name} {last_name}".strip()
                name = sanitize_name(_normalise_player_name(full_name, NFL_PLAYER_ALIASES))

                # Get team abbreviation
                team = normalise_team(str(row.get("team", "")).strip())

                # Get position
                position = str(row.get("position", "")).strip()

                # Get jersey number
                jersey = str(row.get("jersey_number", "")).strip()
                if jersey == "nan" or jersey == "None":
                    jersey = ""

                # Get season
                season = int(row.get("season", 0)) if row.get("season") else None

                # Skip if missing required data or excluded positions
                if not name or not team or team not in NFL_TEAMS:
                    continue
                if position in EXCLUDED_POSITIONS:
                    continue

                if name not in players:
                    players[name] = {
                        "name": name,
                        "teams": [],
                        "weeks_by_team": {},
                        "headshot": "",
                        "position": position,
                        "jersey": jersey,
                        "debut_year": season
                    }
                if team not in players[name]["teams"]:
                    players[name]["teams"].append(team)
                # Each roster entry = roughly 1 week of games
                players[name]["weeks_by_team"][team] = players[name]["weeks_by_team"].get(team, 0) + 1
                if jersey and not players[name]["jersey"]:
                    players[name]["jersey"] = jersey
                if season and (not players[name].get("debut_year") or season < players[name]["debut_year"]):
                    players[name]["debut_year"] = season
            except Exception:
                continue

    except Exception: pass

    # Also load player stats for additional coverage
    try:
        stats_df = nfl.load_player_stats(seasons=list(range(1970, 2026)))

        if hasattr(stats_df, 'to_pandas'):
            stats_df = stats_df.to_pandas()

        for _, row in stats_df.iterrows():
            try:
                # Get player name
                first_name = str(row.get("first_name", "")).strip()
                last_name = str(row.get("last_name", "")).strip()
                full_name = f"{first_name} {last_name}".strip()
                name = sanitize_name(_normalise_player_name(full_name, NFL_PLAYER_ALIASES))

                # Get team
                team = normalise_team(str(row.get("team", "")).strip())

                # Get position
                position = str(row.get("position", "")).strip()

                # Get season
                season = int(row.get("season", 0)) if row.get("season") else None

                if not name or not team or team not in NFL_TEAMS:
                    continue
                if position in EXCLUDED_POSITIONS:
                    continue

                if name not in players:
                    players[name] = {
                        "name": name,
                        "teams": [],
                        "weeks_by_team": {},
                        "headshot": "",
                        "position": position,
                        "jersey": "",
                        "debut_year": season
                    }
                if team not in players[name]["teams"]:
                    players[name]["teams"].append(team)
                # Add weeks from games played (approximately 17 games per season)
                games = int(row.get("games", 0) or row.get("games_played", 0) or 17)
                players[name]["weeks_by_team"][team] = players[name]["weeks_by_team"].get(team, 0) + games
                if season and (not players[name].get("debut_year") or season < players[name]["debut_year"]):
                    players[name]["debut_year"] = season
            except Exception:
                continue

    except Exception: pass

    # Also try weekly rosters for additional data
    try:
        weekly_df = nfl.load_rosters_weekly(seasons=list(range(1970, 2026)))

        if hasattr(weekly_df, 'to_pandas'):
            weekly_df = weekly_df.to_pandas()

        for _, row in weekly_df.iterrows():
            try:
                first_name = str(row.get("first_name", "")).strip()
                last_name = str(row.get("last_name", "")).strip()
                full_name = f"{first_name} {last_name}".strip()
                name = sanitize_name(_normalise_player_name(full_name, NFL_PLAYER_ALIASES))

                team = normalise_team(str(row.get("team", "")).strip())
                position = str(row.get("position", "")).strip()
                jersey = str(row.get("jersey_number", "")).strip()
                if jersey == "nan" or jersey == "None":
                    jersey = ""

                if not name or not team or team not in NFL_TEAMS:
                    continue
                if position in EXCLUDED_POSITIONS:
                    continue

                if name not in players:
                    players[name] = {
                        "name": name,
                        "teams": [],
                        "weeks_by_team": {},
                        "headshot": "",
                        "position": position,
                        "jersey": jersey,
                        "debut_year": None
                    }
                if team not in players[name]["teams"]:
                    players[name]["teams"].append(team)
                players[name]["weeks_by_team"][team] = players[name]["weeks_by_team"].get(team, 0) + 1
                if jersey and not players[name]["jersey"]:
                    players[name]["jersey"] = jersey
            except Exception:
                continue

    except Exception: pass
    # ── ESPN HEADSHOTS ───────────────────────────────────────────────────────
    # Fetch current NFL rosters from ESPN to get headshot URLs
    ESPN_NFL_API = "https://site.api.espn.com/apis/site/v2/sports/football/nfl"
    try:
        url = f"{ESPN_NFL_API}/teams"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=15)
        data = _json.loads(resp.read())
        for sport in data.get("sports", []):
            for league in sport.get("leagues", []):
                for team in league.get("teams", []):
                    team_info = team.get("team", {})
                    abbr = normalise_team(team_info.get("abbreviation", ""))
                    if abbr not in NFL_TEAMS:
                        continue
                    try:
                        roster_url = f"{ESPN_NFL_API}/teams/{team_info.get('id', '')}/roster"
                        roster_req = urllib.request.Request(roster_url, headers={"User-Agent": "Mozilla/5.0"})
                        roster_resp = urllib.request.urlopen(roster_req, timeout=15)
                        roster_data = _json.loads(roster_resp.read())
                        for athlete in roster_data.get("athletes", []):
                            for item in (athlete.get("items") or [athlete]):
                                pname = sanitize_name(str(item.get("fullName", "")).strip())
                                headshot = item.get("headshot", {}).get("href", "")
                                if pname and headshot and pname in players:
                                    players[pname]["headshot"] = headshot
                    except Exception:
                        continue
    except Exception:
        pass
    result = list(players.values())
    save_to_cache('nfl', result)
    return result

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

_ESPN_LOGOS = {
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
def build_team_logos():
    """Build team logos - using ESPN URLs as default"""
    return dict(_ESPN_LOGOS)

print("Building NFL player database...")
PLAYERS_DB = build_player_db()
print(" NFL DB Built", len(PLAYERS_DB), "Players")
TEAM_INDEX = {}
for _p in PLAYERS_DB:
    for _t in _p.get("teams",[]): TEAM_INDEX.setdefault(_t,[]).append(_p)
PLAYER_NAMES_SORTED = sorted(p["name"] for p in PLAYERS_DB)
_NFL_STAT_SEASONS_CACHE = {}
for p in PLAYERS_DB:
    for stat_key, team_dict in p.get("achievements",{}).items():
        if isinstance(team_dict,dict):
            for team,count in team_dict.items():
                _NFL_STAT_SEASONS_CACHE[(team,stat_key)] = _NFL_STAT_SEASONS_CACHE.get((team,stat_key),0)+count
TEAM_LOGOS = build_team_logos()

# ── MLB BUILD ───────────────────────────────────────────────────────────────
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
_ESPN_MLB_LOGOS = {
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
MLB_LOGOS = dict(_ESPN_MLB_LOGOS)

def normalise_mlb_team(t):
    if not isinstance(t,str): return ""
    t = t.strip().upper()
    if t in ("- - -","---","TOT",""): return ""
    return MLB_TEAM_ALIAS.get(t,t)

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

def _build_mlb_achievements(players):
    if not HAS_PYBASEBALL: return
    try:
        from pybaseball import lahman
        batting, pitching, people = lahman.batting(), lahman.pitching(), lahman.people()
        pid_to_name = {}
        for _, row in people.iterrows():
            first,last,pid = str(row.get("nameFirst","")).strip(),str(row.get("nameLast","")).strip(),str(row.get("playerID","")).strip()
            if first and last and pid: pid_to_name[pid] = _normalise_player_name(sanitize_name(f"{first} {last}"),MLB_PLAYER_ALIASES)
        def _process(df, stats_list):
            for _, row in df.iterrows():
                if int(row.get("yearID",0)) < MLB_START_YEAR: continue
                pid = str(row.get("playerID",""))
                name = pid_to_name.get(pid,"")
                team = normalise_mlb_team(str(row.get("teamID","")))
                if not name or not team or team not in MLB_TEAMS or name not in players: continue
                for cat_key,col,threshold in stats_list:
                    try: val = float(row.get(col,0) or 0)
                    except: continue
                    if val >= threshold:
                        players[name].setdefault("achievements",{}).setdefault(cat_key,{})[team] = players[name]["achievements"][cat_key].get(team,0)+1
        # Hitting achievements
        _process(batting,[("hr_30","HR",30),("hr_40","HR",40),("hr_50","HR",50),("rbi_100","RBI",100),("rbi_130","RBI",130),("hits_200","H",200),("hits_220","H",220),("sb_30","SB",30),("sb_40","SB",40),("runs_120","R",120),("runs_130","R",130),("doubles_40","2B",40),("triples_10","3B",10)])
        # Batting average threshold (.300, .320, .350)
        for _,row in batting.iterrows():
            if int(row.get("yearID",0)) < MLB_START_YEAR: continue
            pid = str(row.get("playerID","")); name = pid_to_name.get(pid,""); team = normalise_mlb_team(str(row.get("teamID","")))
            if not name or not team or team not in MLB_TEAMS or name not in players: continue
            try:
                ab,h = float(row.get("AB",0) or 0), float(row.get("H",0) or 0)
                if ab >= 400 and h/ab >= 0.350:
                    players[name].setdefault("achievements",{}).setdefault("avg_350",{})[team] = players[name]["achievements"]["avg_350"].get(team,0)+1
                elif ab >= 400 and h/ab >= 0.320:
                    players[name].setdefault("achievements",{}).setdefault("avg_320",{})[team] = players[name]["achievements"]["avg_320"].get(team,0)+1
                elif ab >= 400 and h/ab >= 0.300:
                    players[name].setdefault("achievements",{}).setdefault("avg_300",{})[team] = players[name]["achievements"]["avg_300"].get(team,0)+1
            except: pass
        # Slugging achievements
        for _,row in batting.iterrows():
            if int(row.get("yearID",0)) < MLB_START_YEAR: continue
            pid = str(row.get("playerID","")); name = pid_to_name.get(pid,""); team = normalise_mlb_team(str(row.get("teamID","")))
            if not name or not team or team not in MLB_TEAMS or name not in players: continue
            try:
                ab,tb = float(row.get("AB",0) or 0), float(row.get("TB",0) or 0)
                if ab >= 400 and tb/ab >= 0.600:
                    players[name].setdefault("achievements",{}).setdefault("slg_600",{})[team] = players[name]["achievements"]["slg_600"].get(team,0)+1
                elif ab >= 400 and tb/ab >= 0.550:
                    players[name].setdefault("achievements",{}).setdefault("slg_550",{})[team] = players[name]["achievements"]["slg_550"].get(team,0)+1
                elif ab >= 400 and tb/ab >= 0.500:
                    players[name].setdefault("achievements",{}).setdefault("slg_500",{})[team] = players[name]["achievements"]["slg_500"].get(team,0)+1
            except: pass
        # Pitching achievements
        _process(pitching,[("wins_15","W",15),("wins_20","W",20),("wins_25","W",25),("k_200","SO",200),("k_250","SO",250),("k_300","SO",300)])
        for _,row in pitching.iterrows():
            if int(row.get("yearID",0)) < MLB_START_YEAR: continue
            pid = str(row.get("playerID","")); name = pid_to_name.get(pid,""); team = normalise_mlb_team(str(row.get("teamID","")))
            if not name or not team or team not in MLB_TEAMS or name not in players: continue
            try:
                ip,era = float(row.get("IPouts",0) or 0)/3.0, float(row.get("ERA",99) or 99)
                if ip >= 100 and era < 2.00:
                    players[name].setdefault("achievements",{}).setdefault("era_sub2",{})[team] = players[name]["achievements"]["era_sub2"].get(team,0)+1
                elif ip >= 100 and era < 2.50:
                    players[name].setdefault("achievements",{}).setdefault("era_sub250",{})[team] = players[name]["achievements"]["era_sub250"].get(team,0)+1
                elif ip >= 100 and era < 3.00:
                    players[name].setdefault("achievements",{}).setdefault("era_sub3",{})[team] = players[name]["achievements"]["era_sub3"].get(team,0)+1
                # Win percentage
                wins,losses = float(row.get("W",0) or 0), float(row.get("L",0) or 0)
                total = wins + losses
                if total >= 25 and wins/total >= 0.700:
                    players[name].setdefault("achievements",{}).setdefault("win_pct_700",{})[team] = players[name]["achievements"]["win_pct_700"].get(team,0)+1
            except: pass
        # Complete games and shutouts
        for _,row in pitching.iterrows():
            if int(row.get("yearID",0)) < MLB_START_YEAR: continue
            pid = str(row.get("playerID","")); name = pid_to_name.get(pid,""); team = normalise_mlb_team(str(row.get("teamID","")))
            if not name or not team or team not in MLB_TEAMS or name not in players: continue
            try:
                cg,sho = int(float(row.get("CG",0) or 0)), int(float(row.get("SHO",0) or 0))
                if cg >= 30:
                    players[name].setdefault("achievements",{}).setdefault("cg_30",{})[team] = players[name]["achievements"]["cg_30"].get(team,0)+1
                if sho >= 5:
                    players[name].setdefault("achievements",{}).setdefault("sho_5",{})[team] = players[name]["achievements"]["sho_5"].get(team,0)+1
            except: pass
    except Exception: pass

def build_mlb_player_db():
    # Try cache first
    cached = load_from_cache('mlb')
    if cached:
        return cached

    players = {}
    print("  Building MLB DB...")

    # Fetch from ESPN for current rosters
    ESPN_MLB_API = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb"
    try:
        url = f"{ESPN_MLB_API}/teams"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=20)
        data = _json.loads(resp.read())
        for sport in data.get("sports", []):
            for league in sport.get("leagues", []):
                for team in league.get("teams", []):
                    team_info = team.get("team", {})
                    abbr = normalise_mlb_team(team_info.get("abbreviation", ""))
                    if abbr not in MLB_TEAMS:
                        continue
                    try:
                        roster_url = team_info.get("links", [{}])[0].get("href", "")
                        if roster_url:
                            roster_req = urllib.request.Request(roster_url, headers={"User-Agent": "Mozilla/5.0"})
                            roster_resp = urllib.request.urlopen(roster_req, timeout=15)
                            roster_data = _json.loads(roster_resp.read())
                            for athlete in roster_data.get("athletes", []):
                                name = sanitize_name(_normalise_player_name(str(athlete.get("fullName", "")).strip(), MLB_PLAYER_ALIASES))
                                position = str(athlete.get("position", {}).get("abbreviation", "")).strip()
                                jersey = str(athlete.get("jersey", "")).strip()
                                headshot = athlete.get("headshot", {}).get("href", "")
                                if not name: continue
                                if name not in players:
                                    players[name] = {"name": name, "teams": [], "games_by_team": {}, "headshot": headshot, "position": position, "jersey": jersey}
                                if abbr not in players[name]["teams"]:
                                    players[name]["teams"].append(abbr)
                                players[name]["games_by_team"][abbr] = players[name]["games_by_team"].get(abbr, 0) + 162
                                if jersey and not players[name]["jersey"]:
                                    players[name]["jersey"] = jersey
                    except Exception:
                        continue
    except Exception: pass


    # Use Lahman database for comprehensive historical coverage
    if HAS_PYBASEBALL:
        try:
            from pybaseball import lahman
            batting, pitching, people = lahman.batting(), lahman.pitching(), lahman.people()
            pid_to_name, pid_to_pos = {}, {}
            for _, row in people.iterrows():
                first, last, pid, pos = str(row.get("nameFirst","")).strip(), str(row.get("nameLast","")).strip(), str(row.get("playerID","")).strip(), str(row.get("primaryPosition","")).strip()
                if first and last and pid:
                    pid_to_name[pid] = _normalise_player_name(sanitize_name(f"{first} {last}"), MLB_PLAYER_ALIASES)
                    pid_to_pos[pid] = pos
            def _ingest_lahman(df_rows, games_col):
                for _, row in df_rows.iterrows():
                    year, pid = int(row.get("yearID",0)), str(row.get("playerID",""))
                    if year < MLB_START_YEAR: continue
                    name, team = pid_to_name.get(pid,""), normalise_mlb_team(str(row.get("teamID","")))
                    if not name or not team or team not in MLB_TEAMS: continue
                    try: g = int(float(row.get(games_col,0) or 0))
                    except: g = 0
                    g = g if g > 0 else round(162*0.75)
                    if name not in players:
                        players[name] = {"name":name,"teams":[],"games_by_team":{},"headshot":"","position":pid_to_pos.get(pid,""),"jersey":""}
                    if team not in players[name]["teams"]: players[name]["teams"].append(team)
                    players[name]["games_by_team"][team] = players[name]["games_by_team"].get(team,0)+g
            _ingest_lahman(batting, "G")
            _ingest_lahman(pitching, "G")
        except Exception: pass

    # Also fetch from MLB Stats API for current players and recent years
    MLB_API = "https://statsapi.mlb.com/api/v1"
    try:
        resp = urllib.request.urlopen(f"{MLB_API}/teams?sportId=1", timeout=15)
        teams_raw = _json.loads(resp.read())
        id_to_abbr = {t["id"]:normalise_mlb_team(t.get("abbreviation","")) for t in teams_raw.get("teams",[]) if normalise_mlb_team(t.get("abbreviation","")) in MLB_TEAMS}
        # Fetch ALL years from 1900 onwards
        for year in range(1900, 2026):
            try:
                resp = urllib.request.urlopen(f"{MLB_API}/sports/1/players?season={year}", timeout=15)
                data = _json.loads(resp.read())
                for p in data.get("people",[]):
                    name = sanitize_name(_normalise_player_name(p.get("fullName","").strip(), MLB_PLAYER_ALIASES))
                    abbr = id_to_abbr.get(p.get("currentTeam",{}).get("id"),"")
                    pos, jersey = p.get("primaryPosition",{}).get("abbreviation",""), str(p.get("primaryNumber","") or "")
                    if not name or not abbr: continue
                    if name not in players:
                        players[name] = {"name":name,"teams":[],"games_by_team":{},"headshot":"","position":pos,"jersey":jersey,"debut_year":year}
                    elif year < (players[name].get("debut_year") or 9999): players[name]["debut_year"] = year
                    if abbr not in players[name]["teams"]: players[name]["teams"].append(abbr)
                    players[name]["games_by_team"][abbr] = players[name]["games_by_team"].get(abbr,0)+1
                    if pos: players[name]["position"] = pos
                    if jersey: players[name]["jersey"] = jersey

            except Exception: pass
    except Exception: pass

    _build_mlb_achievements(players)
    for p in players.values(): p.setdefault("achievements",{})



    # Save to cache
    result = list(players.values())
    save_to_cache('mlb', result)
    return result

print("Building MLB player database...")

MLB_PLAYERS_DB = build_mlb_player_db()
print(f"  MLB DB Built: {len(MLB_PLAYERS_DB)} Players")
MLB_TEAM_INDEX = {}
for _mp in MLB_PLAYERS_DB:
    for _mt in _mp.get("teams",[]): MLB_TEAM_INDEX.setdefault(_mt,[]).append(_mp)
MLB_PLAYER_NAMES_SORTED = sorted(p["name"] for p in MLB_PLAYERS_DB)
_MLB_STAT_SEASONS_CACHE = {}
for p in MLB_PLAYERS_DB:
    for stat_key, team_dict in p.get("achievements",{}).items():
        if isinstance(team_dict,dict):
            for team,count in team_dict.items():
                _MLB_STAT_SEASONS_CACHE[(team,stat_key)] = _MLB_STAT_SEASONS_CACHE.get((team,stat_key),0)+count

# ── NBA BUILD ───────────────────────────────────────────────────────────────
def normalise_nba_team(t):
    if not isinstance(t,str): return ""
    t = t.strip().upper()
    return NBA_TEAM_ALIAS.get(t,t)
NBA_STAT_CATEGORIES = [
    {"key":"pts_25","label":"25+ PPG","desc":"25+ points per game in a season (min 40 GP)"},
    {"key":"pts_30","label":"30+ PPG","desc":"30+ points per game in a season (min 40 GP)"},
    {"key":"reb_10","label":"10+ RPG","desc":"10+ rebounds per game in a season (min 40 GP)"},
    {"key":"ast_10","label":"10+ APG","desc":"10+ assists per game in a season (min 40 GP)"},
    {"key":"pts_2000","label":"2000+ Pts","desc":"2000+ total points in a season"},
    {"key":"blk_150","label":"150+ Blocks","desc":"150+ blocks in a season"},
    {"key":"stl_150","label":"150+ Steals","desc":"150+ steals in a season"},
]

def _build_nba_achievements(players):
    try:
        import pandas as _pd
        _csv = os.path.join(os.path.dirname(__file__), "nba_per_game.csv")
        _tot = os.path.join(os.path.dirname(__file__), "nba_player_totals.csv")
        # Per game achievements
        if os.path.exists(_csv):
            _df = _pd.read_csv(_csv)
            _df.columns = _df.columns.str.lower()
            for _, row in _df.iterrows():
                try:
                    name = sanitize_name(_normalise_player_name(
                        str(row.get("player","")).strip(), NBA_PLAYER_ALIASES))
                    raw_tm = str(row.get("team", row.get("tm", ""))).strip().upper()
                    if raw_tm in ("TOT", "2TM", "3TM", "4TM", "5TM"): continue
                    abbr = normalise_nba_team(raw_tm)
                    gp = int(row.get("g", 0) or 0)
                    if not name or not abbr or abbr not in NBA_TEAMS or gp < 40: continue
                    if name not in players: continue
                    pts = float(row.get("pts", 0) or 0)
                    reb = float(row.get("trb", row.get("reb", 0)) or 0)
                    ast = float(row.get("ast", 0) or 0)
                    if pts >= 30: players[name].setdefault("achievements",{}).setdefault("pts_30",{})[abbr] = players[name]["achievements"]["pts_30"].get(abbr,0)+1
                    elif pts >= 25: players[name].setdefault("achievements",{}).setdefault("pts_25",{})[abbr] = players[name]["achievements"]["pts_25"].get(abbr,0)+1
                    if reb >= 10: players[name].setdefault("achievements",{}).setdefault("reb_10",{})[abbr] = players[name]["achievements"]["reb_10"].get(abbr,0)+1
                    if ast >= 10: players[name].setdefault("achievements",{}).setdefault("ast_10",{})[abbr] = players[name]["achievements"]["ast_10"].get(abbr,0)+1
                except Exception: continue
        # Total achievements
        if os.path.exists(_tot):
            _df = _pd.read_csv(_tot)
            _df.columns = _df.columns.str.lower()
            for _, row in _df.iterrows():
                try:
                    name = sanitize_name(_normalise_player_name(
                        str(row.get("player","")).strip(), NBA_PLAYER_ALIASES))
                    abbr = normalise_nba_team(str(row.get("tm","")).strip().upper())
                    if not name or not abbr or abbr not in NBA_TEAMS: continue
                    if name not in players: continue
                    pts = float(row.get("pts", 0) or 0)
                    blk = float(row.get("blk", 0) or 0)
                    stl = float(row.get("stl", 0) or 0)
                    if pts >= 2000: players[name].setdefault("achievements",{}).setdefault("pts_2000",{})[abbr] = players[name]["achievements"]["pts_2000"].get(abbr,0)+1
                    if blk >= 150: players[name].setdefault("achievements",{}).setdefault("blk_150",{})[abbr] = players[name]["achievements"]["blk_150"].get(abbr,0)+1
                    if stl >= 150: players[name].setdefault("achievements",{}).setdefault("stl_150",{})[abbr] = players[name]["achievements"]["stl_150"].get(abbr,0)+1
                except Exception: continue
    except Exception: pass
NHL_STAT_CATEGORIES = [
    {"key":"goals_30","label":"30+ Goals","desc":"30+ goals in a season"},
    {"key":"goals_50","label":"50+ Goals","desc":"50+ goals in a season"},
    {"key":"points_80","label":"80+ Points","desc":"80+ points in a season"},
    {"key":"points_100","label":"100+ Points","desc":"100+ points in a season"},
    {"key":"goalie_30w","label":"30+ Wins (G)","desc":"30+ wins as a goalie in a season"},
]

def _build_nhl_achievements(players):
    try:
        import pandas as _pd
        _scsv = os.path.join(os.path.dirname(__file__), "nhl_skating.csv")
        _gcsv = os.path.join(os.path.dirname(__file__), "nhl_goalies.csv")
        # Build name lookup from master
        _id_to_name = {}
        _mcsv = os.path.join(os.path.dirname(__file__), "nhl_master.csv")
        if os.path.exists(_mcsv):
            _mdf = _pd.read_csv(_mcsv, encoding='latin-1')
            for _, r in _mdf.iterrows():
                pid = str(r.get("playerID","")).strip()
                first = str(r.get("firstName",r.get("first_name",""))).strip()
                last = str(r.get("lastName",r.get("last_name",""))).strip()
                if pid and first and last: _id_to_name[pid] = sanitize_name(f"{first} {last}")
        # Skater achievements from Scoring.csv
        if os.path.exists(_scsv):
            _df = _pd.read_csv(_scsv, encoding='latin-1')
            for _, row in _df.iterrows():
                try:
                    pid = str(row.get("playerID","")).strip()
                    name = _normalise_player_name(_id_to_name.get(pid,""), NHL_PLAYER_ALIASES)
                    if not name or name not in players: continue
                    abbr = normalise_nhl_team(str(row.get("tmID","")).strip().upper())
                    if not abbr or abbr not in NHL_TEAMS: continue
                    g = int(float(row.get("G", row.get("goals", 0)) or 0))
                    a = int(float(row.get("A", row.get("assists", 0)) or 0))
                    pts = g + a
                    if g >= 50: players[name].setdefault("achievements",{}).setdefault("goals_50",{})[abbr] = players[name]["achievements"]["goals_50"].get(abbr,0)+1
                    elif g >= 30: players[name].setdefault("achievements",{}).setdefault("goals_30",{})[abbr] = players[name]["achievements"]["goals_30"].get(abbr,0)+1
                    if pts >= 100: players[name].setdefault("achievements",{}).setdefault("points_100",{})[abbr] = players[name]["achievements"]["points_100"].get(abbr,0)+1
                    elif pts >= 80: players[name].setdefault("achievements",{}).setdefault("points_80",{})[abbr] = players[name]["achievements"]["points_80"].get(abbr,0)+1
                except Exception: continue
        # Goalie achievements
        if os.path.exists(_gcsv):
            _df = _pd.read_csv(_gcsv, encoding='latin-1')
            for _, row in _df.iterrows():
                try:
                    pid = str(row.get("playerID","")).strip()
                    name = _normalise_player_name(_id_to_name.get(pid,""), NHL_PLAYER_ALIASES)
                    if not name or name not in players: continue
                    abbr = normalise_nhl_team(str(row.get("tmID","")).strip().upper())
                    if not abbr or abbr not in NHL_TEAMS: continue
                    w = int(float(row.get("W", row.get("wins", 0)) or 0))
                    if w >= 30: players[name].setdefault("achievements",{}).setdefault("goalie_30w",{})[abbr] = players[name]["achievements"]["goalie_30w"].get(abbr,0)+1
                except Exception: continue
    except Exception: pass
def nhl_new_board():
    rows = pick_teams_with_shared_players(NHL_TEAMS, NHL_TEAM_INDEX)
    cols = pick_teams_with_shared_players(NHL_TEAMS, NHL_TEAM_INDEX)
    att = 0
    while set(rows) & set(cols) and att < 50:
        cols = pick_teams_with_shared_players(NHL_TEAMS, NHL_TEAM_INDEX); att += 1
    stat_cat = None
    cats = list(NHL_STAT_CATEGORIES); random.shuffle(cats)
    for cat in cats:
        if len([p for p in NHL_PLAYERS_DB if cat["key"] in p.get("achievements",{})]) > 0:
            stat_cat = cat; break
    stat_meta = None
    if stat_cat:
        for _ in range(20):
            if _stat_row_has_valid_cells(stat_cat["key"],cols,NHL_PLAYERS_DB):
                stat_meta = {"key":stat_cat["key"],"label":stat_cat["label"],"desc":stat_cat["desc"]}
                rows[2] = f"STAT:{stat_cat['key']}"; break
            cols = pick_teams_with_shared_players(NHL_TEAMS, NHL_TEAM_INDEX)
    return rows, cols, stat_meta

def build_nba_player_db():
    cached = load_from_cache('nba')
    if cached: return cached
    players = {}
    print("  Building NBA DB...")

    # ── STEP 1: Static name list (instant, gets all ~5000 names) ─────────
    try:
        from nba_api.stats.static import players as nba_static
        for p in nba_static.get_players():
            name = sanitize_name(_normalise_player_name(str(p.get("full_name","")).strip(), NBA_PLAYER_ALIASES))
            if name and name not in players:
                players[name] = {"name":name,"teams":[],"games_by_team":{},"headshot":"","position":"","jersey":""}
    except Exception: pass
    # ── STEP 2: Kaggle CSV — complete 1947-present team data ─────────────
    # Download from: kaggle.com/datasets/sumitrodatta/nba-aba-baa-stats
    # Save Player Totals.csv as nba_player_totals.csv in project folder
    try:
        import pandas as _pd
        _nba_csv = os.path.join(os.path.dirname(__file__), "nba_player_totals.csv")
        if os.path.exists(_nba_csv):
            _df = _pd.read_csv(_nba_csv)
            _df.columns = _df.columns.str.lower().str.strip()
            for _, _row in _df.iterrows():
                try:
                    name = sanitize_name(_normalise_player_name(
                        str(_row.get("player", "")).strip(), NBA_PLAYER_ALIASES))
                    # column is "team" in this dataset, not "tm"
                    raw_tm = str(_row.get("team", _row.get("tm", ""))).strip().upper()
                    # skip aggregate multi-team rows (2TM, 3TM, 4TM, TOT)
                    if not raw_tm or raw_tm in ("TOT", "2TM", "3TM", "4TM", "5TM"): continue
                    abbr = normalise_nba_team(raw_tm)
                    gp = int(float(_row.get("g", 0) or 0))
                    if not name or not abbr or abbr not in NBA_TEAMS or gp < 1: continue
                    if name not in players:
                        players[name] = {"name": name, "teams": [], "games_by_team": {},
                                         "headshot": "", "position": "", "jersey": ""}
                    if abbr not in players[name]["teams"]:
                        players[name]["teams"].append(abbr)
                    players[name]["games_by_team"][abbr] = \
                        players[name]["games_by_team"].get(abbr, 0) + gp
                except Exception:
                    continue
            print(f"    NBA CSV loaded: {len(players)} players")
        else:
            print("    nba_player_totals.csv not found")
    except Exception as e:
        print(f"    NBA CSV error: {e}")
    # ── STEP 3: ESPN current rosters (headshots + validates active teams) ─
    try:
        BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
        req = urllib.request.Request(f"{BASE}/teams", headers={
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept":"application/json"})
        data = _json.loads(urllib.request.urlopen(req, timeout=10).read())
        for sport_data in data.get("sports",[]):
            for league in sport_data.get("leagues",[]):
                for team_entry in league.get("teams",[]):
                    abbr = normalise_nba_team(team_entry.get("team",{}).get("abbreviation",""))
                    if abbr not in NBA_TEAMS: continue
                    try:
                        tid = team_entry.get("team",{}).get("id","")
                        rreq = urllib.request.Request(f"{BASE}/teams/{tid}/roster",
                            headers={"User-Agent":"Mozilla/5.0"})
                        rdata = _json.loads(urllib.request.urlopen(rreq, timeout=10).read())
                        for athlete in rdata.get("athletes",[]):
                            for item in (athlete.get("items") or [athlete]):
                                name = sanitize_name(_normalise_player_name(
                                    str(item.get("fullName","")).strip(), NBA_PLAYER_ALIASES))
                                hs = item.get("headshot",{}).get("href","")
                                pos = str(item.get("position",{}).get("abbreviation","")).strip()
                                jersey = str(item.get("jersey","")).strip()
                                if not name: continue
                                if name not in players:
                                    players[name] = {"name":name,"teams":[],"games_by_team":{},
                                                     "headshot":hs,"position":pos,"jersey":jersey}
                                if abbr not in players[name]["teams"]:
                                    players[name]["teams"].append(abbr)
                                players[name]["games_by_team"][abbr] = \
                                    players[name]["games_by_team"].get(abbr,0) + 82
                                if hs and not players[name]["headshot"]:
                                    players[name]["headshot"] = hs
                                if pos and not players[name]["position"]:
                                    players[name]["position"] = pos
                    except Exception: continue
    except Exception: pass

    _build_nba_achievements(players)
    for p in players.values(): p.setdefault("achievements", {})
    print(f"  NBA DB built: {len(players)} players")
    result = list(players.values())
    save_to_cache('nba', result)
    return result

print("Building NBA player database...")
try:
    NBA_PLAYERS_DB = build_nba_player_db()
except Exception as e:
    print(f"  NBA DB build failed: {e}")
    NBA_PLAYERS_DB = []
print(f"  NBA DB Built: {len(NBA_PLAYERS_DB)} Players")

NBA_TEAM_INDEX = {}
for _np in NBA_PLAYERS_DB:
    for _nt in _np.get("teams",[]): NBA_TEAM_INDEX.setdefault(_nt,[]).append(_np)
NBA_PLAYER_NAMES_SORTED = sorted(p["name"] for p in NBA_PLAYERS_DB)
_NBA_STAT_SEASONS_CACHE = {}
for p in NBA_PLAYERS_DB:
    for stat_key, team_dict in p.get("achievements",{}).items():
        if isinstance(team_dict,dict):
            for team,count in team_dict.items():
                _NBA_STAT_SEASONS_CACHE[(team,stat_key)] = _NBA_STAT_SEASONS_CACHE.get((team,stat_key),0)+count
# ── NHL BUILD ───────────────────────────────────────────────────────────────
def normalise_nhl_team(t):
    if not isinstance(t,str): return ""
    t = t.strip().upper()
    mapped = NHL_TEAM_ALIAS.get(t,t)
    return mapped if mapped in NHL_TEAMS else ""

def build_nhl_player_db():
    # Try cache first
    cached = load_from_cache('nhl')
    if cached:
        return cached

    players = {}
    print("  Building NHL DB...")

    # ── STEP 1: NHL Stats API — aggregate ALL seasons in one request ─────
    # This gets every skater who ever played an NHL game with total GP + team abbrevs
    try:
        # Skaters — all seasons aggregated, unlimited results
        skater_url = (
            "https://api.nhle.com/stats/rest/en/skater/summary"
            "?isAggregate=false&isGame=false"
            "&sort=%5B%7B%22property%22%3A%22gamesPlayed%22%2C%22direction%22%3A%22DESC%22%7D%5D"
            "&start=0&limit=-1"
            "&factCayenneExp=gamesPlayed%3E%3D1"
            "&cayenneExp=gameTypeId%3D2"
        )
        print("    Fetching all NHL skaters from stats API...")
        req = urllib.request.Request(skater_url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=120)
        data = _json.loads(resp.read())
        skater_rows = data.get("data", [])
        print(f"    Got {len(skater_rows)} skater season-rows from NHL API")

        for row in skater_rows:
            try:
                name = sanitize_name(_normalise_player_name(
                    str(row.get("skaterFullName", "")).strip(), NHL_PLAYER_ALIASES))
                if not name: continue
                raw_abbrevs = str(row.get("teamAbbrevs", ""))
                team_abbrevs = [normalise_nhl_team(t.strip()) for t in raw_abbrevs.split(",")]
                team_abbrevs = [t for t in team_abbrevs if t and t in NHL_TEAMS]
                if not team_abbrevs: continue
                pos = str(row.get("positionCode", "")).strip()
                gp = int(row.get("gamesPlayed", 0) or 0)
                if gp < 1: continue

                if name not in players:
                    players[name] = {"name": name, "teams": [], "games_by_team": {},
                                     "headshot": "", "position": pos, "jersey": ""}
                for abbr in team_abbrevs:
                    if abbr not in players[name]["teams"]:
                        players[name]["teams"].append(abbr)
                    share = gp // len(team_abbrevs) if len(team_abbrevs) > 1 else gp
                    players[name]["games_by_team"][abbr] = players[name]["games_by_team"].get(abbr, 0) + max(share, 1)
                if pos and not players[name]["position"]:
                    players[name]["position"] = pos
            except Exception: continue
        print(f"    NHL skaters loaded: {len(players)} unique players")
    except Exception as e:
        print(f"    NHL skater API error: {e}")

    # Goalies — same approach
    try:
        goalie_url = (
            "https://api.nhle.com/stats/rest/en/goalie/summary"
            "?isAggregate=false&isGame=false"
            "&sort=%5B%7B%22property%22%3A%22gamesPlayed%22%2C%22direction%22%3A%22DESC%22%7D%5D"
            "&start=0&limit=-1"
            "&factCayenneExp=gamesPlayed%3E%3D1"
            "&cayenneExp=gameTypeId%3D2"
        )
        print("    Fetching all NHL goalies from stats API...")
        req = urllib.request.Request(goalie_url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=120)
        data = _json.loads(resp.read())
        goalie_rows = data.get("data", [])
        print(f"    Got {len(goalie_rows)} goalie season-rows from NHL API")

        goalies_added = 0
        for row in goalie_rows:
            try:
                name = sanitize_name(_normalise_player_name(
                    str(row.get("goalieFullName", "")).strip(), NHL_PLAYER_ALIASES))
                if not name: continue
                raw_abbrevs = str(row.get("teamAbbrevs", ""))
                team_abbrevs = [normalise_nhl_team(t.strip()) for t in raw_abbrevs.split(",")]
                team_abbrevs = [t for t in team_abbrevs if t and t in NHL_TEAMS]
                if not team_abbrevs: continue
                gp = int(row.get("gamesPlayed", 0) or 0)
                if gp < 1: continue

                if name not in players:
                    players[name] = {"name": name, "teams": [], "games_by_team": {},
                                     "headshot": "", "position": "G", "jersey": ""}
                    goalies_added += 1
                for abbr in team_abbrevs:
                    if abbr not in players[name]["teams"]:
                        players[name]["teams"].append(abbr)
                    share = gp // len(team_abbrevs) if len(team_abbrevs) > 1 else gp
                    players[name]["games_by_team"][abbr] = players[name]["games_by_team"].get(abbr, 0) + max(share, 1)
            except Exception: continue
        print(f"    NHL goalies added: {goalies_added} new, {len(players)} total")
    except Exception as e:
        print(f"    NHL goalie API error: {e}")

    # ── STEP 2: Kaggle CSV — historical fallback for pre-API era ─────────
    try:
        import pandas as _pd
        _master_csv = os.path.join(os.path.dirname(__file__), "nhl_master.csv")
        _skating_csv = os.path.join(os.path.dirname(__file__), "nhl_skating.csv")
        if os.path.exists(_skating_csv) and os.path.exists(_master_csv):
            _mdf = _pd.read_csv(_master_csv)
            _id_to_name = {}
            for _, _row in _mdf.iterrows():
                pid = str(_row.get("playerID", "")).strip()
                first = str(_row.get("firstName", "")).strip()
                last = str(_row.get("lastName", "")).strip()
                if pid and first and last:
                    _id_to_name[pid] = sanitize_name(f"{first} {last}")
            _sdf = _pd.read_csv(_skating_csv)
            csv_added = 0
            for _, _row in _sdf.iterrows():
                try:
                    pid = str(_row.get("playerID", "")).strip()
                    name = _normalise_player_name(_id_to_name.get(pid, ""), NHL_PLAYER_ALIASES)
                    if not name: continue
                    abbr = normalise_nhl_team(str(_row.get("tmID", "")).strip().upper())
                    gp = int(_row.get("GP", 0) or 0)
                    if not abbr or abbr not in NHL_TEAMS or gp < 1: continue
                    if name not in players:
                        players[name] = {"name": name, "teams": [], "games_by_team": {},
                                         "headshot": "", "position": "", "jersey": ""}
                        csv_added += 1
                    if abbr not in players[name]["teams"]:
                        players[name]["teams"].append(abbr)
                    # Only add CSV data if we don't already have API data for this team
                    if players[name]["games_by_team"].get(abbr, 0) == 0:
                        players[name]["games_by_team"][abbr] = gp
                except Exception: continue
            print(f"    NHL CSV fallback: {csv_added} new players added, {len(players)} total")
    except Exception as e:
        print(f"    NHL CSV error: {e}")

    # ── STEP 3: ESPN for headshots + jerseys ─────────────────────────────
    ESPN_NHL_API = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl"
    try:
        url = f"{ESPN_NHL_API}/teams"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=15)
        data = _json.loads(resp.read())
        espn_updates = 0
        for sport in data.get("sports", []):
            for league in sport.get("leagues", []):
                for team in league.get("teams", []):
                    team_info = team.get("team", {})
                    abbr = normalise_nhl_team(team_info.get("abbreviation", ""))
                    if abbr not in NHL_TEAMS: continue
                    try:
                        tid = team_info.get("id", "")
                        roster_req = urllib.request.Request(
                            f"{ESPN_NHL_API}/teams/{tid}/roster",
                            headers={"User-Agent": "Mozilla/5.0"})
                        roster_data = _json.loads(urllib.request.urlopen(roster_req, timeout=10).read())
                        for athlete in roster_data.get("athletes", []):
                            for item in (athlete.get("items") or [athlete]):
                                name = sanitize_name(_normalise_player_name(
                                    str(item.get("fullName", "")).strip(), NHL_PLAYER_ALIASES))
                                if not name: continue
                                hs = item.get("headshot", {}).get("href", "")
                                pos = str(item.get("position", {}).get("abbreviation", "")).strip()
                                jersey = str(item.get("jersey", "")).strip()
                                if name in players:
                                    if hs and not players[name]["headshot"]:
                                        players[name]["headshot"] = hs; espn_updates += 1
                                    if jersey and not players[name]["jersey"]:
                                        players[name]["jersey"] = jersey
                                    if pos and not players[name]["position"]:
                                        players[name]["position"] = pos
                                else:
                                    # New player not in API (very rare)
                                    players[name] = {"name": name, "teams": [abbr],
                                                     "games_by_team": {abbr: 82},
                                                     "headshot": hs, "position": pos, "jersey": jersey}
                                    if abbr not in players[name]["teams"]:
                                        players[name]["teams"].append(abbr)
                    except Exception: continue
        print(f"    ESPN headshots/jerseys updated for {espn_updates} players")
    except Exception as e:
        print(f"    ESPN NHL error: {e}")

    _build_nhl_achievements(players)
    for p in players.values(): p.setdefault("achievements", {})
    print(f"  NHL DB built: {len(players)} players")

    # Save to cache
    result = list(players.values())
    save_to_cache('nhl', result)
    return result

print("Building NHL player database...")
NHL_PLAYERS_DB = build_nhl_player_db()
print(f"  NHL DB Built: {len(NHL_PLAYERS_DB)} Players")

NHL_TEAM_INDEX = {}
for _hp in NHL_PLAYERS_DB:
    for _ht in _hp.get("teams",[]): NHL_TEAM_INDEX.setdefault(_ht,[]).append(_hp)
NHL_PLAYER_NAMES_SORTED = sorted(p["name"] for p in NHL_PLAYERS_DB)
_NHL_STAT_SEASONS_CACHE = {}
for p in NHL_PLAYERS_DB:
    for stat_key, team_dict in p.get("achievements",{}).items():
        if isinstance(team_dict,dict):
            for team,count in team_dict.items():
                _NHL_STAT_SEASONS_CACHE[(team,stat_key)] = _NHL_STAT_SEASONS_CACHE.get((team,stat_key),0)+count
# ── RARITY CALCULATIONS ─────────────────────────────────────────────────────
_CROSSOVER_CACHE = {}
def _crossover_total(team_a, team_b):
    key = (min(team_a,team_b),max(team_a,team_b))
    if key not in _CROSSOVER_CACHE:
        _CROSSOVER_CACHE[key] = sum(p["weeks_by_team"].get(team_a,0)+p["weeks_by_team"].get(team_b,0) for p in PLAYERS_DB if team_a in p.get("teams",[]) and team_b in p.get("teams",[]))
    return _CROSSOVER_CACHE[key]

def calc_rarity(player, team_a, team_b):
    import math
    if team_a.startswith("STAT:"):
        stat_key = team_a.split(":",1)[1]
        p_seasons = player.get("achievements",{}).get(stat_key,{}).get(team_b,0)
        total_seasons = _NFL_STAT_SEASONS_CACHE.get((team_b,stat_key),1)
        if total_seasons <= 0: return 0.5
        return round(max(0.01,min(1.0,p_seasons/total_seasons)),4)
    weeks_a,weeks_b = player["weeks_by_team"].get(team_a,0),player["weeks_by_team"].get(team_b,0)
    total = _crossover_total(team_a,team_b)
    if total <= 0: return 0.5
    n = sum(1 for p in PLAYERS_DB if team_a in p.get("teams",[]) and team_b in p.get("teams",[]))
    if n <= 1: return 0.5
    proportion = (weeks_a + weeks_b) / total
    return round(max(0.01, min(1.0, math.tanh(proportion * n * 0.8))), 4)

_MLB_CROSSOVER_CACHE = {}
def _mlb_crossover_total(team_a, team_b):
    key = (min(team_a,team_b),max(team_a,team_b))
    if key not in _MLB_CROSSOVER_CACHE:
        _MLB_CROSSOVER_CACHE[key] = sum(p["games_by_team"].get(team_a,0)+p["games_by_team"].get(team_b,0) for p in MLB_PLAYERS_DB if team_a in p.get("teams",[]) and team_b in p.get("teams",[]))
    return _MLB_CROSSOVER_CACHE[key]

def calc_mlb_rarity(player, team_a, team_b):
    import math
    if team_a.startswith("STAT:"):
        stat_key = team_a.split(":",1)[1]
        p_seasons = player.get("achievements",{}).get(stat_key,{}).get(team_b,0)
        total_seasons = _MLB_STAT_SEASONS_CACHE.get((team_b,stat_key),1)
        if total_seasons <= 0: return 0.5
        return round(max(0.01,min(1.0,p_seasons/total_seasons)),4)
    games_a,games_b = player["games_by_team"].get(team_a,0),player["games_by_team"].get(team_b,0)
    total = _mlb_crossover_total(team_a,team_b)
    if total <= 0: return 0.5
    n = sum(1 for p in MLB_PLAYERS_DB if team_a in p.get("teams",[]) and team_b in p.get("teams",[]))
    if n <= 1: return 0.5
    proportion = (games_a + games_b) / total
    return round(max(0.01, min(1.0, math.tanh(proportion * n * 0.8))), 4)

_NBA_CROSSOVER_CACHE = {}
def _nba_crossover_total(team_a, team_b):
    key = (min(team_a,team_b),max(team_a,team_b))
    if key not in _NBA_CROSSOVER_CACHE:
        _NBA_CROSSOVER_CACHE[key] = sum(p["games_by_team"].get(team_a,0)+p["games_by_team"].get(team_b,0) for p in NBA_PLAYERS_DB if team_a in p.get("teams",[]) and team_b in p.get("teams",[]))
    return _NBA_CROSSOVER_CACHE[key]

def calc_nba_rarity(player, team_a, team_b):
    import math
    if team_a.startswith("STAT:"):
        stat_key = team_a.split(":",1)[1]
        p_seasons = player.get("achievements",{}).get(stat_key,{}).get(team_b,0)
        total_seasons = _NBA_STAT_SEASONS_CACHE.get((team_b,stat_key),1)
        if total_seasons <= 0: return 0.5
        return round(max(0.01,min(1.0,p_seasons/total_seasons)),4)
    if team_a == team_b:
        g = player["games_by_team"].get(team_a, 0)
        total = sum(p["games_by_team"].get(team_a,0) for p in NBA_PLAYERS_DB if team_a in p.get("teams",[]))
        if total <= 0: return 0.5
        n = sum(1 for p in NBA_PLAYERS_DB if team_a in p.get("teams",[]))
        if n <= 1: return 0.5
        return round(max(0.01, min(1.0, math.tanh((g/total)*n*0.8))), 4)
    g_a,g_b = player["games_by_team"].get(team_a,0),player["games_by_team"].get(team_b,0)
    total = _nba_crossover_total(team_a,team_b)
    if total <= 0: return 0.5
    n = sum(1 for p in NBA_PLAYERS_DB if team_a in p.get("teams",[]) and team_b in p.get("teams",[]))
    if n <= 1: return 0.5
    proportion = (g_a + g_b) / total
    return round(max(0.01, min(1.0, math.tanh(proportion * n * 0.8))), 4)

_NHL_CROSSOVER_CACHE = {}
def _nhl_crossover_total(team_a, team_b):
    key = (min(team_a,team_b),max(team_a,team_b))
    if key not in _NHL_CROSSOVER_CACHE:
        _NHL_CROSSOVER_CACHE[key] = sum(p["games_by_team"].get(team_a,0)+p["games_by_team"].get(team_b,0) for p in NHL_PLAYERS_DB if team_a in p.get("teams",[]) and team_b in p.get("teams",[]))
    return _NHL_CROSSOVER_CACHE[key]

def calc_nhl_rarity(player, team_a, team_b):
    import math
    if team_a.startswith("STAT:"):
        stat_key = team_a.split(":",1)[1]
        p_seasons = player.get("achievements",{}).get(stat_key,{}).get(team_b,0)
        total_seasons = _NHL_STAT_SEASONS_CACHE.get((team_b,stat_key),1)
        if total_seasons <= 0: return 0.5
        return round(max(0.01,min(1.0,p_seasons/total_seasons)),4)
    g_a,g_b = player["games_by_team"].get(team_a,0),player["games_by_team"].get(team_b,0)
    total = _nhl_crossover_total(team_a,team_b)
    if total <= 0: return 0.5
    n = sum(1 for p in NHL_PLAYERS_DB if team_a in p.get("teams",[]) and team_b in p.get("teams",[]))
    if n <= 1: return 0.5
    proportion = (g_a + g_b) / total
    return round(max(0.01, min(1.0, math.tanh(proportion * n * 0.8))), 4)

# ── BOARD GENERATION ────────────────────────────────────────────────────────
WIN_LINES = [(0,1,2),(3,4,5),(6,7,8),(0,3,6),(1,4,7),(2,5,8),(0,4,8),(2,4,6)]

def pick_teams_with_shared_players(pool, index, needed=3, attempts=100):
    for _ in range(attempts):
        sample = random.sample(pool, needed)
        valid = all(any(b in p.get("teams",[]) for p in index.get(a,[])) for i,a in enumerate(sample) for j,b in enumerate(sample) if i != j)
        if valid: return sample
    return random.sample(pool, needed)

def _pick_nfl_stat_category():
    cats = list(NFL_STAT_CATEGORIES); random.shuffle(cats)
    for cat in cats:
        if len([p for p in PLAYERS_DB if cat["key"] in p.get("achievements",{})]) >= 0: return cat
    return None

def _stat_row_has_valid_cells(stat_key, cols, db):
    for team_b in cols:
        if not any(team_b in p.get("teams",[]) and team_b in p.get("achievements",{}).get(stat_key,[]) for p in db): return False
    return True

def new_board():
    rows = pick_teams_with_shared_players(NFL_TEAMS, TEAM_INDEX)
    cols = pick_teams_with_shared_players(NFL_TEAMS, TEAM_INDEX)
    att = 0
    while set(rows) & set(cols) and att < 50:
        cols = pick_teams_with_shared_players(NFL_TEAMS, TEAM_INDEX); att += 1
    stat_cat = _pick_nfl_stat_category(); stat_meta = None
    if stat_cat:
        for _ in range(20):
            if _stat_row_has_valid_cells(stat_cat["key"],cols,PLAYERS_DB):
                stat_meta = {"key":stat_cat["key"],"label":stat_cat["label"],"desc":stat_cat["desc"]}
                rows[2] = f"STAT:{stat_cat['key']}"; break
            cols = pick_teams_with_shared_players(NFL_TEAMS, TEAM_INDEX)
    return rows, cols, stat_meta

def mlb_new_board():
    rows = pick_teams_with_shared_players(MLB_TEAMS, MLB_TEAM_INDEX)
    cols = pick_teams_with_shared_players(MLB_TEAMS, MLB_TEAM_INDEX)
    att = 0
    while set(rows) & set(cols) and att < 50:
        cols = pick_teams_with_shared_players(MLB_TEAMS, MLB_TEAM_INDEX); att += 1
    stat_cat = None
    cats = list(MLB_STAT_CATEGORIES); random.shuffle(cats)
    for cat in cats:
        if len([p for p in MLB_PLAYERS_DB if cat["key"] in p.get("achievements",{})]) >= 0:
            stat_cat = cat; break
    stat_meta = None
    if stat_cat:
        for _ in range(20):
            if _stat_row_has_valid_cells(stat_cat["key"],cols,MLB_PLAYERS_DB):
                stat_meta = {"key":stat_cat["key"],"label":stat_cat["label"],"desc":stat_cat["desc"]}
                rows[2] = f"STAT:{stat_cat['key']}"; break
            cols = pick_teams_with_shared_players(MLB_TEAMS, MLB_TEAM_INDEX)
    return rows, cols, stat_meta


def nba_new_board():
    rows = pick_teams_with_shared_players(NBA_TEAMS, NBA_TEAM_INDEX)
    cols = pick_teams_with_shared_players(NBA_TEAMS, NBA_TEAM_INDEX)
    att = 0
    while set(rows) & set(cols) and att < 50:
        cols = pick_teams_with_shared_players(NBA_TEAMS, NBA_TEAM_INDEX); att += 1
    stat_cat = None
    cats = list(NBA_STAT_CATEGORIES); random.shuffle(cats)
    for cat in cats:
        if len([p for p in NBA_PLAYERS_DB if cat["key"] in p.get("achievements",{})]) > 0:
            stat_cat = cat; break
    stat_meta = None
    if stat_cat:
        for _ in range(20):
            if _stat_row_has_valid_cells(stat_cat["key"],cols,NBA_PLAYERS_DB):
                stat_meta = {"key":stat_cat["key"],"label":stat_cat["label"],"desc":stat_cat["desc"]}
                rows[2] = f"STAT:{stat_cat['key']}"; break
            cols = pick_teams_with_shared_players(NBA_TEAMS, NBA_TEAM_INDEX)
    return rows, cols, stat_meta


# ── WIN & STATE LOGIC ────────────────────────────────────────────────────────
def _get_active_lines(board, uid):
    return [line for line in WIN_LINES if all(board.get(str(i),{}).get("owner")==uid for i in line)]

def _avg_rarity_of_lines(board, lines):
    if not lines: return 1.0
    total,count = 0.0,0
    for line in lines:
        for i in line:
            cell = board.get(str(i))
            if cell and "rarity" in cell: total+=cell["rarity"]; count+=1
    return (total/count) if count > 0 else 1.0

def _resolve_win(s, phrases):
    if s["game_over"]: return
    uid1,uid2,board = s["players"][1]["user_id"],s["players"][2]["user_id"],s["board"]
    lines_p1,lines_p2 = _get_active_lines(board,uid1),_get_active_lines(board,uid2)
    if lines_p1 and lines_p2:
        dtt = s.get("double_ttt")
        if dtt is None:
            s["double_ttt"] = {"turns_held": 0, "turns_remaining": 5}
        else:
            dtt["turns_held"] += 1
            dtt["turns_remaining"] = max(0, 5 - dtt["turns_held"])
            if dtt["turns_held"] >= 5:
                r1,r2 = _avg_rarity_of_lines(board,lines_p1),_avg_rarity_of_lines(board,lines_p2)
                winner_turn = 1 if r1 <= r2 else 2
                wname = s["players"][winner_turn]["username"]
                wr = round(r1*100,1) if winner_turn==1 else round(r2*100,1)
                lr = round(r2*100,1) if winner_turn==1 else round(r1*100,1)
                s["game_over"]=True; s["winner"]=winner_turn
                s["win_reason"]=f"Sudden death! {wname} wins by lower rarity — {wr}% vs {lr}%"
                _flush_stats(s,winner_turn); return
        s["hold_line"] = None
        return
    else:
        s["double_ttt"] = None
    three_owner = uid1 if lines_p1 else (uid2 if lines_p2 else None)
    if three_owner:
        owner_turn = next((t for t,sl in s["players"].items() if sl["user_id"]==three_owner),None)
        if s["hold_line"] and s["hold_line"]["owner"]==three_owner:
            s["hold_line"]["turns_held"] += 1
        else:
            s["hold_line"] = {"owner":three_owner,"owner_turn":owner_turn,"turns_held":0}
        if s["hold_line"]["turns_held"] >= WIN_HOLD_TURNS - 1:
            owner_turn = s["hold_line"].get("owner_turn") or next(
                (t for t, sl in s["players"].items() if sl["user_id"] == three_owner), None)
            if owner_turn is None: return
            wname = s["players"][owner_turn]["username"]
            s["game_over"] = True;
            s["winner"] = owner_turn
            s["win_reason"] = random.choice(phrases).format(winner=wname) + f" (held for {WIN_HOLD_TURNS} turns)"
            _flush_stats(s, owner_turn);
            return
    else:
        s["hold_line"] = None
    _check_alternate_win(s,phrases)

def count_squares(board, pid): return sum(1 for c in board.values() if c and c.get("owner")==pid)
def total_rarity(board, pid): return sum(c["rarity"] for c in board.values() if c and c.get("owner")==pid)

def _check_alternate_win(s, phrases):
    if s["game_over"] or s["miss_streak"] < MAX_MISS_STREAK: return
    uid1,uid2 = s["players"][1]["user_id"],s["players"][2]["user_id"]
    p1,p2 = count_squares(s["board"],uid1),count_squares(s["board"],uid2)
    s["game_over"] = True
    if p1 != p2:
        winner_turn = 1 if p1 > p2 else 2
        s["winner"]=winner_turn
        s["win_reason"]=random.choice(phrases).format(winner=s["players"][winner_turn]["username"])+f" ({max(p1,p2)} vs {min(p1,p2)} squares)"
    else:
        r1,r2 = total_rarity(s["board"],uid1),total_rarity(s["board"],uid2)
        if abs(r1-r2) < 1e-6:
            s["winner"]=0; s["win_reason"]=f"It is a draw! {s['players'][1]['username']} and {s['players'][2]['username']} are perfectly matched."
            _flush_stats(s,None); return
        winner_turn = 1 if r1 <= r2 else 2
        wname=s["players"][winner_turn]["username"]; wr=round(r1*100,1) if winner_turn==1 else round(r2*100,1); lr=round(r2*100,1) if winner_turn==1 else round(r1*100,1)
        s["winner"]=winner_turn; s["win_reason"]=f"Rarity tiebreak! {wname} wins — {wr}% vs {lr}%"

def _flush_stats(s, winner_turn=None):
    if s.get("_stats_flushed"): return
    s["_stats_flushed"] = True
    is_draw = winner_turn == 0
    for turn,slot in s["players"].items():
        if slot.get("is_guest") or slot.get("is_bot"): continue
        if is_draw:
            if slot["session_total"] > 0: update_lifetime_stats(slot["user_id"],slot["session_correct"],slot["session_total"],draw=True)
        else:
            won = (turn==winner_turn) if winner_turn is not None else None
            if slot["session_total"] > 0 or won is not None: update_lifetime_stats(slot["user_id"],slot["session_correct"],slot["session_total"],won=won)

def _find_best_hint(s, db, calc_fn):
    candidates = []
    for cell_idx in range(9):
        ri,ci = cell_idx//3,cell_idx%3
        team_a,team_b = s["rows"][ri],s["cols"][ci]
        existing = s["board"].get(str(cell_idx))
        for p in db:
            if p["name"] in s["used_players"] or p.get("position","") in EXCLUDED_POSITIONS: continue
            if team_a.startswith("STAT:"):
                stat_key = team_a.split(":",1)[1]
                if team_b not in p.get("teams",[]) or team_b not in p.get("achievements",{}).get(stat_key,[]): continue
                r = calc_fn(p,team_b,team_b)
            else:
                if team_a not in p.get("teams",[]) or team_b not in p.get("teams",[]): continue
                r = calc_fn(p,team_a,team_b)
            if existing and r >= existing["rarity"]: continue
            candidates.append((r,p,cell_idx))
    if not candidates: return None,None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1],candidates[0][2]

# ── PLAYER SLOTS & STATE ─────────────────────────────────────────────────────
def make_player_slot(user, sport="nfl"):
    is_guest = str(user.get("id","")).startswith("guest_")
    is_bot = str(user.get("id","")).startswith("bot_")
    fresh = is_guest or is_bot
    mascot_key = {"nfl":"nfl_mascot","mlb":"mlb_mascot","nba":"nba_mascot","nhl":"nhl_mascot"}.get(sport,"nfl_mascot")
    default_mascot = {"nfl":"KC","mlb":"NYY","nba":"LAL","nhl":"BOS"}.get(sport,"KC")
    logos = {"nfl":TEAM_LOGOS,"mlb":MLB_LOGOS,"nba":NBA_LOGOS,"nhl":NHL_LOGOS}.get(sport,TEAM_LOGOS)
    mascot = user.get(mascot_key, default_mascot)
    logo = logos.get(mascot,"")
    return {
        "user_id":user["id"],"username":user["username"],"mascot":mascot,"mascot_logo":logo,
        "hints_remaining":HINTS_PER_PLAYER,"session_correct":0,"session_total":0,
        "lifetime_correct":0 if fresh else user.get("lifetime_correct",0),
        "lifetime_total":0 if fresh else user.get("lifetime_total",0),
        "wins":0 if fresh else user.get("wins",0),
        "losses":0 if fresh else user.get("losses",0),
        "draws":0 if fresh else user.get("draws",0),
        "win_streak":0 if fresh else user.get("win_streak",0),
        "best_streak":0 if fresh else user.get("best_streak",0),
        "is_guest":is_guest,"is_bot":is_bot,
        "bot_difficulty":user.get("bot_difficulty",""),
    }

def _base_state(rows, cols, stat_meta, p1_user, p2_user, sport, data_years):
    return {"rows":rows,"cols":cols,"stat_category":stat_meta,"board":{},"turn":1,"used_players":set(),
            "miss_streak":0,"hold_line":None,"double_ttt":None,"game_over":False,"winner":None,
            "win_reason":None,"turn_number":0,"data_years":data_years,"_stats_flushed":False,
            "players":{1:make_player_slot(p1_user,sport),2:make_player_slot(p2_user,sport)}}

def empty_state(p1, p2):
    rows,cols,stat_meta = new_board()
    return _base_state(rows,cols,stat_meta,p1,p2,"nfl",f"{NFL_START_YEAR}–2026")

def mlb_empty_state(p1, p2):
    rows,cols,stat_meta = mlb_new_board()
    return _base_state(rows,cols,stat_meta,p1,p2,"mlb",f"{MLB_START_YEAR}–2026")

def nba_empty_state(p1, p2):
    rows,cols,stat_meta = nba_new_board()
    return _base_state(rows,cols,stat_meta,p1,p2,"nba",f"{NBA_START_YEAR}–2026")

def nhl_empty_state(p1, p2):
    rows,cols,stat_meta = nhl_new_board()
    return _base_state(rows,cols,stat_meta,p1,p2,"nhl",f"{NHL_START_YEAR}–2026")

STATE = None
MLB_STATE = None
NBA_STATE = None
NHL_STATE = None

# ── SERIALIZE ─────────────────────────────────────────────────────────────────
def _slot_json(p, board):
    lt,lc,st,sc = p["lifetime_total"],p["lifetime_correct"],p["session_total"],p["session_correct"]
    sq = count_squares(board,p["user_id"])
    wins,losses = int(p.get("wins",0) or 0),int(p.get("losses",0) or 0)
    return {"username":p["username"],"mascot":p["mascot"],"mascot_logo":p["mascot_logo"],
            "hints_remaining":p["hints_remaining"],"session_correct":sc,"session_total":st,
            "session_pct":round(sc/st*100,1) if st else 0.0,"lifetime_correct":lc,"lifetime_total":lt,
            "lifetime_pct":round(lc/lt*100,1) if lt else 0.0,"wins":wins,"losses":losses,
            "draws":int(p.get("draws",0) or 0),"win_streak":int(p.get("win_streak",0) or 0),
            "best_streak":int(p.get("best_streak",0) or 0),
            "win_pct":round(wins/(wins+losses)*100,1) if (wins+losses)>0 else 0.0,
            "squares":sq,"rarity_total":round(total_rarity(board,p["user_id"]),3),
            "is_bot":p.get("is_bot",False),"is_guest":p.get("is_guest",False),
            "bot_difficulty":p.get("bot_difficulty","")}

def _serialise(s, team_names_map, team_logos_map, team_mascots_map, data_years_default):
    def _row_name(t):
        if t.startswith("STAT:"): sc=s.get("stat_category"); return sc["label"] if sc else t
        return team_names_map.get(t,t)
    return {"rows":s["rows"],"cols":s["cols"],
            "row_names":[_row_name(t) for t in s["rows"]],
            "col_names":[team_names_map.get(t,t) for t in s["cols"]],
            "row_logos":[team_logos_map.get(t,"") for t in s["rows"]],
            "col_logos":[team_logos_map.get(t,"") for t in s["cols"]],
            "row_mascots":[team_mascots_map.get(t,"") for t in s["rows"]],
            "col_mascots":[team_mascots_map.get(t,"") for t in s["cols"]],
            "stat_category":s.get("stat_category"),"board":dict(s["board"]),
            "turn":s["turn"],"miss_streak":s["miss_streak"],"hold_line":s["hold_line"],
            "double_ttt":s.get("double_ttt"),"game_over":s["game_over"],
            "winner":s["winner"],"win_reason":s["win_reason"],"turn_number":s["turn_number"],
            "data_years":s.get("data_years",data_years_default),
            "player1":_slot_json(s["players"][1],s["board"]),
            "player2":_slot_json(s["players"][2],s["board"])}

def serialise_state(s): return _serialise(s,TEAM_NAMES,TEAM_LOGOS,TEAM_MASCOTS,f"{NFL_START_YEAR}–2026")
def mlb_serialise_state(s): return _serialise(s,MLB_TEAM_NAMES,MLB_LOGOS,MLB_TEAM_MASCOTS,f"{MLB_START_YEAR}–2026")
def nba_serialise_state(s): return _serialise(s,NBA_TEAM_NAMES,NBA_LOGOS,NBA_TEAM_MASCOTS,f"{NBA_START_YEAR}–2026")
def nhl_serialise_state(s): return _serialise(s,NHL_TEAM_NAMES,NHL_LOGOS,NHL_TEAM_MASCOTS,f"{NHL_START_YEAR}–2026")

# ── HELPERS ────────────────────────────────────────────────────────────────────
def _switch_turn(s): s["turn"] = 2 if s["turn"]==1 else 1
def _cell_entry(owner_uid, owner_turn, player, rarity):
    return {"owner":owner_uid,"owner_turn":owner_turn,"player_name":player["name"],"rarity":rarity,"headshot":player.get("headshot","")}

def _resolve_user(player_data):
    if not player_data: return None
    uid = player_data.get("id","")
    if str(uid).startswith("guest_") or str(uid).startswith("bot_"):
        return {"id":uid,"username":player_data.get("username","Guest"),
                "nfl_mascot":player_data.get("nfl_mascot","KC"),"mlb_mascot":player_data.get("mlb_mascot","NYY"),
                "nba_mascot":player_data.get("nba_mascot","LAL"),"nhl_mascot":player_data.get("nhl_mascot","BOS"),
                "lifetime_correct":0,"lifetime_total":0,"wins":0,"losses":0,"draws":0,"win_streak":0,"best_streak":0,
                "bot_difficulty":player_data.get("bot_difficulty","medium")}
    return get_user(player_data.get("username",""))
# ── PHRASES ──────────────────────────────────────────────────────────────────
CORRECT_PHRASES = ["First down!","Great connection!","That's a completion!","Right on target!"]
STEAL_PHRASES   = ["Intercepted! Pick six!","Stripped and returned!","Turnover on the field!"]
MISS_PHRASES    = ["Incomplete pass.","Flag on the play.","False start.","Delay of game."]
WIN_PHRASES     = ["Final whistle — {winner} wins!","Game over! {winner} takes it!","Clock hits zero — {winner} wins!"]

MLB_CORRECT_PHRASES = ["Base hit!","Great swing!","That's a hit!","Right down the line!"]
MLB_STEAL_PHRASES   = ["Stolen base! Safe!","What a steal!","He took that base!"]
MLB_MISS_PHRASES    = ["Strikeout.","Foul ball.","Swung and missed.","Called strike three."]
MLB_WIN_PHRASES     = ["Final out — {winner} wins!","Game over! {winner} takes the pennant!"]

NBA_CORRECT_PHRASES = ["Bucket!","Nothing but net!","That's a bucket!","Two points!","And it counts!"]
NBA_STEAL_PHRASES   = ["And-one steal!","Stripped! Fast break!","Pick-pocketed!","Turnover converted!"]
NBA_MISS_PHRASES    = ["Brick.","Off the rim.","Air ball.","No good.","Rejected!"]
NBA_WIN_PHRASES     = ["Buzzer beater — {winner} wins!","Game over! {winner} takes the championship!","Final horn — {winner} wins!"]

NHL_CORRECT_PHRASES = ["Goal!","Score!","Top shelf!","Bar down!","He scores!"]
NHL_STEAL_PHRASES   = ["Icing called off — steal!","Puck stolen!","Cleared and taken!","Counter-attack goal!"]
NHL_MISS_PHRASES    = ["Wide right.","Saved by the goalie.","Hit the post.","Iced.","Off the iron."]
NHL_WIN_PHRASES     = ["Final buzzer — {winner} wins!","Game over! {winner} lifts the cup!","Three stars: {winner}!"]

def _make_miss(s, result, message, serialise_fn, win_phrases):
    slot = s["players"][s["turn"]]; slot["session_total"]+=1; s["miss_streak"]+=1; s["turn_number"]+=1
    _resolve_win(s,win_phrases)
    if not s["game_over"]: _check_alternate_win(s,win_phrases)
    if s["game_over"]: _flush_stats(s,s.get("winner"))
    _switch_turn(s)
    return jsonify({"result":result,"message":message,"miss_streak":s["miss_streak"],"game_over":s["game_over"],"winner":s["winner"],"win_reason":s["win_reason"],"state":serialise_fn(s)})

def _do_guess(s, db, calc_fn, serialise_fn, team_names_map, aliases, correct_phrases, steal_phrases, miss_phrases, wrong_team_phrases, win_phrases):
    if s["game_over"]: return jsonify({"result":"game_over","message":"Game is already over."}),400
    data = request.json or {}
    cell = int(data.get("cell",-1)); player_name = sanitize_name(str(data.get("player","")).strip())
    if not (0 <= cell <= 8): return jsonify({"result":"error","message":"Invalid cell."}),400
    if not player_name: return jsonify({"result":"error","message":"No player name given."}),400
    canonical = _normalise_player_name(player_name, aliases)

    # 1. Exact match
    player = (
        next((p for p in db if p["name"].lower()==canonical.lower()), None) or
        next((p for p in db if p["name"].lower()==player_name.lower()), None)
    )
    # 2. Accent-stripped match (French Canadian, Scandinavian)
    if not player:
        def _strip(s):
            return ''.join(c for c in unicodedata.normalize('NFD',s) if unicodedata.category(c)!='Mn').lower()
        cs = _strip(canonical)
        player = next((p for p in db if _strip(p["name"])==cs), None)
    # 3. Fuzzy match (transliteration variants, minor typos)
    if not player:
        try:
            from rapidfuzz import process, fuzz
            best = process.extractOne(canonical, [p["name"] for p in db], scorer=fuzz.token_sort_ratio, score_cutoff=92)
            if best: player = next((p for p in db if p["name"]==best[0]), None)
        except ImportError: pass

    if not player: return _make_miss(s,"not_found",random.choice(miss_phrases)+f" '{player_name}' is not in our system.",serialise_fn,win_phrases)
    ri,ci = cell//3,cell%3; team_a,team_b = s["rows"][ri],s["cols"][ci]
    current_turn,slot,uid = s["turn"],s["players"][s["turn"]],s["players"][s["turn"]]["user_id"]
    if player_name.lower() in {n.lower() for n in s["used_players"]}:
        return _make_miss(s,"already_used",f"{player['name']} has already been used this game.",serialise_fn,win_phrases)
    if team_a.startswith("STAT:"):
        stat_key = team_a.split(":",1)[1]
        if team_b not in player.get("teams",[]): return _make_miss(s,"wrong_team",f"{player['name']} didn't play for {team_names_map.get(team_b,team_b)}.",serialise_fn,win_phrases)
        if team_b not in player.get("achievements",{}).get(stat_key,[]):
            sc = s.get("stat_category",{}); return _make_miss(s,"wrong_team",f"{player['name']} didn't achieve {sc.get('label',stat_key)} with {team_names_map.get(team_b,team_b)}.",serialise_fn,win_phrases)
    elif team_a not in player.get("teams",[]) or team_b not in player.get("teams",[]):
        return _make_miss(s,"wrong_team",f"{player['name']} didn't play for both {team_names_map.get(team_a,team_a)} and {team_names_map.get(team_b,team_b)}.",serialise_fn,win_phrases)
    rarity = calc_fn(player,team_b,team_b) if team_a.startswith("STAT:") else calc_fn(player,team_a,team_b)
    cell_key = str(cell); existing = s["board"].get(cell_key); slot["session_total"]+=1
    if existing and existing["owner"]!=uid:
        if rarity < existing["rarity"]:
            s["board"][cell_key]=_cell_entry(uid,current_turn,player,rarity); s["used_players"].add(player["name"])
            slot["session_correct"]+=1; s["miss_streak"]=0; result_label,phrase="steal",random.choice(steal_phrases)
        else:
            _switch_turn(s)
            return jsonify({"result":"steal_failed","message":f"{player['name']} ({rarity*100:.1f}%) couldn't beat {existing['player_name']} ({existing['rarity']*100:.1f}%).","miss_streak":s["miss_streak"],"game_over":False,"winner":None,"win_reason":None,"state":serialise_fn(s)})
    elif existing and existing["owner"]==uid:
        if rarity < existing["rarity"]:
            s["board"][cell_key]=_cell_entry(uid,current_turn,player,rarity); s["used_players"].add(player["name"])
        slot["session_correct"]+=1; s["miss_streak"]=0; result_label,phrase="improved","Upgraded!"
    else:
        s["board"][cell_key]=_cell_entry(uid,current_turn,player,rarity); s["used_players"].add(player["name"])
        slot["session_correct"]+=1; s["miss_streak"]=0; result_label,phrase="correct",random.choice(correct_phrases)
    s["turn_number"]+=1; _resolve_win(s,win_phrases); _switch_turn(s)
    return jsonify({"result":result_label,"message":f"{phrase} {player['name']} — Rarity: {rarity*100:.1f}%","rarity":rarity,"rarity_pct":round(rarity*100,1),"cell":cell,"owner":uid,"owner_turn":current_turn,"player_name":player["name"],"headshot":player.get("headshot",""),"miss_streak":s["miss_streak"],"hold_line":s["hold_line"],"game_over":s["game_over"],"winner":s["winner"],"win_reason":s["win_reason"],"state":serialise_fn(s)})

def _start_game_common(new_state_fn, serialise_fn):
    global STATE, MLB_STATE, NBA_STATE, NHL_STATE
    data = request.json or {}
    p1,p2 = data.get("player1"),data.get("player2")
    if not p1 or not p2: return jsonify({"error":"Both player accounts required."}),400
    u1,u2 = _resolve_user(p1),_resolve_user(p2)
    if not u1 or not u2: return jsonify({"error":"One or both accounts not found."}),404
    real_ids = [x["id"] for x in [u1,u2] if not str(x["id"]).startswith("guest_") and not str(x["id"]).startswith("bot_")]
    if len(real_ids)==2 and real_ids[0]==real_ids[1]: return jsonify({"error":"Both players must be different accounts."}),400
    s = new_state_fn(u1,u2)
    return s, serialise_fn(s)

# ── AUTH ROUTES ────────────────────────────────────────────────────────────────
def _user_json(user, guest=False):
    return {"id":user["id"],"username":user["username"],
            "nfl_mascot":user.get("nfl_mascot",user.get("mascot","KC")),
            "mlb_mascot":user.get("mlb_mascot","NYY"),
            "nba_mascot":user.get("nba_mascot","LAL"),
            "nhl_mascot":user.get("nhl_mascot","BOS"),
            "nfl_mascot_logo":TEAM_LOGOS.get(user.get("nfl_mascot","KC"),""),
            "mlb_mascot_logo":MLB_LOGOS.get(user.get("mlb_mascot","NYY"),""),
            "nba_mascot_logo":NBA_LOGOS.get(user.get("nba_mascot","LAL"),""),
            "nhl_mascot_logo":NHL_LOGOS.get(user.get("nhl_mascot","BOS"),""),
            "lifetime_correct":user.get("lifetime_correct",0),"lifetime_total":user.get("lifetime_total",0),
            "wins":user.get("wins",0),"losses":user.get("losses",0),"draws":user.get("draws",0),
            "win_streak":user.get("win_streak",0),"best_streak":user.get("best_streak",0),"is_guest":guest}

@app.route("/api/auth/register", methods=["POST"])
def register():
    data = request.json or {}
    username,password = str(data.get("username","")).strip(),str(data.get("password","")).strip()
    nfl_mascot = str(data.get("nfl_mascot","KC")).strip().upper()
    mlb_mascot = str(data.get("mlb_mascot","NYY")).strip().upper()
    nba_mascot = str(data.get("nba_mascot","LAL")).strip().upper()
    nhl_mascot = str(data.get("nhl_mascot","BOS")).strip().upper()
    if nfl_mascot not in NFL_TEAMS: nfl_mascot="KC"
    if mlb_mascot not in MLB_TEAMS: mlb_mascot="NYY"
    if nba_mascot not in NBA_TEAMS: nba_mascot="LAL"
    if nhl_mascot not in NHL_TEAMS: nhl_mascot="BOS"
    if not username or not password: return jsonify({"error":"Username and password required."}),400
    if len(username)<2 or len(username)>20: return jsonify({"error":"Username must be 2–20 characters."}),400
    user = create_user(username,password,nfl_mascot,mlb_mascot,nba_mascot,nhl_mascot)
    if not user: return jsonify({"error":"Username already taken."}),409
    return jsonify({"ok":True,"user":_user_json(user)})

@app.route("/api/auth/login", methods=["POST"])
def login():
    data = request.json or {}
    username,password = str(data.get("username","")).strip(),str(data.get("password","")).strip()
    user = get_user(username)
    if not user or user["password_hash"]!=hash_password(password): return jsonify({"error":"Invalid username or password."}),401
    return jsonify({"ok":True,"user":_user_json(user)})

@app.route("/api/auth/guest", methods=["POST"])
def guest_login():
    import random as _random
    guest_id = f"guest_{int(time.time()*1000)%999999}"
    rand_nfl = _random.choice(NFL_TEAMS)
    rand_mlb = _random.choice(MLB_TEAMS)
    rand_nba = _random.choice(NBA_TEAMS)
    rand_nhl = _random.choice(NHL_TEAMS)
    user = {"id":guest_id,"username":f"Guest_{guest_id[-4:]}",
            "nfl_mascot":rand_nfl,"mlb_mascot":rand_mlb,
            "nba_mascot":rand_nba,"nhl_mascot":rand_nhl,
            "lifetime_correct":0,"lifetime_total":0,
            "wins":0,"losses":0,"draws":0,"win_streak":0,"best_streak":0}
    return jsonify({"ok":True,"user":_user_json(user,guest=True)})

@app.route("/api/auth/bot", methods=["POST"])
def create_bot():
    data = request.json or {}
    difficulty = data.get("difficulty", "medium")
    if difficulty not in ("easy","medium","hard"): difficulty = "medium"
    name = data.get("name","") or {"easy":"Rookie Bot","medium":"Pro Bot","hard":"Legend Bot"}[difficulty]
    bot_id = f"bot_{difficulty}_{int(time.time()*1000)%999999}"
    user = {"id":bot_id,"username":name,
            "nfl_mascot":random.choice(NFL_TEAMS),"mlb_mascot":random.choice(MLB_TEAMS),
            "nba_mascot":random.choice(NBA_TEAMS),"nhl_mascot":random.choice(NHL_TEAMS),
            "lifetime_correct":0,"lifetime_total":0,"wins":0,"losses":0,"draws":0,
            "win_streak":0,"best_streak":0,"bot_difficulty":difficulty,"is_bot":True}
    return jsonify({"ok":True,"user":{**_user_json(user),"is_bot":True,"bot_difficulty":difficulty}})

def _bot_pick_move(s, db, calc_fn, difficulty):
    """Bot AI: returns (cell_idx, player) or (None, None) to pass."""
    board = s["board"]
    bot_uid = s["players"][s["turn"]]["user_id"]
    opp_turn = 2 if s["turn"]==1 else 1
    opp_uid = s["players"][opp_turn]["user_id"]

    # Build valid moves: {cell_idx: sorted list of (rarity, player)}
    valid = {}
    for ci in range(9):
        ri, col = ci//3, ci%3
        ta, tb = s["rows"][ri], s["cols"][col]
        cands = []
        for p in db:
            if p["name"] in s["used_players"]: continue
            if p.get("position","") in EXCLUDED_POSITIONS: continue
            if ta.startswith("STAT:"):
                sk = ta.split(":",1)[1]
                if tb not in p.get("teams",[]): continue
                if tb not in p.get("achievements",{}).get(sk,[]): continue
                r = calc_fn(p, tb, tb)
            else:
                if ta not in p.get("teams",[]): continue
                if tb not in p.get("teams",[]): continue
                r = calc_fn(p, ta, tb)
            cands.append((r, p))
        if cands:
            cands.sort(key=lambda x: x[0])
            valid[ci] = cands

    if not valid: return None, None

    WIN_LINES_BOT = [(0,1,2),(3,4,5),(6,7,8),(0,3,6),(1,4,7),(2,5,8),(0,4,8),(2,4,6)]

    if difficulty == "easy":
        ci = random.choice(list(valid.keys()))
        cands = valid[ci]
        # Pick from common (higher rarity = more common)
        cands.sort(key=lambda x: x[0], reverse=True)
        return ci, random.choice(cands[:max(1,len(cands)//2)])[1]

    elif difficulty == "medium":
        # Win if possible
        for line in WIN_LINES_BOT:
            bot_count = sum(1 for i in line if board.get(str(i),{}).get("owner")==bot_uid)
            empty = [i for i in line if not board.get(str(i))]
            if bot_count==2 and len(empty)==1 and empty[0] in valid:
                cands = valid[empty[0]]
                return empty[0], cands[0][1]
        # Block opponent
        for line in WIN_LINES_BOT:
            opp_count = sum(1 for i in line if board.get(str(i),{}).get("owner")==opp_uid)
            empty = [i for i in line if not board.get(str(i))]
            if opp_count==2 and len(empty)==1 and empty[0] in valid:
                cands = valid[empty[0]]
                mid = len(cands)//2
                return empty[0], cands[mid][1]
        ci = random.choice(list(valid.keys()))
        cands = valid[ci]
        return ci, cands[len(cands)//2][1]

    else:  # hard
        # Win if possible
        for line in WIN_LINES_BOT:
            bot_count = sum(1 for i in line if board.get(str(i),{}).get("owner")==bot_uid)
            empty = [i for i in line if not board.get(str(i))]
            if bot_count==2 and len(empty)==1 and empty[0] in valid:
                return empty[0], valid[empty[0]][0][1]
        # Block
        for line in WIN_LINES_BOT:
            opp_count = sum(1 for i in line if board.get(str(i),{}).get("owner")==opp_uid)
            empty = [i for i in line if not board.get(str(i))]
            if opp_count==2 and len(empty)==1 and empty[0] in valid:
                return empty[0], valid[empty[0]][0][1]
        # Steal if rarer player available
        best_steal = None
        for ci, cands in valid.items():
            ex = board.get(str(ci))
            if ex and ex.get("owner")==opp_uid and cands[0][0] < ex["rarity"]:
                if not best_steal or cands[0][0] < best_steal[0]:
                    best_steal = (cands[0][0], cands[0][1], ci)
        if best_steal: return best_steal[2], best_steal[1]
        # Best strategic cell (advances own lines)
        best_ci, best_score = None, -1
        for ci in valid:
            score = sum(1 for l in WIN_LINES_BOT if ci in l and
                        any(board.get(str(i),{}).get("owner")==bot_uid for i in l if i!=ci))
            if score > best_score: best_score=score; best_ci=ci
        if best_ci is None: best_ci = random.choice(list(valid.keys()))
        return best_ci, valid[best_ci][0][1]

def _do_bot_turn(s, db, calc_fn, serialise_fn, win_phrases):
    if s["game_over"]: return jsonify({"error":"Game is over."}),400
    bot_slot = s["players"][s["turn"]]
    if not bot_slot.get("is_bot"): return jsonify({"error":"Not bot's turn."}),400
    difficulty = bot_slot.get("bot_difficulty","medium")
    cell_idx, player = _bot_pick_move(s, db, calc_fn, difficulty)
    if player is None:
        return _do_pass(s, serialise_fn, win_phrases)
    ta, tb = s["rows"][cell_idx//3], s["cols"][cell_idx%3]
    uid = bot_slot["user_id"]; ct = s["turn"]
    rarity = calc_fn(player,tb,tb) if ta.startswith("STAT:") else calc_fn(player,ta,tb)
    ck = str(cell_idx); existing = s["board"].get(ck)
    bot_slot["session_total"] += 1
    if existing and existing["owner"]!=uid:
        if rarity < existing["rarity"]:
            s["board"][ck]=_cell_entry(uid,ct,player,rarity); s["used_players"].add(player["name"])
            bot_slot["session_correct"]+=1; s["miss_streak"]=0
        else:
            _switch_turn(s)
            return jsonify({"result":"bot_steal_failed","message":f"Bot tried to steal but failed.","state":serialise_fn(s)})
    elif existing and existing["owner"]==uid:
        if rarity < existing["rarity"]:
            s["board"][ck]=_cell_entry(uid,ct,player,rarity); s["used_players"].add(player["name"])
        bot_slot["session_correct"]+=1; s["miss_streak"]=0
    else:
        s["board"][ck]=_cell_entry(uid,ct,player,rarity); s["used_players"].add(player["name"])
        bot_slot["session_correct"]+=1; s["miss_streak"]=0
    s["turn_number"]+=1; _resolve_win(s,win_phrases); _switch_turn(s)
    return jsonify({"result":"bot_move","message":f"Bot played {player['name']} — Rarity: {rarity*100:.1f}%",
                    "cell":cell_idx,"player_name":player["name"],"headshot":player.get("headshot",""),
                    "rarity":rarity,"game_over":s["game_over"],"winner":s["winner"],"win_reason":s["win_reason"],
                    "state":serialise_fn(s)})

# ── NFL ROUTES ─────────────────────────────────────────────────────────────────
@app.route("/api/game/start", methods=["POST"])
def start_game():
    global STATE
    result = _start_game_common(empty_state, serialise_state)
    if isinstance(result, tuple) and len(result) == 2 and not isinstance(result[0], dict):
        return result  # error response
    s, json_state = result
    STATE = s; return jsonify(json_state)

@app.route("/api/game")
def get_game():
    if STATE is None: return jsonify({"error":"No active game."}),404
    return jsonify(serialise_state(STATE))

@app.route("/api/guess", methods=["POST"])
def guess():
    if STATE is None: return jsonify({"result":"error","message":"No active game."}),400
    return _do_guess(STATE,PLAYERS_DB,calc_rarity,serialise_state,TEAM_NAMES,NFL_PLAYER_ALIASES,CORRECT_PHRASES,STEAL_PHRASES,MISS_PHRASES,[],WIN_PHRASES)

@app.route("/api/hint", methods=["POST"])
def hint():
    if STATE is None: return jsonify({"error":"No active game."}),400
    return _do_hint(STATE,PLAYERS_DB,calc_rarity,serialise_state,"nfl")

@app.route("/api/pass", methods=["POST"])
def pass_turn():
    if STATE is None: return jsonify({"error":"No active game."}),400
    return _do_pass(STATE,serialise_state,WIN_PHRASES)

@app.route("/api/bot/turn", methods=["POST"])
def nfl_bot_turn():
    if STATE is None: return jsonify({"error":"No active game."}),400
    return _do_bot_turn(STATE,PLAYERS_DB,calc_rarity,serialise_state,WIN_PHRASES)

@app.route("/api/reset", methods=["POST"])
def reset():
    global STATE
    if STATE: _flush_stats(STATE,STATE.get("winner"))
    data = request.json or {}; p1,p2 = data.get("player1"),data.get("player2")
    if p1 and p2:
        u1,u2 = _resolve_user(p1),_resolve_user(p2)
        if u1 and u2: STATE=empty_state(u1,u2); return jsonify({"status":"reset","state":serialise_state(STATE)})
    STATE = None; return jsonify({"status":"cleared"})


# ── MLB ROUTES ─────────────────────────────────────────────────────────────────
@app.route("/api/mlb/game/start", methods=["POST"])
def mlb_start_game():
    global MLB_STATE
    result = _start_game_common(mlb_empty_state, mlb_serialise_state)
    if isinstance(result, tuple) and len(result) == 2 and not isinstance(result[0], dict):
        return result
    s, json_state = result
    MLB_STATE = s; return jsonify(json_state)

@app.route("/api/mlb/game")
def mlb_get_game():
    if MLB_STATE is None: return jsonify({"error":"No active MLB game."}),404
    return jsonify(mlb_serialise_state(MLB_STATE))

@app.route("/api/mlb/guess", methods=["POST"])
def mlb_guess():
    if MLB_STATE is None: return jsonify({"result":"error","message":"No active MLB game."}),400
    return _do_guess(MLB_STATE,MLB_PLAYERS_DB,calc_mlb_rarity,mlb_serialise_state,MLB_TEAM_NAMES,MLB_PLAYER_ALIASES,MLB_CORRECT_PHRASES,MLB_STEAL_PHRASES,MLB_MISS_PHRASES,[],MLB_WIN_PHRASES)

@app.route("/api/mlb/hint", methods=["POST"])
def mlb_hint():
    if MLB_STATE is None: return jsonify({"error":"No active MLB game."}),400
    return _do_hint(MLB_STATE,MLB_PLAYERS_DB,calc_mlb_rarity,mlb_serialise_state,"mlb")

@app.route("/api/mlb/pass", methods=["POST"])
def mlb_pass_turn():
    if MLB_STATE is None: return jsonify({"error":"No active MLB game."}),400
    return _do_pass(MLB_STATE,mlb_serialise_state,MLB_WIN_PHRASES)

@app.route("/api/mlb/bot/turn", methods=["POST"])
def mlb_bot_turn():
    if MLB_STATE is None: return jsonify({"error":"No active MLB game."}),400
    return _do_bot_turn(MLB_STATE,MLB_PLAYERS_DB,calc_mlb_rarity,mlb_serialise_state,MLB_WIN_PHRASES)

@app.route("/api/mlb/reset", methods=["POST"])
def mlb_reset():
    global MLB_STATE
    if MLB_STATE: _flush_stats(MLB_STATE,MLB_STATE.get("winner"))
    data = request.json or {}; p1,p2 = data.get("player1"),data.get("player2")
    if p1 and p2:
        u1,u2 = _resolve_user(p1),_resolve_user(p2)
        if u1 and u2: MLB_STATE=mlb_empty_state(u1,u2); return jsonify({"status":"reset","state":mlb_serialise_state(MLB_STATE)})
    MLB_STATE = None; return jsonify({"status":"cleared"})

# ── NBA ROUTES ─────────────────────────────────────────────────────────────────
@app.route("/api/nba/game/start", methods=["POST"])
def nba_start_game():
    global NBA_STATE
    result = _start_game_common(nba_empty_state, nba_serialise_state)
    if isinstance(result, tuple) and len(result) == 2 and not isinstance(result[0], dict):
        return result
    s, json_state = result
    NBA_STATE = s; return jsonify(json_state)

@app.route("/api/nba/game")
def nba_get_game():
    if NBA_STATE is None: return jsonify({"error":"No active NBA game."}),404
    return jsonify(nba_serialise_state(NBA_STATE))

@app.route("/api/nba/guess", methods=["POST"])
def nba_guess():
    if NBA_STATE is None: return jsonify({"result":"error","message":"No active NBA game."}),400
    return _do_guess(NBA_STATE,NBA_PLAYERS_DB,calc_nba_rarity,nba_serialise_state,NBA_TEAM_NAMES,NBA_PLAYER_ALIASES,NBA_CORRECT_PHRASES,NBA_STEAL_PHRASES,NBA_MISS_PHRASES,[],NBA_WIN_PHRASES)

@app.route("/api/nba/hint", methods=["POST"])
def nba_hint():
    if NBA_STATE is None: return jsonify({"error":"No active NBA game."}),400
    return _do_hint(NBA_STATE,NBA_PLAYERS_DB,calc_nba_rarity,nba_serialise_state,"nba")

@app.route("/api/nba/pass", methods=["POST"])
def nba_pass_turn():
    if NBA_STATE is None: return jsonify({"error":"No active NBA game."}),400
    return _do_pass(NBA_STATE,nba_serialise_state,NBA_WIN_PHRASES)

@app.route("/api/nba/bot/turn", methods=["POST"])
def nba_bot_turn():
    if NBA_STATE is None: return jsonify({"error":"No active NBA game."}),400
    return _do_bot_turn(NBA_STATE,NBA_PLAYERS_DB,calc_nba_rarity,nba_serialise_state,NBA_WIN_PHRASES)

@app.route("/api/nba/reset", methods=["POST"])
def nba_reset():
    global NBA_STATE
    if NBA_STATE: _flush_stats(NBA_STATE,NBA_STATE.get("winner"))
    data = request.json or {}; p1,p2 = data.get("player1"),data.get("player2")
    if p1 and p2:
        u1,u2 = _resolve_user(p1),_resolve_user(p2)
        if u1 and u2: NBA_STATE=nba_empty_state(u1,u2); return jsonify({"status":"reset","state":nba_serialise_state(NBA_STATE)})
    NBA_STATE = None; return jsonify({"status":"cleared"})

# ── NHL ROUTES ─────────────────────────────────────────────────────────────────
@app.route("/api/nhl/game/start", methods=["POST"])
def nhl_start_game():
    global NHL_STATE
    result = _start_game_common(nhl_empty_state, nhl_serialise_state)
    if isinstance(result, tuple) and len(result) == 2 and not isinstance(result[0], dict):
        return result
    s, json_state = result
    NHL_STATE = s; return jsonify(json_state)

@app.route("/api/nhl/game")
def nhl_get_game():
    if NHL_STATE is None: return jsonify({"error":"No active NHL game."}),404
    return jsonify(nhl_serialise_state(NHL_STATE))

@app.route("/api/nhl/guess", methods=["POST"])
def nhl_guess():
    if NHL_STATE is None: return jsonify({"result":"error","message":"No active NHL game."}),400
    return _do_guess(NHL_STATE,NHL_PLAYERS_DB,calc_nhl_rarity,nhl_serialise_state,NHL_TEAM_NAMES,NHL_PLAYER_ALIASES,NHL_CORRECT_PHRASES,NHL_STEAL_PHRASES,NHL_MISS_PHRASES,[],NHL_WIN_PHRASES)

@app.route("/api/nhl/hint", methods=["POST"])
def nhl_hint():
    if NHL_STATE is None: return jsonify({"error":"No active NHL game."}),400
    return _do_hint(NHL_STATE,NHL_PLAYERS_DB,calc_nhl_rarity,nhl_serialise_state,"nhl")

@app.route("/api/nhl/pass", methods=["POST"])
def nhl_pass_turn():
    if NHL_STATE is None: return jsonify({"error":"No active NHL game."}),400
    return _do_pass(NHL_STATE,nhl_serialise_state,NHL_WIN_PHRASES)

@app.route("/api/nhl/bot/turn", methods=["POST"])
def nhl_bot_turn():
    if NHL_STATE is None: return jsonify({"error":"No active NHL game."}),400
    return _do_bot_turn(NHL_STATE,NHL_PLAYERS_DB,calc_nhl_rarity,nhl_serialise_state,NHL_WIN_PHRASES)

@app.route("/api/nhl/reset", methods=["POST"])
def nhl_reset():
    global NHL_STATE
    if NHL_STATE: _flush_stats(NHL_STATE,NHL_STATE.get("winner"))
    data = request.json or {}; p1,p2 = data.get("player1"),data.get("player2")
    if p1 and p2:
        u1,u2 = _resolve_user(p1),_resolve_user(p2)
        if u1 and u2: NHL_STATE=nhl_empty_state(u1,u2); return jsonify({"status":"reset","state":nhl_serialise_state(NHL_STATE)})
    NHL_STATE = None; return jsonify({"status":"cleared"})

# ── SHARED UTILITY ROUTES ──────────────────────────────────────────────────────
@app.route("/api/teams")
def teams():
    return jsonify({t:{"name":TEAM_NAMES[t],"mascot":TEAM_MASCOTS[t],"logo":TEAM_LOGOS.get(t,"")} for t in NFL_TEAMS})

@app.route("/api/mlb/teams")
def mlb_teams():
    return jsonify({t:{"name":MLB_TEAM_NAMES[t],"mascot":MLB_TEAM_MASCOTS[t],"logo":MLB_LOGOS.get(t,"")} for t in MLB_TEAMS})

@app.route("/api/nba/teams")
def nba_teams():
    return jsonify({t:{"name":NBA_TEAM_NAMES[t],"mascot":NBA_TEAM_MASCOTS[t],"logo":NBA_LOGOS.get(t,"")} for t in NBA_TEAMS})

@app.route("/api/nhl/teams")
def nhl_teams():
    return jsonify({t:{"name":NHL_TEAM_NAMES[t],"mascot":NHL_TEAM_MASCOTS[t],"logo":NHL_LOGOS.get(t,"")} for t in NHL_TEAMS})

@app.route("/api/cache/clear", methods=["POST"])
def clear_cache_api():
    """Clear player data cache"""
    data = request.json or {}
    sport = data.get("sport")  # None means all
    if sport and sport not in ['nfl', 'mlb', 'nba', 'nhl']:
        return jsonify({"error": "Invalid sport. Use: nfl, mlb, nba, nhl"}), 400
    clear_cache(sport)
    return jsonify({"status": "ok", "message": f"Cache cleared for {sport or 'all sports'}"})

@app.route("/api/search")
def search_players():
    q = sanitize_name(request.args.get("q","")).strip().lower()
    sport = request.args.get("sport","nfl")
    limit = min(int(request.args.get("limit",10)),25)
    if len(q) < 2: return jsonify([])
    names_map = {"nfl":PLAYER_NAMES_SORTED,"mlb":MLB_PLAYER_NAMES_SORTED,"nba":NBA_PLAYER_NAMES_SORTED,"nhl":NHL_PLAYER_NAMES_SORTED}
    names = names_map.get(sport, PLAYER_NAMES_SORTED)
    def _strip(s):
        return ''.join(c for c in unicodedata.normalize('NFD',s) if unicodedata.category(c)!='Mn').lower()
    qs = _strip(q)
    starts    = [n for n in names if _strip(n).startswith(qs)]
    last_name = [n for n in names if not _strip(n).startswith(qs) and _strip(n.split()[-1]).startswith(qs)]
    contains  = [n for n in names if qs in _strip(n) and not _strip(n).startswith(qs) and not _strip(n.split()[-1]).startswith(qs)]
    return jsonify((starts+last_name+contains)[:limit])

@app.route("/")
def home():
    # Try multiple locations for index.html
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
@app.route("/api/player_counts")
def player_counts():
    return jsonify({
        "nfl": len(PLAYERS_DB),
        "mlb": len(MLB_PLAYERS_DB),
        "nba": len(NBA_PLAYERS_DB),
        "nhl": len(NHL_PLAYERS_DB),
    })

if __name__ == "__main__":
    app.run(debug=True)