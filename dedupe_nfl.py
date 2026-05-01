#!/usr/bin/env python3
"""
Standalone deduplication for nfl_players.json.
Merges entries that are the same player split by position label
(e.g. 'Deion Sanders (CB)' and 'Deion Sanders (DB)' become one).

Run on the server:
    cd ~/Grid-Battle
    cp nfl_players.json nfl_players.json.backup
    python3 dedupe_nfl.py
    sudo systemctl restart statcheck
"""
import json
from collections import defaultdict

INPUT_FILE = "nfl_players.json"
OUTPUT_FILE = "nfl_players.json"

POSITION_GROUPS = [
    {'DB', 'CB', 'S', 'SS', 'FS'},
    {'LB', 'OLB', 'ILB', 'MLB'},
    {'DL', 'DE', 'DT', 'NT', 'EDGE'},
    {'OL', 'C', 'G', 'T', 'OT', 'OG'},
    {'TE', 'FB'},
    {'WR', 'TE'},
]
POS_SPECIFICITY = {
    'CB': 2, 'S': 2, 'SS': 2, 'FS': 2,
    'OLB': 2, 'ILB': 2, 'MLB': 2,
    'DE': 2, 'DT': 2, 'NT': 2,
    'C': 2, 'G': 2, 'T': 2, 'OT': 2, 'OG': 2,
    'DB': 1, 'LB': 1, 'DL': 1, 'OL': 1,
}


def positions_compatible(p1, p2):
    if p1 == p2: return True
    return any(p1 in g and p2 in g for g in POSITION_GROUPS)


def teams_overlap_significantly(t1, t2):
    s1, s2 = set(t1), set(t2)
    if not s1 or not s2: return False
    return len(s1 & s2) / min(len(s1), len(s2)) >= 0.6


def merge_two_players(p1, p2):
    s1 = POS_SPECIFICITY.get(p1.get('position', ''), 1)
    s2 = POS_SPECIFICITY.get(p2.get('position', ''), 1)
    primary = p1 if s1 >= s2 else p2
    other = p2 if primary is p1 else p1
    merged = dict(primary)
    merged['teams'] = sorted(set(p1.get('teams', [])) | set(p2.get('teams', [])))
    d1 = p1.get('debut_year') or 9999
    d2 = p2.get('debut_year') or 9999
    earliest = min(d1, d2)
    merged['debut_year'] = earliest if earliest < 9999 else None
    for key in ('weeks_by_team', 'snaps_by_team', 'games_by_team'):
        m = dict(p1.get(key, {}))
        for t, v in p2.get(key, {}).items():
            m[t] = max(m.get(t, 0), v)
        if m: merged[key] = m
    a1 = p1.get('achievements', {})
    a2 = p2.get('achievements', {})
    merged_ach = dict(a1)
    for stat, teams in a2.items():
        if stat in merged_ach:
            if isinstance(merged_ach[stat], list) and isinstance(teams, list):
                merged_ach[stat] = sorted(set(merged_ach[stat]) | set(teams))
        else:
            merged_ach[stat] = teams
    if merged_ach: merged['achievements'] = merged_ach
    if not merged.get('headshot') and other.get('headshot'):
        merged['headshot'] = other['headshot']
    merged['name'] = primary['name'].split('(')[0].strip()
    return merged


def main():
    print(f"Loading {INPUT_FILE}...")
    with open(INPUT_FILE) as f:
        db = json.load(f)
    print(f"  Loaded {len(db)} players")

    buckets = defaultdict(list)
    for p in db:
        base = p.get('name', '').split('(')[0].strip().lower()
        if base: buckets[base].append(p)

    merged_db = []
    merged_count = 0
    examples = []
    for base, items in buckets.items():
        if len(items) == 1:
            merged_db.append(items[0])
            continue
        remaining = list(items)
        while remaining:
            p = remaining.pop(0)
            merged_p = p
            to_remove = []
            merged_with = []
            for i, other in enumerate(remaining):
                if positions_compatible(merged_p.get('position', ''), other.get('position', '')) and \
                   teams_overlap_significantly(merged_p.get('teams', []), other.get('teams', [])):
                    merged_with.append(other.get('name', ''))
                    merged_p = merge_two_players(merged_p, other)
                    to_remove.append(i)
                    merged_count += 1
            for i in reversed(to_remove):
                remaining.pop(i)
            if to_remove:
                merged_p['name'] = merged_p['name'].split('(')[0].strip()
                if len(examples) < 10:
                    examples.append((merged_p['name'], [p.get('name')] + merged_with))
            merged_db.append(merged_p)

    print(f"\nResults:")
    print(f"  Players merged: {merged_count}")
    print(f"  Final count:    {len(merged_db)}")
    if examples:
        print(f"\nExamples of merges:")
        for name, originals in examples:
            print(f"  {name}")
            for o in originals:
                print(f"    <- {o}")
    print(f"\nSaving to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(merged_db, f)
    print("Done. Restart statcheck service for changes to take effect.")


if __name__ == "__main__":
    main()
