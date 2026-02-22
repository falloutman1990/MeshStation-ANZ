##############################
### MeshStation By IronGiu ###
##############################
# Please respect the GNU General Public v3.0 license terms and conditions.
import sys
import os
import time
import argparse
import base64
import socket
import zmq
import json
import asyncio
import threading
import html
import asyncio
import locale
import re
from datetime import datetime
from collections import deque
import platform
import subprocess
import multiprocessing
import atexit
import secrets
import urllib.request
import urllib.error

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from meshtastic import mesh_pb2, admin_pb2, telemetry_pb2, config_pb2

from nicegui import ui, app
from fastapi import Request

# --- CONSTANTS ---
PROGRAM_NAME = "MeshStation"
PROGRAM_SHORT_DESC = "Meshtastic SDR Analyzer & Desktop GUI"
AUTHOR = "IronGiu"
VERSION = "1.0.0"
LICENSE = "GNU General Public License v3.0"
GITHUB_URL = "https://github.com/IronGiu/MeshStation"
DONATION_URL = "https://ko-fi.com/irongiu"
SUPPORTERS_URL = "https://github.com/IronGiu/MeshStation/SUPPORTERS.md"
GITHUB_RELEASES_URL = f"{GITHUB_URL}/releases"
LANG_FILE_NAME = "languages.json"
DEBUGGING = False
SHOW_DEV_TOOLS = DEBUGGING
SHUTDOWN_TOKEN = secrets.token_urlsafe(24)

def _parse_version_tuple(v: str) -> tuple[int, int, int]:
    s = (v or "").strip()
    if s.startswith("v") or s.startswith("V"):
        s = s[1:]
    m = re.search(r"(\d+)(?:\.(\d+))?(?:\.(\d+))?", s)
    if not m:
        return (0, 0, 0)
    return (int(m.group(1) or 0), int(m.group(2) or 0), int(m.group(3) or 0))

def _is_newer_version(current: str, latest: str) -> bool:
    cur = _parse_version_tuple(current)
    lat = _parse_version_tuple(latest)
    if cur == (0, 0, 0) or lat == (0, 0, 0):
        return (latest or "").strip() != (current or "").strip()
    return lat > cur

def _github_repo_slug() -> str | None:
    s = (GITHUB_URL or "").strip()
    m = re.match(r"^https?://github\.com/([^/]+)/([^/]+)", s, flags=re.IGNORECASE)
    if not m:
        if DEBUGGING:
            print(f"Invalid GITHUB_URL: {s}")
        return None
    owner = m.group(1)
    repo = m.group(2).rstrip("/")
    if repo.endswith(".git"):
        repo = repo[:-4]
    if DEBUGGING:
        print(f"Repo slug: {owner}/{repo}")
    return f"{owner}/{repo}"

def _fetch_latest_github_release(timeout_sec: float = 10.0) -> dict | None:
    slug = _github_repo_slug()
    if not slug:
        if DEBUGGING:
            print(f"Invalid repo slug: {slug}")
        return None
    api_url = f"https://api.github.com/repos/{slug}/releases/latest"
    req = urllib.request.Request(
        api_url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": f"{PROGRAM_NAME}/{VERSION}",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        data = json.loads(raw)
        tag = (data.get("tag_name") or "").strip()
        url = (data.get("html_url") or "").strip() or GITHUB_RELEASES_URL
        if not tag:
            if DEBUGGING:
                print(f"Invalid tag_name: {tag}")
            return None
        return {"tag": tag, "url": url}
    except Exception as e:
        if DEBUGGING:
            print(f"Error fetching latest release: {e}")
        return None

LORA_PRESETS = {
    "Medium Fast": {
        "EU_868": {"center_freq": 869_525_000, "samp_rate": 1_000_000, "lora_bw": 250_000, "sf": 9},
        "EU_433": {"center_freq": 433_125_000, "samp_rate": 1_000_000, "lora_bw": 250_000, "sf": 9},
        "US_915": {"center_freq": 913_125_000, "samp_rate": 1_000_000, "lora_bw": 250_000, "sf": 9},
    },
    "Long Fast": {
        "EU_868": {"center_freq": 869_525_000, "samp_rate": 1_000_000, "lora_bw": 250_000, "sf": 11},
        "EU_433": {"center_freq": 433_875_000, "samp_rate": 1_000_000, "lora_bw": 250_000, "sf": 11},
        "US_915": {"center_freq": 906_875_000, "samp_rate": 1_000_000, "lora_bw": 250_000, "sf": 11},
    },
    "Medium Slow": {
        "EU_868": {"center_freq": 869_525_000, "samp_rate": 1_000_000, "lora_bw": 250_000, "sf": 10},
        "EU_433": {"center_freq": 433_875_000, "samp_rate": 1_000_000, "lora_bw": 250_000, "sf": 10},
        "US_915": {"center_freq": 914_875_000, "samp_rate": 1_000_000, "lora_bw": 250_000, "sf": 10},
    },
    "Long Slow (depr.)": {
        "EU_868": {"center_freq": 869_462_500, "samp_rate": 1_000_000, "lora_bw": 125_000, "sf": 12},
        "EU_433": {"center_freq": 433_312_500, "samp_rate": 1_000_000, "lora_bw": 125_000, "sf": 12},
        "US_915": {"center_freq": 905_312_500, "samp_rate": 1_000_000, "lora_bw": 125_000, "sf": 12},
    },
    "Long Moderate": {
        "EU_868": {"center_freq": 869_587_500, "samp_rate": 1_000_000, "lora_bw": 125_000, "sf": 11},
        "EU_433": {"center_freq": 433_687_500, "samp_rate": 1_000_000, "lora_bw": 125_000, "sf": 11},
        "US_915": {"center_freq": 902_687_500, "samp_rate": 1_000_000, "lora_bw": 125_000, "sf": 11},
    },
    "Short Slow": {
        "EU_868": {"center_freq": 869_525_000, "samp_rate": 1_000_000, "lora_bw": 250_000, "sf": 8},
        "EU_433": {"center_freq": 433_625_000, "samp_rate": 1_000_000, "lora_bw": 250_000, "sf": 8},
        "US_915": {"center_freq": 920_625_000, "samp_rate": 1_000_000, "lora_bw": 250_000, "sf": 8},
    },
    "Short Fast": {
        "EU_868": {"center_freq": 869_525_000, "samp_rate": 1_000_000, "lora_bw": 250_000, "sf": 7},
        "EU_433": {"center_freq": 433_875_000, "samp_rate": 1_000_000, "lora_bw": 250_000, "sf": 7},
        "US_915": {"center_freq": 918_875_000, "samp_rate": 1_000_000, "lora_bw": 250_000, "sf": 7},
    },
    "Short Turbo": {
        "EU_433": {"center_freq": 433_750_000, "samp_rate": 1_000_000, "lora_bw": 500_000, "sf": 7},
        "US_915": {"center_freq": 926_750_000, "samp_rate": 1_000_000, "lora_bw": 500_000, "sf": 7},
    },
}

# SVG Icon (Envelope with Antenna)
APP_ICON_SVG = """
<svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
  <!-- Envelope Body -->
  <rect x="10" y="35" width="80" height="50" rx="5" fill="#4CAF50" />
  <!-- Envelope Flap -->
  <path d="M 10 35 L 50 65 L 90 35" stroke="white" stroke-width="4" fill="none" />
  <!-- Antenna Pole -->
  <line x1="75" y1="35" x2="75" y2="10" stroke="#4CAF50" stroke-width="4" />
  <!-- Antenna Tip -->
  <circle cx="75" cy="10" r="3" fill="#4CAF50" />
  <!-- Radio Waves -->
  <path d="M 65 15 Q 55 10 65 5" stroke="#4CAF50" stroke-width="2" fill="none" />
  <path d="M 85 15 Q 95 10 85 5" stroke="#4CAF50" stroke-width="2" fill="none" />
</svg>
"""

def ensure_app_icon_file():
    if getattr(sys, 'frozen', False):
        return
    try:
        base_path = os.path.dirname(os.path.abspath(__file__))
        svg_path = os.path.join(base_path, "app_icon.svg")
        if not os.path.isfile(svg_path):
            with open(svg_path, "w", encoding="utf-8") as f:
                f.write(APP_ICON_SVG)
    except Exception:
        pass

def setup_static_files():
    maps_dir = get_resource_path('offlinemaps') if getattr(sys, 'frozen', False) else os.path.join(get_app_path(), 'offlinemaps')
    if not os.path.isdir(maps_dir):
        try:
            os.makedirs(maps_dir, exist_ok=True)
        except:
            pass
    
    if os.path.isdir(maps_dir):
        app.add_static_files('/static/offlinemaps', maps_dir)

def has_tile_internet():
    try:
        import urllib.request
        urllib.request.urlopen('https://tile.openstreetmap.org/0/0/0.png', timeout=2)
        return True
    except Exception:
        return False

_offline_topology_cache = {}
_offline_geo_cache = {}

def get_offline_topology():
    maps_dir = get_resource_path('offlinemaps') if getattr(sys, 'frozen', False) else os.path.join(get_app_path(), 'offlinemaps')
    topo_path = os.path.join(maps_dir, 'map.json')
    if not os.path.isfile(topo_path):
        return None
    cached = _offline_topology_cache.get(topo_path)
    if cached is not None:
        return cached
    try:
        with open(topo_path, 'r', encoding='utf-8') as f:
            topo = json.load(f)
        _offline_topology_cache[topo_path] = topo
        return topo
    except Exception:
        return None

def _decode_topology_arcs(topology):
    key = id(topology)
    cached = _offline_geo_cache.get(('arcs', key))
    if cached is not None:
        return cached
    arcs = topology.get('arcs') or []
    transform = topology.get('transform') or {}
    scale = transform.get('scale')
    translate = transform.get('translate')
    decoded = []
    for arc in arcs:
        x = 0
        y = 0
        coords = []
        for point in arc:
            x += point[0]
            y += point[1]
            if scale and translate:
                xx = x * scale[0] + translate[0]
                yy = y * scale[1] + translate[1]
            else:
                xx = x
                yy = y
            coords.append([xx, yy])
        decoded.append(coords)
    _offline_geo_cache[('arcs', key)] = decoded
    return decoded

def _topology_transform_coords(coords, scale, translate):
    if not scale or not translate:
        return coords
    if not coords:
        return coords
    first = coords[0]
    if isinstance(first, (int, float)):
        return [coords[0] * scale[0] + translate[0], coords[1] * scale[1] + translate[1]]
    return [_topology_transform_coords(c, scale, translate) for c in coords]

def _topology_object_to_feature_collection(topology, object_name):
    cache_key = ('object', object_name)
    cached = _offline_geo_cache.get(cache_key)
    if cached is not None:
        return cached
    objects = topology.get('objects') or {}
    obj = objects.get(object_name)
    if not obj:
        return None
    decoded_arcs = _decode_topology_arcs(topology)
    transform = topology.get('transform') or {}
    scale = transform.get('scale')
    translate = transform.get('translate')
    def build_line(arc_indices):
        coords = []
        for ai in arc_indices:
            idx = ai if ai >= 0 else ~ai
            if idx < 0 or idx >= len(decoded_arcs):
                continue
            arc = decoded_arcs[idx]
            if ai < 0:
                arc = list(reversed(arc))
            if coords:
                arc = arc[1:]
            coords.extend(arc)
        return coords
    def geometry_to_geo(geom):
        gtype = geom.get('type')
        if gtype == 'Point':
            coords = geom.get('coordinates') or []
            return {'type': 'Point', 'coordinates': _topology_transform_coords(coords, scale, translate)}
        if gtype == 'MultiPoint':
            coords = geom.get('coordinates') or []
            return {'type': 'MultiPoint', 'coordinates': _topology_transform_coords(coords, scale, translate)}
        if gtype == 'LineString':
            arcs = geom.get('arcs') or []
            return {'type': 'LineString', 'coordinates': build_line(arcs)}
        if gtype == 'MultiLineString':
            lines = []
            for part in geom.get('arcs') or []:
                lines.append(build_line(part))
            return {'type': 'MultiLineString', 'coordinates': lines}
        if gtype == 'Polygon':
            rings = []
            for ring_arcs in geom.get('arcs') or []:
                rings.append(build_line(ring_arcs))
            return {'type': 'Polygon', 'coordinates': rings}
        if gtype == 'MultiPolygon':
            polys = []
            for poly_arcs in geom.get('arcs') or []:
                rings = []
                for ring_arcs in poly_arcs:
                    rings.append(build_line(ring_arcs))
                polys.append(rings)
            return {'type': 'MultiPolygon', 'coordinates': polys}
        return None
    def geometry_to_features(geom):
        gtype = geom.get('type')
        if gtype == 'GeometryCollection':
            result = []
            for sub in geom.get('geometries') or []:
                result.extend(geometry_to_features(sub))
            return result
        mapped = geometry_to_geo(geom)
        if not mapped:
            return []
        properties = geom.get('properties') or {}
        return [{'type': 'Feature', 'properties': properties, 'geometry': mapped}]
    if obj.get('type') == 'GeometryCollection':
        features = []
        for g in obj.get('geometries') or []:
            features.extend(geometry_to_features(g))
    else:
        features = geometry_to_features(obj)
    feature_collection = {'type': 'FeatureCollection', 'features': features}
    _offline_geo_cache[cache_key] = feature_collection
    return feature_collection

def _feature_polygon_centroid(geometry):
    gtype = geometry.get('type')
    coords = geometry.get('coordinates')
    if not coords:
        return None
    if gtype == 'Polygon':
        ring = coords[0]
    elif gtype == 'MultiPolygon':
        ring = coords[0][0]
    else:
        return None
    if len(ring) < 3:
        return None
    area = 0.0
    cx = 0.0
    cy = 0.0
    for i in range(len(ring) - 1):
        x1, y1 = ring[i]
        x2, y2 = ring[i + 1]
        cross = x1 * y2 - x2 * y1
        area += cross
        cx += (x1 + x2) * cross
        cy += (y1 + y2) * cross
    if area == 0.0:
        return ring[0]
    area *= 0.5
    return [cx / (6.0 * area), cy / (6.0 * area)]

def _geometry_bbox(geometry):
    if not geometry:
        return None
    gtype = geometry.get('type')
    coords = geometry.get('coordinates')
    if coords is None:
        return None

    def iter_points(c):
        if not c:
            return
        first = c[0]
        if isinstance(first, (int, float)) and len(c) >= 2:
            yield c
            return
        for sub in c:
            yield from iter_points(sub)

    if gtype == 'Point':
        try:
            lon, lat = coords
        except Exception:
            return None
        return {'south': lat, 'west': lon, 'north': lat, 'east': lon}

    min_lon = None
    min_lat = None
    max_lon = None
    max_lat = None
    for pt in iter_points(coords):
        try:
            lon, lat = pt[0], pt[1]
        except Exception:
            continue
        if min_lon is None:
            min_lon = max_lon = lon
            min_lat = max_lat = lat
        else:
            if lon < min_lon:
                min_lon = lon
            if lon > max_lon:
                max_lon = lon
            if lat < min_lat:
                min_lat = lat
            if lat > max_lat:
                max_lat = lat
    if min_lon is None:
        return None
    return {'south': min_lat, 'west': min_lon, 'north': max_lat, 'east': max_lon}

def _bbox_intersects(view_bounds: dict, feature_bbox: dict) -> bool:
    if not view_bounds or not feature_bbox:
        return False
    return not (
        feature_bbox['north'] < view_bounds['south'] or
        feature_bbox['south'] > view_bounds['north'] or
        feature_bbox['east'] < view_bounds['west'] or
        feature_bbox['west'] > view_bounds['east']
    )

def _ensure_feature_indexes(fc: dict):
    feats = (fc or {}).get('features') or []
    for f in feats:
        if not isinstance(f, dict):
            continue
        if f.get('_mesh_bbox') is None:
            geom = f.get('geometry') or {}
            f['_mesh_bbox'] = _geometry_bbox(geom)
        if f.get('_mesh_centroid') is None:
            geom = f.get('geometry') or {}
            f['_mesh_centroid'] = _feature_polygon_centroid(geom)
    return fc

def _extract_feature_name_en(properties):
    if not properties:
        return None
    for key in ['name_en', 'NAME_EN', 'NAMEEN', 'NAME_ENGLI', 'NAME_ENGL', 'NAMEENG']:
        if key in properties and properties[key]:
            return str(properties[key])
    return None

def _extract_feature_name(properties):
    if not properties:
        return None
    name_en = _extract_feature_name_en(properties)
    if name_en:
        return name_en
    for key in ['region', 'REGION', 'NAME', 'NAME_LONG', 'ADMIN', 'admin', 'name']:
        if key in properties and properties[key]:
            return str(properties[key])
    for value in properties.values():
        if isinstance(value, str) and value:
            return value
    return None

def _normalize_topo_key(s: str) -> str:
    if not isinstance(s, str):
        return ''
    out = []
    prev_us = False
    for ch in s.strip().lower():
        if ch.isalnum():
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append('_')
                prev_us = True
    norm = ''.join(out).strip('_')
    while '__' in norm:
        norm = norm.replace('__', '_')
    return norm

def _pick_topo_object_name(topo: dict, preferred: list[str]) -> str | None:
    objects = (topo or {}).get('objects') or {}
    if not objects:
        return None

    for name in preferred:
        if name in objects:
            return name

    norm_to_key = {}
    for k in objects.keys():
        norm_to_key[_normalize_topo_key(k)] = k

    for name in preferred:
        nk = _normalize_topo_key(name)
        if nk in norm_to_key:
            return norm_to_key[nk]

    preferred_tokens = []
    for name in preferred:
        nk = _normalize_topo_key(name)
        if nk:
            preferred_tokens.append(nk.split('_'))

    for k in objects.keys():
        ok = _normalize_topo_key(k)
        if not ok:
            continue
        for toks in preferred_tokens:
            if not toks:
                continue
            if all(t in ok.split('_') for t in toks):
                return k
    return None

def _topo_object_stats(obj: dict) -> dict:
    stats = {
        'points': 0,
        'polys': 0,
        'lines': 0,
        'has_name_en': False,
        'type_values': set(),
    }
    if not obj:
        return stats
    geoms = []
    if obj.get('type') == 'GeometryCollection':
        geoms = obj.get('geometries') or []
    else:
        geoms = [obj]
    for g in geoms[:5000]:
        gtype = (g or {}).get('type')
        props = (g or {}).get('properties') or {}
        if isinstance(props, dict) and (props.get('name_en') or props.get('NAME_EN')):
            stats['has_name_en'] = True
        tv = (props.get('type') or props.get('TYPE'))
        if isinstance(tv, str) and tv:
            stats['type_values'].add(tv.strip().lower())
        if gtype in ('Point', 'MultiPoint'):
            stats['points'] += 1
        elif gtype in ('Polygon', 'MultiPolygon'):
            stats['polys'] += 1
        elif gtype in ('LineString', 'MultiLineString'):
            stats['lines'] += 1
    return stats

def _detect_topo_object_names(topo: dict) -> dict:
    objects = (topo or {}).get('objects') or {}
    if not objects:
        return {}

    candidates = []
    for key, obj in objects.items():
        stats = _topo_object_stats(obj)
        candidates.append((key, stats))

    def score_country(key: str, s: dict) -> int:
        nk = _normalize_topo_key(key)
        score = 0
        if 'admin_0' in nk or 'admin0' in nk or 'countries' in nk or 'country' in nk:
            score += 50
        score += min(40, s['polys'])
        if s['has_name_en']:
            score += 20
        return score

    def score_admin1(key: str, s: dict) -> int:
        nk = _normalize_topo_key(key)
        score = 0
        if 'admin_1' in nk or 'admin1' in nk or 'states' in nk or 'provinces' in nk:
            score += 50
        score += min(40, s['polys'])
        if 'province' in s['type_values']:
            score += 40
        if s['has_name_en']:
            score += 10
        return score

    def score_places(key: str, s: dict) -> int:
        nk = _normalize_topo_key(key)
        score = 0
        if 'populated' in nk or 'places' in nk or 'cities' in nk or 'towns' in nk:
            score += 50
        score += min(40, s['points'])
        if s['has_name_en']:
            score += 20
        return score

    def score_regions(key: str, s: dict) -> int:
        nk = _normalize_topo_key(key)
        score = 0
        if 'regions' in nk or 'regioni' in nk:
            score += 60
        score += min(40, s['polys'])
        if s['has_name_en']:
            score += 20
        return score

    best = {'countries': None, 'admin1': None, 'places': None, 'regions': None}
    best_score = {k: -1 for k in best.keys()}
    for key, s in candidates:
        sc = score_country(key, s)
        if sc > best_score['countries']:
            best_score['countries'] = sc
            best['countries'] = key
        sa = score_admin1(key, s)
        if sa > best_score['admin1']:
            best_score['admin1'] = sa
            best['admin1'] = key
        sp = score_places(key, s)
        if sp > best_score['places']:
            best_score['places'] = sp
            best['places'] = key
        sr = score_regions(key, s)
        if sr > best_score['regions']:
            best_score['regions'] = sr
            best['regions'] = key

    return best

def _topo_objects_debug(topo: dict) -> list[str]:
    # Return available object keys for troubleshooting
    objects = (topo or {}).get('objects') or {}
    return sorted(objects.keys())

# --- CONFIGURATION & STATE ---

class AppState:
    def __init__(self):
        self.connect_mode = None  # None | "direct" | "external"
        self.engine_proc = None
        self.last_rx_ts = 0.0
        self.rx_seen_once = False
        self.autosave_interval_sec = 30
        self.autosave_last_ts = 0.0

        self.direct_region = "EU_868"
        self.direct_preset = "Medium Fast"
        self.direct_ppm = 0
        self.direct_gain = 30
        self.direct_port = "20002"
        self.direct_key_b64 = "AQ=="

        self.external_ip = "127.0.0.1"
        self.external_port = "20002"
        self.external_key_b64 = "AQ=="

        self.connected = False
        self.ip_address = "127.0.0.1"
        self.port = "20002"
        self.aes_key_b64 = "AQ==" # Default Meshtastic Key representation (means default)
        self.aes_key_bytes = None
        
        # Data Stores
        self.nodes = {} # Key: NodeID (e.g., "!322530e5"), Value: Dict with info
        self.messages = deque(maxlen=100) # List of chat messages
        self.logs = deque(maxlen=500) # Raw logs
        self.seen_packets = deque(maxlen=300) # Deduplication buffer (Sender, PacketID)
        self.raw_packet_count = 0
        
        # UI Update Flags (simple dirty checking)
        self.new_logs = []
        self.new_messages = []
        self.nodes_updated = False
        self.nodes_list_updated = False # Separate flag for grid to avoid conflict
        self.nodes_list_force_refresh = False # Force full reload of grid (e.g. after import)
        self.chat_force_refresh = False # Force full reload of chat (e.g. after import or name change)
        self.chat_force_scroll = False # Flag to force scroll to bottom (e.g. after import)
        self.dirty_nodes = set() # Track modified nodes for delta updates
        self.lock = threading.Lock() # Thread safety for dirty_nodes
        self.verbose_logging = True # Default to verbose logging
        self.theme = "light"

        # Error checking
        self.rtlsdr_error_pending = False
        self.rtlsdr_error_text = ""

        # Connection Popup auto state
        self.connection_dialog_shown = False

        self.update_check_done = False
        self.update_check_running = False
        self.update_available = False
        self.latest_version = None
        self.latest_release_url = None
        self.update_popup_shown = False
        self.update_popup_ack_version = None

class MeshStatsManager:
    def __init__(self):
        self._lock = threading.Lock()
        self.enabled = False
        self.freeze_now = time.time()
        self.reset()

    def set_enabled(self, enabled: bool):
        with self._lock:
            self.enabled = bool(enabled)
            if self.enabled:
                self.freeze_now = None
            else:
                self.freeze_now = time.time()

    def reset(self):
        now = time.time()
        with self._lock:
            self.started_ts = now
            if not self.enabled:
                self.freeze_now = now

            self.total_packets = 0
            self.packet_ts_60s = deque()

            self.node_last_seen_ts = {}
            self.node_first_seen_ts = {}
            self.per_node_packet_count = {}

            self.crc_ok = 0
            self.crc_fail = 0
            self.decrypt_ok = 0
            self.decrypt_fail = 0
            self.invalid_protobuf = 0
            self.unknown_portnum = 0

            self.direct_packets = 0
            self.multihop_packets = 0
            self.hop_sum = 0
            self.hop_count = 0

            self.snr_values = deque(maxlen=250)
            self.rssi_values = deque(maxlen=250)

            self.channel_util_samples = deque()
            self.air_util_tx_samples = deque()

            self.ppm_history = deque(maxlen=180)

            self._crc_invalid_by_packet = {}

    def mark_crc_invalid_packet(self, sender_bytes: bytes, packet_id_bytes: bytes, ts: float | None = None):
        if not sender_bytes or not packet_id_bytes:
            return
        if ts is None:
            ts = time.time()
        k = (bytes(sender_bytes), bytes(packet_id_bytes))
        with self._lock:
            self._crc_invalid_by_packet[k] = float(ts)
            cutoff = float(ts) - 5.0
            stale = [kk for kk, tts in self._crc_invalid_by_packet.items() if tts < cutoff]
            for kk in stale:
                self._crc_invalid_by_packet.pop(kk, None)

    def consume_crc_invalid_packet(self, sender_bytes: bytes, packet_id_bytes: bytes, now: float | None = None) -> bool:
        if not sender_bytes or not packet_id_bytes:
            return False
        if now is None:
            now = time.time()
        k = (bytes(sender_bytes), bytes(packet_id_bytes))
        with self._lock:
            ts = self._crc_invalid_by_packet.pop(k, None)
            if ts is None:
                return False
            return (float(now) - float(ts)) <= 5.0

    @staticmethod
    def _clamp01(x: float) -> float:
        if x < 0.0:
            return 0.0
        if x > 1.0:
            return 1.0
        return x

    def on_frame_ok(self):
        with self._lock:
            if not self.enabled:
                return
            self.crc_ok += 1

    def on_frame_fail(self):
        with self._lock:
            if not self.enabled:
                return
            self.crc_fail += 1

    def on_packet_received(self, sender_id: str | None, hops: int | None, snr: float | None, rssi: float | None, ts: float | None = None):
        with self._lock:
            if not self.enabled:
                return
            if ts is None:
                ts = time.time()
            self.total_packets += 1
            self.packet_ts_60s.append(ts)
            cutoff = ts - 60.0
            while self.packet_ts_60s and self.packet_ts_60s[0] < cutoff:
                self.packet_ts_60s.popleft()

            if sender_id:
                self.node_last_seen_ts[sender_id] = ts
                if sender_id not in self.node_first_seen_ts:
                    self.node_first_seen_ts[sender_id] = ts
                self.per_node_packet_count[sender_id] = self.per_node_packet_count.get(sender_id, 0) + 1

            if isinstance(hops, int):
                if hops <= 0:
                    self.direct_packets += 1
                else:
                    self.multihop_packets += 1
                self.hop_sum += max(0, int(hops))
                self.hop_count += 1

            if isinstance(snr, (int, float)):
                self.snr_values.append(float(snr))
            if isinstance(rssi, (int, float)):
                self.rssi_values.append(float(rssi))

    def on_decrypt_ok(self):
        with self._lock:
            if not self.enabled:
                return
            self.decrypt_ok += 1

    def on_decrypt_fail(self):
        with self._lock:
            if not self.enabled:
                return
            self.decrypt_fail += 1

    def on_invalid_protobuf(self):
        with self._lock:
            if not self.enabled:
                return
            self.invalid_protobuf += 1

    def on_portnum_seen(self, portnum: int, supported: bool):
        if supported:
            return
        with self._lock:
            if not self.enabled:
                return
            self.unknown_portnum += 1

    def on_telemetry(self, node_id: str | None, metrics: dict, ts: float | None = None):
        cu = metrics.get("channel_utilization")
        au = metrics.get("air_util_tx")
        with self._lock:
            if not self.enabled:
                return
            if ts is None:
                ts = time.time()
            if node_id:
                self.node_last_seen_ts[node_id] = ts
            if isinstance(cu, (int, float)):
                self.channel_util_samples.append((ts, float(cu)))
            if isinstance(au, (int, float)):
                self.air_util_tx_samples.append((ts, float(au)))
            cutoff = ts - 600.0
            while self.channel_util_samples and self.channel_util_samples[0][0] < cutoff:
                self.channel_util_samples.popleft()
            while self.air_util_tx_samples and self.air_util_tx_samples[0][0] < cutoff:
                self.air_util_tx_samples.popleft()

    def snapshot(self, now: float | None = None) -> dict:
        with self._lock:
            if now is None:
                now = time.time()
            if not self.enabled and self.freeze_now is not None:
                now = self.freeze_now
            cutoff_60 = now - 60.0
            while self.packet_ts_60s and self.packet_ts_60s[0] < cutoff_60:
                self.packet_ts_60s.popleft()

            ppm = len(self.packet_ts_60s)

            active_5m = 0
            active_10m = 0
            cutoff_5 = now - 300.0
            cutoff_10 = now - 600.0
            for _nid, ts in self.node_last_seen_ts.items():
                if ts >= cutoff_5:
                    active_5m += 1
                if ts >= cutoff_10:
                    active_10m += 1

            new_nodes_last_hour = 0
            cutoff_h = now - 3600.0
            for _nid, ts in self.node_first_seen_ts.items():
                if ts >= cutoff_h:
                    new_nodes_last_hour += 1

            most_active_node = None
            most_active_count = 0
            for nid, cnt in self.per_node_packet_count.items():
                if cnt > most_active_count:
                    most_active_node = nid
                    most_active_count = cnt

            snr_avg = (sum(self.snr_values) / len(self.snr_values)) if self.snr_values else None
            rssi_avg = (sum(self.rssi_values) / len(self.rssi_values)) if self.rssi_values else None

            direct_ratio = None
            multihop_ratio = None
            hop_avg = None
            denom_hops = self.direct_packets + self.multihop_packets
            if denom_hops > 0:
                direct_ratio = (self.direct_packets / denom_hops) * 100.0
                multihop_ratio = (self.multihop_packets / denom_hops) * 100.0
            if self.hop_count > 0:
                hop_avg = self.hop_sum / self.hop_count

            cu_vals = [v for _ts, v in self.channel_util_samples if _ts >= cutoff_10]
            au_vals = [v for _ts, v in self.air_util_tx_samples if _ts >= cutoff_10]
            cu_avg = (sum(cu_vals) / len(cu_vals)) if cu_vals else None
            au_max = (max(au_vals)) if au_vals else None

            errors_total = self.crc_fail + self.invalid_protobuf
            error_rate = (errors_total / self.total_packets) * 100.0 if self.total_packets > 0 else 0.0

            def _dyn_score(pairs: list[tuple[float | None, float]]) -> int:
                num = 0.0
                den = 0.0
                for v, w in pairs:
                    if v is None:
                        continue
                    num += float(v) * float(w)
                    den += float(w)
                if den <= 0.0:
                    return 0
                return int(round(self._clamp01(num / den) * 100.0))

            def _level4(score: int) -> tuple[str, str]:
                if score >= 75:
                    return ("excellent", "green")
                if score >= 55:
                    return ("good", "yellow")
                if score >= 35:
                    return ("fair", "orange")
                return ("poor", "red")

            def _health4(score: int) -> tuple[str, str]:
                if score >= 75:
                    return ("stable", "green")
                if score >= 55:
                    return ("intermittent", "yellow")
                if score >= 35:
                    return ("unstable", "orange")
                return ("critical", "red")

            traffic_score = 0
            integrity_score = 0
            signal_score = 0
            global_health_score = 0
            traffic_level, traffic_color = _level4(0)
            integrity_level, integrity_color = _level4(0)
            signal_level, signal_color = _level4(0)
            global_health_level, global_health_color = _health4(0)

            if self.total_packets > 0:
                if ppm > 0:
                    pps = float(ppm) / 60.0
                    pps_table = [(0.2, 1.0), (0.5, 0.85), (1.0, 0.65), (2.0, 0.35), (4.0, 0.15), (999.0, 0.05)]
                    base = pps_table[-1][1]
                    prev_t, prev_v = 0.0, pps_table[0][1]
                    for t, v in pps_table:
                        if pps <= t:
                            if t <= prev_t:
                                base = v
                            else:
                                frac = (pps - prev_t) / (t - prev_t)
                                base = prev_v + (v - prev_v) * frac
                            break
                        prev_t, prev_v = t, v

                    recent = [t for t in self.packet_ts_60s if t >= (now - 10.0)]
                    burst_mul = 1.0
                    if len(recent) >= 2:
                        recent.sort()
                        min_dt = min((recent[i] - recent[i - 1]) for i in range(1, len(recent)))
                        if min_dt < 0.5:
                            burst_mul = 0.7 + 0.3 * self._clamp01(min_dt / 0.5)
                    traffic_score = int(round(self._clamp01(base * burst_mul) * 100.0))

                ok_pb = max(0, int(self.crc_ok) - int(self.invalid_protobuf))
                bad_crc = int(self.crc_fail)
                bad_pb = int(self.invalid_protobuf)
                integrity_score = _dyn_score([(ok_pb / max(1.0, float(ok_pb + bad_crc + bad_pb)), 1.0)]) if (ok_pb + bad_crc + bad_pb) > 0 else 0

                snr_norm = None
                if snr_avg is not None:
                    snr_norm = self._clamp01((float(snr_avg) - (-20.0)) / (10.0 - (-20.0)))
                rssi_norm = None
                if rssi_avg is not None:
                    rssi_norm = self._clamp01((float(rssi_avg) - (-120.0)) / (-30.0 - (-120.0)))
                signal_score = _dyn_score([(snr_norm, 0.60), (rssi_norm, 0.40)])

                global_health_score = int(round((traffic_score + integrity_score + signal_score) / 3.0))

                traffic_level, traffic_color = _level4(traffic_score)
                integrity_level, integrity_color = _level4(integrity_score)
                signal_level, signal_color = _level4(signal_score)
                global_health_level, global_health_color = _health4(global_health_score)

            return {
                "started_ts": self.started_ts,
                "uptime_sec": max(0.0, now - self.started_ts),

                "total_packets": self.total_packets,
                "packets_per_minute": ppm,
                "active_nodes_5m": active_5m,
                "active_nodes_10m": active_10m,
                "new_nodes_last_hour": new_nodes_last_hour,
                "global_error_rate_pct": error_rate,

                "crc_ok": self.crc_ok,
                "crc_fail": self.crc_fail,
                "decrypt_ok": self.decrypt_ok,
                "decrypt_fail": self.decrypt_fail,
                "invalid_protobuf": self.invalid_protobuf,
                "unknown_portnum": self.unknown_portnum,

                "snr_avg": snr_avg,
                "rssi_avg": rssi_avg,
                "direct_ratio_pct": direct_ratio,
                "multihop_ratio_pct": multihop_ratio,
                "hop_avg": hop_avg,

                "channel_utilization_avg": cu_avg,
                "air_util_tx_max": au_max,
                "most_active_node": most_active_node,
                "most_active_node_packets": most_active_count,

                "mesh_traffic_score": traffic_score,
                "mesh_traffic_level": traffic_level,
                "mesh_traffic_color": traffic_color,
                "packet_integrity_score": integrity_score,
                "packet_integrity_level": integrity_level,
                "packet_integrity_color": integrity_color,
                "mesh_signal_score": signal_score,
                "mesh_signal_level": signal_level,
                "mesh_signal_color": signal_color,
                "mesh_health_score": global_health_score,
                "mesh_health_level": global_health_level,
                "mesh_health_color": global_health_color,
            }

    def sample_packets_per_minute(self, now: float | None = None) -> list[int]:
        with self._lock:
            if now is None:
                now = time.time()
            if not self.enabled and self.freeze_now is not None:
                return list(self.ppm_history)
            cutoff_60 = now - 60.0
            while self.packet_ts_60s and self.packet_ts_60s[0] < cutoff_60:
                self.packet_ts_60s.popleft()
            ppm = len(self.packet_ts_60s)
            self.ppm_history.append(int(ppm))
            return list(self.ppm_history)

    def to_dict(self) -> dict:
        snap = self.snapshot()
        series = self.sample_packets_per_minute()
        return {
            "version": 1,
            "snapshot": snap,
            "ppm_series": series,
        }

    def load_from_dict(self, data: dict | None):
        if not isinstance(data, dict):
            return
        snap = data.get("snapshot") if isinstance(data.get("snapshot"), dict) else {}
        series = data.get("ppm_series") if isinstance(data.get("ppm_series"), list) else []
        with self._lock:
            try:
                self.started_ts = float(snap.get("started_ts", time.time()))
            except Exception:
                self.started_ts = time.time()
            self.total_packets = int(snap.get("total_packets", 0) or 0)
            self.crc_ok = int(snap.get("crc_ok", 0) or 0)
            self.crc_fail = int(snap.get("crc_fail", 0) or 0)
            self.decrypt_ok = int(snap.get("decrypt_ok", 0) or 0)
            self.decrypt_fail = int(snap.get("decrypt_fail", 0) or 0)
            self.invalid_protobuf = int(snap.get("invalid_protobuf", 0) or 0)
            self.unknown_portnum = int(snap.get("unknown_portnum", 0) or 0)
            self.ppm_history = deque([int(x) for x in series if isinstance(x, (int, float))], maxlen=180)

state = AppState()
mesh_stats = MeshStatsManager()

splash_anim_state = {'running': False}

status_label_ref = None
current_language = "en"
languages_data = {}
user_language_from_config = False
language_select_ref = None

def set_connection_status_ui(connected: bool, mode: str | None = None):
    global status_label_ref
    if status_label_ref is None:
        return
    if connected:
        if mode == "direct":
            status_label_ref.text = translate("status.connected_internal", "Connected (Internal)")
            status_label_ref.classes('font-bold mr-4 self-center')
        elif mode == "external":
            status_label_ref.text = translate("status.connected_external", "Connected (External)")
            status_label_ref.classes('font-bold mr-4 self-center')
        else:
            status_label_ref.text = translate("status.connected", "Connected")
        status_label_ref.classes(replace='text-green-500', remove='text-red-500').classes('font-bold mr-4 self-center')
    else:
        status_label_ref.text = translate("status.disconnected", "Disconnected")
        status_label_ref.classes(replace='text-red-500', remove='text-green-500').classes('font-bold mr-4 self-center')

def _shutdown_cleanup():
    try:
        stop_connection()
    except Exception:
        pass
    try:
        stop_engine_direct()
    except Exception:
        pass

atexit.register(_shutdown_cleanup)

# --- HELPER FUNCTIONS ---

def hexStringToBinary(hexString):
    try:
        return bytes.fromhex(hexString)
    except ValueError:
        return b''

def bytesToHexString(byteString):
    return byteString.hex()

def msb2lsb(msb):
    # Converts 32-bit ID from MSB (GnuRadio) to LSB (Meshtastic standard)
    if len(msb) < 8: return msb
    lsb = msb[6] + msb[7] + msb[4] + msb[5] + msb[2] + msb[3] + msb[0] + msb[1]
    return lsb

def parseAESKey(key_b64):
    try:
        # Default Key Handling
        # If user enters "AQ==" (which is technically just 0x01), treat it as the Meshtastic Default Channel Key
        if key_b64 in ["0", "NOKEY", "nokey", "NONE", "none", "HAM", "ham", "AQ=="]:
            key_b64 = "1PG7OiApB1nwvP+rz05pAQ==" # The actual default AES256 key
        
        decoded = base64.b64decode(key_b64)
        if len(decoded) not in [16, 32]: # 128 or 256 bit
             log_to_console(f"Invalid Key Length: {len(decoded)}. Using default.")
             return base64.b64decode("1PG7OiApB1nwvP+rz05pAQ==")
        return decoded
    except Exception as e:
        log_to_console(f"Key Parse Error: {e}. Using default.")
        return base64.b64decode("1PG7OiApB1nwvP+rz05pAQ==")

def log_to_console(msg, style="info"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    formatted_msg = f"[{timestamp}] {msg}"
    state.new_logs.append(formatted_msg)
    state.logs.append(formatted_msg)

def get_languages_path():
    base = get_app_path()
    candidate = os.path.join(base, LANG_FILE_NAME)
    try:
        if os.path.isfile(candidate):
            return candidate
    except Exception:
        pass
    if getattr(sys, 'frozen', False):
        try:
            embedded = get_resource_path(LANG_FILE_NAME)
            if os.path.isfile(embedded):
                return embedded
        except Exception:
            pass
    return candidate

def load_languages():
    global languages_data
    path = get_languages_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            languages_data = json.load(f)
    except Exception:
        languages_data = {}

def get_available_languages():
    if not languages_data:
        return ["en"]
    return sorted(languages_data.keys())

def translate(key: str, default: str | None = None) -> str:
    lang = current_language if current_language in languages_data else "en"
    section = languages_data.get(lang) or languages_data.get("en") or {}
    value = section.get(key)
    if value is None:
        if default is not None:
            return default
        return key
    return value

def get_app_path():
    if getattr(sys, 'frozen', False):
        system = platform.system()
        exe_dir = os.path.dirname(sys.executable)
        if system == "Darwin":
            contents_dir = os.path.dirname(exe_dir)
            app_dir = os.path.dirname(contents_dir)
            parent_dir = os.path.dirname(app_dir)
            return parent_dir
        return exe_dir
    return os.path.dirname(os.path.abspath(__file__))

def get_data_path():
    base = get_app_path()
    data_dir = os.path.join(base, "data")
    try:
        os.makedirs(data_dir, exist_ok=True)
    except Exception:
        pass
    return data_dir

def get_autosave_path():
    base = get_data_path()
    base_name = PROGRAM_NAME.replace(" ", "")
    filename = f"{base_name}-autosave.json"
    return os.path.join(base, filename)

def get_config_path():
    base = get_data_path()
    base_name = PROGRAM_NAME.replace(" ", "")
    filename = f"Config_{base_name}.json"
    return os.path.join(base, filename)

def load_user_config():
    try:
        path = get_config_path()
        if not os.path.isfile(path):
            return
        with open(path, "r") as f:
            data = json.load(f)
    except Exception as e:
        log_to_console(f"Config load error: {e}")
        return
    s = state
    v = data.get("direct_region")
    if isinstance(v, str):
        s.direct_region = v
    v = data.get("direct_preset")
    if isinstance(v, str):
        s.direct_preset = v
    v = data.get("direct_ppm")
    if v is not None:
        try:
            s.direct_ppm = int(v)
        except Exception:
            pass
    v = data.get("direct_gain")
    if v is not None:
        try:
            s.direct_gain = int(v)
        except Exception:
            pass
    v = data.get("direct_port")
    if isinstance(v, str):
        s.direct_port = v
    v = data.get("direct_key_b64")
    if isinstance(v, str):
        s.direct_key_b64 = v
    v = data.get("external_ip")
    if isinstance(v, str):
        s.external_ip = v
    v = data.get("external_port")
    if isinstance(v, str):
        s.external_port = v
    v = data.get("external_key_b64")
    if isinstance(v, str):
        s.external_key_b64 = v
    v = data.get("autosave_interval_sec")
    if v is not None:
        try:
            s.autosave_interval_sec = int(v)
        except Exception:
            pass
    v = data.get("verbose_logging")
    if isinstance(v, bool):
        s.verbose_logging = v
    v = data.get("theme")
    if isinstance(v, str):
        tv = v.strip().lower()
        if tv in ("auto", "dark", "light"):
            s.theme = "light" if tv == "auto" else tv
    v = data.get("language")
    if isinstance(v, str):
        global current_language, user_language_from_config
        current_language = v
        user_language_from_config = True

def save_user_config():
    try:
        path = get_config_path()
        data = {
            "direct_region": state.direct_region,
            "direct_preset": state.direct_preset,
            "direct_ppm": state.direct_ppm,
            "direct_gain": state.direct_gain,
            "direct_port": state.direct_port,
            "direct_key_b64": state.direct_key_b64,
            "external_ip": state.external_ip,
            "external_port": state.external_port,
            "external_key_b64": state.external_key_b64,
            "autosave_interval_sec": state.autosave_interval_sec,
            "verbose_logging": state.verbose_logging,
            "theme": getattr(state, "theme", "light"),
            "language": current_language,
        }
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        log_to_console(f"Config save error: {e}")

def check_linux_native_deps() -> bool:
    if platform.system() != "Linux":
        return True

    import ctypes

    # Required shared libs for QtWebEngine/Qt backend on Linux (Raspberry etc.)
    required = [
        ("libxcb-cursor.so.0", "libxcb-cursor0"),
        ("libminizip.so.1", "libminizip1"),
    ]

    missing = []
    for soname, pkg in required:
        try:
            ctypes.CDLL(soname)
        except OSError:
            missing.append((soname, pkg))

    if not missing:
        return True

    missing_pkgs = sorted({pkg for _, pkg in missing})
    missing_sonames = sorted({soname for soname, _ in missing})
    pkg_list = " ".join(missing_pkgs)
    soname_list = ", ".join(missing_sonames)

    msg = (
        f"Missing system libraries: {soname_list}\n"
        "These packages are required to run the native GUI on Linux.\n"
        "Install them with:\n"
        f"  sudo apt-get update && sudo apt-get install -y {pkg_list}\n"
        "Then restart this application."
    )

    if getattr(sys, "frozen", False):
        # Try to show a friendly installer prompt in a terminal first
        script = (
            f"echo 'Missing system libraries: {soname_list}'; "
            "echo; "
            "echo 'These packages are required to run the native GUI.'; "
            "echo; "
            f"echo '  sudo apt-get update && sudo apt-get install -y {pkg_list}'; "
            "echo; "
            "read -p 'Install now? [Y/n] ' ans; "
            "if [ \"$ans\" = \"\" ] || [ \"$ans\" = \"y\" ] || [ \"$ans\" = \"Y\" ]; then "
            f"sudo apt-get update && sudo apt-get install -y {pkg_list}; "
            "fi; "
            "echo; "
            "read -n1 -r -p 'Press any key to close this window...' key"
        )

        launched = False

        terminal_attempts = [
            ["x-terminal-emulator", "-e", "bash", "-lc", script],
            ["xterm", "-e", "bash", "-lc", script],
            # gnome-terminal prefers "--" on many distros
            ["gnome-terminal", "--", "bash", "-lc", script],
            ["konsole", "-e", "bash", "-lc", script],
        ]

        for cmd in terminal_attempts:
            try:
                subprocess.Popen(cmd)
                launched = True
                break
            except Exception:
                continue

        if not launched:
            # Fall back to GUI dialogs if terminals are not available
            alert_cmds = [
                ["zenity", "--info", "--title=Meshtastic GUI Analyzer", f"--text={msg}"],
                ["kdialog", "--msgbox", msg, "--title", "Meshtastic GUI Analyzer"],
                ["xmessage", "-center", msg],
            ]
            for cmd in alert_cmds:
                try:
                    subprocess.Popen(cmd)
                    launched = True
                    break
                except Exception:
                    continue

        if not launched:
            print(msg, file=sys.stderr)
    else:
        print(msg, file=sys.stderr)

    return False

def get_resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.dirname(os.path.abspath(__file__))

    return os.path.join(base_path, relative_path)

# --- MESHTASTIC DECODING LOGIC ---

def dataExtractor(data_hex):
    # Expected min length: dest(8) + sender(8) + packetID(8) + flags(2) + hash(2) + reserved(4) = 32 chars
    if len(data_hex) < 32:
        raise ValueError(f"Packet too short: {len(data_hex)} chars")

    try:
        meshPacketHex = {
            'dest' : hexStringToBinary(data_hex[0:8]),
            'sender' : hexStringToBinary(data_hex[8:16]),
            'packetID' : hexStringToBinary(data_hex[16:24]),
            'flags' : hexStringToBinary(data_hex[24:26]),
            'channelHash' : hexStringToBinary(data_hex[26:28]),
            'reserved' : hexStringToBinary(data_hex[28:32]),
            'data' : hexStringToBinary(data_hex[32:])
        }
        return meshPacketHex
    except Exception as e:
        raise ValueError(f"Extraction failed: {e}")

def dataDecryptor(meshPacketHex, aesKey):
    # Nonce must be 16 bytes.
    # Structure: packetID (4) + 0000 (4) + sender (4) + 0000 (4)
    
    p_id = meshPacketHex['packetID']
    sender = meshPacketHex['sender']
    
    # Ensure 4 bytes each
    if len(p_id) < 4: p_id = p_id.rjust(4, b'\x00')
    if len(sender) < 4: sender = sender.rjust(4, b'\x00')
    
    aesNonce = p_id + b'\x00\x00\x00\x00' + sender + b'\x00\x00\x00\x00'
    
    if len(aesNonce) != 16:
        raise ValueError(f"Invalid nonce size constructed: {len(aesNonce)}")

    cipher = Cipher(algorithms.AES(aesKey), modes.CTR(aesNonce), backend=default_backend())
    decryptor = cipher.decryptor()
    decryptedOutput = decryptor.update(meshPacketHex['data']) + decryptor.finalize()
    return decryptedOutput

def update_node(node_id, **kwargs):
    node_id = str(node_id)
    if not node_id.startswith("!"):
        try:
            node_id = f"!{int(node_id, 16):x}"
        except Exception:
            try:
                node_id = f"!{int(node_id):x}"
            except Exception:
                pass

    is_new = node_id not in state.nodes
    now_ts = time.time()
    
    if is_new:
        state.nodes[node_id] = {
            "id": node_id, 
            "last_seen": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "last_seen_ts": now_ts,
            "lat": None, "lon": None, "location_source": "Unknown", "altitude": None,
            "short_name": "???", "long_name": "Unknown",
            "hw_model": "Unknown", "role": "Unknown",
            "public_key": None, "macaddr": None, "is_unmessagable": False,
            "battery": None, "voltage": None,
            "snr": None, "rssi": None,
            "snr_indirect": None, "rssi_indirect": None,
            "hops": None, "hop_label": None,
            "temperature": None, "relative_humidity": None, "barometric_pressure": None,
            "channel_utilization": None, "air_util_tx": None, "uptime_seconds": None
        }
        state.nodes_updated = True
        state.nodes_list_updated = True # Ensure new nodes appear immediately
        log_to_console(f"New node {node_id}")

    if "hops" in kwargs:
        new_hops = kwargs.get("hops")
        if new_hops is None:
            kwargs.pop("hops", None)
            kwargs.pop("hop_label", None)
        else:
            prev_hops = state.nodes[node_id].get("hops")
            prev_ts = state.nodes[node_id].get("last_seen_ts")
            if prev_hops is not None and prev_ts is not None:
                window = 10 * 60
                if now_ts - prev_ts < window and new_hops > prev_hops:
                    kwargs.pop("hops", None)
                    kwargs.pop("hop_label", None)
    
    # Check if anything actually changed to avoid redundant updates
    changed = False
    
    # If it's a new node, we definitely have changes (initial values)
    if is_new:
        changed = True
        
    for k, v in kwargs.items():
        if state.nodes[node_id].get(k) != v:
            state.nodes[node_id][k] = v
            changed = True
    
    # Always update last seen
    state.nodes[node_id]["last_seen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    state.nodes[node_id]["last_seen_ts"] = now_ts
    
    # Force update if new data arrived or it's a new node
    # We also want to update the grid for "last_seen" changes to show real-time activity
    if changed or True: # Force update on every packet for real-time "Last Seen"
        state.nodes_updated = True
        with state.lock:
            state.dirty_nodes.add(node_id) # Track specific node for efficient delta update
        state.nodes_list_updated = True
        
        # If name changed, we might need to refresh chat history to reflect new name
        if changed and ('short_name' in kwargs or 'long_name' in kwargs):
            state.chat_force_refresh = True

def decodeProtobuf(packetData, sourceID, destID, cryptplainprefix, *, count_invalid: bool = True):
    try:
        data = mesh_pb2.Data()
        data.ParseFromString(packetData)
    except Exception as e:
        if count_invalid:
            mesh_stats.on_invalid_protobuf()
        return f"INVALID PROTOBUF: {e}"

    log_msg = ""
    decoded_obj = None # Store the parsed protobuf object for verbose logging

    msg_id = None
    try:
        if hasattr(data, "id"):
            msg_id = int(data.id)
        elif hasattr(data, "message_id"):
            msg_id = int(data.message_id)
    except Exception:
        msg_id = None

    try:
        supported_portnums = set(int(v) for v in mesh_pb2.PortNum.values())
    except Exception:
        supported_portnums = {1, 3, 4, 67, 70}
    try:
        mesh_stats.on_portnum_seen(int(data.portnum), int(data.portnum) in supported_portnums)
    except Exception:
        pass

    if data.portnum == 1: # TEXT_MESSAGE_APP
        text = data.payload.decode('utf-8', errors='ignore')
        
        if msg_id is not None and msg_id != 0:
            dedup_key = (sourceID, "MID", msg_id)
        else:
            dedup_key = (sourceID, text)
        if dedup_key in state.seen_packets:
            log_msg = f"{cryptplainprefix} DUPLICATE TEXT from {sourceID} (Ignored)"
            # We can return a log msg for debug (or could be empty string to hide completely)
            # We do NOT append to state.messages or state.new_messages
            # If you want to show it in console for logging: return log_msg
            return "" 
            
        state.seen_packets.append(dedup_key)

        # Determine Sender Name
        sender_name = sourceID
        if sourceID in state.nodes:
            n = state.nodes[sourceID]
            s_name = n.get('short_name', '???')
            l_name = n.get('long_name', 'Unknown')
            
            has_short = s_name and s_name != "???"
            has_long = l_name and l_name != "Unknown"
            
            if has_long and has_short:
                sender_name = f"{l_name} ({s_name})"
            elif has_short:
                sender_name = s_name
            elif has_long:
                sender_name = l_name

        now_dt = datetime.now()
        msg_obj = {
            "time": now_dt.strftime("%H:%M"),
            "date": now_dt.strftime("%d/%m/%Y"),
            "from": sender_name,
            "from_id": sourceID, # Store ID for dynamic name resolution
            "to": destID,
            "text": text,
            "is_me": False # We are just a listener for now
        }
        state.messages.append(msg_obj)
        state.new_messages.append(msg_obj)
        log_msg = f"{cryptplainprefix} TEXT MSG from {sourceID}: {text}"
        update_node(sourceID)
        
    elif data.portnum == 3: # POSITION_APP
        pos = mesh_pb2.Position()
        try:
            pos.ParseFromString(data.payload)
        except Exception as e:
            log_to_console(f"POSITION parse error from {sourceID}: {e}")
            return ""
        decoded_obj = pos
        lat = pos.latitude_i * 1e-7
        lon = pos.longitude_i * 1e-7
        altitude_m = None

        try:
            for desc, value in pos.ListFields():
                if desc.name in ('altitude', 'altitude_m'):
                    altitude_m = value
                    break
        except Exception:
            altitude_m = None
        
        loc_source = "Unknown"
        try:
            val = pos.location_source
            # Robust way to get Enum Name using the object's descriptor
            # This avoids issues with checking hasattr on scalar fields (which always exist in proto3)
            # and avoids hardcoding the class path if it varies by protobuf version.
            # this also ensure some future and retro compatibility.
            loc_source = pos.DESCRIPTOR.fields_by_name['location_source'].enum_type.values_by_number[val].name
        except Exception as e:
            loc_source = f"Enum_{pos.location_source}"
            print(f"Error extracting LocationSource: {e}")

        kwargs = {"lat": lat, "lon": lon, "location_source": loc_source}
        if altitude_m is not None:
            kwargs["altitude"] = altitude_m
        update_node(sourceID, **kwargs)
        log_msg = f"{cryptplainprefix} POSITION from {sourceID}: {lat}, {lon} ({loc_source})"
        
    elif data.portnum == 4: # NODEINFO_APP
        info = mesh_pb2.User()
        try:
            info.ParseFromString(data.payload)
        except Exception as e:
            log_to_console(f"NODEINFO parse error from {sourceID}: {e}")
            return ""
        decoded_obj = info
        
        role_name = "Unknown"
        try:
            if hasattr(info, 'role'):
                role_name = config_pb2.Config.DeviceConfig.Role.Name(info.role)
        except Exception:
            pass

        hw_model_name = "Unknown"
        try:
            if hasattr(info, 'hw_model'):
                hw_model_name = mesh_pb2.HardwareModel.Name(info.hw_model)
        except Exception:
            # Fallback if enum value is not known (e.g. newer firmware)
            hw_model_name = f"Model_{info.hw_model}"

        public_key = None
        try:
            pk = getattr(info, 'public_key', None)
            if isinstance(pk, (bytes, bytearray)) and pk:
                public_key = base64.b64encode(bytes(pk)).decode('ascii')
            elif isinstance(pk, str) and pk:
                s = pk.strip()
                if len(s) == 64 and all(c in "0123456789abcdefABCDEF" for c in s):
                    public_key = base64.b64encode(bytes.fromhex(s)).decode('ascii')
                else:
                    public_key = s
        except Exception:
            public_key = None

        macaddr = None
        try:
            mac_val = getattr(info, 'macaddr', None)
            if isinstance(mac_val, (bytes, bytearray)) and len(mac_val) == 6:
                macaddr = ":".join(f"{b:02x}" for b in mac_val)
            elif isinstance(mac_val, int) and mac_val:
                mac_bytes = mac_val.to_bytes(6, byteorder="big", signed=False)
                macaddr = ":".join(f"{b:02x}" for b in mac_bytes)
            elif isinstance(mac_val, str) and mac_val:
                macaddr = mac_val
        except Exception:
            macaddr = None

        is_unmessagable = None
        for field_name in ("is_unmessagable", "is_unmessageable"):
            try:
                if hasattr(info, field_name) and bool(getattr(info, field_name)):
                    is_unmessagable = True
                    break
            except Exception:
                pass

        nodeinfo_kwargs = {
            "short_name": info.short_name,
            "long_name": info.long_name,
            "hw_model": hw_model_name,
            "role": role_name,
        }
        if public_key is not None:
            nodeinfo_kwargs["public_key"] = public_key
        if macaddr is not None:
            nodeinfo_kwargs["macaddr"] = macaddr
        if is_unmessagable is not None:
            nodeinfo_kwargs["is_unmessagable"] = is_unmessagable

        update_node(sourceID, **nodeinfo_kwargs)
        log_msg = f"{cryptplainprefix} NODEINFO from {sourceID}: {info.short_name} ({info.long_name})"
        
    elif data.portnum == 67: # TELEMETRY_APP
        tel = telemetry_pb2.Telemetry()
        try:
            tel.ParseFromString(data.payload)
        except Exception as e:
            log_to_console(f"TELEMETRY parse error from {sourceID}: {e}")
            return ""
        decoded_obj = tel
        metrics = {}
        
        # Use ListFields to only capture present fields (avoiding 0 for missing fields)
        if tel.HasField('device_metrics'):
            for desc, value in tel.device_metrics.ListFields():
                if desc.name == 'battery_level':
                    metrics['battery'] = value
                elif desc.name == 'voltage':
                    metrics['voltage'] = value
                elif desc.name == 'channel_utilization':
                    metrics['channel_utilization'] = value
                elif desc.name == 'air_util_tx':
                    metrics['air_util_tx'] = value
                elif desc.name == 'uptime_seconds':
                    metrics['uptime_seconds'] = value
            
        if tel.HasField('environment_metrics'):
            for desc, value in tel.environment_metrics.ListFields():
                if desc.name == 'temperature':
                    metrics['temperature'] = value
                elif desc.name == 'relative_humidity':
                    metrics['relative_humidity'] = value
                elif desc.name == 'barometric_pressure':
                    metrics['barometric_pressure'] = value
            
        update_node(sourceID, **metrics)
        try:
            mesh_stats.on_telemetry(sourceID, metrics)
        except Exception:
            pass
        log_msg = f"{cryptplainprefix} TELEMETRY from {sourceID}"
        
    elif data.portnum == 70: # TRACEROUTE
        route = mesh_pb2.RouteDiscovery()
        try:
            route.ParseFromString(data.payload)
        except Exception as e:
            log_to_console(f"TRACEROUTE parse error from {sourceID}: {e}")
            return ""
        decoded_obj = route
        log_msg = f"{cryptplainprefix} TRACEROUTE from {sourceID}"
        update_node(sourceID) # Update last seen for traceroute source
        
    else:
        log_msg = f"{cryptplainprefix} APP Packet ({data.portnum}) from {sourceID}"
        update_node(sourceID)

    if state.verbose_logging and decoded_obj:
        try:
            # Append clean protobuf string representation
            log_msg += f"\n{decoded_obj}"
        except:
            pass

    return log_msg

# --- FRAME PARSER ---
def parse_framed_stream_bytes(rx_buf: bytearray):
    # Parse frames [type:1][len:2][payload:len] from rx_buf and process them.
    def _i16_from_be(b0, b1):
        v = (b0 << 8) | b1
        return v - 0x10000 if v & 0x8000 else v

    while True:
        if len(rx_buf) < 3:
            return

        ftype = rx_buf[0]
        flen = (rx_buf[1] << 8) | rx_buf[2]

        if len(rx_buf) < 3 + flen:
            return

        body = bytes(rx_buf[3:3 + flen])
        del rx_buf[:3 + flen]

        state.raw_packet_count += 1
        state.last_rx_ts = time.time()
        state.rx_seen_once = True

        # --- Frame type 0x03: Unified (payload + optional metrics) ---
        if ftype == 0x03:
            try:
                if len(body) < 2 + 1 + 4:
                    # payload_len(2) + flags(1) + snr_i16(2) + rssi_i16(2)
                    raise ValueError(f"Unified frame too short: {len(body)} bytes")

                payload_len = (body[0] << 8) | body[1]
                need_min = 2 + payload_len + 1 + 4
                if len(body) < need_min:
                    raise ValueError(f"Unified frame truncated: have {len(body)} need {need_min}")

                payload = body[2:2 + payload_len]

                flags_off = 2 + payload_len
                flags = body[flags_off]

                snr10 = _i16_from_be(body[flags_off + 1], body[flags_off + 2])
                rssi10 = _i16_from_be(body[flags_off + 3], body[flags_off + 4])

                has_metrics = (flags & 0x01) != 0
                snr_val = (snr10 / 10.0) if has_metrics else None
                rssi_val = (rssi10 / 10.0) if has_metrics else None

                # 1) Extract Meshtastic fields
                extracted = dataExtractor(payload.hex())

                # Hop parsing
                hops_val = None
                hop_label = None
                try:
                    flags_bytes = extracted.get('flags', b'')
                    if flags_bytes:
                        fb = flags_bytes[0]
                        hop_limit = fb & 0x07
                        hop_start = (fb >> 5) & 0x07
                        hops_val = hop_start - hop_limit
                        if hops_val < 0:
                            hops_val = 0
                        hop_label = "direct" if hops_val == 0 else str(hops_val)
                except Exception as e:
                    log_to_console(f"Hop parse error: {e}")

                try:
                    if mesh_stats.consume_crc_invalid_packet(extracted.get("sender"), extracted.get("packetID")):
                        mesh_stats.on_packet_received(None, hops_val, snr_val, rssi_val)
                        continue
                except Exception:
                    pass

                mesh_stats.on_frame_ok()

                # 2) Decode IDs Before decrypting
                s_id = msb2lsb(extracted['sender'].hex())
                d_id = msb2lsb(extracted['dest'].hex())
                s_id_fmt = f"!{int(s_id, 16):x}"
                d_id_fmt = f"!{int(d_id, 16):x}"

                try:
                    mesh_stats.on_packet_received(s_id_fmt, hops_val, snr_val, rssi_val)
                except Exception:
                    pass
                
                # 3) Try decrypt first, then fallback to plaintext if parsing fails
                #    (This approach is more robust than relying on channelHash alone,
                #     ensuring compatibility even if the field is malformed or evolves)
                info = None
                decrypted_ok = False
                plaintext_ok = False
                
                try:
                    decrypted = dataDecryptor(extracted, state.aes_key_bytes)
                    info = decodeProtobuf(decrypted, s_id_fmt, d_id_fmt, "[DECRYPTED]", count_invalid=False)
                    
                    # If protobuf parsing succeeds, packet was encrypted
                    if info and not str(info).startswith("INVALID PROTOBUF"):
                        decrypted_ok = True
                        pass
                    else:
                        # Protobuf parsing failed, try plaintext
                        raise ValueError("Protobuf parse failed, trying plaintext")
                        
                except Exception:
                    # Decryption failed, try plaintext
                    try:
                        raw_data = extracted['data']  # Use raw data
                        info = decodeProtobuf(raw_data, s_id_fmt, d_id_fmt, "[UNENCRYPTED]", count_invalid=False)
                        
                        if info and not str(info).startswith("INVALID PROTOBUF"):
                            plaintext_ok = True
                            pass
                        else:
                            info = None
                            
                    except Exception as e2:
                        log_to_console(f"[ERROR] Complete parse failure: {e2}")
                        info = None

                try:
                    if decrypted_ok:
                        mesh_stats.on_decrypt_ok()
                    elif not plaintext_ok:
                        try:
                            raw_data = extracted.get('data') or b""
                            first = raw_data[0] if raw_data else None
                            looks_like_plain_pb = first in {0x0A, 0x12, 0x1A, 0x22, 0x2A, 0x32, 0x3A, 0x42, 0x4A, 0x52}
                            if looks_like_plain_pb:
                                mesh_stats.on_invalid_protobuf()
                            else:
                                mesh_stats.on_decrypt_fail()
                        except Exception:
                            mesh_stats.on_decrypt_fail()
                except Exception:
                    pass
                
                # 4) Store metrics if available
                if info:
                    log_to_console(info)
                
                if info and not str(info).startswith("INVALID PROTOBUF"):
                    if hops_val is not None or hop_label is not None:
                        update_node(s_id_fmt, hops=hops_val, hop_label=hop_label)
                    
                    if has_metrics:
                        if hops_val == 0:
                            update_node(s_id_fmt, snr=snr_val, rssi=rssi_val)
                        else:
                            update_node(s_id_fmt, snr_indirect=snr_val, rssi_indirect=rssi_val)

            except Exception as e:
                try:
                    mesh_stats.on_frame_fail()
                    mesh_stats.on_packet_received(None, None, None, None)
                except Exception:
                    pass
                log_to_console(f"Parse Error (0x03): {e}")
            continue

        # Unknown frame type
        log_to_console(f"Unknown frame type: 0x{ftype:02X} len={flen}")

# --- ZMQ WORKER ---

def zmq_worker():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    
    log_to_console(f"Connecting to tcp://{state.ip_address}:{state.port}...")
    try:
        socket.connect(f"tcp://{state.ip_address}:{state.port}")
        socket.setsockopt(zmq.SUBSCRIBE, b'')
        log_to_console("Connected!")
    except Exception as e:
        log_to_console(f"Connection Failed: {e}")
        if state.connected and state.connect_mode == "external":
            stop_connection()
        return

    # Buffer for reconstructing frames (type+len+data) even if split across multiple recv()
    rx_buf = bytearray()

    def _i16_from_be(b0, b1):
        # Decode signed int16 from big-endian bytes
        v = (b0 << 8) | b1
        return v - 0x10000 if v & 0x8000 else v

    while state.connected and state.connect_mode == "external":
        try:
            if socket.poll(100) != 0:
                chunk = socket.recv()
                if not chunk:
                    continue

                rx_buf.extend(chunk)
                # parse frame via function
                parse_framed_stream_bytes(rx_buf)

            else:
                # Idle
                pass

        except Exception as e:
            log_to_console(f"Socket Error: {e}")
            break
            
    log_to_console("Disconnected.")
    if state.connected and state.connect_mode == "external":
        stop_connection()

# --- TCP WORKER ---

def tcp_worker():
    log_to_console(f"[INTERNAL] Connecting TCP to {state.ip_address}:{state.port} ...")
    rx_buf = bytearray()

    s = None
    max_wait = 300.0
    retry_sleep = 3.0
    connect_timeout = 2.0
    start_ts = time.time()
    attempt = 0

    while state.connected and state.connect_mode == "direct":
        if state.engine_proc is not None and state.engine_proc.poll() is not None:
            log_to_console("[INTERNAL] Engine process exited while waiting for TCP")
            s = None
            break

        elapsed = time.time() - start_ts
        if elapsed >= max_wait:
            break

        attempt += 1
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(connect_timeout)
            s.connect((state.ip_address, int(state.port)))
            s.settimeout(0.5)
            break
        except Exception as e:
            log_to_console(f"[INTERNAL][WAIT] TCP connect failed (attempt {attempt}): {e}")
            try:
                s.close()
            except Exception:
                pass
            s = None
            if (time.time() - start_ts) >= max_wait:
                break
            time.sleep(retry_sleep)

    if s is None:
        log_to_console("[INTERNAL] Giving up TCP connection to engine")
        if state.connected and state.connect_mode == "direct":
            stop_connection()
        return

    log_to_console("[INTERNAL] TCP connected (waiting for data...)")

    while state.connected and state.connect_mode == "direct":
        try:
            chunk = s.recv(4096)
            if not chunk:
                # peer closed
                log_to_console("[INTERNAL] TCP closed by peer")
                break
            rx_buf.extend(chunk)
            parse_framed_stream_bytes(rx_buf)
        except socket.timeout:
            continue
        except Exception as e:
            log_to_console(f"[INTERNAL] TCP error: {e}")
            break

    try:
        s.close()
    except Exception:
        pass

    log_to_console("[INTERNAL] TCP worker stopped")
    if state.connected and state.connect_mode == "direct":
        stop_connection()

# Start/Stop internal radio engine

def show_engine_error_dialog(message: str):
    with ui.dialog() as dlg, ui.card().classes('w-110'):
        ui.label(translate("popup.error.internalengine.title", "Internal Engine Error")).classes('text-lg font-bold mb-2 text-red-600')
        ui.label(message).classes('text-sm text-gray-800 mb-2')
        ui.label(
            translate("popup.error.internalengine.body1", "To use the internal SDR engine, the 'engine' folder with the correct runtime must be located in the same directory as this application.")
        ).classes('text-sm text-gray-700 mb-1')
        ui.label(
            translate("popup.error.internalengine.body2", "Alternatively, you can select External mode and use a GNU Radio flowgraph that is specifically configured for this GUI and its custom frame format.")
        ).classes('text-sm text-gray-700')
        ui.button('OK', on_click=dlg.close).classes('w-full mt-3 bg-red-600 text-white')
    dlg.open()

def show_rtlsdr_device_error_dialog():
    with ui.dialog() as dlg, ui.card().classes('w-110'):
        ui.label(translate("popup.error.rtlsdrdevice.title", "SDR Device Error")).classes('text-lg font-bold mb-2 text-red-600')
        ui.label(
            translate("popup.error.rtlsdrdevice.body1", "Wrong RTL-SDR device index was reported by the internal engine.")
        ).classes('text-sm text-gray-800 mb-2')
        ui.label(
            translate("popup.error.rtlsdrdevice.body2", "Please connect a compatible RTL-SDR dongle and install the correct drivers for your operating system (Windows or Linux).")
        ).classes('text-sm text-gray-700 mb-1')
        ui.label(
            translate("popup.error.rtlsdrdevice.body3", "If you need help, consult the Wiki section on the project's GitHub repository, reachable from the About menu.")
        ).classes('text-sm text-gray-700')
        ui.button(translate("button.close", "Close"), on_click=dlg.close).classes('w-full mt-3 bg-red-600 text-white')
    dlg.open()

def _engine_paths():
    system = platform.system()
    machine = platform.machine().lower()

    roots = []

    if getattr(sys, 'frozen', False):
        exe_dir = os.path.dirname(sys.executable)
        roots.append(exe_dir)
        if system == "Darwin":
            contents_dir = os.path.dirname(exe_dir)
            app_dir = os.path.dirname(contents_dir)
            parent_dir = os.path.dirname(app_dir)
            roots.append(app_dir)
            roots.append(parent_dir)
    else:
        roots.append(os.path.dirname(os.path.abspath(__file__)))

    engine_dir = None
    engine_os_root = "os"
    for r in roots:
        candidate = os.path.join(r, "engine")
        if os.path.isdir(candidate):
            engine_dir = candidate
            break
    if engine_dir is None:
        engine_dir = os.path.join(roots[0], "engine")

    if system == "Windows":
        runtime = os.path.join(engine_dir, engine_os_root, "win_x86_64", "runtime")
        py = os.path.join(runtime, "python.exe")
        return engine_dir, runtime, py, system

    if system == "Darwin":
        if machine in ("x86_64", "amd64", "i386"):
            runtime = os.path.join(engine_dir, engine_os_root, "macos_x86_64", "runtime")
        elif machine in ("arm64", "aarch64"):
            runtime = os.path.join(engine_dir, engine_os_root, "macos_arm64", "runtime")
        else:
            runtime = os.path.join(engine_dir, engine_os_root, "macos_x86_64", "runtime")
        py = os.path.join(runtime, "bin", "python")
        return engine_dir, runtime, py, system

    if system == "Linux":
        if machine in ("aarch64", "arm64"):
            runtime = os.path.join(engine_dir, engine_os_root, "linux_aarch64", "runtime")
        else:
            runtime = os.path.join(engine_dir, engine_os_root, "linux_x86_64", "runtime")
        py = os.path.join(runtime, "bin", "python")
        return engine_dir, runtime, py, system

    raise RuntimeError(f"Unsupported platform: {system} / {machine}")

def _conda_unpack_path(runtime: str, system: str) -> str | None:
    if system == "Windows":
        p = os.path.join(runtime, "Scripts", "conda-unpack.exe")
        return p if os.path.isfile(p) else None
    if system == "Linux":
        p = os.path.join(runtime, "bin", "conda-unpack")
        return p if os.path.isfile(p) else None
    return None


def ensure_conda_unpacked(runtime: str, system: str) -> None:
    # Run conda-unpack only once (portable runtime, even if not really needed, just to be sure and ensure future/backward compatibility and support for various env)
    marker = os.path.join(runtime, ".conda_unpacked_ok")
    app_path = get_app_path()
    normalized_app_path = os.path.normcase(os.path.abspath(app_path))

    stored_path = None
    if os.path.isfile(marker):
        try:
            with open(marker, "r", encoding="utf-8") as f:
                lines = [l.strip() for l in f.readlines() if l.strip()]
            for line in lines:
                if line.startswith("app_path="):
                    stored_path = line.split("=", 1)[1]
                    break
        except Exception:
            stored_path = None

        if stored_path:
            stored_path_norm = os.path.normcase(os.path.abspath(stored_path))
            if stored_path_norm == normalized_app_path:
                return

    unpack = _conda_unpack_path(runtime, system)
    if not unpack:
        # No conda-unpack available; consider it OK (common on macOS or in our actual approach)
        with open(marker, "w", encoding="utf-8") as f:
            f.write("no conda-unpack found; skipping\n")
            f.write(f"app_path={normalized_app_path}\n")
        return

    # Run from runtime dir
    try:
        creationflags = 0
        if os.name == "nt" and hasattr(subprocess, "CREATE_NO_WINDOW"):
            creationflags = subprocess.CREATE_NO_WINDOW
        p = subprocess.run(
            [unpack],
            cwd=runtime,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            creationflags=creationflags,
        )
        # Save output for debugging
        with open(os.path.join(runtime, "conda-unpack.log"), "w", encoding="utf-8") as f:
            f.write(p.stdout or "")
        if p.returncode != 0:
            raise RuntimeError(f"conda-unpack failed (code {p.returncode})")
        with open(marker, "w", encoding="utf-8") as f:
            f.write("ok\n")
            f.write(f"app_path={normalized_app_path}\n")
    except Exception as e:
        # Don't crash silently: bubble up so caller can show dialog
        raise RuntimeError(f"conda-unpack error: {e}")

def start_engine_direct():
    if state.engine_proc is not None and state.engine_proc.poll() is None:
        return

    try:
        engine_dir, runtime, py, system = _engine_paths()
    except Exception as e:
        msg = (
            "Internal SDR engine could not be started: "
            f"{e}."
        )
        log_to_console(f"[ENGINE] {msg}")
        show_engine_error_dialog(msg)
        state.engine_proc = None
        return

    if not os.path.isdir(engine_dir):
        msg = (
            "Internal SDR engine could not be started: no 'engine' folder found."
        )
        log_to_console(f"[ENGINE] {msg}")
        show_engine_error_dialog(msg)
        state.engine_proc = None
        return

    if not os.path.isdir(runtime) or not os.path.isfile(py):
        msg = (
            "Internal SDR engine could not be started: engine runtime for this "
            "platform is missing or incomplete."
        )
        log_to_console(f"[ENGINE] {msg}")
        show_engine_error_dialog(msg)
        state.engine_proc = None
        return

    try:
        ensure_conda_unpacked(runtime, system)
    except Exception as e:
        msg = f"Internal SDR engine could not prepare portable runtime: {e}"
        log_to_console(f"[ENGINE] {msg}")
        show_engine_error_dialog(msg)
        state.engine_proc = None
        return
        
    env = os.environ.copy()
    env["PYTHONPATH"] = engine_dir

    if system == "Windows":
        env["PATH"] = (
            f"{os.path.join(runtime,'Library','bin')};"
            f"{os.path.join(runtime,'Scripts')};"
            f"{runtime};"
            f"{env.get('PATH','')}"
        )
    else:
        # macOS/Linux: no DYLD_LIBRARY_PATH!
        env["PATH"] = f"{os.path.join(runtime,'bin')}:{env.get('PATH','')}"
        env["CONDA_PREFIX"] = runtime
        env["PYTHONNOUSERSITE"] = "1"

    region = getattr(state, "direct_region", "EU_868")
    preset_name = state.direct_preset
    try:
        cfg = LORA_PRESETS[preset_name][region]
        center_freq = cfg["center_freq"]
        samp_rate = cfg["samp_rate"]
        lora_bw = cfg["lora_bw"]
        sf = cfg["sf"]
    except Exception:
        msg = f"Invalid preset/region combination: {preset_name} / {region}"
        log_to_console(f"[ENGINE] {msg}")
        show_engine_error_dialog(msg)
        state.engine_proc = None
        return

    cmd = [
        py, "-m", "meshtastic_engine.run_engine",
        "--host", "127.0.0.1",
        "--port", str(state.port),
        "--device-args", "rtl=0",
        "--center-freq", str(center_freq),
        "--samp-rate",   str(samp_rate),
        "--lora-bw",     str(lora_bw),
        "--sf",          str(sf),
        "--gain",        str(int(state.direct_gain)),
        "--ppm",         str(int(state.direct_ppm)),
    ]

    try:
        creationflags = 0
        if os.name == "nt" and hasattr(subprocess, "CREATE_NO_WINDOW"):
            creationflags = subprocess.CREATE_NO_WINDOW
        state.engine_proc = subprocess.Popen(
            cmd,
            cwd=engine_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            creationflags=creationflags,
        )
    except Exception as e:
        msg = (
            "Internal SDR engine failed to start: "
            f"{e}."
        )
        log_to_console(f"[ENGINE] {msg}")
        show_engine_error_dialog(msg)
        state.engine_proc = None
        return

    def _pump():
        rtlsdr_notified = False
        ansi_re = re.compile(r"\x1b\[[0-9;]*m")
        last_rx_msg_bytes = None

        def _parse_engine_rx_msg(s: str) -> bytes | None:
            try:
                idx = s.find("rx msg:")
                if idx < 0:
                    return None
                payload = s[idx + len("rx msg:") :].strip()
                if not payload:
                    return None
                parts = [p.strip() for p in payload.split(",")]
                out = bytearray()
                for p in parts:
                    if not p:
                        continue
                    if p.lower().startswith("0x"):
                        v = int(p, 16)
                    else:
                        v = int(p, 10)
                    if v < 0 or v > 255:
                        return None
                    out.append(v)
                return bytes(out) if out else None
            except Exception:
                return None

        try:
            for line in state.engine_proc.stdout:
                line = line.rstrip("\n")
                if line:
                    log_to_console(f"[ENGINE] {line}")
                    plain = ansi_re.sub("", line)
                    if "rx msg:" in plain:
                        last_rx_msg_bytes = _parse_engine_rx_msg(plain)
                    if "CRC invalid" in plain:
                        try:
                            if last_rx_msg_bytes:
                                extracted = dataExtractor(last_rx_msg_bytes.hex())
                                mesh_stats.mark_crc_invalid_packet(extracted.get("sender"), extracted.get("packetID"))
                                last_rx_msg_bytes = None
                            mesh_stats.on_frame_fail()
                        except Exception:
                            pass
                    if (not rtlsdr_notified) and "Wrong rtlsdr device index" in line:
                        rtlsdr_notified = True
                        state.rtlsdr_error_pending = True
                        state.rtlsdr_error_text = line

        except Exception:
            pass
        rc = state.engine_proc.poll()
        if rc is not None and state.connected and state.connect_mode == "direct":
            # engine crashed while we were in direct mode
            log_to_console(f"[ENGINE] EXIT code={rc}")
            ui.notify(translate("notification.error.enginecrash", "Engine crashed/exited (code {code})".format(code=rc)), color="negative")
            stop_connection()

    threading.Thread(target=_pump, daemon=True).start()

def stop_engine_direct():
    if state.engine_proc is None:
        return
    try:
        if state.engine_proc.poll() is None:
            state.engine_proc.terminate()
            try:
                state.engine_proc.wait(timeout=3)
            except Exception:
                state.engine_proc.kill()
    except Exception:
        pass
    state.engine_proc = None


def start_connection(mode: str):
    if state.connected:
        return

    state.aes_key_bytes = parseAESKey(state.aes_key_b64)
    state.rx_seen_once = False
    state.last_rx_ts = 0.0
    state.connect_mode = mode
    state.connected = True
    mesh_stats.set_enabled(True)

    if mode == "external":
        t = threading.Thread(target=zmq_worker, daemon=True)
        t.start()
    elif mode == "direct":
        start_engine_direct()
        t = threading.Thread(target=tcp_worker, daemon=True)
        t.start()

def stop_connection():
    mode = state.connect_mode
    state.connected = False
    mesh_stats.set_enabled(False)
    if mode == "direct":
        stop_engine_direct()
    state.connect_mode = None
    set_connection_status_ui(False, mode)

def close_pyinstaller_splash():
    if getattr(sys, 'frozen', False):
        splash_anim_state['running'] = False
        try:
            import pyi_splash
            pyi_splash.close()
        except Exception:
            pass

@app.post('/shutdown_app')
async def shutdown_app(request: Request):
    token = request.query_params.get('token')
    if token != SHUTDOWN_TOKEN:
        return {'status': 'ignored'}
    def do_shutdown():
        time.sleep(0.2)
        app.shutdown()
    threading.Thread(target=do_shutdown, daemon=True).start()
    return {'status': 'shutting down'}

@app.post('/set_theme')
async def set_theme(request: Request):
    try:
        data = await request.json()
    except Exception:
        data = {}
    v = data.get('theme') if isinstance(data, dict) else None
    if isinstance(v, str):
        tv = v.strip().lower()
        if tv in ('auto', 'dark', 'light'):
            state.theme = "light" if tv == "auto" else tv
            state.chat_force_refresh = True
            save_user_config()
            return {'status': 'ok'}
    return {'status': 'ignored'}

# --- GUI ---

@ui.page('/')
def main_page():
    setup_static_files()
    load_user_config()
    if platform.system() == 'Darwin':
        ui.add_head_html('<meta name="google" content="notranslate" />')
        ui.add_head_html(f'<script>window.mesh_shutdown_token = {json.dumps(SHUTDOWN_TOKEN)};</script>')
        ui.add_head_html('''
            <script>
            window.addEventListener('beforeunload', function () {
                try {
                    if (window.sessionStorage && sessionStorage.getItem('mesh_skip_shutdown') === '1') {
                        sessionStorage.removeItem('mesh_skip_shutdown');
                        return;
                    }
                    const url = '/shutdown_app?token=' + encodeURIComponent(window.mesh_shutdown_token || '');
                    if (navigator && navigator.sendBeacon) {
                        navigator.sendBeacon(url, '');
                    } else {
                        fetch(url, {method: 'POST', keepalive: true});
                    }
                } catch (e) {}
            });
            </script>
        ''')
    else:
        close_pyinstaller_splash() # useless on MacOS (splash not working)

    # Style
    ui.add_head_html(f'<script>window.mesh_initial_theme = {json.dumps(getattr(state, "theme", "light"))};</script>')
    ui.add_head_html('''
        <script>
        (function () {
            const KEY = 'mesh_theme';
            function getSaved() {
                try { return localStorage.getItem(KEY); } catch (e) { return null; }
            }
            function save(mode) {
                try { localStorage.setItem(KEY, mode); } catch (e) { }
            }
            function updateToggle(isDark) {
                try {
                    const btn = document.getElementById('mesh-theme-toggle');
                    if (!btn) return;
                    if (isDark) btn.classList.add('is-dark');
                    else btn.classList.remove('is-dark');
                    const sun = btn.querySelector('.mesh-theme-icon.sun');
                    const moon = btn.querySelector('.mesh-theme-icon.moon');
                    if (sun) sun.style.opacity = isDark ? '0.55' : '1';
                    if (moon) moon.style.opacity = isDark ? '1' : '0.55';
                } catch (e) { }
            }
            function persistBackend(mode) {
                try {
                    fetch('/set_theme', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ theme: mode }),
                        keepalive: true,
                    });
                } catch (e) { }
            }
            function normalize(mode) {
                const v = String(mode || '').trim().toLowerCase();
                if (v === 'dark' || v === 'light') return v;
                if (v === 'auto') return 'light';
                return 'light';
            }
            function getMainMap() {
                try {
                    const id = window.mesh_main_map_id;
                    if (!id || typeof getElement !== 'function') return null;
                    const el = getElement(id);
                    const map = el && el.map;
                    return map || null;
                } catch (e) {
                    return null;
                }
            }
            function applyMapTheme(isDark) {
                try {
                    if (!window.L) return;
                    const map = getMainMap();
                    if (!map) return;

                    const canUseTiles = window.mesh_tile_internet !== false;
                    try {
                        const container = map.getContainer && map.getContainer();
                        if (container && container.style) {
                            if (canUseTiles) {
                                container.style.backgroundColor = isDark ? '#0f1a2f' : '';
                            } else {
                                container.style.backgroundColor = isDark ? '#0f1a2f' : '#aad3df';
                            }
                        }
                    } catch (e) { }

                    if (canUseTiles) {
                        const lightUrl = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
                        const lightAttr = '&copy; OpenStreetMap contributors';

                        if (!map._meshTileLayerLight) {
                            map._meshTileLayerLight = L.tileLayer(lightUrl, { maxZoom: 19, attribution: lightAttr });
                        }
                        const target = map._meshTileLayerLight;
                        map.eachLayer(function (layer) {
                            try {
                                if (layer && layer instanceof L.TileLayer && layer !== target) {
                                    map.removeLayer(layer);
                                }
                            } catch (e) { }
                        });
                        try {
                            if (!map.hasLayer(target)) target.addTo(map);
                        } catch (e) { }
                    } else {
                        map.eachLayer(function (layer) {
                            try {
                                if (layer && layer instanceof L.TileLayer) {
                                    map.removeLayer(layer);
                                }
                            } catch (e) { }
                        });
                    }

                    map.eachLayer(function (layer) {
                        try {
                            if (!layer || !(layer instanceof L.GeoJSON) || typeof layer.setStyle !== 'function') return;
                            if (!layer._meshOrigStyle) {
                                const s = (layer.options && layer.options.style) ? layer.options.style : {};
                                layer._meshOrigStyle = JSON.parse(JSON.stringify(s || {}));
                            }
                            if (!isDark) {
                                layer.setStyle(layer._meshOrigStyle);
                                return;
                            }
                            const orig = layer._meshOrigStyle || {};
                            const fillOpacity = (orig.fillOpacity !== undefined && orig.fillOpacity !== null) ? Number(orig.fillOpacity) : 0;
                            const hasFill = fillOpacity > 0;
                            const newStyle = {
                                color: '#334155',
                                weight: (orig.weight !== undefined && orig.weight !== null) ? orig.weight : 1.2,
                                opacity: (orig.opacity !== undefined && orig.opacity !== null) ? orig.opacity : 1.0,
                                fillOpacity: hasFill ? 0.92 : 0.0,
                            };
                            if (hasFill) newStyle.fillColor = '#111c33';
                            layer.setStyle(newStyle);
                        } catch (e) { }
                    });

                    try {
                        if (map._meshOfflineLabelLayer && typeof map._meshOfflineLabelLayer._redraw === 'function') {
                            map._meshOfflineLabelLayer._redraw();
                        }
                    } catch (e) { }
                } catch (e) { }
            }
            window.meshApplyThemeToMapWhenReady = (tries) => {
                let remaining = Number.isFinite(Number(tries)) ? Number(tries) : 40;
                const tick = () => {
                    try {
                        const map = getMainMap();
                        if (map) {
                            try {
                                if (map._loaded || map._initHooksCalled || map._panes) {
                                    applyMapTheme(document.documentElement.classList.contains('mesh-dark'));
                                    return;
                                }
                            } catch (e) {
                                applyMapTheme(document.documentElement.classList.contains('mesh-dark'));
                                return;
                            }
                        }
                    } catch (e) { }
                    remaining -= 1;
                    if (remaining <= 0) return;
                    try { setTimeout(tick, 120); } catch (e) { }
                };
                tick();
            };
            function apply(mode, persistLocal, persistServer) {
                const root = document.documentElement;
                const m = normalize(mode);
                const isDark = m === 'dark';
                root.classList.toggle('mesh-dark', isDark);
                root.classList.toggle('mesh-light', !isDark);
                if (persistLocal) save(isDark ? 'dark' : 'light');
                updateToggle(isDark);
                if (persistServer) persistBackend(isDark ? 'dark' : 'light');
                applyMapTheme(isDark);
            }
            window.meshGetTheme = () => {
                const root = document.documentElement;
                return root.classList.contains('mesh-dark') ? 'dark' : 'light';
            };
            window.meshSetTheme = (mode) => {
                apply(String(mode || '').toLowerCase() === 'dark' ? 'dark' : 'light', false, false);
            };
            window.meshToggleTheme = () => {
                apply(window.meshGetTheme() === 'dark' ? 'light' : 'dark', true, true);
            };

            const initial = normalize(window.mesh_initial_theme);
            if (initial === 'dark' || initial === 'light') {
                apply(initial, false, false);
            } else {
                const saved = getSaved();
                if (saved === 'dark' || saved === 'light') {
                    apply(saved, false, false);
                } else {
                    apply('light', false, false);
                }
            }

            window.addEventListener('DOMContentLoaded', () => {
                try { updateToggle(window.meshGetTheme() === 'dark'); } catch (e) { }
                try { if (window.meshApplyThemeToMapWhenReady) window.meshApplyThemeToMapWhenReady(); } catch (e) { }
            });
        })();

        window.meshNotify = (message, color) => {
            const msg = (message === null || message === undefined) ? '' : String(message);
            const col = color ? String(color) : 'positive';
            try {
                if (window.Quasar && window.Quasar.Notify && typeof window.Quasar.Notify.create === 'function') {
                    window.Quasar.Notify.create({ message: msg, color: col, timeout: 900, position: 'top' });
                    return true;
                }
            } catch (e) { }
            try {
                if (window.$q && typeof window.$q.notify === 'function') {
                    window.$q.notify({ message: msg, color: col, timeout: 900, position: 'top' });
                    return true;
                }
            } catch (e) { }
            return false;
        };

        window.meshT = (key, fallback) => {
            try {
                if (window.mesh_i18n && window.mesh_i18n[key]) return window.mesh_i18n[key];
            } catch (e) { }
            return fallback ? String(fallback) : String(key || '');
        };

        window.meshCopyToClipboard = async (text) => {
            const val = (text === null || text === undefined) ? '' : String(text);
            try {
                if (navigator && navigator.clipboard && navigator.clipboard.writeText) {
                    await navigator.clipboard.writeText(val);
                    return true;
                }
            } catch (e) { }
            try {
                const ta = document.createElement('textarea');
                ta.value = val;
                ta.style.position = 'fixed';
                ta.style.left = '-9999px';
                ta.style.top = '-9999px';
                document.body.appendChild(ta);
                ta.focus();
                ta.select();
                const ok = document.execCommand('copy');
                document.body.removeChild(ta);
                return ok;
            } catch (e) { }
            return false;
        };

        window.meshCopyCellRenderer = (params) => {
            const value = params && params.value !== undefined && params.value !== null ? String(params.value) : '';
            const isClickable = !!(params && params.data && params.data.lat && params.data.lon);
            const wrap = document.createElement('span');
            wrap.style.display = 'inline-flex';
            wrap.style.alignItems = 'center';
            wrap.style.gap = '6px';
            wrap.style.width = '100%';
            if (isClickable) {
                wrap.title = window.meshT('tooltip.openinmap.title', 'Click to open in map');
                wrap.style.cursor = 'pointer';
            }

            const textEl = document.createElement('span');
            textEl.textContent = value;
            textEl.style.userSelect = 'text';
            textEl.style.overflow = 'hidden';
            textEl.style.textOverflow = 'ellipsis';
            textEl.style.whiteSpace = 'nowrap';

            const btn = document.createElement('button');
            btn.type = 'button';
            try { btn.classList.add('mesh-copy-btn'); } catch (e) { }
            btn.textContent = '⧉';
            btn.title = window.meshT('tooltip.copytext.title', 'Copy');
            btn.style.cursor = 'pointer';
            btn.style.border = '1px solid rgba(0,0,0,0.15)';
            btn.style.borderRadius = '6px';
            btn.style.padding = '0 6px';
            btn.style.lineHeight = '16px';
            btn.style.height = '18px';
            btn.style.fontSize = '12px';
            btn.style.userSelect = 'none';

            btn.addEventListener('click', async (ev) => {
                try { ev.stopPropagation(); } catch (e) { }
                const ok = await window.meshCopyToClipboard(value);
                if (ok) {
                    window.meshNotify(window.meshT('notification.positive.copytext', 'Copied to clipboard'), 'positive');
                } else {
                    window.meshNotify(window.meshT('notification.error.copytext', 'Copy text failed'), 'negative');
                }
            });

            wrap.appendChild(textEl);
            if (value) wrap.appendChild(btn);
            return wrap;
        };

        window.upsertNodeData = (elementId, newRows) => {
            // Use global API reference registered by onGridReady
            const api = window.mesh_grid_api;
            
            if (!api) {
                // Check if we are simply not visible yet (e.g. Map tab active)
                // In this case, we don't need to panic. The data is in state.nodes.
                // When the user switches to the tab, the grid will init with full data.
                return;
            }
            
            // Check if destroyed
            if (api.isDestroyed && api.isDestroyed()) return;

            const toAdd = [];
            const toUpdate = [];
            
            newRows.forEach(row => {
                // Use getRowNode to check existence
                if (api.getRowNode(row.id)) {
                    toUpdate.push(row);
                } else {
                    toAdd.push(row);
                }
            });
            
            if (toAdd.length > 0 || toUpdate.length > 0) {
                api.applyTransaction({ add: toAdd, update: toUpdate });
            }
        };
        
        // Helper to bridge Map Popup clicks to Python
        window.goToNode = (nodeId) => {
            const input = document.querySelector('.node-target-input input');
            if (input) {
                // Always reset first to ensure change detection even for same ID
                // We use a small timeout to ensure the clear event is processed 
                // separately from the set event, guaranteeing the change triggers.
                input.value = "";
                input.dispatchEvent(new Event('input', { bubbles: true }));
                
                setTimeout(() => {
                    input.value = nodeId;
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                }, 50);
            }
        };

        window.meshEscapeHtml = (s) => {
            try {
                return String(s ?? '')
                    .replace(/&/g, '&amp;')
                    .replace(/</g, '&lt;')
                    .replace(/>/g, '&gt;')
                    .replace(/\"/g, '&quot;')
                    .replace(/'/g, '&#39;');
            } catch (e) {
                return '';
            }
        };

        window.meshNodeAgeClass = (hours) => {
            const h = Number(hours);
            if (!Number.isFinite(h)) return 'mesh-node-orange';
            if (h <= 3) return 'mesh-node-green';
            if (h <= 6) return 'mesh-node-yellow';
            return 'mesh-node-orange';
        };

        window.meshEnsureNodeLegend = (map) => {
            try {
                if (!window.L || !map) return;
                if (map._meshNodeLegendControl) return;
                const legend = window.L.control({ position: 'bottomleft' });
                legend.onAdd = function () {
                    const div = window.L.DomUtil.create('div', 'mesh-node-legend');
                    const title = (window.meshT ? window.meshT('map.legend.lastheard', 'Last Heard') : 'Last Heard');
                    div.innerHTML =
                        '<div class="mesh-node-legend-title">' + window.meshEscapeHtml(title) + '</div>' +
                        '<div class="mesh-node-legend-row"><span class="mesh-node-legend-swatch mesh-node-green"></span><span>≤ 3h</span></div>' +
                        '<div class="mesh-node-legend-row"><span class="mesh-node-legend-swatch mesh-node-yellow"></span><span>≤ 6h</span></div>' +
                        '<div class="mesh-node-legend-row"><span class="mesh-node-legend-swatch mesh-node-orange"></span><span>&gt; 6h</span></div>';
                    return div;
                };
                legend.addTo(map);
                map._meshNodeLegendControl = legend;
            } catch (e) { }
        };

        window.meshMarkerHtml = (label, cls) => {
            const text = window.meshEscapeHtml(label ?? '');
            const c = window.meshEscapeHtml(cls ?? '');
            return '<div class="mesh-node-marker ' + c + '"><div class="mesh-node-text">' + text + '</div></div>';
        };

        window.meshGetNodeMarkerDims = (map) => {
            try {
                if (map && map._meshNodeMarkerDims && typeof map._meshNodeMarkerDims === 'object') {
                    const d = map._meshNodeMarkerDims;
                    if (Number.isFinite(d.w) && Number.isFinite(d.h) && Number.isFinite(d.t)) {
                        if ((Date.now() - d.t) < 2000) return { w: d.w, h: d.h };
                    }
                }
            } catch (e) { }
            let w = 35;
            let h = 35;
            try {
                const probe = document.createElement('div');
                probe.style.position = 'fixed';
                probe.style.left = '-10000px';
                probe.style.top = '-10000px';
                probe.style.pointerEvents = 'none';
                probe.innerHTML = window.meshMarkerHtml('TEST', 'mesh-node-green');
                document.body.appendChild(probe);
                const node = probe.querySelector('.mesh-node-marker');
                if (node) {
                    const r = node.getBoundingClientRect();
                    if (r && Number.isFinite(r.width) && Number.isFinite(r.height)) {
                        w = Math.max(8, Math.round(r.width));
                        h = Math.max(8, Math.round(r.height));
                    }
                }
                document.body.removeChild(probe);
            } catch (e) { }
            try {
                if (map) map._meshNodeMarkerDims = { w: w, h: h, t: Date.now() };
            } catch (e) { }
            return { w: w, h: h };
        };

        window.meshPulseMarker = (marker, totalMs = 9000, intervalMs = 1000) => {
            try {
                if (!marker || typeof marker.getElement !== 'function') return;
                const el = marker.getElement();
                if (!el) return;
                const node = el.querySelector('.mesh-node-marker');
                if (!node) return;

                try {
                    if (marker._meshPulseInterval) {
                        window.clearInterval(marker._meshPulseInterval);
                        marker._meshPulseInterval = null;
                    }
                    if (marker._meshPulseStopTimeout) {
                        window.clearTimeout(marker._meshPulseStopTimeout);
                        marker._meshPulseStopTimeout = null;
                    }
                } catch (e) { }

                const doPulse = () => {
                    try {
                        node.classList.remove('mesh-pulse');
                        void node.offsetWidth;
                        node.classList.add('mesh-pulse');
                    } catch (e) { }
                };

                doPulse();

                const iv = Math.max(200, Number(intervalMs) || 1000);
                const total = Math.max(0, Number(totalMs) || 0);
                if (total <= 0) return;

                marker._meshPulseInterval = window.setInterval(doPulse, iv);
                marker._meshPulseStopTimeout = window.setTimeout(() => {
                    try {
                        if (marker._meshPulseInterval) {
                            window.clearInterval(marker._meshPulseInterval);
                            marker._meshPulseInterval = null;
                        }
                        marker._meshPulseStopTimeout = null;
                    } catch (e) { }
                }, total);
            } catch (e) { }
        };

        window.meshRefreshNodeMarkerColors = (map) => {
            try {
                if (!map || !map._meshNodeMarkers) return;
                const store = map._meshNodeMarkers;
                const now = (Date.now() / 1000);
                for (const nid in store) {
                    const marker = store[nid];
                    if (!marker) continue;
                    const lastSeenTs = Number(marker._mesh_last_seen_ts || 0);
                    const ageH = lastSeenTs > 0 ? ((now - lastSeenTs) / 3600) : 1e9;
                    const cls = window.meshNodeAgeClass(ageH);
                    if (typeof marker.getElement === 'function') {
                        const el = marker.getElement();
                        if (!el) continue;
                        const node = el.querySelector('.mesh-node-marker');
                        if (!node) continue;
                        node.classList.remove('mesh-node-green', 'mesh-node-yellow', 'mesh-node-orange');
                        node.classList.add(cls);
                    }
                }
            } catch (e) { }
        };

        window.meshUpsertNodesOnMap = (mapElementId, nodes) => {
            try {
                const el = (typeof getElement === 'function') ? getElement(mapElementId) : null;
                const map = el && el.map;
                if (!window.L || !map) return;
                window.meshEnsureNodeLegend(map);
                if (!map._meshNodeMarkers) map._meshNodeMarkers = {};
                const store = map._meshNodeMarkers;
                const now = (Date.now() / 1000);
                const dims = window.meshGetNodeMarkerDims(map);
                const iconW = Math.max(8, Number(dims.w) || 48);
                const iconH = Math.max(8, Number(dims.h) || 48);
                const anchorX = iconW / 2;
                const anchorY = iconH / 2;
                const popupY = -anchorY + 4;

                if (!map._meshNodeColorTimer) {
                    map._meshNodeColorTimer = window.setInterval(() => {
                        window.meshRefreshNodeMarkerColors(map);
                    }, 30000);
                }

                const list = Array.isArray(nodes) ? nodes : [];
                for (let i = 0; i < list.length; i++) {
                    const n = list[i] || {};
                    const nid = String(n.id ?? '');
                    if (!nid) continue;
                    const lat = Number(n.lat);
                    const lon = Number(n.lon);
                    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
                    const lastSeenTs = Number(n.last_seen_ts || 0);
                    const ageH = lastSeenTs > 0 ? ((now - lastSeenTs) / 3600) : 1e9;
                    const cls = window.meshNodeAgeClass(ageH);
                    const label = String(n.marker_label ?? '');
                    const popup = String(n.popup ?? '');

                    const html = window.meshMarkerHtml(label, cls);
                    const icon = window.L.divIcon({
                        className: 'mesh-node-divicon',
                        html: html,
                        iconSize: [iconW, iconH],
                        iconAnchor: [anchorX, anchorY],
                        popupAnchor: [0, popupY],
                    });

                    if (!store[nid]) {
                        const marker = window.L.marker([lat, lon], { icon: icon, interactive: true });
                        if (popup) marker.bindPopup(popup);
                        marker._mesh_last_seen_ts = lastSeenTs;
                        marker._mesh_icon_html = html;
                        marker.addTo(map);
                        store[nid] = marker;
                        continue;
                    }

                    const marker = store[nid];
                    try { marker.setLatLng([lat, lon]); } catch (e) { }
                    if (popup) {
                        try { marker.bindPopup(popup); } catch (e) { }
                    }
                    if (marker._mesh_icon_html !== html) {
                        try { marker.setIcon(icon); } catch (e) { }
                        marker._mesh_icon_html = html;
                    }
                    const prevTs = Number(marker._mesh_last_seen_ts || 0);
                    if (lastSeenTs > 0 && prevTs > 0 && lastSeenTs > prevTs + 0.5) {
                        window.meshPulseMarker(marker, 9000, 1000);
                    }
                    marker._mesh_last_seen_ts = lastSeenTs;
                }
            } catch (e) { }
        };

        window.meshOpenNodePopup = (mapElementId, nodeId) => {
            try {
                const el = (typeof getElement === 'function') ? getElement(mapElementId) : null;
                const map = el && el.map;
                if (!map || !map._meshNodeMarkers) return;
                const marker = map._meshNodeMarkers[String(nodeId ?? '')];
                if (!marker) return;
                try { marker.openPopup(); } catch (e) { }
            } catch (e) { }
        };
        </script>
        <style>
            body {
                margin: 0;
                overflow: hidden;
            }
            .ag-row.mesh-row-clickable {
                cursor: pointer;
            }
            .matrix-log {
                background-color: black;
                color: #00FF00;
                font-family: 'Courier New', monospace;
                padding: 10px;
                height: 100%;
                overflow-y: auto;
                font-size: 0.85em;
            }
            .dashboard-card {
                height: 100%;
                display: flex;
                flex-direction: column;
            }
            .support-link {
                color: white;
                text-decoration: none;
            }
            .language-select .q-field__native,
            .language-select .q-field__append {
                color: white !important;
            }
            .language-select .q-menu .q-item__label {
                text-transform: uppercase;
            }
            .language-select .q-field__native {
                text-align: center;
            }
            .mesh-label-text, .mesh-city-text {
                background: transparent !important;
                border: 0 !important;
                box-shadow: none !important;
                color: #111 !important;
                font-weight: 600;
                font-size: 12px;
                padding: 0 !important;
                margin: 0 !important;
                pointer-events: none !important;  /* IMPORTANT: don't steal clicks from node markers */
                user-select: none !important;
            }
            .mesh-city-text {
                font-weight: 500;
                font-size: 11px;
            }
            .mesh-offline-label-canvas {
                position: absolute;
                top: 0;
                left: 0;
                pointer-events: none;
            }
            .mesh-offline-loading .q-dialog__backdrop {
                background: rgba(0, 0, 0, 0.78) !important;
            }
            .mesh-node-divicon {
                background: transparent !important;
                border: 0 !important;
            }
            .mesh-node-marker {
                width: 35px;
                height: 35px;
                border-radius: 999px;
                display: flex;
                align-items: center;
                justify-content: center;
                font-weight: 800;
                font-size: 9px;
                letter-spacing: 0.5px;
                color: rgba(255,255,255,0.98);
                border: 1px solid rgba(116,116,116,0.92);
                box-shadow: 0 8px 18px rgba(0,0,0,0.28);
                position: relative;
                user-select: none;
            }
            .mesh-node-text {
                pointer-events: none;
                text-shadow: 0 1px 2px rgba(0,0,0,0.35);
            }
            .mesh-node-green { background: #4CAF50; }
            .mesh-node-yellow { background: #FBBF24; color: rgba(17,24,39,0.95); border-color: rgba(255,255,255,0.92); }
            .mesh-node-yellow .mesh-node-text { text-shadow: none; }
            .mesh-node-orange { background: #FB923C; }

            .mesh-node-marker.mesh-pulse::after {
                content: '';
                position: absolute;
                left: 50%;
                top: 50%;
                width: 100%;
                height: 100%;
                border-radius: 999px;
                transform: translate(-50%, -50%) scale(1);
                opacity: 0.75;
                box-shadow: 0 0 0 3px rgba(255,255,255,0.65);
                animation: meshPulse 1.4s ease-out 1;
                pointer-events: none;
            }
            @keyframes meshPulse {
                0% { transform: translate(-50%, -50%) scale(1); opacity: 0.75; }
                100% { transform: translate(-50%, -50%) scale(2.35); opacity: 0; }
            }

            .mesh-node-legend {
                background: rgba(255,255,255,0.92);
                border: 1px solid rgba(0,0,0,0.12);
                border-radius: 10px;
                padding: 8px 10px;
                font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
                font-size: 12px;
                color: #111827;
                box-shadow: 0 8px 18px rgba(0,0,0,0.20);
            }
            .mesh-node-legend-title {
                font-weight: 700;
                margin-bottom: 6px;
            }
            .mesh-node-legend-row {
                display: flex;
                align-items: center;
                gap: 8px;
                line-height: 1.2;
                margin-top: 4px;
            }
            .mesh-node-legend-swatch {
                width: 14px;
                height: 14px;
                border-radius: 999px;
                border: 2px solid rgba(255,255,255,0.92);
                box-shadow: 0 2px 6px rgba(0,0,0,0.18);
                display: inline-block;
            }

            .mesh-theme-toggle {
                display: inline-flex;
                align-items: center;
                gap: 8px;
                height: 30px;
                padding: 0 10px;
                border-radius: 999px;
                border: 1px solid rgba(255,255,255,0.25);
                background: linear-gradient(135deg, rgba(250,204,21,0.35), rgba(251,146,60,0.25));
                box-shadow: 0 10px 25px rgba(0,0,0,0.22);
                cursor: pointer;
                user-select: none;
                transition: background 180ms ease, border-color 180ms ease, transform 100ms ease;
            }
            .mesh-theme-toggle:hover {
                transform: translateY(-1px);
            }
            .mesh-theme-toggle:active {
                transform: translateY(0);
            }
            .mesh-theme-toggle.is-dark {
                border-color: rgba(59,130,246,0.45);
                background: linear-gradient(135deg, rgba(30,58,138,0.55), rgba(2,6,23,0.85));
                box-shadow: 0 10px 25px rgba(2,6,23,0.55);
            }
            .mesh-theme-icon {
                font-family: 'Material Icons';
                font-size: 18px;
                line-height: 18px;
                display: inline-block;
                transition: opacity 180ms ease;
            }
            .mesh-theme-icon.sun { color: #fde68a; text-shadow: 0 1px 10px rgba(250,204,21,0.35); }
            .mesh-theme-icon.moon { color: #93c5fd; text-shadow: 0 1px 10px rgba(59,130,246,0.35); }

            .mesh-dark body {
                background: #0b1220;
                color: #e5e7eb;
            }
            .mesh-dark .q-layout,
            .mesh-dark .q-page-container {
                background: #0b1220 !important;
                color: #e5e7eb;
            }
            .mesh-dark .q-tab-panels,
            .mesh-dark .q-tab-panel,
            .mesh-dark .nicegui-tab-panel {
                background: #0f172a !important;
                color: #e5e7eb !important;
            }
            .mesh-dark .q-card,
            .mesh-dark .q-dialog__inner > div {
                background: #0f172a !important;
                color: #e5e7eb !important;
                border: 1px solid rgba(148,163,184,0.16);
            }
            .mesh-dark .q-menu,
            .mesh-dark .q-list {
                background: #0f172a !important;
                color: #e5e7eb !important;
                border: 1px solid rgba(148,163,184,0.16);
            }
            .mesh-dark .q-item__label,
            .mesh-dark .q-item__section {
                color: #e5e7eb !important;
            }
            .mesh-dark .q-separator {
                background: rgba(148,163,184,0.16) !important;
            }
            .mesh-dark .leaflet-popup-content-wrapper,
            .mesh-dark .leaflet-popup-tip {
                background: #0f172a !important;
                color: #e5e7eb !important;
                border: 1px solid rgba(148,163,184,0.16);
            }
            .mesh-dark .leaflet-control-attribution {
                background: rgba(2,6,23,0.62) !important;
                color: rgba(226,232,240,0.8) !important;
            }
            .mesh-dark .mesh-node-legend {
                background: rgba(15,23,42,0.92);
                border-color: rgba(148,163,184,0.18);
                color: #e5e7eb;
            }

            .mesh-muted { color: #4b5563; }
            .mesh-chat-meta { font-size: 0.75rem; color: #6b7280; }

            .mesh-dark .mesh-muted { color: #94a3b8 !important; }
            .mesh-dark .mesh-chat-meta { color: #94a3b8 !important; }

            .mesh-dark .bg-gray-50 { background-color: rgba(15,23,42,0.92) !important; }
            .mesh-dark .bg-gray-100 { background-color: #0f172a !important; }
            .mesh-dark .bg-slate-50 { background-color: rgba(15,23,42,0.92) !important; }

            .mesh-dark .text-gray-500 { color: #94a3b8 !important; }
            .mesh-dark .text-gray-600 { color: #cbd5e1 !important; }
            .mesh-dark .text-gray-700 { color: #e5e7eb !important; }
            .mesh-dark .text-slate-900 { color: #e5e7eb !important; }
            .mesh-dark .text-blue-500 { color: #60a5fa !important; }
            .mesh-dark .hover\\:text-blue-600:hover { color: #93c5fd !important; }

            .mesh-dark .border,
            .mesh-dark .border-b {
                border-color: rgba(148,163,184,0.16) !important;
            }

            .mesh-dark .q-field__control,
            .mesh-dark .q-field__native,
            .mesh-dark .q-field__marginal {
                color: #e5e7eb !important;
            }
            .mesh-dark .q-field--filled .q-field__control,
            .mesh-dark .q-field--outlined .q-field__control {
                background: rgba(2,6,23,0.55) !important;
            }
            .mesh-dark .q-field__control:before,
            .mesh-dark .q-field__control:after {
                border-color: rgba(148,163,184,0.22) !important;
            }
            .mesh-dark .q-field__label {
                color: #cbd5e1 !important;
            }
            .mesh-dark .q-field--highlighted .q-field__label,
            .mesh-dark .q-field--focused .q-field__label {
                color: #93c5fd !important;
            }
            .mesh-dark .q-field__bottom,
            .mesh-dark .q-field__messages,
            .mesh-dark .q-field__hint {
                color: #94a3b8 !important;
            }

            .mesh-dark .q-message-text {
                color: #e5e7eb !important;
            }
            .mesh-dark .q-message-name {
                color: #cbd5e1 !important;
            }

            .mesh-dark .ag-root-wrapper,
            .mesh-dark .ag-root-wrapper-body,
            .mesh-dark .ag-center-cols-clipper {
                background: #0b1220 !important;
                color: #e5e7eb !important;
            }
            .mesh-dark .ag-header,
            .mesh-dark .ag-header-row,
            .mesh-dark .ag-header-cell,
            .mesh-dark .ag-header-group-cell {
                background: #0f172a !important;
                color: #e5e7eb !important;
                border-color: rgba(148,163,184,0.16) !important;
            }
            .mesh-dark .ag-row {
                background: #0b1220 !important;
                color: #e5e7eb !important;
                border-color: rgba(148,163,184,0.10) !important;
            }
            .mesh-dark .ag-row-hover {
                background: rgba(59,130,246,0.10) !important;
            }
            .mesh-dark .ag-cell {
                border-color: rgba(148,163,184,0.08) !important;
            }
            .mesh-dark .ag-paging-panel,
            .mesh-dark .ag-status-bar {
                background: #0f172a !important;
                color: #e5e7eb !important;
                border-color: rgba(148,163,184,0.16) !important;
            }
            .mesh-dark .ag-input-field-input,
            .mesh-dark .ag-filter-filter,
            .mesh-dark .ag-text-field-input {
                background: rgba(2,6,23,0.55) !important;
                color: #e5e7eb !important;
                border-color: rgba(148,163,184,0.22) !important;
            }
            .mesh-dark .ag-popup-child {
                background: #0f172a !important;
                color: #e5e7eb !important;
                border: 1px solid rgba(148,163,184,0.16) !important;
            }
            .mesh-dark .leaflet-container {
                background: #0b1220 !important;
            }
            .mesh-dark .leaflet-tile-pane {
                filter: invert(100%) hue-rotate(200deg) brightness(1.8) contrast(0.9) saturate(0.5);
            }

            .mesh-copy-btn {
                background: rgba(255,255,255,0.9);
            }
            .mesh-dark .mesh-copy-btn {
                background: rgb(67 67 67 / 90%);
                border-color: rgba(148,163,184,0.22) !important;
                color: #e5e7eb;
            }

            .mesh-dark * {
                scrollbar-color: rgba(148,163,184,0.35) rgba(2,6,23,0.55);
            }
            .mesh-dark *::-webkit-scrollbar {
                width: 10px;
                height: 10px;
            }
            .mesh-dark *::-webkit-scrollbar-track {
                background: rgba(2,6,23,0.55);
            }
            .mesh-dark *::-webkit-scrollbar-thumb {
                background: rgba(148,163,184,0.35);
                border-radius: 10px;
                border: 2px solid rgba(2,6,23,0.55);
            }
            .mesh-dark *::-webkit-scrollbar-thumb:hover {
                background: rgba(148,163,184,0.52);
            }
        </style>
        <script>
        (function () {
            function ensurePane(map) {
                try {
                    if (!map.getPane('meshOfflineLabels')) {
                        var pane = map.createPane('meshOfflineLabels');
                        pane.style.zIndex = 450;
                        pane.style.pointerEvents = 'none';
                        try { pane.classList.add('leaflet-zoom-animated'); } catch (e) { }
                    }
                } catch (e) { }
            }

            function createLayer(map) {
                ensurePane(map);
                var Layer = L.Layer.extend({
                    initialize: function () {
                        this._labels = [];
                        this._zoom = null;
                    },
                    onAdd: function (map) {
                        this._map = map;
                        var pane = map.getPane('meshOfflineLabels') || map.getPane('overlayPane');
                        this._canvas = L.DomUtil.create('canvas', 'mesh-offline-label-canvas', pane);
                        this._ctx = this._canvas.getContext('2d');
                        try { this._canvas.style.opacity = '1'; } catch (e) { }
                        this._updateSize();
                        map.on('move resize zoomend', this._redraw, this);
                        map.on('zoomstart', this._hide, this);
                        map.on('zoomend', this._show, this);
                        this._redraw();
                    },
                    onRemove: function (map) {
                        map.off('move resize zoomend', this._redraw, this);
                        map.off('zoomstart', this._hide, this);
                        map.off('zoomend', this._show, this);
                        if (this._canvas && this._canvas.parentNode) {
                            this._canvas.parentNode.removeChild(this._canvas);
                        }
                        this._map = null;
                        this._canvas = null;
                        this._ctx = null;
                    },
                    _hide: function () {
                        try { if (this._canvas) this._canvas.style.opacity = '0'; } catch (e) { }
                    },
                    _show: function () {
                        try { if (this._canvas) this._canvas.style.opacity = '1'; } catch (e) { }
                        this._redraw();
                    },
                    setLabels: function (labels, zoom) {
                        this._labels = Array.isArray(labels) ? labels : [];
                        this._zoom = zoom;
                        this._redraw();
                    },
                    _updateSize: function () {
                        if (!this._map || !this._canvas) return;
                        var size = this._map.getSize();
                        var ratio = window.devicePixelRatio || 1;
                        this._canvas.width = Math.round(size.x * ratio);
                        this._canvas.height = Math.round(size.y * ratio);
                        this._canvas.style.width = size.x + 'px';
                        this._canvas.style.height = size.y + 'px';
                        if (this._ctx) {
                            this._ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
                        }
                    },
                    _redraw: function () {
                        if (!this._map || !this._canvas || !this._ctx) return;
                        if (this._map._animatingZoom || this._map._zooming) return;
                        this._updateSize();
                        var ctx = this._ctx;
                        var size = this._map.getSize();
                        try {
                            var topLeft = this._map.containerPointToLayerPoint([0, 0]);
                            L.DomUtil.setPosition(this._canvas, topLeft);
                        } catch (e) { }
                        ctx.clearRect(0, 0, size.x, size.y);
                        var zoom = this._zoom;
                        if (zoom === null || zoom === undefined) {
                            zoom = this._map.getZoom();
                        }
                        var topLeft2 = null;
                        try { topLeft2 = this._map.containerPointToLayerPoint([0, 0]); } catch (e) { }
                        var placed = [];
                        function overlaps(r) {
                            for (var i = 0; i < placed.length; i++) {
                                var p = placed[i];
                                if (!(r.x + r.w < p.x || p.x + p.w < r.x || r.y + r.h < p.y || p.y + p.h < r.y)) {
                                    return true;
                                }
                            }
                            return false;
                        }

                        for (var i = 0; i < this._labels.length; i++) {
                            var lab = this._labels[i];
                            if (!lab || typeof lab.text !== 'string') continue;
                            var lat = lab.lat;
                            var lon = lab.lon;
                            if (typeof lat !== 'number' || typeof lon !== 'number') continue;
                            var pt;
                            try {
                                var lp = this._map.latLngToLayerPoint([lat, lon]);
                                if (topLeft2) {
                                    pt = lp.subtract(topLeft2);
                                } else {
                                    pt = this._map.latLngToContainerPoint([lat, lon]);
                                }
                            } catch (e) {
                                pt = this._map.latLngToContainerPoint([lat, lon]);
                            }
                            if (!pt) continue;
                            if (pt.x < -50 || pt.y < -50 || pt.x > size.x + 50 || pt.y > size.y + 50) continue;

                            var kind = lab.kind || 'label';
                            var fontSize = (kind === 'city') ? 11 : 12;
                            var weight = (kind === 'city') ? '500' : '600';
                            var darkMode = false;
                            try { darkMode = document.documentElement.classList.contains('mesh-dark'); } catch (e) { }
                            var color = darkMode ? '#e5e7eb' : '#111';
                            if (kind === 'country') {
                                fontSize = 12;
                                weight = '700';
                            }
                            if (kind === 'province') {
                                fontSize = 10;
                                weight = '600';
                                color = darkMode ? '#94a3b8' : '#444';
                            }
                            ctx.font = weight + ' ' + fontSize + 'px sans-serif';
                            ctx.fillStyle = color;
                            ctx.textAlign = 'center';
                            ctx.textBaseline = 'middle';
                            var w = ctx.measureText(lab.text).width;
                            var h = fontSize + 4;
                            var rect = { x: pt.x - w / 2 - 2, y: pt.y - h / 2, w: w + 4, h: h };
                            if (overlaps(rect)) continue;
                            placed.push(rect);
                            ctx.fillText(lab.text, pt.x, pt.y);
                        }
                    },
                });

                var layer = new Layer();
                layer.addTo(map);
                return layer;
            }

            window.meshOfflineLabels = {
                set: function (map, labels, zoom) {
                    if (!window.L) return;
                    this._pending = { labels: labels, zoom: zoom };
                    var m = map || window.mesh_offline_map;
                    if (!m) return;
                    if (!m._meshOfflineLabelLayer) {
                        m._meshOfflineLabelLayer = createLayer(m);
                    }
                    try {
                        m._meshOfflineLabelLayer.setLabels(labels, zoom);
                    } catch (e) { }
                },
            };
        })();
        </script>
    ''')

    load_languages()
    js_i18n = {
        "tooltip.openinmap.title": translate("tooltip.openinmap.title", "Click to open in map"),
        "tooltip.copytext.title": translate("tooltip.copytext.title", "Copy"),
        "notification.positive.copytext": translate("notification.positive.copytext", "Copied to clipboard"),
        "notification.error.copytext": translate("notification.error.copytext", "Copy text failed"),
        "map.legend.lastheard": translate("map.legend.lastheard", "Last Heard"),
        "button.toggletheme": translate("button.toggletheme", "Toggle Theme"),
    }
    ui.run_javascript(f'window.mesh_i18n = {json.dumps(js_i18n)};')

    with ui.header().classes('bg-slate-900 text-white'):
        with ui.row().classes('items-center gap-3 self-center'):
            ui.label(f'{PROGRAM_NAME} - {PROGRAM_SHORT_DESC}').classes('text-xl font-bold')
            ui.html(f'''
                <button id="mesh-theme-toggle" class="mesh-theme-toggle" type="button" onclick="try{{window.meshToggleTheme();}}catch(e){{}}" title="{translate("button.toggletheme", "Toggle Theme")}">
                    <span class="mesh-theme-icon sun">light_mode</span>
                    <span class="mesh-theme-icon moon">dark_mode</span>
                </button>
            ''', sanitize=False)
        ui.space()
        with ui.link('', DONATION_URL, new_tab=True).classes('mr-4 support-link inline-flex items-center whitespace-nowrap self-center'):
            ui.icon('favorite').classes('text-pink-400 mr-1')
            ui.label(translate("header.support", "Support the project"))
        available_langs = get_available_languages()
        pending_language_change = {"value": None}
        with ui.dialog() as language_change_dialog:
            with ui.card().classes('w-110'):
                ui.label(
                    translate("popup.language_change.requires_disconnect.title", "Disconnect required")
                ).classes('text-lg font-bold mb-2')
                ui.label(
                    translate(
                        "popup.language_change.requires_disconnect.body",
                        "To change language, disconnect first. The app will reload to apply the new language.",
                    )
                ).classes('text-sm text-gray-700 mb-3')
                with ui.row().classes('w-full justify-end gap-2'):
                    ui.button(
                        translate("button.cancel", "Cancel"),
                        on_click=lambda: (pending_language_change.__setitem__("value", None), language_change_dialog.close()),
                    ).classes('bg-slate-200 text-slate-900')

                    async def disconnect_and_reload_for_language_change():
                        global current_language, user_language_from_config
                        requested_lang = pending_language_change.get("value")
                        pending_language_change["value"] = None
                        language_change_dialog.close()
                        if state.connected:
                            stop_connection()
                            ui.notify(translate("status.disconnected", "Disconnected"))
                        if isinstance(requested_lang, str) and requested_lang in available_langs:
                            current_language = requested_lang
                            user_language_from_config = True
                            save_user_config()
                            try:
                                await ui.run_javascript("sessionStorage.setItem('mesh_skip_shutdown','1'); location.reload()")
                            except Exception:
                                pass

                    ui.button(
                        translate("button.disconnect", "Disconnect"),
                        on_click=disconnect_and_reload_for_language_change,
                    ).classes('bg-blue-600 text-white')

        async def on_language_change(e):
            global current_language, user_language_from_config
            value = e.value
            if isinstance(value, str) and value in available_langs:
                if value == current_language:
                    return
                if state.connected:
                    if language_select_ref is not None:
                        language_select_ref.value = current_language
                    pending_language_change["value"] = value
                    language_change_dialog.open()
                    return
                current_language = value
                user_language_from_config = True
                save_user_config()
                try:
                    await ui.run_javascript("sessionStorage.setItem('mesh_skip_shutdown','1'); location.reload()")
                except Exception:
                    pass
        global language_select_ref
        language_select_ref = ui.select(
            options=available_langs,
            value=current_language if current_language in available_langs else "en",
            on_change=on_language_change,
        ).props("dense options-dense borderless").style(
            'color: white; text-transform: uppercase;'
        ).classes('mr-2 w-auto self-center language-select')
        status_label = ui.label(translate("status.disconnected", "Disconnected")).classes('text-red-500 font-bold mr-4 self-center')
        global status_label_ref
        status_label_ref = status_label
        

    if not user_language_from_config:
        async def _auto_detect_language():
            try:
                lang = await ui.run_javascript("navigator.language || navigator.userLanguage || 'en'", timeout=5.0)
                if not isinstance(lang, str):
                    return
                lang = lang.split('-')[0].lower()
                available = get_available_languages()
                if lang in available:
                    global current_language
                    current_language = lang
                    if language_select_ref is not None:
                        language_select_ref.value = lang
                    save_user_config()
                    await ui.run_javascript("sessionStorage.setItem('mesh_skip_shutdown','1'); location.reload()")
            except Exception:
                pass

        ui.timer(0.1, _auto_detect_language, once=True)

    def on_direct_ppm_change(e):
        if e.value in (None, ''):
            return
        try:
            state.direct_ppm = int(e.value)
        except (TypeError, ValueError):
            pass
        save_user_config()

    def on_direct_gain_change(e):
        if e.value in (None, ''):
            return
        try:
            state.direct_gain = int(e.value)
        except (TypeError, ValueError):
            pass
        save_user_config()

    def update_config_field(name):
        def inner(e):
            setattr(state, name, e.value)
            save_user_config()
        return inner

    def on_autosave_interval_change(e):
        try:
            if e.value in (None, ''):
                v = 0
            else:
                v = int(e.value)
        except Exception:
            v = 0
        if v < 0:
            v = 0
        state.autosave_interval_sec = v
        save_user_config()

    def _rtlsdr_error_ui_tick():
        if not state.rtlsdr_error_pending:
            return
        state.rtlsdr_error_pending = False
        ui.notify(translate("notification.error.rtlsdrdevice", "SDR device error: Wrong RTL-SDR device index."), color="negative")
        show_rtlsdr_device_error_dialog()

    ui.timer(0.2, _rtlsdr_error_ui_tick)

    with ui.dialog() as connection_dialog:
        with ui.card().classes('w-110').style('height: 100%; max-height: 760px'):
            with ui.scroll_area().style('height: 100%;'):
                with ui.column().classes('w-full'):
                    ui.label(translate("panel.connection.settings.title", "Connection Settings")).classes('text-lg font-bold mb-2')
                    with ui.tabs().classes('w-full mb-2') as tabs:
                        tab_direct = ui.tab(translate("panel.connection.settings.internaltab", "Internal"))
                        tab_ext = ui.tab(translate("panel.connection.settings.externaltab", "External"))

                    with ui.tab_panels(tabs, value=tab_direct).classes('w-full'):
                        with ui.tab_panel(tab_direct):
                            ui.label(translate("panel.connection.settings.internal.title", "Internal SDR Engine")).classes('font-bold mb-0')
                            ui.markdown(translate("panel.connection.settings.internal.help", 'The app manages the internal SDR engine for you.<br> Just select Region, Channel, PPM for your device and a suitable RF Gain.')).classes('text-sm text-gray-600')
                            ui.select(
                                ['EU_868', 'EU_433', 'US_915'],
                                value=state.direct_region,
                                on_change=update_config_field('direct_region'),
                                label=translate("panel.connection.settings.internal.label.region", "Region")
                            ).props('dense').classes('w-full mb-0')
                            ui.select(
                                list(LORA_PRESETS.keys()),
                                value=state.direct_preset,
                                on_change=update_config_field('direct_preset'),
                                label=translate("panel.connection.settings.internal.label.channel", "Channel")
                            ).props('dense').classes('w-full mb-0')
                            ui.number(
                                translate("panel.connection.settings.internal.label.ppm", "PPM correction"),
                                value=state.direct_ppm,
                                on_change=on_direct_ppm_change
                            ).props('dense').classes('w-full mb-0')
                            ui.number(
                                translate("panel.connection.settings.internal.label.rf_gain", "RF Gain"),
                                value=state.direct_gain,
                                on_change=on_direct_gain_change
                            ).props('dense').classes('w-full mb-0')
                            ui.input(
                                translate("panel.connection.settings.internal.label.port", "Port (don't change if everything works)"),
                                value=state.direct_port,
                                on_change=update_config_field('direct_port')
                            ).classes('w-full mb-0')
                            ui.input(
                                translate("panel.connection.settings.internal.label.aes_key", "AES Key (Base64)"),
                                value=state.direct_key_b64,
                                on_change=update_config_field('direct_key_b64')
                            ).classes('w-full mb-1')
                            with ui.row().classes('w-full justify-end gap-2'):
                                ui.button(translate("button.cancel", "Cancel"), on_click=connection_dialog.close).classes('bg-slate-200 text-slate-900')
                                ui.button(translate("button.connect", "Connect"), on_click=lambda: ( _do_connect_direct(), connection_dialog.close() )).classes('bg-blue-600 text-white')

                        with ui.tab_panel(tab_ext):
                            ui.label(translate("panel.connection.settings.external.title", "External GNU Radio / ZMQ stream")).classes('font-bold mb-1')
                            ui.label(translate("panel.connection.settings.external.help1", "Requires an external specific (our custom frame) GNU Radio flowgraph with a ZMQ PUB block.")).classes('text-sm text-gray-600')
                            ui.label(translate("panel.connection.settings.external.help2", "Configure here the IP, port and AES key of that source.")).classes('text-sm text-gray-600 mb-1')
                            ui.input(
                                translate("panel.connection.settings.external.label.ip", "IP Address"),
                                value=state.external_ip,
                                on_change=update_config_field('external_ip')
                            ).classes('w-full mb-1')
                            ui.input(
                                translate("panel.connection.settings.external.label.port", "Port"),
                                value=state.external_port,
                                on_change=update_config_field('external_port')
                            ).classes('w-full mb-1')
                            ui.input(
                                translate("panel.connection.settings.external.label.aes_key", "AES Key (Base64)"),
                                value=state.external_key_b64,
                                on_change=update_config_field('external_key_b64')
                            ).classes('w-full mb-2')
                            with ui.row().classes('w-full justify-end gap-2'):
                                ui.button(translate("button.cancel", "Cancel"), on_click=connection_dialog.close).classes('bg-slate-200 text-slate-900')
                                ui.button(translate("button.connect", "Connect"), on_click=lambda: ( _do_connect_external(), connection_dialog.close() )).classes('bg-blue-600 text-white')

    def _do_connect_direct():
        state.ip_address = "127.0.0.1"
        state.port = state.direct_port
        state.aes_key_b64 = state.direct_key_b64
        save_user_config()
        start_connection("direct")
        set_connection_status_ui(True, "direct")
        ui.notify(translate("notification.positive.directenginestarting", "Direct engine starting..."), color='positive')

    def _do_connect_external():
        state.ip_address = state.external_ip
        state.port = state.external_port
        state.aes_key_b64 = state.external_key_b64
        save_user_config()
        start_connection("external")
        set_connection_status_ui(True, "external")
        ui.notify(translate("notification.positive.externalconnect", "External connect..."), color='positive')

    def _import_data_from_dict(data):
        imported_nodes_count = 0
        total_nodes_in_file = 0

        def _msg_signature(msg: dict):
            if not isinstance(msg, dict):
                return None
            msg_id_val = msg.get('id', None)
            if msg_id_val is None:
                msg_id_val = msg.get('message_id', None)
            if msg_id_val is None:
                msg_id_val = msg.get('mid', None)
            if msg_id_val is not None:
                return (
                    str(msg.get('from_id', '')).strip(),
                    str(msg.get('to', '')).strip(),
                    str(msg_id_val).strip(),
                )
            return (
                str(msg.get('from_id', '')).strip(),
                str(msg.get('to', '')).strip(),
                str(msg.get('date', '')).strip(),
                str(msg.get('time', '')).strip(),
                str(msg.get('text', '')).strip(),
                str(msg.get('is_me', '')).strip(),
            )

        if "nodes" in data:
            total_nodes_in_file = len(data["nodes"])
            for k, v in data["nodes"].items():
                try:
                    node_id_int = int(k)
                    canonical_id = f"!{node_id_int:x}"
                except ValueError:
                    canonical_id = k
                v['id'] = canonical_id
                try:
                    raw_unmsg = v.get("is_unmessagable", None)
                    if raw_unmsg is None:
                        raw_unmsg = v.get("is_unmessageable", None)
                    if raw_unmsg is None:
                        raw_unmsg = v.get("isUnmessagable", None)
                    if raw_unmsg is None:
                        raw_unmsg = v.get("isUnmessageable", None)

                    if isinstance(raw_unmsg, bool):
                        v["is_unmessagable"] = raw_unmsg
                    elif isinstance(raw_unmsg, (int, float)):
                        v["is_unmessagable"] = bool(raw_unmsg)
                    elif isinstance(raw_unmsg, str):
                        s = raw_unmsg.strip().lower()
                        v["is_unmessagable"] = s in {"1", "true", "yes", "y", "on"}
                    else:
                        v["is_unmessagable"] = False
                except Exception:
                    v["is_unmessagable"] = False
                if 'last_seen_ts' not in v:
                    v['last_seen_ts'] = time.time()
                state.nodes[canonical_id] = v
                imported_nodes_count += 1

            state.nodes_updated = True
            state.nodes_list_updated = True
            state.nodes_list_force_refresh = True
            state.chat_force_refresh = True

        if "messages" in data:
            imported_msgs = data["messages"]
            existing_sigs = set()
            for existing_msg in state.messages:
                sig = _msg_signature(existing_msg)
                if sig is not None:
                    existing_sigs.add(sig)

            unique_imported_msgs = []
            for msg in imported_msgs:
                if not isinstance(msg, dict):
                    continue
                if 'from_id' not in msg:
                    sender_val = msg.get('from', '')
                    if sender_val.startswith('!'):
                        msg['from_id'] = sender_val
                    else:
                        for nid, n in state.nodes.items():
                            if n.get('short_name') == sender_val or n.get('long_name') == sender_val or f"{n.get('long_name')} ({n.get('short_name')})" == sender_val:
                                msg['from_id'] = nid
                                break

                sig = _msg_signature(msg)
                if sig is None or sig in existing_sigs:
                    continue
                existing_sigs.add(sig)
                unique_imported_msgs.append(msg)

            if unique_imported_msgs:
                state.messages.extend(unique_imported_msgs)
                state.new_messages.extend(unique_imported_msgs)
                state.chat_force_scroll = True

        if "mesh_stats" in data:
            try:
                mesh_stats.load_from_dict(data.get("mesh_stats"))
            except Exception:
                pass

        return imported_nodes_count, total_nodes_in_file

    def _extract_meshtastic_nodes_from_info_text(content: str) -> dict:
        if not isinstance(content, str):
            raise ValueError("Invalid content type")
        marker = "Nodes in mesh:"
        idx = content.find(marker)
        if idx < 0:
            raise ValueError("No 'Nodes in mesh:' section found")
        brace_idx = content.find("{", idx)
        if brace_idx < 0:
            raise ValueError("Malformed 'Nodes in mesh:' section")
        decoder = json.JSONDecoder()
        nodes_obj, _ = decoder.raw_decode(content[brace_idx:])
        if not isinstance(nodes_obj, dict):
            raise ValueError("Nodes section is not a JSON object")
        return nodes_obj

    def _node_from_meshtastic_cli(node_id: str, node_entry: dict) -> dict:
        if not isinstance(node_id, str):
            node_id = str(node_id)
        if not isinstance(node_entry, dict):
            node_entry = {}

        user = node_entry.get("user") if isinstance(node_entry.get("user"), dict) else {}
        pos = node_entry.get("position") if isinstance(node_entry.get("position"), dict) else {}
        metrics = node_entry.get("deviceMetrics") if isinstance(node_entry.get("deviceMetrics"), dict) else {}

        num = node_entry.get("num")
        if num is None:
            try:
                if node_id.startswith("!"):
                    num = int(node_id[1:], 16)
            except Exception:
                num = None

        lat = pos.get("latitude")
        lon = pos.get("longitude")
        if lat is None:
            try:
                lat_i = pos.get("latitudeI")
                if lat_i is not None:
                    lat = float(lat_i) * 1e-7
            except Exception:
                lat = None
        if lon is None:
            try:
                lon_i = pos.get("longitudeI")
                if lon_i is not None:
                    lon = float(lon_i) * 1e-7
            except Exception:
                lon = None

        last_seen_ts = None
        try:
            last_heard = node_entry.get("lastHeard")
            if last_heard is not None:
                last_seen_ts = float(last_heard)
        except Exception:
            last_seen_ts = None
        if last_seen_ts is None:
            last_seen_ts = time.time()

        last_seen_str = None
        try:
            last_seen_str = datetime.fromtimestamp(last_seen_ts).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            last_seen_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        hops = node_entry.get("hopsAway")
        hop_label = None
        try:
            if hops is not None:
                hops = int(hops)
                hop_label = "direct" if hops == 0 else str(hops)
        except Exception:
            hops = None
            hop_label = None

        is_unmessagable = False
        try:
            if "isUnmessagable" in user:
                is_unmessagable = bool(user.get("isUnmessagable"))
            elif "isUnmessageable" in user:
                is_unmessagable = bool(user.get("isUnmessageable"))
            elif "is_unmessagable" in user:
                is_unmessagable = bool(user.get("is_unmessagable"))
            elif "is_unmessageable" in user:
                is_unmessagable = bool(user.get("is_unmessageable"))
        except Exception:
            is_unmessagable = False

        return {
            "id": node_id,
            "num": num,
            "last_seen": last_seen_str,
            "last_seen_ts": last_seen_ts,
            "lat": lat,
            "lon": lon,
            "location_source": pos.get("locationSource") or pos.get("location_source") or "Unknown",
            "altitude": pos.get("altitude"),
            "short_name": user.get("shortName") or user.get("short_name") or "???",
            "long_name": user.get("longName") or user.get("long_name") or "Unknown",
            "hw_model": user.get("hwModel") or user.get("hw_model") or "Unknown",
            "role": user.get("role") or node_entry.get("role") or "Unknown",
            "public_key": user.get("publicKey") or user.get("public_key"),
            "macaddr": user.get("macaddr"),
            "is_unmessagable": is_unmessagable,
            "battery": metrics.get("batteryLevel") if isinstance(metrics.get("batteryLevel"), (int, float)) else None,
            "voltage": metrics.get("voltage") if isinstance(metrics.get("voltage"), (int, float)) else None,
            "snr": node_entry.get("snr") if isinstance(node_entry.get("snr"), (int, float)) else None,
            "rssi": node_entry.get("rssi") if isinstance(node_entry.get("rssi"), (int, float)) else None,
            "snr_indirect": None,
            "rssi_indirect": None,
            "hops": hops,
            "hop_label": hop_label,
            "temperature": None,
            "relative_humidity": None,
            "barometric_pressure": None,
            "channel_utilization": metrics.get("channelUtilization") if isinstance(metrics.get("channelUtilization"), (int, float)) else None,
            "air_util_tx": metrics.get("airUtilTx") if isinstance(metrics.get("airUtilTx"), (int, float)) else None,
            "uptime_seconds": metrics.get("uptimeSeconds") if isinstance(metrics.get("uptimeSeconds"), (int, float)) else None,
        }

    def _import_meshtastic_info_text(content: str):
        nodes_cli = _extract_meshtastic_nodes_from_info_text(content)
        nodes = {}
        for nid, entry in nodes_cli.items():
            if not isinstance(nid, str):
                nid = str(nid)
            if not nid.startswith("!"):
                try:
                    nid = f"!{int(nid):x}"
                except Exception:
                    pass
            nodes[nid] = _node_from_meshtastic_cli(nid, entry)
        return _import_data_from_dict({"nodes": nodes})

    # Import Dialog
    with ui.dialog() as import_dialog, ui.card().classes('w-96'):
        ui.label(translate("popup.importdata.title", "Import Data")).classes('text-lg font-bold mb-2')
        ui.label(translate("popup.importdata.help", "Select a JSON file to import nodes and messages.")).classes('text-sm text-gray-600 mb-4')
        
        async def handle_upload(e):
            try:
                content = None
                if hasattr(e, 'content'):
                    read_result = e.content.read()
                    if asyncio.iscoroutine(read_result):
                        read_result = await read_result
                    content = read_result.decode('utf-8')
                elif hasattr(e, 'files') and e.files:
                    read_result = e.files[0].content.read()
                    if asyncio.iscoroutine(read_result):
                        read_result = await read_result
                    content = read_result.decode('utf-8')
                elif hasattr(e, 'file') and hasattr(e.file, 'read'):
                    read_result = e.file.read()
                    if asyncio.iscoroutine(read_result):
                         read_result = await read_result
                    content = read_result.decode('utf-8')
                elif hasattr(e, 'file') and hasattr(e.file, 'file') and hasattr(e.file.file, 'read'):
                     read_result = e.file.file.read()
                     if asyncio.iscoroutine(read_result):
                         read_result = await read_result
                     content = read_result.decode('utf-8')
                
                if content is None:
                    # Fallback for debugging if we can't find it
                    raise ValueError(f"Could not extract content from upload event. Attributes: {dir(e)}")

                imported_nodes_count = 0
                total_nodes_in_file = 0
                try:
                    data = json.loads(content)
                    imported_nodes_count, total_nodes_in_file = _import_data_from_dict(data)
                except Exception:
                    imported_nodes_count, total_nodes_in_file = _import_meshtastic_info_text(content)
                    
                import_dialog.close()
                
                # Show Summary Dialog
                with ui.dialog() as summary_dialog, ui.card().classes('w-96'):
                    ui.label(translate("popup.importdata.success.title", "Import Summary")).classes('text-xl font-bold text-green-600 mb-4')
                    
                    with ui.column().classes('w-full gap-2'):
                         ui.label(translate("popup.importdata.success.nodesinfile", "Nodes in File: {nodes_count}").format(nodes_count=total_nodes_in_file)).classes('text-lg')
                         ui.label(translate("popup.importdata.success.nodesimported", "Nodes Imported: {nodes_imported_count}").format(nodes_imported_count=imported_nodes_count)).classes('text-lg font-bold')
                         ui.separator()
                         ui.label(translate("popup.importdata.success.totalnodesinapp", "Total Nodes in App: {total_nodes_in_app}").format(total_nodes_in_app=len(state.nodes))).classes('text-md text-gray-600')
                    
                    ui.button('OK', on_click=summary_dialog.close).classes('w-full mt-4 bg-green-600')
                summary_dialog.open()
                
            except Exception as ex:
                print(f"Import Error: {ex}")
                ui.notify(translate("popup.importdata.failed.importfailed", "Import Failed: {error}").format(error=ex), type='negative')

        # Custom Dropzone Area Container
        with ui.element('div').classes('w-full h-32 relative border-2 border-dashed border-blue-300 rounded-lg hover:bg-blue-50 transition-colors group flex items-center justify-center'):
             # Visuals (Centered)
             with ui.column().classes('items-center gap-0 pointer-events-none'):
                 ui.icon('upload_file', size='3em').classes('text-blue-400 group-hover:scale-110 transition-transform')
                 ui.label(translate("popup.importdata.body1", "Drop JSON File Here")).classes('text-blue-600 font-bold text-lg')
                 ui.label(translate("popup.importdata.body2", "or click to select")).classes('text-blue-400 text-sm')

             # Invisible Uploader Overlay
             # We position it absolutely to cover the parent, make it invisible (opacity-0)
             # This captures both clicks and drops.
             uploader = ui.upload(on_upload=handle_upload, auto_upload=True) \
                .props('accept=.json,.txt flat unbordered hide-upload-btn max-files=1') \
                .classes('absolute inset-0 w-full h-full opacity-0 z-10 cursor-pointer')
             
             # Manually trigger picker on click because q-uploader background isn't clickable by default
             uploader.on('click', lambda: uploader.run_method('pickFiles'))
        
        # Fallback for systems where overlay events fail (e.g. Linux GTK Webview)
        # A clear, standard button that sits outside the overlay logic
        with ui.expansion(translate("popup.importdata.fallback", "Trouble uploading? Click here for standard button"), icon='help_outline').classes('w-full text-sm text-gray-500'):
             ui.upload(on_upload=handle_upload, auto_upload=True, label=translate("popup.importdata.fallback.label", "Standard Upload")).props('accept=.json,.txt color=blue').classes('w-full')
        
        ui.button(translate("button.cancel", "Cancel"), on_click=import_dialog.close).classes('w-full mt-2')


    with ui.dialog() as autosave_dialog, ui.card().classes('w-96'):
        ui.label(translate("popup.autosave.title", "Autosave Settings")).classes('text-lg font-bold mb-2')
        ui.label(translate("popup.autosave.help", "Configure automatic export interval in seconds (0 disables).")).classes('text-sm text-gray-600 mb-4')
        ui.number(
            translate("popup.autosave.label.label", "Interval (seconds, 0 = off)"),
            value=state.autosave_interval_sec,
            on_change=on_autosave_interval_change
        ).props('dense').classes('w-full mb-4')
        with ui.row().classes('w-full justify-end gap-2'):
            ui.button('OK', on_click=autosave_dialog.close).classes('bg-slate-200 text-slate-900')

    with ui.dialog() as about_dialog, ui.card().classes('w-96'):
        ui.label(translate("about.title", "About")).classes('text-lg font-bold mb-2')
        ui.label(f'{PROGRAM_NAME} - {PROGRAM_SHORT_DESC}').classes('text-md font-semibold')
        with ui.row().classes('w-full items-center gap-2'):
            ui.label(translate("about.version", "Version: {version}").format(version=VERSION)).classes('text-sm text-gray-600')
            about_update_status = ui.html("", sanitize=False).classes('text-sm')
        ui.label(translate("about.author", "Author: {author}").format(author=AUTHOR)).classes('text-sm text-gray-600 mb-1')
        current_year = datetime.now().year
        copyright_year = 2026
        copyright_year_label = f"{copyright_year}" if current_year <= copyright_year else f"{copyright_year}-{current_year}"
        ui.label(f'Copyright © {copyright_year_label} {AUTHOR}').classes('text-xs text-gray-600')
        ui.label(translate("about.license", "License: {license}").format(license=LICENSE)).classes('text-xs text-gray-600')
        ui.link(translate("about.view_license", "View License"), f'{GITHUB_URL}/blob/main/LICENSE', new_tab=True).classes('text-xs text-blue-500 mb-2')
        ui.separator().classes('my-2')
        ui.label(
            translate(
                "about.description",
                f"{PROGRAM_NAME} is a graphical tool to decode and analyze/debug/store Meshtastic packets.",
            ).format(program=PROGRAM_NAME)
        ).classes('text-sm text-gray-600 mb-2')
        ui.label(translate("about.no_warranty", "This program comes with ABSOLUTELY NO WARRANTY.")).classes('text-xs text-red-500 mb-1')
        ui.label(
            translate(
                "about.not_affiliated",
                "This software is not affiliated with Meshtastic; it is developed by an independent enthusiast.",
            )
        ).classes('text-xs text-gray-600 mb-2')
        ui.label(
            translate(
                "about.repo_help",
                "For bug reports, feature requests and help, please visit the official GitHub repository:",
            )
        ).classes('text-sm text-gray-600 mb-2')
        ui.link(GITHUB_URL, GITHUB_URL, new_tab=True).classes('text-blue-500 mb-2')
        with ui.row().classes('w-full justify-end mt-2'):
            ui.button(translate("button.close", "Close"), on_click=about_dialog.close).classes('bg-slate-200 text-slate-900')

    with ui.dialog().props('persistent') as update_dialog, ui.card().classes('w-[560px]'):
        update_popup_title = ui.label(translate("update.popup.title", "Update available")).classes('text-lg font-bold mb-2')
        update_popup_body = ui.label("").classes('text-sm text-gray-700 whitespace-pre-line mb-3')
        update_popup_link = ui.html("", sanitize=False).classes('mb-3')
        with ui.row().classes('w-full justify-end'):
            update_popup_ok = ui.button('OK').classes('bg-slate-200 text-slate-900')

    _update_popup_state = {"tag": None}

    def _ack_update_popup(_e=None):
        tag = _update_popup_state.get("tag")
        if tag:
            state.update_popup_ack_version = tag
        update_dialog.close()

    update_popup_ok.on_click(_ack_update_popup)

    def _set_about_update_status(is_update_available: bool, release_url: str | None):
        if not about_update_status:
            return
        if is_update_available:
            label = html.escape(translate("update.status.update", "Update"))
            url = html.escape(release_url or GITHUB_RELEASES_URL, quote=True)
            about_update_status.content = f'<a class="text-blue-600 underline font-semibold" href="{url}" target="_blank">{label}</a>'
        else:
            label = html.escape(translate("update.status.updated", "Updated"))
            about_update_status.content = f'<span class="text-green-600 font-semibold">{label}</span>'

    async def _check_for_updates(show_popup: bool):
        if getattr(state, "update_check_running", False):
            return
        if getattr(state, "update_check_done", False) and not show_popup:
            return
        state.update_check_running = True
        try:
            info = await asyncio.to_thread(_fetch_latest_github_release, 10.0)
            if not info:
                return
            latest_tag = info.get("tag").replace("v", "").replace("V", "")
            release_url = info.get("url") or GITHUB_RELEASES_URL
            is_update = _is_newer_version(VERSION, latest_tag)
            state.latest_version = latest_tag
            state.latest_release_url = release_url
            state.update_available = bool(is_update)
            state.update_check_done = True
            _set_about_update_status(is_update, release_url)
            if is_update and show_popup and getattr(state, "update_popup_ack_version", None) != latest_tag:
                _update_popup_state["tag"] = latest_tag
                update_popup_title.text = translate("update.popup.title", "Update available")
                update_popup_body.text = translate(
                    "update.popup.body",
                    "A new version is available.\nCurrent: {current}\nLatest: {latest}",
                ).format(current=VERSION, latest=latest_tag)
                link_label = html.escape(translate("update.popup.open_release", "Open release page"))
                url = html.escape(release_url, quote=True)
                update_popup_link.content = f'<a class="text-blue-600 underline" href="{url}" target="_blank">{link_label}</a>'
                update_dialog.open()
        finally:
            state.update_check_running = False

    ui.timer(2.0, lambda: asyncio.create_task(_check_for_updates(show_popup=True)), once=True)

    # Main Layout
    with ui.row().classes('w-full h-[calc(100vh-80px)] no-wrap'):
        
        # Left Column: Navigation/Controls (Small)
        with ui.column().classes('w-16 bg-gray-100 p-2 items-center h-full'):
            
            with ui.button(icon='menu').props('flat round'):
                with ui.menu():
                    def toggle_connection():
                        if state.connected:
                            stop_connection()
                            ui.notify(translate("status.disconnected", "Disconnected"))
                        else:
                            state.connection_dialog_shown = True
                            connection_dialog.open()
                    
                    conn_menu_item = ui.menu_item(on_click=toggle_connection)
                    with conn_menu_item:
                        ui.icon('power_settings_new').classes('mr-2 mt-auto mb-auto')
                        conn_label = ui.label(translate("menu.connect", "Connect")).classes('mt-auto mb-auto')
                    
                    def update_menu_text():
                         new_text = translate("menu.disconnect", "Disconnect") if state.connected else translate("menu.connect", "Connect")
                         if conn_label.text != new_text:
                             conn_label.text = new_text

                    ui.timer(0.5, update_menu_text)

                    def open_tx_goal_dialog():
                        with ui.dialog() as dlg, ui.card().classes('w-110'):
                            ui.label(translate("tx_goal.title", "TX Mode (Goal)")).classes('text-lg font-bold mb-2')
                            ui.label(translate("tx_goal.milestone", "🚀 Next Milestone: Secure TX Implementation")).classes('text-sm mb-2')
                            ui.label(translate("tx_goal.why_title", "Why a goal for TX?")).classes('text-sm font-semibold mb-1')
                            ui.label(
                                translate(
                                    "tx_goal.why_body",
                                    'MeshStation is an open project developed in my spare time. Moving from an '
                                    '"Analyzer" to a "Transceiver" is a major leap that requires dedicated time, '
                                    'deep protocol study, and specific hardware for stress-testing.',
                                )
                            ).classes('text-sm mb-2')
                            ui.label(translate("tx_goal.doing_title", "Doing it the right way:")).classes('text-sm font-semibold mb-1')
                            ui.label(
                                translate(
                                    "tx_goal.doing_body",
                                    'We are not just "enabling" a button. To protect the mesh ecosystem and ensure '
                                    'reliability, the TX implementation will focus on:',
                                )
                            ).classes('text-sm mb-1')
                            ui.label(
                                translate(
                                    "tx_goal.bullet.collision",
                                    '- Collision Avoidance: Professional management of the Duty Cycle to respect '
                                    'regulatory limits and network airtime.',
                                )
                            ).classes('text-sm mb-1')
                            ui.label(
                                translate(
                                    "tx_goal.bullet.identity",
                                    '- Unique Identity: Automatic ID assignment based on MAC Address to prevent '
                                    'clones and network conflicts.',
                                )
                            ).classes('text-sm mb-1')
                            ui.label(
                                translate(
                                    "tx_goal.bullet.integrity",
                                    '- Network Integrity: Rigorous testing to ensure MeshStation remains a '
                                    '"good citizen" on the RF spectrum.',
                                )
                            ).classes('text-sm mb-2')
                            ui.label(translate("tx_goal.support_title", "Your support makes this possible.")).classes('text-sm font-semibold mb-1')
                            ui.label(
                                translate(
                                    "tx_goal.support_body",
                                    'Development funds will go directly towards dedicated testing hardware and will '
                                    'allow me to allocate more time away from my daily job to speed up the release.',
                                )
                            ).classes('text-sm mb-2')
                            ui.link(
                                translate("tx_goal.donation_link", "Link for donations"),
                                DONATION_URL,
                                new_tab=True,
                            ).classes('text-sm text-blue-500 mb-2')
                            ui.button(translate("button.close", "Close"), on_click=dlg.close).classes('w-full mt-2 bg-slate-200 text-slate-900')
                        dlg.open()

                    with ui.menu_item(on_click=open_tx_goal_dialog):
                        ui.icon('radio_button_checked').classes('mr-2 mt-auto mb-auto')
                        ui.label(translate("menu.tx_goal", "TX Mode (Goal)")).classes('mt-auto mb-auto')

                    def toggle_verbose():
                        state.verbose_logging = not state.verbose_logging
                        state_text = "ON" if state.verbose_logging else "OFF"
                        verbose_label.text = translate("menu.verbose", "Verbose Log: {state}").format(state=state_text)
                        ui.notify(verbose_label.text)
                        save_user_config()
                        
                    verbose_item = ui.menu_item(on_click=toggle_verbose)
                    with verbose_item:
                        ui.icon('bug_report').classes('mr-2 mt-auto mb-auto')
                        initial_state = "ON" if state.verbose_logging else "OFF"
                        verbose_label = ui.label(
                            translate("menu.verbose", "Verbose Log: {state}").format(state=initial_state)
                        ).classes('mt-auto mb-auto')

                    def _collect_export_data():
                        return {
                            "nodes": state.nodes,
                            "messages": list(state.messages),
                            "logs": list(state.logs),
                            "mesh_stats": mesh_stats.to_dict(),
                        }

                    def open_support_dialog():
                        with ui.dialog() as dlg, ui.card().classes('w-110'):
                            ui.label(translate("support_dialog.title", "Support the Project")).classes('text-lg font-bold mb-2')
                            ui.label(
                                translate(
                                    "support_dialog.body",
                                    'If you enjoy MeshStation and want to support its development, you can help '
                                    'by contributing a donation using the official donation link.',
                                )
                            ).classes('text-sm mb-2')
                            ui.label(
                                translate(
                                    "support_dialog.tiers",
                                    'Donors in specific tiers will be listed in the Top Contributors section of the project.',
                                )
                            ).classes('text-sm mb-2')
                            ui.link(
                                translate("support_dialog.top_contributors", "View Top Contributors"),
                                SUPPORTERS_URL,
                                new_tab=True,
                            ).classes('text-sm text-blue-500 mb-2')
                            ui.link(
                                translate("support_dialog.donation_page", "Donation page"),
                                DONATION_URL,
                                new_tab=True,
                            ).classes('text-sm text-blue-500 mb-2')
                            ui.button(translate("button.close", "Close"), on_click=dlg.close).classes('w-full mt-2 bg-slate-200 text-slate-900')
                        dlg.open()

                    def export_data():
                        try:
                            data = _collect_export_data()
                            
                            # Set locale to system default to get local date format
                            try:
                                locale.setlocale(locale.LC_TIME, '')
                            except:
                                pass

                            # Get formatted date string based on system locale
                            # %c is "Locale's appropriate date and time representation"
                            timestamp = datetime.now().strftime("%c")
                            
                            # Sanitize for filename (replace invalid chars like : / \ with - or _)
                            # Remove spaces completely to avoid problems with file name
                            safe_timestamp = timestamp.replace(":", "-").replace("/", "-").replace("\\", "-").replace(" ", "_")
                            
                            filename = f"{PROGRAM_NAME}_{safe_timestamp}.json".replace(" ", "")
                            
                            # Save in the application directory
                            export_path = os.path.join(get_app_path(), filename)
                            
                            with open(export_path, 'w') as f:
                                json.dump(data, f, indent=4)
                            
                            # Popup confirmation
                            with ui.dialog() as saved_dialog, ui.card():
                                ui.label(translate("popup.exportdata.success.title", "Export Successful")).classes('text-lg font-bold text-green-500')
                                ui.label(translate("popup.exportdata.success.filename", "File saved: {filename}").format(filename=filename))
                                ui.label(translate("popup.exportdata.success.location", "Location: {location}").format(location=get_app_path()))
                                ui.button(translate("button.close", "Close"), on_click=saved_dialog.close).classes('w-full')
                            
                            saved_dialog.open()
                            
                        except Exception as e:
                            ui.notify(translate("popup.exportdata.success.exportfailed", "Export Failed: {error}").format(error=e), type='negative')
                    
                    def _autosave_tick():
                        if state.autosave_interval_sec is None or state.autosave_interval_sec <= 0:
                            return
                        now = time.time()
                        if state.autosave_last_ts and (now - state.autosave_last_ts) < state.autosave_interval_sec:
                            return
                        try:
                            data = _collect_export_data()
                            if not data.get("nodes") and not data.get("messages"):
                                return
                            autosave_path = get_autosave_path()
                            with open(autosave_path, 'w') as f:
                                json.dump(data, f, indent=4)
                            state.autosave_last_ts = now
                        except Exception as e:
                            log_to_console(f"Autosave error: {e}")
                    
                    ui.separator()

                    with ui.menu_item(on_click=export_data):
                        ui.icon('file_download').classes('mr-2 mt-auto mb-auto')
                        ui.label(translate("menu.export", "Export Data")).classes('mt-auto mb-auto')

                    with ui.menu_item(on_click=lambda: import_dialog.open()):
                        ui.icon('file_upload').classes('mr-2 mt-auto mb-auto')
                        ui.label(translate("menu.import", "Import Data")).classes('mt-auto mb-auto')

                    with ui.menu_item(on_click=lambda: autosave_dialog.open()):
                        ui.icon('schedule').classes('mr-2 mt-auto mb-auto')
                        ui.label(translate("menu.autosave", "Autosave/Autoexport")).classes('mt-auto mb-auto')

                    def open_about():
                        asyncio.create_task(_check_for_updates(show_popup=False))
                        about_dialog.open()

                    ui.separator()

                    with ui.menu_item(on_click=open_support_dialog):
                        ui.icon('volunteer_activism').classes('mr-2 mt-auto mb-auto')
                        ui.label(translate("menu.support", "Support the Project")).classes('mt-auto mb-auto')

                    with ui.menu_item(on_click=open_about):
                        ui.icon('info').classes('mr-2 mt-auto mb-auto')
                        ui.label(translate("menu.about", "About")).classes('mt-auto mb-auto')

            ui.tooltip('Menu')
            ui.separator()
            
    # Center: Dashboard (Map & Chat)
        with ui.splitter(value=60).classes('w-full h-[99%]') as splitter:
            
            with splitter.before:
                with ui.tabs().classes('w-full') as tabs:
                    map_tab = ui.tab(translate("ui.nodesmap", "Nodes Map"))
                    nodes_tab = ui.tab(translate("ui.nodeslist", "Nodes List"))
                    overview_tab = ui.tab(translate("ui.meshoverview", "Mesh Overview"))
                
                # Hidden Bridge for Map Interaction
                target_node_input = ui.input().classes('hidden node-target-input')
                
                def process_node_filter(e):
                    val = e.value
                    if val:
                        tabs.set_value(nodes_tab)
                        ui.run_javascript(f'''
                            if (window.mesh_grid_api) {{
                                if (typeof window.mesh_grid_api.setGridOption === 'function') {{
                                    window.mesh_grid_api.setGridOption('quickFilterText', "{val}");
                                }} else if (typeof window.mesh_grid_api.setQuickFilter === 'function') {{
                                    window.mesh_grid_api.setQuickFilter("{val}");
                                }}
                            }}
                        ''')
                        ui.notify(translate("notification.positive.filteringnode", "Filtering node: {val}").format(val=val))
                        target_node_input.value = None # Reset for next click
                        
                target_node_input.on_value_change(process_node_filter)

                with ui.tab_panels(tabs, value=map_tab).props('keep-alive').classes('w-full h-full'):
                    
                    # MAP PANEL
                    with ui.tab_panel(map_tab).classes('p-0 h-full'):
                        m = ui.leaflet(center=(41.9, 12.5), zoom=6).classes('w-full h-full')

                        tile_internet = has_tile_internet()
                        ui.run_javascript(
                            f"window.mesh_main_map_id = {json.dumps(m.id)}; window.mesh_tile_internet = {json.dumps(tile_internet)};"
                        )
                        ui.run_javascript("try { if (window.meshApplyThemeToMapWhenReady) { window.meshApplyThemeToMapWhenReady(); } } catch (e) {}")

                        if not tile_internet:
                            offline_loading_dialog = ui.dialog().props('persistent').classes('mesh-offline-loading')
                            with offline_loading_dialog:
                                with ui.card().classes('w-96'):
                                    ui.icon('wifi_off').classes('text-4xl text-gray-500')
                                    ui.label(translate("popup.alert.offlinemaps.title", "Offline Maps Are Loading")).classes('text-lg font-bold text-red-500')
                                    ui.label(translate("popup.alert.offlinemaps.body", "No internet connection.\nLoading offline maps, please wait...")).classes('text-base font-semibold whitespace-pre-line')
                                    ui.label(translate("popup.alert.offlinemaps.help", "Offline maps are less detailed, while still retaining the most important details\nin a small file size for a worldwide map.")).classes('text-xs text-gray-500 whitespace-pre-line')

                            _offline_refresh = {'fn': None}
                            _last_view_payload = {'bounds': None, 'zoom': None}
                            _view_task = {'handle': None}
                            from nicegui import background_tasks

                            async def _debounced_refresh():
                                await asyncio.sleep(0.02)
                                b = _last_view_payload.get('bounds')
                                z = _last_view_payload.get('zoom')
                                fn = _offline_refresh.get('fn')
                                if fn and b and z is not None:
                                    fn(b, z)

                            async def _poll_view():
                                if not _offline_refresh.get('fn'):
                                    return
                                try:
                                    with m:
                                        payload = await ui.run_javascript("""
                                            (() => {
                                                try {
                                                    const el = getElement(%s);
                                                    const map = el && el.map;
                                                    if (!map) return null;
                                                    if (map._animatingZoom || map._zooming) {
                                                        return {animating: true};
                                                    }
                                                    const b = map.getBounds();
                                                    return {
                                                        animating: false,
                                                        zoom: map.getZoom(),
                                                        bounds: {south: b.getSouth(), west: b.getWest(), north: b.getNorth(), east: b.getEast()},
                                                    };
                                                } catch (e) {
                                                    return null;
                                                }
                                            })()
                                        """ % json.dumps(m.id), timeout=1.0)
                                except Exception:
                                    return
                                if not isinstance(payload, dict):
                                    return
                                if payload.get('animating'):
                                    return
                                b = payload.get('bounds') or {}
                                z = payload.get('zoom')
                                if not b or z is None:
                                    return

                                if _last_view_payload.get('bounds') == b and _last_view_payload.get('zoom') == z:
                                    return

                                _last_view_payload['bounds'] = b
                                _last_view_payload['zoom'] = z

                                try:
                                    h = _view_task.get('handle')
                                    if h and not h.done():
                                        h.cancel()
                                except Exception:
                                    pass
                                _view_task['handle'] = asyncio.create_task(_debounced_refresh())

                            ui.timer(0.25, lambda: background_tasks.create(_poll_view()))

                            async def _load_offline_map():
                                try:
                                    with m:
                                        offline_loading_dialog.open()
                                    await m.client.connected()
                                    while not m.is_initialized:
                                        await asyncio.sleep(0.05)

                                    topo = await asyncio.to_thread(get_offline_topology)
                                    if not topo:
                                        return

                                    def _build_offline_layers(topo_obj: dict):
                                        detected_names = _detect_topo_object_names(topo_obj)
                                        countries_name = _pick_topo_object_name(topo_obj, [
                                            'ne_10m_admin_0_countries',
                                            'Admin-0 countries',
                                            'Admin-0 Countries',
                                            'admin_0_countries',
                                            'countries',
                                        ]) or detected_names.get('countries')

                                        admin1_name = _pick_topo_object_name(topo_obj, [
                                            'ne_10m_admin_1_states_provinces',
                                            'Admin-1 states provinces',
                                            'Admin-1 States Provinces',
                                            'admin_1_states_provinces',
                                            'admin1',
                                            'states_provinces',
                                        ]) or detected_names.get('admin1')

                                        regions_name = _pick_topo_object_name(topo_obj, [
                                            'ne_10m_admin_1_regions',
                                            'admin_1_regions',
                                            'regions',
                                        ]) or detected_names.get('regions')

                                        places_name = _pick_topo_object_name(topo_obj, [
                                            'ne_10m_populated_places',
                                            'Populated places',
                                            'Populated Places',
                                            'populated_places',
                                            'places',
                                        ]) or detected_names.get('places')

                                        countries_geo = _topology_object_to_feature_collection(topo_obj, countries_name) if countries_name else None
                                        admin1_geo = _topology_object_to_feature_collection(topo_obj, admin1_name) if admin1_name else None
                                        regions_geo = _topology_object_to_feature_collection(topo_obj, regions_name) if regions_name else None
                                        cities_geo = _topology_object_to_feature_collection(topo_obj, places_name) if places_name else None

                                        if countries_geo:
                                            _ensure_feature_indexes(countries_geo)
                                        if admin1_geo:
                                            _ensure_feature_indexes(admin1_geo)
                                        if regions_geo:
                                            _ensure_feature_indexes(regions_geo)
                                        if cities_geo:
                                            _ensure_feature_indexes(cities_geo)

                                        admin1_has_province = False
                                        if admin1_geo:
                                            for f in (admin1_geo.get('features') or []):
                                                p = f.get('properties') or {}
                                                tv = (p.get('type') or p.get('TYPE') or '')
                                                if isinstance(tv, str) and tv.strip().lower() == 'province':
                                                    admin1_has_province = True
                                                    break

                                        return countries_geo, admin1_geo, regions_geo, cities_geo, admin1_has_province

                                    countries_geo, admin1_geo, regions_geo, cities_geo, admin1_has_province = await asyncio.to_thread(_build_offline_layers, topo)

                                    with m:
                                        ui.run_javascript("""
                                            try {
                                                const dark = document.documentElement.classList.contains('mesh-dark');
                                                const col = dark ? '#0f1a2f' : '#aad3df';
                                                document.querySelectorAll('.leaflet-container').forEach(c => { c.style.backgroundColor = col; });
                                            } catch (e) {}
                                        """)
                                        if countries_geo:
                                            m.generic_layer(
                                                name='geoJSON',
                                                args=[countries_geo, {
                                                    'style': {
                                                        'color': '#000000',
                                                        'weight': 1.5,
                                                        'fillColor': '#EFF2DE',
                                                        'fillOpacity': 1.0,
                                                    },
                                                }],
                                            )

                                        if regions_geo:
                                            m.generic_layer(
                                                name='geoJSON',
                                                args=[regions_geo, {
                                                    'style': {
                                                        'color': '#000000',
                                                        'weight': 1.4,
                                                        'fillOpacity': 0.0,
                                                    },
                                                }],
                                            )

                                        admin1_layer = m.generic_layer(
                                            name='geoJSON',
                                            args=[{'type': 'FeatureCollection', 'features': []}, {
                                                'style': {
                                                    'color': '#bdbdbd',
                                                    'weight': 0.6,
                                                    'fillOpacity': 0.0,
                                                },
                                            }],
                                        )
                                        admin1_layer.run_method('setStyle', {'opacity': 0.0, 'fillOpacity': 0.0})
                                        ui.run_javascript("try { if (window.meshGetTheme && window.meshSetTheme) { window.meshSetTheme(window.meshGetTheme()); } } catch (e) {}")

                                    def _in_bounds(lat: float, lon: float, b: dict) -> bool:
                                        return (
                                            lat is not None and lon is not None and
                                            b['south'] <= lat <= b['north'] and
                                            b['west'] <= lon <= b['east']
                                        )

                                    def _feature_centroid_latlon(f: dict):
                                        c = (f or {}).get('_mesh_centroid')
                                        if not c:
                                            geom = (f or {}).get('geometry') or {}
                                            c = _feature_polygon_centroid(geom)
                                        if not c:
                                            return None
                                        lon, lat = c
                                        return (lat, lon)

                                    def _name_for_feature(f: dict) -> str | None:
                                        return _extract_feature_name_en((f or {}).get('properties') or {}) or _extract_feature_name((f or {}).get('properties') or {})

                                    def _city_importance(feat: dict) -> tuple:
                                        p = feat.get('properties') or {}
                                        pop = (
                                            p.get('POP_MAX') or
                                            p.get('POP2020') or
                                            p.get('POP2015') or
                                            p.get('POP2000') or
                                            0
                                        )
                                        sr = p.get('SCALERANK') if p.get('SCALERANK') is not None else 99
                                        lr = p.get('LABELRANK') if p.get('LABELRANK') is not None else 99
                                        featurecla = (p.get('FEATURECLA') or '')
                                        if not isinstance(featurecla, str):
                                            featurecla = ''
                                        is_capital = (
                                            p.get('ADM0CAP') == 1 or
                                            p.get('ADM1CAP') == 1 or
                                            ('capital' in featurecla.lower())
                                        )
                                        is_mega = (p.get('MEGACITY') == 1)
                                        return (0 if is_mega else 1, 0 if is_capital else 1, -int(pop or 0), sr, lr)

                                    def refresh_labels_and_admin1(bounds: dict, zoom_value: float):
                                        try:
                                            z = float(zoom_value)
                                        except:
                                            return

                                        labels = []
                                        city_name_set = set()

                                        if countries_geo:
                                            if z <= 4:
                                                for f in (countries_geo.get('features') or []):
                                                    name = _name_for_feature(f)
                                                    c = _feature_centroid_latlon(f)
                                                    if not name or not c:
                                                        continue
                                                    lat, lon = c
                                                    if _in_bounds(lat, lon, bounds):
                                                        labels.append({'lat': lat, 'lon': lon, 'text': name, 'kind': 'country'})

                                        if regions_geo:
                                            if 5 <= z <= 7:
                                                candidates = []
                                                for f in (regions_geo.get('features') or []):
                                                    name = _name_for_feature(f)
                                                    c = _feature_centroid_latlon(f)
                                                    if not name or not c:
                                                        continue
                                                    lat, lon = c
                                                    if _in_bounds(lat, lon, bounds):
                                                        fb = f.get('_mesh_bbox') or {}
                                                        try:
                                                            area = abs(float(fb.get('east', 0.0)) - float(fb.get('west', 0.0))) * abs(float(fb.get('north', 0.0)) - float(fb.get('south', 0.0)))
                                                        except Exception:
                                                            area = 0.0
                                                        candidates.append((area, {'lat': lat, 'lon': lon, 'text': name, 'kind': 'region'}))

                                                candidates.sort(key=lambda t: t[0], reverse=True)
                                                for _, lab in candidates[:80]:
                                                    labels.append(lab)

                                        if cities_geo:
                                            if z >= 8:
                                                if z < 9:
                                                    city_cap = 80
                                                    min_pop = 150000
                                                elif z < 10:
                                                    city_cap = 140
                                                    min_pop = 50000
                                                else:
                                                    city_cap = 250
                                                    min_pop = 0

                                                visible = []
                                                for f in (cities_geo.get('features') or []):
                                                    geom = f.get('geometry') or {}
                                                    if geom.get('type') != 'Point':
                                                        continue
                                                    lon, lat = geom.get('coordinates') or [None, None]
                                                    if lat is None or lon is None:
                                                        continue
                                                    if _in_bounds(lat, lon, bounds):
                                                        p = f.get('properties') or {}
                                                        featurecla = (p.get('FEATURECLA') or '')
                                                        if not isinstance(featurecla, str):
                                                            featurecla = ''
                                                        is_capital = (
                                                            p.get('ADM0CAP') == 1 or
                                                            p.get('ADM1CAP') == 1 or
                                                            ('capital' in featurecla.lower())
                                                        )
                                                        pop = (
                                                            p.get('POP_MAX') or
                                                            p.get('POP2020') or
                                                            p.get('POP2015') or
                                                            p.get('POP2000') or
                                                            0
                                                        )
                                                        if not is_capital and int(pop or 0) < min_pop:
                                                            continue
                                                        visible.append(f)

                                                visible.sort(key=_city_importance)
                                                visible = visible[:city_cap]

                                                for f in visible:
                                                    p = f.get('properties') or {}
                                                    name = p.get('name_en') or p.get('NAME_EN') or p.get('NAME') or p.get('name')
                                                    geom = f.get('geometry') or {}
                                                    lon, lat = geom.get('coordinates') or [None, None]
                                                    if not name or lat is None or lon is None:
                                                        continue
                                                    try:
                                                        city_name_set.add(str(name).strip().lower())
                                                    except Exception:
                                                        pass
                                                    labels.append({'lat': float(lat), 'lon': float(lon), 'text': str(name), 'kind': 'city'})

                                        if admin1_geo:
                                            if 6 <= z <= 10:
                                                candidates = []
                                                for f in (admin1_geo.get('features') or []):
                                                    p = f.get('properties') or {}
                                                    tv = (p.get('type') or p.get('TYPE') or '')
                                                    if isinstance(tv, str):
                                                        tv = tv.strip().lower()
                                                    else:
                                                        tv = ''
                                                    if tv != 'province':
                                                        continue
                                                    name = _extract_feature_name_en(p) or p.get('name_en') or p.get('NAME_EN')
                                                    if not name:
                                                        continue
                                                    try:
                                                        if str(name).strip().lower() in city_name_set:
                                                            continue
                                                    except Exception:
                                                        pass
                                                    c = _feature_centroid_latlon(f)
                                                    if not c:
                                                        continue
                                                    lat, lon = c
                                                    if _in_bounds(lat, lon, bounds):
                                                        fb = f.get('_mesh_bbox') or {}
                                                        try:
                                                            area = abs(float(fb.get('east', 0.0)) - float(fb.get('west', 0.0))) * abs(float(fb.get('north', 0.0)) - float(fb.get('south', 0.0)))
                                                        except Exception:
                                                            area = 0.0
                                                        candidates.append((area, {'lat': lat, 'lon': lon, 'text': str(name), 'kind': 'province'}))

                                                candidates.sort(key=lambda t: t[0], reverse=True)
                                                for _, lab in candidates[:120]:
                                                    labels.append(lab)

                                        try:
                                            with m:
                                                ui.run_javascript("""
                                                    try {
                                                        const el = getElement(%s);
                                                        const map = el && el.map;
                                                        if (window.meshOfflineLabels) {
                                                            window.meshOfflineLabels.set(map, %s, %s);
                                                        }
                                                    } catch (e) {}
                                                """ % (json.dumps(m.id), json.dumps(labels, ensure_ascii=False), json.dumps(z)))
                                        except Exception:
                                            pass

                                        if admin1_geo:
                                            if z >= 6:
                                                bounded_features = []
                                                for f in (admin1_geo.get('features') or []):
                                                    if admin1_has_province:
                                                        p = f.get('properties') or {}
                                                        tv = (p.get('type') or p.get('TYPE') or '')
                                                        if not (isinstance(tv, str) and tv.strip().lower() == 'province'):
                                                            continue
                                                    fb = f.get('_mesh_bbox')
                                                    if fb and _bbox_intersects(bounds, fb):
                                                        bounded_features.append(f)
                                                fc = {'type': 'FeatureCollection', 'features': bounded_features[:500]}
                                                try:
                                                    with m:
                                                        admin1_layer.run_method('clearLayers')
                                                        admin1_layer.run_method('addData', fc)
                                                        w = 0.5
                                                        if z >= 7:
                                                            w = 0.65
                                                        if z >= 9:
                                                            w = 0.85
                                                        admin1_layer.run_method('setStyle', {'color': '#bdbdbd', 'opacity': 1.0, 'weight': w, 'fillOpacity': 0.0})
                                                except Exception:
                                                    with m:
                                                        admin1_layer.run_method('setStyle', {'opacity': 0.0, 'fillOpacity': 0.0})
                                            else:
                                                try:
                                                    with m:
                                                        admin1_layer.run_method('clearLayers')
                                                except Exception:
                                                    pass
                                                with m:
                                                    admin1_layer.run_method('setStyle', {'opacity': 0.0, 'fillOpacity': 0.0})

                                    _offline_refresh['fn'] = refresh_labels_and_admin1
                                    b = _last_view_payload.get('bounds') or {'south': -90, 'west': -180, 'north': 90, 'east': 180}
                                    z = _last_view_payload.get('zoom')
                                    if z is None:
                                        z = m.zoom
                                    refresh_labels_and_admin1(b, z)
                                    await asyncio.sleep(0.05)
                                except Exception as e:
                                    try:
                                        log_to_console(f"Offline map error: {e}")
                                    except Exception:
                                        pass
                                finally:
                                    try:
                                        with m:
                                            offline_loading_dialog.close()
                                    except Exception:
                                        pass

                            background_tasks.create(_load_offline_map())
                        
                        map_markers_ready = {'value': False}
                        
                        def format_uptime(seconds):
                            try:
                                s_val = int(seconds)
                            except:
                                return "0s"
                                
                            d = s_val // 86400
                            h = (s_val % 86400) // 3600
                            m = (s_val % 3600) // 60
                            s = s_val % 60
                            
                            parts = []
                            if d > 0: parts.append(f"{d}d")
                            if h > 0: parts.append(f"{h}h")
                            if m > 0: parts.append(f"{m}m")
                            if s > 0 or not parts: parts.append(f"{s}s")
                            
                            return ", ".join(parts)

                        def update_map():
                            if (not state.nodes_updated) and map_markers_ready.get('value'):
                                return
                            
                            nodes_payload = []
                            for nid, n in state.nodes.items():
                                if n['lat'] and n['lon']:
                                    try:
                                        lat = float(n['lat'])
                                        lon = float(n['lon'])
                                    except Exception:
                                        continue

                                    label_raw = n.get('short_name')
                                    if not isinstance(label_raw, str):
                                        label_raw = ""
                                    label_raw = label_raw.strip()
                                    if not label_raw or label_raw == "???":
                                        label = str(nid)[-4:]
                                    else:
                                        label = label_raw[:4]
                                    
                                    # Construct popup content
                                    # Header with clickable name/ID
                                    name_display = n['long_name']
                                    short_display = n['short_name'] if n['short_name'] != "???" else ""
                                    
                                    popup_content = f"<div style='cursor:pointer' onclick='window.goToNode(\"{nid}\")'>"
                                    popup_content += f"<b style='font-size:16px; margin-bottom: 8px; display: block;'>{name_display}</b>"
                                    
                                    if short_display:
                                        popup_content += f"<i class='material-icons' style='font-size:16px; vertical-align:text-bottom;'>label</i> Short Name: {short_display}<br>"
                                        
                                    popup_content += f"<i class='material-icons' style='font-size:16px; vertical-align:text-bottom;'>fingerprint</i> ID: {nid}</div>"
                                    
                                    popup_content += f"<i class='material-icons' style='font-size:16px; vertical-align:text-bottom;'>memory</i> Model: {n['hw_model']}<br>"
                                    
                                    # Environment Metrics
                                    if n.get('temperature') is not None:
                                        popup_content += f"<i class='material-icons' style='font-size:16px; vertical-align:text-bottom;'>thermostat</i> {n['temperature']:.1f}°C<br>"
                                    if n.get('barometric_pressure') is not None:
                                        popup_content += f"<i class='material-icons' style='font-size:16px; vertical-align:text-bottom;'>speed</i> {n['barometric_pressure']:.1f} hPa<br>"
                                    if n.get('relative_humidity') is not None:
                                        popup_content += f"<i class='material-icons' style='font-size:16px; vertical-align:text-bottom;'>water_drop</i> {n['relative_humidity']:.1f}%<br>"
                                        
                                    # Device Metrics
                                    if n.get('battery') is not None:
                                        popup_content += f"<i class='material-icons' style='font-size:16px; vertical-align:text-bottom;'>battery_std</i> {n['battery']}%<br>"
                                    if n.get('channel_utilization') is not None:
                                        popup_content += f"<i class='material-icons' style='font-size:16px; vertical-align:text-bottom;'>bar_chart</i> Util: {n['channel_utilization']:.1f}%<br>"
                                    if n.get('altitude') is not None:
                                        popup_content += f"<i class='material-icons' style='font-size:16px; vertical-align:text-bottom;'>terrain</i> Alt: {n['altitude']} m<br>"
                                    if n.get('uptime_seconds') is not None:
                                        up_str = format_uptime(n['uptime_seconds'])
                                        popup_content += f"<i class='material-icons' style='font-size:16px; vertical-align:text-bottom;'>schedule</i> Up: {up_str}<br>"
                                    
                                    hop_label = n.get('hop_label')
                                    if hop_label is not None:
                                        popup_content += f"<i class='material-icons' style='font-size:16px; vertical-align:text-bottom;'>alt_route</i> Hops: {hop_label}<br>"
                                    
                                    snr_val = n.get('snr')
                                    rssi_val = n.get('rssi')
                                    snr_indirect = n.get('snr_indirect')
                                    rssi_indirect = n.get('rssi_indirect')
                                    hops_val = n.get('hops')
                                    if hops_val == 0:
                                        if snr_val is not None:
                                            try:
                                                snr_float = float(snr_val)
                                            except:
                                                snr_float = None
                                            if snr_float is not None:
                                                min_snr = -20.0
                                                max_snr = 10.0
                                                if snr_float < min_snr:
                                                    snr_norm = 0.0
                                                elif snr_float > max_snr:
                                                    snr_norm = 1.0
                                                else:
                                                    snr_norm = (snr_float - min_snr) / (max_snr - min_snr)
                                                pos_percent = int(snr_norm * 100)
                                                if pos_percent < 0:
                                                    pos_percent = 0
                                                if pos_percent > 100:
                                                    pos_percent = 100
                                                popup_content += (
                                                    "<div style='margin-top:4px;'>"
                                                    "<div style='display:flex;align-items:center;gap:4px;'>"
                                                    "<i class='material-icons' style='font-size:16px; vertical-align:text-bottom;'>signal_cellular_alt</i>"
                                                    "<div style='position:relative;width:120px;height:10px;"
                                                    "background:linear-gradient(to right, #ef4444, #facc15, #22c55e);"
                                                    "border-radius:999px;'>"
                                                    f"<div style='position:absolute;left:{pos_percent}%;top:50%;"
                                                    "transform:translate(-50%, -50%);width:12px;height:12px;"
                                                    "border-radius:999px;background:#111827;border:2px solid white;'></div>"
                                                    "</div>"
                                                    "</div>"
                                                    f"<div class='mesh-muted' style='font-size:11px;margin-top:2px;'>└ SNR: {snr_float:.1f} dB</div>"
                                                    "</div>"
                                                )
                                        
                                        if rssi_val is not None:
                                            try:
                                                rssi_float = float(rssi_val)
                                            except:
                                                rssi_float = None
                                            if rssi_float is not None:
                                                popup_content += (
                                                    f"<div class='mesh-muted' style='font-size:11px;margin-top:2px;'>└ RSSI: {rssi_float:.1f} dB</div>"
                                                )
                                    else:
                                        if snr_indirect is not None:
                                            try:
                                                snr_indirect_float = float(snr_indirect)
                                            except:
                                                snr_indirect_float = None
                                            if snr_indirect_float is not None:
                                                popup_content += (
                                                    "<i class='material-icons' style='font-size:16px; vertical-align:text-bottom;'>waves</i> "
                                                    f"RX SNR (indirect): {snr_indirect_float:.1f} dB<br>"
                                                )
                                        
                                    popup_content += f"<i class='material-icons' style='font-size:16px; vertical-align:text-bottom;'>access_time</i> Last Seen: {n['last_seen']}<br>"
                                    
                                    if n.get('location_source'):
                                        popup_content += f"<i class='material-icons' style='font-size:16px; vertical-align:text-bottom;'>my_location</i> Loc Source: {n['location_source']}<br>"

                                    nodes_payload.append({
                                        "id": nid,
                                        "lat": lat,
                                        "lon": lon,
                                        "last_seen_ts": n.get("last_seen_ts"),
                                        "marker_label": label.upper(),
                                        "popup": popup_content,
                                    })

                            if nodes_payload:
                                with m:
                                    ui.run_javascript(
                                        "try { window.meshUpsertNodesOnMap(%s, %s); } catch (e) {}"
                                        % (json.dumps(m.id), json.dumps(nodes_payload, ensure_ascii=False, default=str))
                                    )
                                map_markers_ready['value'] = True
                            
                            state.nodes_updated = False # Reset flag after update

                    # NODES LIST PANEL
                    with ui.tab_panel(nodes_tab).classes('h-full p-0 flex flex-col'):
                        # Header with Count and Reset Filter Button
                        with ui.row().classes('w-full items-center justify-between p-2 bg-gray-50 border-b'):
                             node_count_label = ui.label('Total Nodes: 0').classes('font-bold text-gray-700')
                             
                             def reset_filters():
                                 ui.run_javascript(f'''
                                    if (window.mesh_grid_api) {{
                                        // Clear Quick Filter
                                        if (typeof window.mesh_grid_api.setGridOption === 'function') {{
                                            window.mesh_grid_api.setGridOption('quickFilterText', "");
                                        }} else if (typeof window.mesh_grid_api.setQuickFilter === 'function') {{
                                            window.mesh_grid_api.setQuickFilter("");
                                        }}
                                        
                                        // Clear Column Filters
                                        window.mesh_grid_api.setFilterModel(null);
                                        
                                        // Refresh cells to update row numbers
                                        window.mesh_grid_api.refreshCells({{columns: ['rowNum'], force: true}});
                                    }}
                                 ''')
                                 ui.notify(translate("notification.positive.filtersreset", "Filters Reset"))

                             ui.button(translate("ui.resetfilters", "Reset Filters"), on_click=reset_filters, icon='filter_alt_off').props('dense flat color=red')

                        # Safe initial data load
                        initial_rows = []
                        try:
                            initial_rows = [n.copy() for n in state.nodes.values()]
                        except:
                            pass

                        nodes_grid = ui.aggrid({
                            'defaultColDef': {
                                'resizable': True,
                                'sortable': True,
                                'filter': True,
                                'minWidth': 100,
                            },
                            'columnDefs': [
                                {'headerName': '#', 'colId': 'rowNum', 'valueGetter': 'node.rowIndex + 1', 'width': 65, 'minWidth': 65, 'sortable': False, 'filter': False, 'pinned': 'left'},
                                {'headerName': 'Name', 'field': 'short_name', 'width': 100, ':cellRenderer': 'window.meshCopyCellRenderer'},
                                {'headerName': 'Long Name', 'field': 'long_name', 'width': 180, 'minWidth': 180, ':cellRenderer': 'window.meshCopyCellRenderer'},
                                {'headerName': 'ID', 'field': 'id', 'width': 150, 'minWidth': 150, ':cellRenderer': 'window.meshCopyCellRenderer'},
                                {'headerName': 'MAC', 'field': 'macaddr', 'width': 160, 'minWidth': 160, ':cellRenderer': 'window.meshCopyCellRenderer'},
                                {'headerName': 'Public Key', 'field': 'public_key', 'width': 240, 'minWidth': 240, ':cellRenderer': 'window.meshCopyCellRenderer'},
                                {'headerName': 'Unmessagable', 'field': 'is_unmessagable', 'width': 120, ':valueFormatter': '(p) => (p.value === true ? \"true\" : \"false\")'},
                                {'headerName': 'Model', 'field': 'hw_model', 'width': 160, 'minWidth': 160, ':cellRenderer': 'window.meshCopyCellRenderer'},
                                {'headerName': 'Role', 'field': 'role', 'width': 140, 'minWidth': 140, ':cellRenderer': 'window.meshCopyCellRenderer'},
                                {
                                    'headerName': 'Hops',
                                    'field': 'hops',
                                    'width': 80,
                                    ':comparator': '(valueA, valueB, nodeA, nodeB, isInverted) => { const isNullA = valueA === null || valueA === undefined; const isNullB = valueB === null || valueB === undefined; if (isNullA && isNullB) return 0; if (isNullA && !isNullB) { return isInverted ? -1 : 1; } if (!isNullA && isNullB) { return isInverted ? 1 : -1; } const a = Number(valueA); const b = Number(valueB); if (Number.isNaN(a) && Number.isNaN(b)) return 0; if (Number.isNaN(a) && !Number.isNaN(b)) return isInverted ? -1 : 1; if (!Number.isNaN(a) && Number.isNaN(b)) return isInverted ? 1 : -1; if (a === b) return 0; return a < b ? -1 : 1; }'
                                },
                                {'headerName': 'SNR (dB)', 'field': 'snr', 'width': 100},
                                {'headerName': 'RSSI (rel dB)', 'field': 'rssi', 'width': 110},
                                {'headerName': 'Last Seen', 'field': 'last_seen', 'width': 180, 'minWidth': 180, ':cellRenderer': 'window.meshCopyCellRenderer'},
                                {'headerName': 'Lat', 'field': 'lat', 'width': 120, 'minWidth': 120, ':cellRenderer': 'window.meshCopyCellRenderer'},
                                {'headerName': 'Lon', 'field': 'lon', 'width': 120, 'minWidth': 120, ':cellRenderer': 'window.meshCopyCellRenderer'},
                                {'headerName': 'Alt (m)', 'field': 'altitude', 'width': 65, 'minWidth': 65},
                                {'headerName': 'Loc Source', 'field': 'location_source', 'width': 110},
                                {'headerName': 'SNR Indirect (dB)', 'field': 'snr_indirect', 'width': 130},
                                {'headerName': 'RSSI Indirect (rel dB)', 'field': 'rssi_indirect', 'width': 150},
                                {'headerName': 'Batt', 'field': 'battery', 'width': 100},
                                {'headerName': 'Volt', 'field': 'voltage', 'width': 100},
                                {'headerName': 'Temp', 'field': 'temperature', 'width': 100},
                                {'headerName': 'Hum', 'field': 'relative_humidity', 'width': 100},
                                {'headerName': 'Press', 'field': 'barometric_pressure', 'width': 100},
                                {'headerName': 'Ch Util', 'field': 'channel_utilization', 'width': 100},
                                {'headerName': 'Air Util', 'field': 'air_util_tx', 'width': 100},
                                {'headerName': 'Uptime', 'field': 'uptime_seconds', 'width': 140, 'minWidth': 140, ':cellRenderer': 'window.meshCopyCellRenderer'},
                            ],
                            'rowData': initial_rows,
                            ':getRowId': '(params) => params.data.id',
                            ':rowClassRules': '{ "mesh-row-clickable": (p) => !!(p && p.data && p.data.lat && p.data.lon) }',
                            ':onGridReady': '(params) => { window.mesh_grid_api = params.api; }',
                        }).classes('flex-grow w-full')
                        
                        # Handle Grid Ready event to force sync when tab is opened/refreshed
                        def handle_grid_ready(e):
                            state.nodes_list_force_refresh = True
                            
                        nodes_grid.on('gridReady', handle_grid_ready)
                        
                        # We must force refresh the 'rowNum' column when sort/filter changes
                        nodes_grid.on('sortChanged', lambda: nodes_grid.run_grid_method('refreshCells', {'columns': ['rowNum'], 'force': True}))
                        nodes_grid.on('filterChanged', lambda: nodes_grid.run_grid_method('refreshCells', {'columns': ['rowNum'], 'force': True}))
                        
                        # Handle Row Click -> Pan to Map
                        def on_row_click(e):
                            row = e.args.get('data', {})
                            lat = row.get('lat')
                            lon = row.get('lon')
                            nid = row.get('id')
                            
                            if lat and lon:
                                tabs.set_value(map_tab) # Switch tab
                                m.set_center((lat, lon)) # Center map
                                try:
                                    with m:
                                        ui.run_javascript(
                                            "try { window.meshOpenNodePopup(%s, %s); } catch (e) {}"
                                            % (json.dumps(m.id), json.dumps(nid))
                                        )
                                except Exception:
                                    pass
                                     
                        nodes_grid.on('rowClicked', on_row_click, args=['data'])
                        
                        # Initial sync flag for this session
                        first_sync = True

                        def update_grid():
                            nonlocal first_sync
                            
                            # Always update count label
                            node_count_label.text = f"{translate('ui.totalnodes', 'Total Nodes')}: {len(state.nodes)}"
                            
                            try:
                                to_process = []
                                force = state.nodes_list_force_refresh
                                
                                # If it's the first run for this client OR a force refresh is requested
                                if first_sync or force:
                                    try:
                                        to_process = [n.copy() for n in state.nodes.values()]
                                    except RuntimeError:
                                        return # Dict changed size, skip frame
                                        
                                    state.nodes_list_force_refresh = False # Reset global flag
                                    first_sync = False
                                    
                                    # Clear dirty nodes since we are syncing everything
                                    with state.lock:
                                        state.dirty_nodes.clear()
                                        
                                else:
                                    # Normal Delta Update
                                    with state.lock:
                                        if not state.dirty_nodes:
                                            return
                                        dirty_ids = state.dirty_nodes.copy()
                                        state.dirty_nodes.clear()
                                    
                                    for nid in dirty_ids:
                                        if nid in state.nodes:
                                            to_process.append(state.nodes[nid].copy())
                                
                                if to_process:
                                    # Send to client for robust Upsert
                                    # Use json.dumps to ensure proper serialization
                                    # We use run_javascript to execute the safe Upsert logic on the client
                                    ui.run_javascript(f'upsertNodeData({nodes_grid.id}, {json.dumps(to_process, default=str)})')
                                    
                            except Exception as e:
                                print(f"Grid Update Error: {e}")
                            
                    # Overview/stats Tab
                    with ui.tab_panel(overview_tab).classes('p-3 h-full overflow-auto'):
                        ui.label(translate("mesh_overview.title", "Mesh Overview")).classes('text-xl font-bold mb-2')

                        def _fmt_num(x, digits: int = 0):
                            if x is None:
                                return "-"
                            try:
                                if digits <= 0:
                                    return f"{int(round(float(x)))}"
                                return f"{float(x):.{digits}f}"
                            except Exception:
                                return str(x)

                        def _sparkline_svg(values: list[int], width: int = 860, height: int = 120, pad: int = 8) -> str:
                            if not values:
                                values = [0]
                            vals = [int(v) for v in values[-120:]]
                            vmin = min(vals)
                            vmax = max(vals)
                            if vmax <= vmin:
                                vmax = vmin + 1
                            w = max(100, int(width))
                            h = max(60, int(height))
                            n = len(vals)
                            if n < 2:
                                n = 2
                                vals = [vals[0], vals[0]]
                            x_step = (w - 2 * pad) / (n - 1)
                            pts = []
                            for i, v in enumerate(vals):
                                x = pad + i * x_step
                                y = pad + (h - 2 * pad) * (1.0 - ((v - vmin) / (vmax - vmin)))
                                pts.append(f"{x:.1f},{y:.1f}")
                            poly = " ".join(pts)
                            return f"""
                                <svg width="100%" height="{h}" viewBox="0 0 {w} {h}" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
                                  <rect x="0" y="0" width="{w}" height="{h}" rx="10" fill="rgba(148,163,184,0.10)"/>
                                  <polyline points="{poly}" fill="none" stroke="rgba(96,165,250,0.95)" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
                                  <text x="{pad}" y="{h - pad}" font-size="12" fill="rgba(148,163,184,0.95)">{html.escape(translate("mesh_overview.traffic.graph_label", "Packets/min (rolling)"))}</text>
                                </svg>
                            """

                        with ui.row().classes('w-full gap-3 mb-3 flex-wrap'):
                            with ui.card().classes('p-3 w-full sm:w-[calc(50%-0.75rem)] lg:w-[calc(25%-0.75rem)]'):
                                ui.label(translate("mesh_overview.kpi.total_packets", "Total packets")).classes('text-sm text-gray-500')
                                kpi_total = ui.label("-").classes('text-2xl font-bold')
                            with ui.card().classes('p-3 w-full sm:w-[calc(50%-0.75rem)] lg:w-[calc(25%-0.75rem)]'):
                                ui.label(translate("mesh_overview.kpi.ppm", "Packets/min")).classes('text-sm text-gray-500')
                                kpi_ppm = ui.label("-").classes('text-2xl font-bold')
                            with ui.card().classes('p-3 w-full sm:w-[calc(50%-0.75rem)] lg:w-[calc(25%-0.75rem)]'):
                                ui.label(translate("mesh_overview.kpi.active_5m", "Active nodes (5m)")).classes('text-sm text-gray-500')
                                kpi_active_5m = ui.label("-").classes('text-2xl font-bold')
                            with ui.card().classes('p-3 w-full sm:w-[calc(50%-0.75rem)] lg:w-[calc(25%-0.75rem)]'):
                                ui.label(translate("mesh_overview.kpi.error_rate", "Global error rate")).classes('text-sm text-gray-500')
                                kpi_err = ui.label("-").classes('text-2xl font-bold')
                        with ui.row().classes('w-full gap-3 mb-2 flex-nowrap'):
                            with ui.card().classes('p-2 sm:p-3 flex-1 min-w-0 overflow-hidden'):
                                with ui.row().classes('w-full items-center gap-1 flex-nowrap'):
                                    ui.label(translate("mesh_overview.kpi.mesh_traffic", "Mesh Traffic")).classes('flex-1 basis-0 min-w-0 whitespace-nowrap overflow-hidden text-ellipsis text-[clamp(0.35rem,0.755vw,0.875rem)] text-gray-500')
                                    ui.icon("help_outline").classes('text-sky-800 p-[2px] text-[clamp(7px,1.6vw,16px)] cursor-help select-none')
                                    ui.tooltip(translate("mesh_overview.kpi.mesh_traffic.tooltip", "0–100 score based on mesh traffic congestion.\nHigher score = less congestion.\nLabels: Excellent / Good / Fair / Poor.")).classes('whitespace-pre-line')
                                with ui.row().classes('w-full items-center gap-2 flex-nowrap'):
                                    mesh_traffic_value = ui.label("-").classes('text-[clamp(1.05rem,2.2vw,1.5rem)] font-bold whitespace-nowrap')
                                    mesh_traffic_badge = ui.badge("-").classes('text-white text-[clamp(0.6rem,1vw,0.75rem)] whitespace-nowrap')

                            with ui.card().classes('p-2 sm:p-3 flex-1 min-w-0 overflow-hidden'):
                                with ui.row().classes('w-full items-center gap-1 flex-nowrap'):
                                    ui.label(translate("mesh_overview.kpi.packet_integrity", "Packet Integrity")).classes('flex-1 basis-0 min-w-0 whitespace-nowrap overflow-hidden text-ellipsis text-[clamp(0.35rem,0.755vw,0.875rem)] text-gray-500')
                                    ui.icon("help_outline").classes('text-sky-800 p-[2px] text-[clamp(7px,1.6vw,16px)] cursor-help select-none')
                                    ui.tooltip(translate("mesh_overview.kpi.packet_integrity.tooltip", "0–100 score based on packet validity.\nComputed from CRC OK / CRC Fail / Invalid Protobuf.\nLabels: Excellent / Good / Fair / Poor.")).classes('whitespace-pre-line')
                                with ui.row().classes('w-full items-center gap-2 flex-nowrap'):
                                    packet_integrity_value = ui.label("-").classes('text-[clamp(1.05rem,2.2vw,1.5rem)] font-bold whitespace-nowrap')
                                    packet_integrity_badge = ui.badge("-").classes('text-white text-[clamp(0.6rem,1vw,0.75rem)] whitespace-nowrap')

                            with ui.card().classes('p-2 sm:p-3 flex-1 min-w-0 overflow-hidden'):
                                with ui.row().classes('w-full items-center gap-1 flex-nowrap'):
                                    ui.label(translate("mesh_overview.kpi.mesh_signal", "Mesh Signal (RF)")).classes('flex-1 basis-0 min-w-0 whitespace-nowrap overflow-hidden text-ellipsis text-[clamp(0.35rem,0.755vw,0.875rem)] text-gray-500')
                                    ui.icon("help_outline").classes('text-sky-800 p-[2px] text-[clamp(7px,1.6vw,16px)] cursor-help select-none')
                                    ui.tooltip(translate("mesh_overview.kpi.mesh_signal.tooltip", "0–100 score based on RF signal quality.\nComputed from average SNR and RSSI (direct + indirect packets).\nLabels: Excellent / Good / Fair / Poor.")).classes('whitespace-pre-line')
                                with ui.row().classes('w-full items-center gap-2 flex-nowrap'):
                                    mesh_signal_value = ui.label("-").classes('text-[clamp(1.05rem,2.2vw,1.5rem)] font-bold whitespace-nowrap')
                                    mesh_signal_badge = ui.badge("-").classes('text-white text-[clamp(0.6rem,1vw,0.75rem)] whitespace-nowrap')

                            with ui.card().classes('p-2 sm:p-3 flex-1 min-w-0 overflow-hidden'):
                                with ui.row().classes('w-full items-center gap-1 flex-nowrap'):
                                    ui.label(translate("mesh_overview.kpi.mesh_health", "Mesh Health (Global)")).classes('flex-1 basis-0 min-w-0 whitespace-nowrap overflow-hidden text-ellipsis text-[clamp(0.35rem,0.755vw,0.875rem)] text-gray-500')
                                    ui.icon("help_outline").classes('text-sky-800 p-[2px] text-[clamp(7px,1.6vw,16px)] cursor-help select-none')
                                    ui.tooltip(translate("mesh_overview.kpi.mesh_health.tooltip", "0–100 score computed as the arithmetic mean of Mesh Traffic, Packet Integrity and Mesh Signal.\nLabels: Stable / Intermittent / Unstable / Critical.")).classes('whitespace-pre-line')
                                with ui.row().classes('w-full items-center gap-2 flex-nowrap'):
                                    mesh_health_value = ui.label("-").classes('text-[clamp(1.05rem,2.2vw,1.5rem)] font-bold whitespace-nowrap')
                                    mesh_health_badge = ui.badge("-").classes('text-white text-[clamp(0.6rem,1vw,0.75rem)] whitespace-nowrap')

                        ui.label(translate("mesh_overview.quality.note", "Note: 1–3 hours of listening are recommended for a more stable overview.")).classes('text-xs text-gray-500 mb-3')

                        traffic_graph = ui.html("", sanitize=False).classes('w-full mb-3')

                        with ui.row().classes('w-full gap-3 mb-3 flex-wrap'):
                            with ui.card().classes('p-3 w-full md:w-[calc(50%-0.75rem)]'):
                                ui.label(translate("mesh_overview.section.integrity", "Packet Integrity")).classes('text-lg font-semibold mb-2')
                                integrity_crc_ok = ui.label("-").classes('text-sm')
                                integrity_crc_fail = ui.label("-").classes('text-sm')
                                integrity_dec_ok = ui.label("-").classes('text-sm')
                                integrity_dec_fail = ui.label("-").classes('text-sm')
                                integrity_pb = ui.label("-").classes('text-sm')
                                integrity_port = ui.label("-").classes('text-sm')

                            with ui.card().classes('p-3 w-full md:w-[calc(50%-0.75rem)]'):
                                ui.label(translate("mesh_overview.section.rf", "RF Quality (recent)")).classes('text-lg font-semibold mb-2')
                                rf_rssi = ui.label("-").classes('text-sm')
                                rf_snr = ui.label("-").classes('text-sm')
                                rf_direct = ui.label("-").classes('text-sm')
                                rf_multihop = ui.label("-").classes('text-sm')
                                rf_hopavg = ui.label("-").classes('text-sm pb-9')

                        top_node_state = {"id": None}

                        def _filter_top_node():
                            nid = top_node_state.get("id")
                            if nid:
                                target_node_input.value = str(nid)

                        with ui.row().classes('w-full gap-3 mb-3 flex-wrap'):
                            with ui.card().classes('p-3 w-full'):
                                ui.label(translate("mesh_overview.section.activity", "Mesh Activity")).classes('text-lg font-semibold mb-2')
                                act_active_10m = ui.label("-").classes('text-sm')
                                act_new_hour = ui.label("-").classes('text-sm')
                                act_cu = ui.label("-").classes('text-sm')
                                act_private_msgs = ui.label("-").classes('text-sm')
                                ui.separator().classes('my-2')
                                ui.label(translate("mesh_overview.section.single_node_activity", "Single node activity")).classes('text-sm font-semibold text-gray-600')
                                with ui.row().classes('w-full items-center gap-1'):
                                    act_top_node_prefix = ui.label("-").classes('text-sm')
                                    act_top_node_id = ui.label("-").classes('text-sm')
                                    act_top_node_cnt = ui.label("").classes('text-sm')
                                act_top_node_id.on('click', lambda _e: _filter_top_node())
                                act_au = ui.label("-").classes('text-sm')

                        with ui.row().classes('w-full justify-end gap-2'):
                            with ui.dialog() as reset_stats_dialog:
                                with ui.card().classes('w-[520px]'):
                                    ui.label(
                                        translate("mesh_overview.reset_confirm.title", "Reset mesh statistics?")
                                    ).classes('text-lg font-bold text-red-600 mb-2')
                                    ui.label(
                                        translate("mesh_overview.reset_confirm.warning", "This will reset all Mesh Overview statistics.")
                                    ).classes('text-sm text-red-600 font-semibold whitespace-pre-line')
                                    ui.label(
                                        translate(
                                            "mesh_overview.reset_confirm.detail",
                                            "Nodes and node information will NOT be deleted.\nDo you want to proceed?",
                                        )
                                    ).classes('text-sm text-gray-700 whitespace-pre-line mb-3')

                                    with ui.row().classes('w-full justify-end gap-2'):
                                        ui.button(
                                            translate("button.cancel", "Cancel"),
                                            on_click=reset_stats_dialog.close,
                                        ).classes('bg-slate-200 text-slate-900')

                                        def _do_reset_stats():
                                            mesh_stats.reset()
                                            reset_stats_dialog.close()
                                            ui.notify(translate("mesh_overview.notification.reset", "Mesh stats reset"), type='positive')

                                        ui.button(
                                            translate("button.yes", "Yes"),
                                            on_click=_do_reset_stats,
                                        ).classes('bg-red-600 text-white')

                            def _ask_reset_stats():
                                reset_stats_dialog.open()

                            def _export_stats_json():
                                try:
                                    try:
                                        locale.setlocale(locale.LC_TIME, '')
                                    except Exception:
                                        pass
                                    timestamp = datetime.now().strftime("%c")
                                    safe_timestamp = timestamp.replace(":", "-").replace("/", "-").replace("\\", "-").replace(" ", "_")
                                    filename = f"{PROGRAM_NAME}_MeshOverview_{safe_timestamp}.json".replace(" ", "")
                                    export_path = os.path.join(get_app_path(), filename)
                                    with open(export_path, 'w') as f:
                                        json.dump(mesh_stats.to_dict(), f, indent=4)
                                    with ui.dialog() as saved_dialog, ui.card():
                                        ui.label(translate("mesh_overview.export.success.title", "Export Successful")).classes('text-lg font-bold text-green-500')
                                        ui.label(translate("mesh_overview.export.success.filename", "File saved: {filename}").format(filename=filename))
                                        ui.label(translate("mesh_overview.export.success.location", "Location: {location}").format(location=get_app_path()))
                                        ui.label(
                                            translate(
                                                "mesh_overview.export.note_autosave",
                                                "Note: Mesh Overview data is also automatically saved and included in the normal Export Data.",
                                            )
                                        ).classes('text-sm text-gray-600')
                                        ui.button(translate("button.close", "Close"), on_click=saved_dialog.close).classes('w-full')
                                    saved_dialog.open()
                                except Exception as e:
                                    ui.notify(translate("mesh_overview.export.failed", "Export Failed: {error}").format(error=e), type='negative')

                            ui.button(translate("mesh_overview.button.reset", "Reset Stats"), on_click=_ask_reset_stats).classes('bg-slate-200 text-slate-900')
                            ui.button(translate("mesh_overview.button.export", "Export JSON"), on_click=_export_stats_json).classes('bg-blue-600 text-white')

                        def _update_mesh_overview():
                            snap = mesh_stats.snapshot()
                            series = mesh_stats.sample_packets_per_minute()

                            kpi_total.text = _fmt_num(snap.get("total_packets"))
                            kpi_ppm.text = _fmt_num(snap.get("packets_per_minute"))
                            kpi_active_5m.text = _fmt_num(snap.get("active_nodes_5m"))
                            kpi_err.text = f"{_fmt_num(snap.get('global_error_rate_pct'), 1)}%"

                            traffic_graph.content = _sparkline_svg(series)

                            integrity_crc_ok.text = translate("mesh_overview.integrity.crc_ok", "CRC OK: {v}").format(v=_fmt_num(snap.get("crc_ok")))
                            integrity_crc_fail.text = translate("mesh_overview.integrity.crc_fail", "CRC Fail: {v}").format(v=_fmt_num(snap.get("crc_fail")))
                            integrity_dec_ok.text = translate("mesh_overview.integrity.decrypt_ok", "Decrypt OK: {v}").format(v=_fmt_num(snap.get("decrypt_ok")))
                            integrity_dec_fail.text = translate("mesh_overview.integrity.decrypt_fail", "Decrypt Fail: {v}").format(v=_fmt_num(snap.get("decrypt_fail")))
                            integrity_pb.text = translate("mesh_overview.integrity.invalid_protobuf", "Invalid protobuf: {v}").format(v=_fmt_num(snap.get("invalid_protobuf")))
                            integrity_port.text = translate("mesh_overview.integrity.unknown_portnum", "Unknown portnum: {v}").format(v=_fmt_num(snap.get("unknown_portnum")))

                            rf_rssi.text = translate("mesh_overview.rf.rssi_avg", "Avg RSSI: {v}").format(v=_fmt_num(snap.get("rssi_avg"), 1))
                            rf_snr.text = translate("mesh_overview.rf.snr_avg", "Avg SNR: {v}").format(v=_fmt_num(snap.get("snr_avg"), 1))
                            rf_direct.text = translate("mesh_overview.rf.direct_pct", "Direct packets: {v}%").format(v=_fmt_num(snap.get("direct_ratio_pct"), 1))
                            rf_multihop.text = translate("mesh_overview.rf.multihop_pct", "Multi-hop packets: {v}%").format(v=_fmt_num(snap.get("multihop_ratio_pct"), 1))
                            rf_hopavg.text = translate("mesh_overview.rf.hop_avg", "Avg hops: {v}").format(v=_fmt_num(snap.get("hop_avg"), 2))

                            act_active_10m.text = translate("mesh_overview.activity.active_10m", "Active nodes (10m): {v}").format(v=_fmt_num(snap.get("active_nodes_10m")))
                            act_new_hour.text = translate("mesh_overview.activity.new_nodes_hour", "New nodes/hour: {v}").format(v=_fmt_num(snap.get("new_nodes_last_hour")))
                            top_node_state["id"] = snap.get("most_active_node")
                            act_top_node_prefix.text = translate("mesh_overview.activity.top_node.prefix", "Most active:")
                            _nid = top_node_state["id"]
                            act_top_node_id.text = str(_nid or "-")
                            act_top_node_cnt.text = translate("mesh_overview.activity.top_node.count", "({cnt})").format(
                                cnt=_fmt_num(snap.get("most_active_node_packets"))
                            ) if _nid else ""
                            if _nid:
                                act_top_node_id.classes(add="text-blue-600 underline cursor-pointer select-none", remove="text-gray-600 cursor-default")
                            else:
                                act_top_node_id.classes(add="text-gray-600 cursor-default select-none", remove="text-blue-600 underline cursor-pointer")
                            act_cu.text = translate("mesh_overview.activity.channel_util_avg", "Avg channel_utilization: {v}").format(v=_fmt_num(snap.get("channel_utilization_avg"), 1))
                            _aes_b64 = (getattr(state, "aes_key_b64", "") or "").strip()
                            if _aes_b64 == "AQ==":
                                _df = int(snap.get("decrypt_fail") or 0)
                                _do = int(snap.get("decrypt_ok") or 0)
                                _den = _do + _df
                                _pct = (float(_df) / float(_den) * 100.0) if _den > 0 else 0.0
                                act_private_msgs.text = translate(
                                    "mesh_overview.activity.private_messages",
                                    "Private messages/Channels (est.): {v} ({pct}%)",
                                ).format(v=_fmt_num(_df), pct=_fmt_num(_pct, 1))
                                act_private_msgs.classes(remove="hidden")
                            else:
                                act_private_msgs.classes(add="hidden")
                            act_au.text = translate("mesh_overview.activity.air_util_tx_max", "Peak Node Transmission: {v}%").format(v=_fmt_num(snap.get("air_util_tx_max"), 1))

                            def _set_badge_color(badge, col: str):
                                badge.classes(
                                    remove="bg-primary bg-blue-600 bg-sky-600 bg-green-600 bg-yellow-600 bg-orange-600 bg-red-600 !bg-green-600 !bg-yellow-600 !bg-orange-600 !bg-red-600"
                                )
                                if col == "green":
                                    badge.classes(add="!bg-green-600 !text-white")
                                elif col == "yellow":
                                    badge.classes(add="!bg-yellow-600 !text-white")
                                elif col == "orange":
                                    badge.classes(add="!bg-orange-600 !text-white")
                                elif col == "red":
                                    badge.classes(add="!bg-red-600 !text-white")

                            mesh_traffic_value.text = _fmt_num(snap.get("mesh_traffic_score"))
                            mt_level = str(snap.get("mesh_traffic_level") or "")
                            mesh_traffic_badge.text = translate(f"mesh_overview.level.{mt_level}", mt_level.title()) if mt_level else "-"
                            mt_col = str(snap.get("mesh_traffic_color") or "")
                            _set_badge_color(mesh_traffic_badge, mt_col)

                            packet_integrity_value.text = _fmt_num(snap.get("packet_integrity_score"))
                            pi_level = str(snap.get("packet_integrity_level") or "")
                            packet_integrity_badge.text = translate(f"mesh_overview.level.{pi_level}", pi_level.title()) if pi_level else "-"
                            pi_col = str(snap.get("packet_integrity_color") or "")
                            _set_badge_color(packet_integrity_badge, pi_col)

                            mesh_signal_value.text = _fmt_num(snap.get("mesh_signal_score"))
                            ms_level = str(snap.get("mesh_signal_level") or "")
                            mesh_signal_badge.text = translate(f"mesh_overview.level.{ms_level}", ms_level.title()) if ms_level else "-"
                            ms_col = str(snap.get("mesh_signal_color") or "")
                            _set_badge_color(mesh_signal_badge, ms_col)

                            mesh_health_value.text = _fmt_num(snap.get("mesh_health_score"))
                            mh_level = str(snap.get("mesh_health_level") or "")
                            mesh_health_badge.text = translate(f"mesh_overview.health.{mh_level}", mh_level.title()) if mh_level else "-"
                            mh_col = str(snap.get("mesh_health_color") or "")
                            _set_badge_color(mesh_health_badge, mh_col)

                        ui.timer(1.0, _update_mesh_overview)
        
            with splitter.after:
                with ui.column().classes('h-full w-full no-wrap'):
                    # Top: Chat
                    with ui.row().classes('w-full items-center justify-between p-2'):
                        ui.label(translate("ui.chatmessages", "Chat Messages")).classes('font-bold')
                        chat_resume_btn = ui.button(translate("button.resumeautoscroll", "Resume Auto-Scroll"), icon='arrow_downward', on_click=lambda: enable_chat_scroll()).props('dense color=blue').classes('hidden')

                    chat_scroll = ui.scroll_area().classes('w-full flex-grow p-2 bg-slate-50 border rounded')
                    with chat_scroll:
                        chat_container = ui.column().classes('w-full')
                    
                    # Chat Scroll State
                    chat_scroll_state = {'auto': True, 'suppress_until': 0}

                    def handle_chat_scroll(e):
                        if time.time() < chat_scroll_state['suppress_until']: return
                        
                        # Pixel-based logic
                        if 'verticalPosition' in e.args and 'verticalSize' in e.args and 'verticalContainerSize' in e.args:
                            v_pos = e.args['verticalPosition']
                            v_size = e.args['verticalSize']
                            v_container = e.args['verticalContainerSize']
                            
                            dist_from_bottom = v_size - v_container - v_pos
                            
                            # If user scrolls up > 20px from bottom, disable auto-scroll
                            if dist_from_bottom > 20:
                                chat_scroll_state['auto'] = False
                                chat_resume_btn.classes(remove='hidden')
                            # If user scrolls back to very bottom, re-enable
                            elif dist_from_bottom < 5:
                                chat_scroll_state['auto'] = True
                                chat_resume_btn.classes(add='hidden')

                    chat_scroll.on('scroll', handle_chat_scroll, args=['verticalPosition', 'verticalSize', 'verticalContainerSize'])

                    def enable_chat_scroll():
                        chat_scroll_state['auto'] = True
                        chat_resume_btn.classes(add='hidden')
                        chat_scroll_state['suppress_until'] = time.time() + 0.5
                        chat_scroll.scroll_to(percent=1.0)

                    def get_sender_display_name(msg):
                        # Use cached ID if available to resolve latest name
                        s_id = msg.get('from_id')
                        if s_id and s_id in state.nodes:
                            n = state.nodes[s_id]
                            s_name = n.get('short_name', '???')
                            l_name = n.get('long_name', 'Unknown')
                            
                            has_short = s_name and s_name != "???"
                            has_long = l_name and l_name != "Unknown"
                            
                            if has_long and has_short:
                                return f"{l_name} ({s_name})"
                            elif has_short:
                                return s_name
                            elif has_long:
                                return l_name
                        
                        # Fallback to stored static name
                        return msg['from']

                    def chat_name_click(msg):
                        s_id = msg.get('from_id')
                        if not s_id and msg.get('from', '').startswith('!'):
                            s_id = msg.get('from')
                            
                        if s_id:
                            # Switch to Nodes Tab
                            tabs.set_value(nodes_tab)
                            
                            # Try multiple methods to set quick filter (supporting different AG Grid versions)
                            # v31+ uses setGridOption('quickFilterText', val)
                            # Older uses setQuickFilter(val)
                            # Even if we use specific version, is good keep it for backward/future compatibility or different versions
                            ui.run_javascript(f'''
                                if (window.mesh_grid_api) {{
                                    if (typeof window.mesh_grid_api.setGridOption === 'function') {{
                                        window.mesh_grid_api.setGridOption('quickFilterText', "{s_id}");
                                    }} else if (typeof window.mesh_grid_api.setQuickFilter === 'function') {{
                                        window.mesh_grid_api.setQuickFilter("{s_id}");
                                    }} else {{
                                        console.warn("No quick filter method found on api");
                                    }}
                                }}
                            ''')
                            ui.notify(translate("notification.positive.filterednodesby", "Filtered nodes by: {s_id}").format(s_id=s_id))

                    def update_chat():
                        # Handle Force Scroll (e.g. after import)
                        if state.chat_force_scroll:
                            chat_scroll_state['auto'] = True
                            chat_resume_btn.classes(add='hidden')
                            state.chat_force_scroll = False

                        # Check for forced refresh (e.g. names updated)
                        if state.chat_force_refresh:
                            chat_container.clear()
                            # Re-render ALL messages
                            for msg in state.messages:
                                with chat_container:
                                    sent = msg['is_me']
                                    name = get_sender_display_name(msg)
                                    time_str = msg.get('time', '')
                                    date_str = msg.get('date', '')
                                    
                                    meta = ""
                                    if time_str and date_str:
                                        meta = f"{time_str} | {date_str}"
                                    elif time_str:
                                        meta = time_str
                                    elif date_str:
                                        meta = date_str
                                    
                                    text_escaped = html.escape(msg.get('text', ''))
                                    body_html = text_escaped
                                    if meta:
                                        body_html = f"{body_html}<br><span class='mesh-chat-meta'>{meta}</span>"
                                    is_dark = state.theme == 'dark'
                                    if is_dark:
                                        bg_col = 'blue-6' if sent else 'blue-10'
                                        text_col = 'white'
                                    else:
                                        bg_col = 'green-9' if sent else 'green-6'
                                        text_col = 'gray'
                                    
                                    cm = ui.chat_message(sent=sent, stamp='')
                                    cm.props(f'bg-color={bg_col} text-color={text_col}')
                                    
                                    with cm.add_slot('name'):
                                        ui.label(name).classes('text-md font-bold text-gray-600 cursor-pointer hover:text-blue-600 hover:underline').on('click', lambda m=msg: chat_name_click(m))
                                    
                                    with cm:
                                        ui.html(body_html, sanitize=False)
                                    
                            state.chat_force_refresh = False
                            state.new_messages.clear() # We just rendered everything
                            
                            if chat_scroll_state['auto']:
                                chat_scroll_state['suppress_until'] = time.time() + 0.5
                                ui.timer(0.1, lambda: chat_scroll.scroll_to(percent=1.0), once=True)
                            return

                        if not state.new_messages: return
                        
                        while state.new_messages:
                            msg = state.new_messages.pop(0)
                            with chat_container:
                                sent = msg['is_me']
                                name = get_sender_display_name(msg)
                                time_str = msg.get('time', '')
                                date_str = msg.get('date', '')
                                
                                meta = ""
                                if time_str and date_str:
                                    meta = f"{time_str} | {date_str}"
                                elif time_str:
                                    meta = time_str
                                elif date_str:
                                    meta = date_str
                                
                                text_escaped = html.escape(msg.get('text', ''))
                                body_html = text_escaped
                                if meta:
                                    body_html = f"{body_html}<br><span class='mesh-chat-meta'>{meta}</span>"
                                is_dark = state.theme == 'dark'
                                if is_dark:
                                    bg_col = 'blue-6' if sent else 'blue-10'
                                    text_col = 'white'
                                else:
                                    bg_col = 'green-9' if sent else 'green-6'
                                    text_col = 'gray'
                                
                                cm = ui.chat_message(sent=sent, stamp='')
                                cm.props(f'bg-color={bg_col} text-color={text_col}')
                                
                                with cm.add_slot('name'):
                                    ui.label(name).classes('text-xs font-bold text-gray-600 cursor-pointer hover:text-blue-600 hover:underline').on('click', lambda m=msg: chat_name_click(m))
                                
                                with cm:
                                    ui.html(body_html, sanitize=False)
                        
                        if chat_scroll_state['auto']:
                            chat_scroll_state['suppress_until'] = time.time() + 0.5
                            ui.timer(0.1, lambda: chat_scroll.scroll_to(percent=1.0), once=True)

                    # Bottom: Console log
                    ui.separator()
                    with ui.row().classes('w-full items-center justify-between p-2 mb-1'):
                        ui.label(translate("ui.consolelog", "Console Log")).classes('font-bold')
                        
                        # Scroll to bottom button (hidden by default)
                        scroll_btn = ui.button(translate("button.resumeautoscroll", "Resume Auto-Scroll"), icon='arrow_downward', on_click=lambda: enable_auto_scroll()).props('dense color=green').classes('hidden')
                        
                    log_container = ui.scroll_area().classes('h-1/3 w-full bg-black text-green-500 p-2 font-mono text-xs select-text')
                    
                    # Scroll State
                    scroll_state = {'auto': True, 'suppress_until': 0}

                    def handle_scroll(e):
                        # Ignore scroll events immediately after programmatic scroll
                        if time.time() < scroll_state['suppress_until']:
                            return

                        # Pixel-based logic for robust "stick to bottom"
                        if 'verticalPosition' in e.args and 'verticalSize' in e.args and 'verticalContainerSize' in e.args:
                            v_pos = e.args['verticalPosition']
                            v_size = e.args['verticalSize']
                            v_container = e.args['verticalContainerSize']
                            
                            dist_from_bottom = v_size - v_container - v_pos
                            
                            # If user scrolls up > 20px from bottom, disable auto-scroll
                            if dist_from_bottom > 20:
                                 scroll_state['auto'] = False
                                 scroll_btn.classes(remove='hidden')
                            # If user scrolls back to very bottom, re-enable
                            elif dist_from_bottom < 5:
                                 scroll_state['auto'] = True
                                 scroll_btn.classes(add='hidden')
                    
                    log_container.on('scroll', handle_scroll, args=['verticalPosition', 'verticalSize', 'verticalContainerSize'])

                    def enable_auto_scroll():
                        scroll_state['auto'] = True
                        scroll_btn.classes(add='hidden')
                        scroll_state['suppress_until'] = time.time() + 0.5 # Ignore events
                        log_container.scroll_to(percent=1.0) # Correct method for ui.scroll_area

                    # Initial population of logs (history)
                    with log_container:
                        for l in state.logs:
                            ui.label(l).classes('select-text')
                        # Scroll to bottom initially
                        ui.timer(0.1, lambda: enable_auto_scroll(), once=True)

                    def update_log():
                        if not state.new_logs: return

                        # Batch process logs to avoid UI freezing
                        while state.new_logs:
                            l = state.new_logs.pop(0)
                            with log_container:
                                ui.label(l).classes('select-text').style('white-space: pre-wrap; font-family: monospace;')
                            
                        # ONLY scroll if auto-scroll is enabled
                        if scroll_state['auto']:
                            scroll_state['suppress_until'] = time.time() + 0.5 # Ignore events caused by this scroll
                            log_container.scroll_to(percent=1.0)


    def _check_autosave_on_start():
        autosave_path = get_autosave_path()
        if not os.path.isfile(autosave_path):
            return

        with ui.dialog() as dlg, ui.card().classes('w-96'):
            ui.label(translate("popup.autosave.found.title", "Autosave Found")).classes('text-lg font-bold mb-2')
            ui.label(translate("popup.autosave.found.body", "Previous autosave data has been found. Do you want to load it?")).classes('text-sm text-gray-600 mb-4')

            def do_load():
                try:
                    with open(autosave_path, 'r') as f:
                        data = json.load(f)
                    imported_nodes_count, total_nodes_in_file = _import_data_from_dict(data)
                    dlg.close()
                    with ui.dialog() as summary_dialog, ui.card().classes('w-96'):
                        ui.label(translate("popup.autosave.import.summary.title", "Autosave Import Summary")).classes('text-xl font-bold text-green-600 mb-4')
                        with ui.column().classes('w-full gap-2'):
                            ui.label(translate("popup.importdata.success.nodesinfile", "Nodes in File: {nodes_count}").format(nodes_count=total_nodes_in_file)).classes('text-lg')  
                            ui.label(translate("popup.importdata.success.nodesimported", "Nodes Imported: {nodes_imported_count}").format(nodes_imported_count=imported_nodes_count)).classes('text-lg font-bold')
                            ui.separator()
                            ui.label(translate("popup.importdata.success.totalnodesinapp", "Total Nodes in App: {total_nodes_in_app}").format(total_nodes_in_app=len(state.nodes))).classes('text-md text-gray-600')
                        ui.button('OK', on_click=summary_dialog.close).classes('w-full mt-4 bg-green-600')
                    summary_dialog.open()
                except Exception as e:
                    dlg.close()
                    ui.notify(translate("notification.error.autosaveimportfailed", "Autosave import failed: {error}").format(error=e), type='negative')

            def skip_load():
                dlg.close()
                try:
                    base_dir = os.path.dirname(autosave_path)
                    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                    base_name = os.path.basename(autosave_path)
                    new_name = f"backup_data_{ts}_{base_name}"
                    new_path = os.path.join(base_dir, new_name)
                    os.rename(autosave_path, new_path)
                    with ui.dialog() as info_dlg, ui.card().classes('w-96'):
                        ui.label(translate("popup.autosave.archived.title", "Autosave Archived")).classes('text-lg font-bold mb-2')
                        ui.label(translate("popup.autosave.archived.body", "Previous autosave file has been renamed to:")).classes('text-sm text-gray-600')
                        ui.label(new_path).classes('text-sm font-mono break-all')
                        ui.separator().classes('my-2')
                        ui.label(translate("popup.autosave.archived.help1", "If you want to keep working with this data, import this backup file using the Import Data function.")).classes('text-sm text-gray-600')
                        ui.label(translate("popup.autosave.archived.help2", "Imported data will be merged with the current session, and future autosaves will include both new data and the imported backup.")).classes('text-sm text-gray-600')
                        ui.button('OK', on_click=info_dlg.close).classes('w-full mt-2 bg-slate-200 text-slate-900')
                    info_dlg.open()
                except Exception as e:
                    ui.notify(translate("notification.error.autosaverenamefailed", "Autosave rename failed: {error}").format(error=e), type='negative')

            with ui.row().classes('w-full justify-end gap-2'):
                ui.button(translate("button.no", "No"), on_click=skip_load).classes('bg-slate-200 text-slate-900')
                ui.button(translate("button.yes", "Yes"), on_click=do_load).classes('bg-blue-600 text-white')

        dlg.open()

    ui.timer(1.0, update_map)
    ui.timer(0.2, update_grid)
    ui.timer(0.5, update_chat)
    ui.timer(0.1, update_log)
    ui.timer(1.0, _autosave_tick)

    _check_autosave_on_start()
    if not state.connection_dialog_shown:
        state.connection_dialog_shown = True
        connection_dialog.open()

def open_chrome_app(url: str):
    chrome_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if not os.path.exists(chrome_path):
        try:
            subprocess.run([
                'osascript', '-e',
                'display dialog "Google Chrome not found. Please install Chrome to use this application." buttons {"OK"} default button 1 with icon stop'
            ])
        except:
            pass
        sys.exit(1)  # Exit app if Chrome not found

    return subprocess.Popen([
        chrome_path,
        "--app=" + url,
        "--disable-features=Translate,TranslateUI",
        "--disable-translate",
        "--disable-session-crashed-bubble",
        "--no-first-run",
        "--disable-gpu",
    ])

def find_free_port(start_port: int = 8000, max_tries: int = 100) -> int:
    for port in range(start_port, start_port + max_tries):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(('127.0.0.1', port))
            s.close()
            return port
        except OSError:
            s.close()
            continue
    raise RuntimeError("No free port found")

def _detect_window_size():
    base_w, base_h = 1200, 720
    try:
        import tkinter as tk
        root = tk.Tk()
        root.withdraw()
        sw = root.winfo_screenwidth()
        sh = root.winfo_screenheight()
        root.destroy()
        w = min(base_w, max(800, sw - 80))
        h = min(base_h, max(600, sh - 80))
        return w, h
    except Exception:
        return base_w, base_h

if __name__ in {"__main__", "__mp_main__"}:
    # Mandatory for PyInstaller on macOS/Linux to prevent infinite spawn loop
    multiprocessing.freeze_support()
    
    try:
        ensure_app_icon_file()
        system = platform.system()

        if system == "Linux":
            if not check_linux_native_deps():
                sys.exit(1)

        if system == "Darwin":
            port = find_free_port(8000)
            
            def on_startup():
                import time
                time.sleep(0.5)
                
                url = f"http://127.0.0.1:{port}"
                open_chrome_app(url)
            
            app.on_startup(on_startup)

            ui.run(
                title=f'{PROGRAM_NAME} v{VERSION} By {AUTHOR}',
                favicon=get_resource_path('app_icon.svg'),
                host='127.0.0.1',
                port=port,
                reload=False,
                show=False,
            )

        else:
            # Windows / Linux: use native pywebview
            # Enable dev tools if configured (must be done before ui.run implicitly or explicitly)
            if SHOW_DEV_TOOLS:
                # Enable debug mode for pywebview using NiceGUI's native configuration
                # This is the correct way to pass arguments to webview.start()
                app.native.start_args['debug'] = True
                
                # Also try setting the env var as a backup, though start_args should prevail
                os.environ['pywebview_debug'] = 'true'
                
                print("Dev Tools Enabled: Use Right-Click -> Inspect or F12 (if supported)")

            if system == "Linux":
                os.environ['LANG'] = 'C.UTF-8'
                os.environ['LC_ALL'] = 'C.UTF-8'

                # 2. mute qt video driver logging
                os.environ['QT_LOGGING_RULES'] = '*.debug=false;qt.qpa.*=false'
                os.environ['MESA_LOG_LEVEL'] = '0'
                try:
                    app.native.start_args['gui'] = 'qt'
                except Exception:
                    pass

            # Use bundled icon (SVG, supported as NiceGUI favicon)
            icon_path = get_resource_path('app_icon.svg')
            
            # Native mode arguments for better compatibility
            # macOS often needs specific flags to avoid crashes (e.g. reload=False is crucial)
            # Linux GTK can also be picky.
            win_w, win_h = _detect_window_size()
            ui.run(
                title=f'{PROGRAM_NAME} - {PROGRAM_SHORT_DESC} v{VERSION} By {AUTHOR}', 
                favicon=icon_path, 
                native=True, 
                host='127.0.0.1',
                reload=False, # Important for stability in native mode
                window_size=(win_w, win_h),
                storage_secret='meshstation_secret' # Adding a secret often helps with pywebview storage init
            )
    except KeyboardInterrupt:
        # Suppress KeyboardInterrupt traceback on exit
        pass
    except Exception as e:
        # Log other unexpected errors but don't show traceback if it's just a shutdown thing
        print(f"App closed with error: {e}")
