#!/usr/bin/env python3
"""
Flarvify Python Backend - ytmusicapi Integration
Fast, reliable YouTube Music search and streaming

Features:
- YouTube Music search via ytmusicapi
- Stream URL extraction for direct audio playback
- Lyrics integration
- Download support with progress
- Rate limiting and caching
"""

import os
import json
import hashlib
import time
import re
import math
import asyncio
from functools import lru_cache
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
from urllib.parse import urlencode
from urllib.request import urlopen, Request as UrlRequest
from urllib.error import URLError, HTTPError

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
import uvicorn
import requests
import yt_dlp
from ytmusicapi import YTMusic

app = FastAPI(
    title="Flarvify Music API",
    description="YouTube Music backend for Flarvify music player",
    version="2.0.0"
)

allowed_origin_csv = os.getenv("ALLOWED_ORIGINS", "")
allowed_origins = [
    origin.strip()
    for origin in allowed_origin_csv.split(",")
    if origin.strip()
]
allowed_origins = list(dict.fromkeys(
    allowed_origins + [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        # Capacitor Android WebView uses https://localhost for app requests.
        "https://localhost",
        "capacitor://localhost",
        "ionic://localhost",
        "https://flarv-backend-production.up.railway.app",
    ]
))

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def public_music_search_cors(request: Request, call_next):
    if request.url.path.rstrip("/") == "/search" and request.method == "OPTIONS":
        origin = request.headers.get("origin")
        headers = {
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": request.headers.get("access-control-request-headers", "*"),
            "Access-Control-Max-Age": "600",
            "Vary": "Origin",
        }
        if origin:
            headers["Access-Control-Allow-Origin"] = origin
        return Response(status_code=204, headers=headers)

    response = await call_next(request)
    if request.url.path.rstrip("/") == "/search":
        origin = request.headers.get("origin")
        if origin:
            response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
        response.headers["Vary"] = "Origin"
    return response


ytmusic = YTMusic()

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

cache_store: Dict[str, Dict[str, Any]] = {}
LRCLIB_BASE_URL = "https://lrclib.net/api"

class Track(BaseModel):
    videoId: str
    title: str
    artist: str
    thumbnail: str
    duration: int
    album: Optional[str] = None
    views: Optional[int] = None

class SearchResult(BaseModel):
    success: bool
    data: List[Track]
    error: Optional[str] = None
    source: str = "ytmusicapi"

class StreamInfo(BaseModel):
    videoId: str
    audioUrl: Optional[str] = None
    duration: int
    title: str
    artist: str
    thumbnail: str
    formats: List[Dict[str, Any]] = []

class DownloadProgress(BaseModel):
    videoId: str
    status: str
    progress: float
    speed: Optional[str] = None
    eta: Optional[str] = None
    filepath: Optional[str] = None

download_progress: Dict[str, DownloadProgress] = {}


def get_cache_key(prefix: str, *args) -> str:
    key_str = "_".join(str(a) for a in args)
    return f"{prefix}_{hashlib.md5(key_str.encode()).hexdigest()}"


def get_cached(key: str, max_age_seconds: int = 3600) -> Optional[Any]:
    if key in cache_store:
        entry = cache_store[key]
        if datetime.now() - entry["timestamp"] < timedelta(seconds=max_age_seconds):
            return entry["data"]
        del cache_store[key]
    return None


def set_cache(key: str, data: Any) -> None:
    cache_store[key] = {
        "data": data,
        "timestamp": datetime.now()
    }
    if len(cache_store) > 1000:
        oldest = min(cache_store.items(), key=lambda x: x[1]["timestamp"])
        del cache_store[oldest[0]]


def delete_cache(key: str) -> None:
    if key in cache_store:
        del cache_store[key]


class QuietYTDLPLogger:
    def debug(self, msg):
        return None

    def warning(self, msg):
        return None

    def error(self, msg):
        return None


def classify_stream_resolution_error(error: Exception) -> Tuple[int, str]:
    normalized = str(error or "").strip().lower()

    if (
        "not available in your country" in normalized
        or "geo-restricted" in normalized
        or "not available in your region" in normalized
    ):
        return 451, "This video is not available in your region"

    if "private video" in normalized or "is private" in normalized:
        return 403, "This video is private"

    if "members-only" in normalized or "members only" in normalized:
        return 403, "This video is members-only"

    if "sign in to confirm your age" in normalized or "age-restricted" in normalized:
        return 403, "This video is age-restricted"

    if (
        "video unavailable" in normalized
        or "this video is unavailable" in normalized
        or "this video is not available" in normalized
        or "video has been removed" in normalized
        or "video is no longer available" in normalized
        or "does not exist" in normalized
    ):
        return 404, "This video is not available"

    return 502, "Could not resolve stream metadata"


def parse_duration(duration_str: str) -> int:
    if not duration_str:
        return 0
    parts = [p.strip() for p in str(duration_str).split(":") if p.strip()]
    try:
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        if len(parts) == 2 and all(p.isdigit() for p in parts):
            return int(parts[0]) * 60 + int(parts[1])
    except Exception:
        return 0
    return 0


def to_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def build_stream_format_selector(normalized_quality: str) -> str:
    """
    Prefer Android/WebView-safe codecs/containers first.
    Fallbacks remain to keep availability high.
    """
    if normalized_quality.isdigit():
        max_abr = normalized_quality
        return (
            f"bestaudio[ext=m4a][abr<={max_abr}]/"
            f"bestaudio[ext=mp4][abr<={max_abr}]/"
            f"bestaudio[acodec*=mp4a][abr<={max_abr}]/"
            f"bestaudio[acodec*=aac][abr<={max_abr}]/"
            f"bestaudio[ext=mp3][abr<={max_abr}]/"
            f"bestaudio[abr<={max_abr}]/"
            "bestaudio/best"
        )
    return (
        "bestaudio[ext=m4a]/"
        "bestaudio[ext=mp4]/"
        "bestaudio[acodec*=mp4a]/"
        "bestaudio[acodec*=aac]/"
        "bestaudio[ext=mp3]/"
        "bestaudio/best"
    )


def score_audio_format(candidate: Dict[str, Any], max_abr: Optional[float] = None) -> float:
    ext = str(candidate.get("ext", "") or "").strip().lower()
    acodec = str(candidate.get("acodec", "") or "").strip().lower()
    abr = to_float(candidate.get("abr"))

    score = 0.0

    # Prefer formats that are reliable inside Android WebView.
    ext_score = {
        "m4a": 110.0,
        "mp4": 100.0,
        "aac": 95.0,
        "mp3": 90.0,
        "webm": 30.0,
    }
    score += ext_score.get(ext, 20.0)

    if "mp4a" in acodec or "aac" in acodec:
        score += 80.0
    elif "mp3" in acodec:
        score += 70.0
    elif "opus" in acodec or "vorbis" in acodec:
        # Commonly problematic on some Android WebViews.
        score -= 40.0

    if abr > 0:
        # Keep some quality preference while avoiding huge overshoot.
        score += min(abr, 256.0) / 6.0

    if max_abr and max_abr > 0 and abr > 0:
        if abr <= max_abr:
            score += 120.0
        else:
            # Still allow overshoot fallback, but penalize.
            score -= min(80.0, (abr - max_abr) * 0.6)

    return score


NOISE_WORDS_RE = re.compile(
    r"\b(official|lyrics?|lyric\s*video|video|audio|mv|hd|4k|feat\.?|ft\.?|featuring|topic)\b",
    re.IGNORECASE,
)


def parse_view_count(value: Any) -> Optional[int]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return int(value)

    text = str(value).strip().lower()
    if not text:
        return None

    text = text.replace("views", "").replace("view", "").replace(",", "").strip()
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*([kmb])?", text)
    if not match:
        return None

    number = float(match.group(1))
    suffix = (match.group(2) or "").lower()
    multiplier = 1
    if suffix == "k":
        multiplier = 1_000
    elif suffix == "m":
        multiplier = 1_000_000
    elif suffix == "b":
        multiplier = 1_000_000_000

    return int(number * multiplier)


def parse_subscriber_count(value: Any) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    text = text.replace("subscribers", "").replace("subscriber", "").strip()
    return parse_view_count(text)


def normalize_search_text(value: Any) -> str:
    text = str(value or "").lower()
    text = text.replace("&", " and ")
    text = re.sub(r"\s*-\s*topic\b", " ", text)
    text = NOISE_WORDS_RE.sub(" ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_artist_query_match(query: str, artist_name: str) -> bool:
    normalized_query = normalize_search_text(query)
    normalized_artist = normalize_search_text(artist_name)
    if not normalized_query or not normalized_artist:
        return False
    if normalized_artist == normalized_query:
        return True
    if normalized_artist.startswith(normalized_query):
        return True
    if normalized_query in normalized_artist and len(normalized_query) >= 4:
        return True
    if normalized_artist in normalized_query and len(normalized_artist) >= 4:
        return True
    return False


def score_track_relevance(
    track: Dict[str, Any],
    query: str,
    boosted_artist: Optional[str] = None,
) -> int:
    normalized_query = normalize_search_text(query)
    title = normalize_search_text(track.get("title", ""))
    artist = normalize_search_text(track.get("artist", ""))
    album = normalize_search_text(track.get("album", ""))
    boosted = normalize_search_text(boosted_artist or "")

    if not normalized_query:
        return 0

    score = 0

    if artist == normalized_query:
        score += 130
    elif artist.startswith(normalized_query):
        score += 95
    elif normalized_query in artist:
        score += 65

    if title == normalized_query:
        score += 95
    elif title.startswith(normalized_query):
        score += 70
    elif normalized_query in title:
        score += 45

    if normalized_query in album:
        score += 20

    query_tokens = [token for token in normalized_query.split(" ") if token]
    if query_tokens:
        score += sum(6 for token in query_tokens if token in title)
        score += sum(8 for token in query_tokens if token in artist)

    if boosted:
        if artist == boosted:
            score += 90
        elif artist.startswith(boosted):
            score += 70
        elif boosted in artist:
            score += 45

    views = parse_view_count(track.get("views"))
    if views and views > 0:
        score += min(18, int(math.log10(views + 1) * 2.2))

    return score


def normalize_lyrics_query_text(value: str) -> str:
    text = str(value or "")
    text = re.sub(r"[\[\(\{].*?[\]\)\}]", " ", text)
    text = re.sub(r"\s*-\s*topic\b", " ", text, flags=re.IGNORECASE)
    text = NOISE_WORDS_RE.sub(" ", text)
    text = re.sub(r"[_|]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" -")
    return text


def build_lyric_query_pairs(artist: str, title: str, extra_pairs: Optional[List[Dict[str, str]]] = None) -> List[Dict[str, str]]:
    normalized_artist = normalize_lyrics_query_text(artist)
    normalized_title = normalize_lyrics_query_text(title)
    title_before_dash = normalize_lyrics_query_text(title.split(" - ")[0] if title else "")
    has_artist = bool(normalized_artist or str(artist or "").strip())

    candidates: List[Dict[str, str]] = [
        {"artist": artist.strip(), "title": title.strip()},
        {"artist": normalized_artist, "title": normalized_title or title.strip()},
        {"artist": normalized_artist, "title": title_before_dash or normalized_title or title.strip()},
    ]

    if not has_artist:
        candidates.extend(
            [
                {"artist": "", "title": normalized_title or title.strip()},
                {"artist": "", "title": title_before_dash or normalized_title or title.strip()},
            ]
        )

    if extra_pairs:
        for pair in extra_pairs:
            candidates.append(
                {
                    "artist": normalize_lyrics_query_text(str(pair.get("artist", "") or "")),
                    "title": normalize_lyrics_query_text(str(pair.get("title", "") or "")),
                }
            )

    unique: List[Dict[str, str]] = []
    seen: set[str] = set()
    for pair in candidates:
        a = str(pair.get("artist", "") or "").strip()
        t = str(pair.get("title", "") or "").strip()
        if not t:
            continue
        key = f"{a.lower()}::{t.lower()}"
        if key in seen:
            continue
        seen.add(key)
        unique.append({"artist": a, "title": t})

    return unique


def tokenize_lyrics_match_text(value: str) -> List[str]:
    return [token for token in normalize_search_text(value).split(" ") if token]


def score_lyrics_text_match(expected: str, candidate: str) -> int:
    normalized_expected = normalize_search_text(expected)
    normalized_candidate = normalize_search_text(candidate)
    if not normalized_expected or not normalized_candidate:
        return 0

    if normalized_expected == normalized_candidate:
        return 140

    compact_expected = normalized_expected.replace(" ", "")
    compact_candidate = normalized_candidate.replace(" ", "")
    score = 0

    if compact_expected == compact_candidate:
        score = max(score, 132)

    if (
        normalized_candidate.startswith(normalized_expected)
        or normalized_expected.startswith(normalized_candidate)
    ):
        score = max(score, 112)
    elif normalized_expected in normalized_candidate or normalized_candidate in normalized_expected:
        score = max(score, 92)

    expected_tokens = tokenize_lyrics_match_text(expected)
    candidate_tokens = tokenize_lyrics_match_text(candidate)
    if expected_tokens and candidate_tokens:
        expected_set = set(expected_tokens)
        candidate_set = set(candidate_tokens)
        overlap = expected_set & candidate_set
        if overlap:
            overlap_ratio = len(overlap) / max(1, len(expected_set))
            coverage_bonus = int(52 + overlap_ratio * 54)
            if expected_set.issubset(candidate_set):
                coverage_bonus += 10
            score = max(score, coverage_bonus)

    return score


def score_lyrics_candidate(
    expected_artist: str,
    expected_title: str,
    candidate_artist: str,
    candidate_title: str,
) -> int:
    title_score = score_lyrics_text_match(expected_title, candidate_title)
    artist_score = score_lyrics_text_match(expected_artist, candidate_artist) if expected_artist else 0
    total = title_score * 2 + artist_score

    normalized_expected_artist = normalize_search_text(expected_artist)
    normalized_candidate_artist = normalize_search_text(candidate_artist)
    normalized_expected_title = normalize_search_text(expected_title)
    normalized_candidate_title = normalize_search_text(candidate_title)

    if normalized_expected_artist and normalized_candidate_artist:
        if artist_score == 0:
            total -= 120
        elif artist_score < 60:
            total -= 35

    penalty_words = ("live", "karaoke", "instrumental", "remix", "sped up", "slowed")
    for word in penalty_words:
        if word in normalized_candidate_title and word not in normalized_expected_title:
            total -= 34

    if normalized_expected_title and normalized_candidate_title:
        expected_tokens = set(tokenize_lyrics_match_text(expected_title))
        candidate_tokens = set(tokenize_lyrics_match_text(candidate_title))
        if expected_tokens and candidate_tokens:
            extra_tokens = candidate_tokens - expected_tokens
            if len(extra_tokens) >= 4:
                total -= 22

    return total


def minimum_lyrics_match_score(pair: Dict[str, str]) -> int:
    artist = str(pair.get("artist", "") or "").strip()
    return 185 if artist else 220


def extract_lrclib_artist_title(item: Dict[str, Any]) -> Tuple[str, str]:
    candidate_artist = str(
        item.get("artistName")
        or item.get("artist")
        or item.get("artist_name")
        or ""
    ).strip()
    candidate_title = str(
        item.get("trackName")
        or item.get("name")
        or item.get("title")
        or item.get("track_name")
        or ""
    ).strip()
    return candidate_artist, candidate_title


def select_best_lrclib_search_result(
    search_data: List[Dict[str, Any]],
    pair: Dict[str, str],
) -> Optional[Dict[str, Any]]:
    if not search_data:
        return None

    expected_artist = str(pair.get("artist", "") or "").strip()
    expected_title = str(pair.get("title", "") or "").strip()
    best_item: Optional[Dict[str, Any]] = None
    best_score = float("-inf")

    for item in search_data:
        if not isinstance(item, dict):
            continue
        synced_lrc = item.get("syncedLyrics")
        if not isinstance(synced_lrc, str) or not synced_lrc.strip():
            continue
        candidate_artist, candidate_title = extract_lrclib_artist_title(item)
        score = score_lyrics_candidate(
            expected_artist,
            expected_title,
            candidate_artist,
            candidate_title,
        )
        if score > best_score:
            best_score = score
            best_item = item

    if best_item and best_score >= minimum_lyrics_match_score(pair):
        return best_item

    return None


def collect_ytmusic_lyric_candidate_video_ids(
    query_pairs: List[Dict[str, str]],
    limit: int = 4,
) -> List[str]:
    ranked_candidates: Dict[str, int] = {}

    for pair in query_pairs:
        search_query = " ".join(
            [part for part in [pair.get("artist", ""), pair.get("title", "")] if part]
        ).strip()
        if not search_query:
            continue

        try:
            search_results = ytmusic.search(search_query, filter="songs", limit=5)
        except Exception:
            continue

        if not isinstance(search_results, list):
            continue

        for item in search_results:
            normalized_item = normalize_track_item(item)
            if not normalized_item:
                continue

            video_id = str(normalized_item.get("videoId", "") or "").strip()
            if not video_id:
                continue

            score = score_lyrics_candidate(
                str(pair.get("artist", "") or ""),
                str(pair.get("title", "") or ""),
                str(normalized_item.get("artist", "") or ""),
                str(normalized_item.get("title", "") or ""),
            )
            if score < minimum_lyrics_match_score(pair):
                continue

            current_score = ranked_candidates.get(video_id)
            if current_score is None or score > current_score:
                ranked_candidates[video_id] = score

    ranked_ids = sorted(ranked_candidates.items(), key=lambda item: item[1], reverse=True)
    return [video_id for video_id, _ in ranked_ids[:limit]]


def _normalize_image_url(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = f"https:{raw}"
    if raw.startswith("http://"):
        raw = f"https://{raw[7:]}"
    if raw.startswith("https://"):
        return raw
    return ""


def _extract_image_from_node(node: Any) -> str:
    if isinstance(node, str):
        return _normalize_image_url(node)
    if isinstance(node, dict):
        direct = _normalize_image_url(node.get("url"))
        if direct:
            return direct
        thumbs = node.get("thumbnails")
        if isinstance(thumbs, list):
            for candidate in reversed(thumbs):
                parsed = _extract_image_from_node(candidate)
                if parsed:
                    return parsed
        if isinstance(node.get("thumbnail"), dict):
            parsed = _extract_image_from_node(node.get("thumbnail"))
            if parsed:
                return parsed
    if isinstance(node, list):
        for candidate in reversed(node):
            parsed = _extract_image_from_node(candidate)
            if parsed:
                return parsed
    return ""


def _last_thumbnail(item: Dict[str, Any]) -> str:
    if not isinstance(item, dict):
        return ""

    for key in ("thumbnails", "thumbnail", "thumbnailRenderer", "cover"):
        if key in item:
            parsed = _extract_image_from_node(item.get(key))
            if parsed:
                return parsed

    # Some ytmusicapi payloads keep imagery in nested renderer nodes.
    for nested_key in ("musicThumbnailRenderer", "musicResponsiveListItemThumbnailRenderer"):
        if nested_key in item and isinstance(item[nested_key], dict):
            parsed = _extract_image_from_node(item[nested_key])
            if parsed:
                return parsed

    return ""


def _artist_name_from_item(item: Dict[str, Any], fallback: str = "Unknown Artist") -> str:
    artists = item.get("artists", []) if isinstance(item, dict) else []
    if isinstance(artists, list) and len(artists) > 0 and isinstance(artists[0], dict):
        return str(artists[0].get("name", fallback) or fallback)
    return fallback


def normalize_track_item(item: Dict[str, Any], fallback_artist: str = "Unknown Artist") -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None

    video_id = str(item.get("videoId", "") or "")
    if not video_id:
        return None

    title = str(item.get("title", "Unknown Title") or "Unknown Title")
    artist_name = _artist_name_from_item(item, fallback_artist)
    thumbnail = _last_thumbnail(item)

    duration_value = item.get("duration_seconds")
    if isinstance(duration_value, (int, float)):
        duration = int(duration_value)
    else:
        duration = parse_duration(str(item.get("duration", "") or ""))

    if not thumbnail:
        thumbnail = f"https://img.youtube.com/vi/{video_id}/mqdefault.jpg"

    views = parse_view_count(
        item.get("views")
        or item.get("viewCount")
        or item.get("plays")
        or item.get("playCount")
    )

    return {
        "videoId": video_id,
        "title": title,
        "artist": artist_name,
        "thumbnail": thumbnail,
        "duration": duration,
        "views": views,
    }


def normalize_artist_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None

    artist_id = str(item.get("browseId", "") or "")
    if not artist_id:
        return None

    name = str(item.get("artist", "") or item.get("title", "") or "Unknown Artist")
    if not name:
        name = "Unknown Artist"
    subscribers_raw = item.get("subscribers")
    subscribers = str(subscribers_raw).strip() if subscribers_raw is not None else ""
    subscribers_count = parse_subscriber_count(subscribers)

    return {
        "id": artist_id,
        "name": name,
        "thumbnail": _last_thumbnail(item),
        "subscribers": subscribers or None,
        "subscribersCount": subscribers_count,
        "listeners": subscribers or None,
        "listenersCount": subscribers_count,
    }


def normalize_collection_item(item: Dict[str, Any], item_type: str) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None

    browse_id = str(
        item.get("browseId", "")
        or item.get("playlistId", "")
        or item.get("audioPlaylistId", "")
        or item.get("albumId", "")
        or ""
    )
    title = str(item.get("title", "") or item.get("name", "") or "").strip()
    if not title:
        return None

    stable_id = browse_id or hashlib.md5(f"{item_type}:{title}".encode()).hexdigest()
    release_date = (
        item.get("releaseDate")
        or item.get("release_date")
        or item.get("date")
        or item.get("year")
    )
    return {
        "id": stable_id,
        "browseId": browse_id or None,
        "title": title,
        "thumbnail": _last_thumbnail(item) or _last_thumbnail(item.get("album", {})),
        "year": item.get("year"),
        "releaseDate": release_date,
        "type": item_type,
    }


def parse_ytmusic_track(item: Dict) -> Optional[Track]:
    try:
        if not isinstance(item, dict):
            return None

        if item.get("resultType") not in ["song", "video"]:
            return None
        
        video_id = item.get("videoId")
        if not video_id:
            return None
        
        title = str(item.get("title", "Unknown Title") or "Unknown Title")
        
        artists = item.get("artists", [])
        if isinstance(artists, list) and artists and isinstance(artists[0], dict):
            artist = str(artists[0].get("name", "Unknown Artist") or "Unknown Artist")
        else:
            artist = "Unknown Artist"
        
        thumbnails = item.get("thumbnails", [])
        if isinstance(thumbnails, list) and thumbnails and isinstance(thumbnails[-1], dict):
            thumbnail = str(thumbnails[-1].get("url", "") or "")
        else:
            thumbnail = f"https://img.youtube.com/vi/{video_id}/mqdefault.jpg"
        
        duration_value = item.get("duration_seconds")
        if isinstance(duration_value, (int, float)):
            duration = int(duration_value)
        else:
            duration = parse_duration(str(item.get("duration", "") or ""))
        
        album = None
        if "album" in item and item["album"]:
            album = item["album"].get("name")
        
        views = parse_view_count(item.get("views"))
        
        return Track(
            videoId=video_id,
            title=title,
            artist=artist,
            thumbnail=thumbnail,
            duration=duration,
            album=album,
            views=views
        )
    except Exception:
        return None


@app.get("/")
async def root():
    return {
        "status": "OK",
        "service": "Flarvify Music API",
        "version": "2.0.0",
        "endpoints": [
            "/search",
            "/artists/search",
            "/artist/{channel_id}/catalog",
            "/artist/{channel_id}",
            "/album/{browse_id}",
            "/stream/{video_id}",
            "/lyrics",
            "/download/{video_id}",
        ]
    }


@app.get("/health")
async def health():
    return {"status": "OK", "timestamp": datetime.now().isoformat()}


@app.get("/update-check")
async def update_check(current: str = Query("", description="Current app version")):
    """
    Lightweight compatibility endpoint when mobile clients point directly to python_api.
    Real OTA update serving is handled by server/index.js.
    """
    current_version = str(current or "").strip()
    fallback_version = str(os.getenv("APP_VERSION", "1.0.0") or "1.0.0").strip()
    return {
        "hasUpdate": False,
        "version": current_version or fallback_version,
        "url": "",
    }


@app.get("/search", response_model=SearchResult)
async def search(
    response: Response,
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(25, ge=1, le=50, description="Number of results"),
):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Vary"] = "Origin"

    cache_key = get_cache_key("search", q, limit)
    cached = get_cached(cache_key, max_age_seconds=1800)
    if cached:
        return SearchResult(success=True, data=cached, source="cache")
    
    try:
        tracks: List[Dict[str, Any]] = []
        seen_video_ids: set[str] = set()

        def add_track(track_dict: Optional[Dict[str, Any]]) -> None:
            if not track_dict:
                return
            video_id = str(track_dict.get("videoId", "") or "").strip()
            if not video_id or video_id in seen_video_ids:
                return
            seen_video_ids.add(video_id)
            tracks.append(track_dict)

        primary_results = ytmusic.search(q, filter="songs", limit=max(limit * 2, 30))
        for item in primary_results:
            parsed = parse_ytmusic_track(item)
            add_track(parsed.model_dump() if parsed else None)

        if len(tracks) < max(8, limit // 2):
            video_results = ytmusic.search(q, filter="videos", limit=max(limit, 18))
            for item in video_results:
                parsed = parse_ytmusic_track(item)
                add_track(parsed.model_dump() if parsed else None)

        boosted_artist_name = ""
        try:
            artist_candidates = ytmusic.search(q, filter="artists", limit=3)
            if isinstance(artist_candidates, list) and artist_candidates:
                top_artist = normalize_artist_item(artist_candidates[0])
                if top_artist and is_artist_query_match(q, top_artist.get("name", "")):
                    boosted_artist_name = str(top_artist.get("name", "") or "").strip()
                    artist_payload = ytmusic.get_artist(str(top_artist.get("id", "") or ""))
                    songs_section = artist_payload.get("songs", {}) if isinstance(artist_payload, dict) else {}
                    artist_song_items = songs_section.get("results", []) if isinstance(songs_section, dict) else []
                    for item in artist_song_items:
                        add_track(normalize_track_item(item, boosted_artist_name))
        except Exception:
            boosted_artist_name = ""

        tracks.sort(
            key=lambda item: (
                score_track_relevance(item, q, boosted_artist_name),
                parse_view_count(item.get("views")) or 0,
            ),
            reverse=True,
        )

        final_tracks = tracks[:limit]
        set_cache(cache_key, final_tracks)
        return SearchResult(success=True, data=final_tracks, source="ytmusicapi")
    
    except Exception as e:
        return SearchResult(success=False, data=[], error=str(e))


@app.get("/artists/search")
async def search_artists(
    q: str = Query(..., min_length=1, description="Artist search query"),
    limit: int = Query(8, ge=1, le=20, description="Number of artist results")
):
    cache_key = get_cache_key("artists_search", q, limit)
    cached = get_cached(cache_key, max_age_seconds=1800)
    if cached:
        return {"success": True, "data": cached, "source": "cache"}

    try:
        results = ytmusic.search(q, filter="artists", limit=limit)
        artists: List[Dict[str, Any]] = []
        seen = set()

        for item in results:
            parsed = normalize_artist_item(item)
            if not parsed:
                continue
            artist_id = parsed["id"]
            if artist_id in seen:
                continue
            seen.add(artist_id)
            artists.append(parsed)

        if len(artists) == 0:
            mixed_results = ytmusic.search(q, limit=max(limit * 2, 10))
            for item in mixed_results:
                result_type = str(item.get("resultType", "") or "").lower()
                if result_type != "artist":
                    continue
                parsed = normalize_artist_item(item)
                if not parsed:
                    continue
                artist_id = parsed["id"]
                if artist_id in seen:
                    continue
                seen.add(artist_id)
                artists.append(parsed)
                if len(artists) >= limit:
                    break

        set_cache(cache_key, artists[:limit])
        return {"success": True, "data": artists[:limit], "source": "ytmusicapi"}

    except Exception as e:
        return {"success": False, "error": str(e), "data": []}


@app.get("/stream/{video_id}", response_model=StreamInfo)
async def get_stream_info(
    video_id: str,
    request: Request,
    quality: str = Query("best", description="Audio quality: best, 128, 192, 256, 320"),
    proxy: Optional[str] = Query(None, description="Set to 1/true to proxy audio bytes"),
    pipe: Optional[str] = Query(None, description="Alias for proxy"),
):
    normalized_quality = str(quality or "best").strip().lower()
    if normalized_quality not in {"best", "128", "192", "256", "320"}:
        if normalized_quality == "low":
            normalized_quality = "128"
        elif normalized_quality == "medium":
            normalized_quality = "192"
        elif normalized_quality == "high":
            normalized_quality = "256"
        else:
            normalized_quality = "best"

    proxy_raw = proxy if proxy is not None else pipe
    force_proxy = str(proxy_raw or "").strip().lower() in {"1", "true", "yes"}

    cache_key = get_cache_key("stream", video_id, normalized_quality)
    error_cache_key = get_cache_key("stream_err", video_id, normalized_quality)

    cached_error = get_cached(error_cache_key, max_age_seconds=300)
    if isinstance(cached_error, dict):
        status_code = int(cached_error.get("status_code") or 502)
        detail = str(cached_error.get("detail") or "Could not resolve stream metadata")
        raise HTTPException(status_code=status_code, detail=detail)

    cached = get_cached(cache_key, max_age_seconds=3600)
    resolved: Optional[Dict[str, Any]] = cached if isinstance(cached, dict) else None
    if resolved:
        cached_audio_url = str(resolved.get("audioUrl") or "").strip()
        if not cached_audio_url:
            delete_cache(cache_key)
            set_cache(error_cache_key, {"status_code": 404, "detail": "No playable audio stream found"})
            resolved = None
        elif not force_proxy:
            return resolved

    if not resolved:
        try:
            format_str = build_stream_format_selector(normalized_quality)
            ydl_opts = {
                "quiet": True,
                "no_warnings": True,
                "format": format_str,
                "extract_flat": False,
                "logger": QuietYTDLPLogger(),
            }

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)

                audio_url = None
                formats = []
                selected_ext = ""
                selected_acodec = ""
                max_abr = to_float(normalized_quality) if normalized_quality.isdigit() else None

                for f in info.get("formats", []):
                    if f.get("acodec") != "none" and f.get("vcodec") == "none":
                        abr_val = f.get("abr")
                        formats.append({
                            "format_id": f.get("format_id"),
                            "ext": f.get("ext"),
                            "acodec": f.get("acodec"),
                            "abr": abr_val,
                            "url": f.get("url")
                        })
                formats.sort(
                    key=lambda item: (
                        score_audio_format(item, max_abr=max_abr),
                        to_float(item.get("abr")),
                    ),
                    reverse=True,
                )

                for f in formats:
                    candidate_url = str(f.get("url") or "").strip()
                    if not candidate_url:
                        continue
                    audio_url = candidate_url
                    selected_ext = str(f.get("ext") or "").strip().lower()
                    selected_acodec = str(f.get("acodec") or "").strip().lower()
                    break

                result = StreamInfo(
                    videoId=video_id,
                    audioUrl=audio_url,
                    duration=info.get("duration", 0),
                    title=info.get("title", ""),
                    artist=info.get("uploader", ""),
                    thumbnail=info.get("thumbnail", ""),
                    formats=formats
                )

                resolved = result.model_dump()
                resolved["selectedExt"] = selected_ext
                resolved["selectedAcodec"] = selected_acodec
                set_cache(cache_key, resolved)
                delete_cache(error_cache_key)
        except Exception as e:
            status_code, detail = classify_stream_resolution_error(e)
            set_cache(error_cache_key, {"status_code": status_code, "detail": detail})
            print(f"ERROR: [stream] Failed to resolve {video_id}: {e}")
            raise HTTPException(status_code=status_code, detail=detail)

    if not resolved:
        set_cache(error_cache_key, {"status_code": 502, "detail": "Could not resolve stream metadata"})
        raise HTTPException(status_code=502, detail="Could not resolve stream metadata")

    if not str(resolved.get("audioUrl") or "").strip():
        set_cache(error_cache_key, {"status_code": 404, "detail": "No playable audio stream found"})
        raise HTTPException(status_code=404, detail="No playable audio stream found")

    if not force_proxy:
        return resolved

    audio_url = str(resolved.get("audioUrl") or "").strip()
    if not audio_url:
        raise HTTPException(status_code=404, detail="No audio URL found")

    req_range = str(request.headers.get("range", "") or "").strip()
    upstream_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Accept-Ranges": "bytes",
        "Referer": "https://www.youtube.com/",
    }
    if req_range:
        upstream_headers["Range"] = req_range

    def open_upstream_audio():
        session = requests.Session()
        session.headers.update(upstream_headers)
        try:
            return session, session.get(
                audio_url,
                allow_redirects=True,
                stream=True,
                timeout=(10, 30),
            )
        except Exception:
            session.close()
            raise

    try:
        upstream_session, upstream = await asyncio.to_thread(open_upstream_audio)
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Upstream audio HTTP {e.response.status_code if e.response else 'error'}")
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Upstream audio unavailable: {e}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to open upstream audio: {e}")

    if upstream.status_code >= 400:
        status_code = upstream.status_code
        upstream.close()
        upstream_session.close()
        raise HTTPException(status_code=502, detail=f"Upstream audio HTTP {status_code}")

    status_code = int(upstream.status_code or 200)
    proxy_headers: Dict[str, str] = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges, Content-Type",
        "Cache-Control": "no-store",
    }
    passthrough_headers = [
        "Content-Type",
        "Content-Length",
        "Accept-Ranges",
        "Content-Range",
        "ETag",
        "Last-Modified",
    ]
    for header_name in passthrough_headers:
        header_value = upstream.headers.get(header_name)
        if header_value:
            proxy_headers[header_name] = header_value

    if "Accept-Ranges" not in proxy_headers:
        proxy_headers["Accept-Ranges"] = "bytes"
    if "Content-Type" not in proxy_headers:
        selected_ext = str(resolved.get("selectedExt") or "").strip().lower()
        if selected_ext in {"m4a", "mp4", "aac"}:
            proxy_headers["Content-Type"] = "audio/mp4"
        elif selected_ext == "webm":
            proxy_headers["Content-Type"] = "audio/webm"
        else:
            proxy_headers["Content-Type"] = "audio/mpeg"

    if request.method.upper() == "HEAD":
        upstream.close()
        upstream_session.close()
        return Response(status_code=status_code, headers=proxy_headers)

    def stream_upstream_audio():
        try:
            for chunk in upstream.iter_content(chunk_size=64 * 1024):
                if chunk:
                    yield chunk
        finally:
            upstream.close()
            upstream_session.close()

    return StreamingResponse(
        stream_upstream_audio(),
        status_code=status_code,
        headers=proxy_headers,
    )


@app.get("/download/{video_id}")
async def download_track(
    video_id: str,
    background_tasks: BackgroundTasks,
    quality: str = Query("best", description="Audio quality: best, 128, 192, 256, 320")
):
    output_dir = os.path.join(CACHE_DIR, "downloads")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{video_id}.m4a")
    
    # If file already exists, stream it directly
    if os.path.exists(output_path):
        try:
            file_size = os.path.getsize(output_path)
            def file_iterator():
                with open(output_path, "rb") as f:
                    while True:
                        chunk = f.read(8192)  # 8KB chunks
                        if not chunk:
                            break
                        yield chunk
            
            return StreamingResponse(
                file_iterator(),
                media_type="audio/mpeg",
                headers={
                    "Content-Disposition": f"attachment; filename=\"{video_id}.m4a\"",
                    "Content-Length": str(file_size),
                    "Cache-Control": "no-store",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Expose-Headers": "Content-Length,Content-Type",
                }
            )
        except Exception as e:
            return JSONResponse(
                {"error": f"Failed to stream file: {str(e)}"},
                status_code=500
            )
    
    # File doesn't exist yet - download it first
    def download_and_stream():
        try:
            format_str = "bestaudio/best"
            if quality.isdigit():
                format_str = f"bestaudio[abr<={quality}]/bestaudio/best"
            
            ydl_opts = {
                "quiet": True,
                "no_warnings": True,
                "format": format_str,
                "outtmpl": output_path.replace(".m4a", ""),
                "postprocessors": [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "m4a",
                }],
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([f"https://www.youtube.com/watch?v={video_id}"])
            
            # After download, stream the file
            file_size = os.path.getsize(output_path)
            with open(output_path, "rb") as f:
                while True:
                    chunk = f.read(8192)
                    if not chunk:
                        break
                    yield chunk
        except Exception as e:
            yield f"Error: {str(e)}".encode()
    
    return StreamingResponse(
        download_and_stream(),
        media_type="audio/mpeg",
        headers={
            "Content-Disposition": f"attachment; filename=\"{video_id}.m4a\"",
            "Cache-Control": "no-store",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Expose-Headers": "Content-Length,Content-Type",
        }
    )


@app.get("/download/{video_id}/progress")
async def get_download_progress(video_id: str):
    if video_id not in download_progress:
        return {"videoId": video_id, "status": "not_found"}
    return download_progress[video_id].model_dump()


@app.get("/download/{video_id}/file")
async def get_downloaded_file(video_id: str):
    output_path = os.path.join(CACHE_DIR, "downloads", f"{video_id}.m4a")
    
    if not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    def file_generator():
        with open(output_path, "rb") as f:
            while chunk := f.read(8192):
                yield chunk
    
    return StreamingResponse(
        file_generator(),
        media_type="audio/mp4",
        headers={
            "Content-Disposition": f'attachment; filename="{video_id}.m4a"'
        }
    )


@app.get("/lyrics")
async def get_lyrics(
    request: Request,
    artist: str = Query(..., description="Artist name"),
    title: str = Query(..., description="Song title"),
    video_id: Optional[str] = Query(None, description="YouTube video ID")
):
    """
    Get lyrics for a song from multiple sources with priority:
    1. LRCLIB (synced LRC format - real timestamps)
    2. YTMusic via ytmusicapi
    3. Returns unsynced plain text if synced not available
    """
    resolved_video_id = str(video_id or request.query_params.get("videoId") or "").strip()
    cache_key = get_cache_key("lyrics_v3", artist, title, resolved_video_id)
    cached = get_cached(cache_key, max_age_seconds=86400 * 7)
    if cached:
        return {"success": True, "data": cached, "source": "cache"}
    
    try:
        query_pairs = build_lyric_query_pairs(artist, title)
        metadata_pairs: List[Dict[str, str]] = []

        if resolved_video_id:
            try:
                song_data = ytmusic.get_song(resolved_video_id)
                video_details = song_data.get("videoDetails", {}) if isinstance(song_data, dict) else {}
                md_title = str(video_details.get("title", "") or "").strip()
                md_artist = str(video_details.get("author", "") or "").strip()
                if md_title:
                    metadata_pairs.append({"artist": md_artist, "title": md_title})
            except Exception:
                pass

        if metadata_pairs:
            query_pairs = build_lyric_query_pairs(artist, title, metadata_pairs)

        # Step 1: Try to get synced lyrics from LRCLIB first
        synced_lines = fetch_lrclib_synced_lyrics(query_pairs)
        if synced_lines and len(synced_lines) > 0:
            result = {
                "lines": synced_lines,
                "source": "lrclib",
                "isSynced": True
            }
            set_cache(cache_key, result)
            return {"success": True, "data": result, "source": "lrclib"}

        # Step 2: Fall back to YTMusic for plain text lyrics
        try:
            candidate_video_ids: List[str] = []
            if resolved_video_id:
                candidate_video_ids.append(resolved_video_id)

            if not candidate_video_ids:
                candidate_video_ids = collect_ytmusic_lyric_candidate_video_ids(query_pairs)

            for candidate_video_id in candidate_video_ids:
                try:
                    watch_playlist = ytmusic.get_watch_playlist(videoId=candidate_video_id)
                    lyrics_id = watch_playlist.get("lyrics")
                    if not lyrics_id:
                        continue

                    lyrics_data = ytmusic.get_lyrics(lyrics_id)
                    if lyrics_data and lyrics_data.get("lyrics"):
                        result = {
                            "lines": parse_lyrics_to_lines(lyrics_data.get("lyrics", "")),
                            "source": lyrics_data.get("source", "ytmusic"),
                            "isSynced": False  # YTMusic lyrics are plain text, NOT synced
                        }
                        set_cache(cache_key, result)
                        return {"success": True, "data": result, "source": "ytmusic"}
                except Exception:
                    continue
        except Exception:
            pass
        
        return {"success": False, "error": "Lyrics not available from any source"}
    
    except Exception as e:
        return {"success": False, "error": str(e)}


def fetch_lrclib_synced_lyrics(query_pairs: List[Dict[str, str]]) -> Optional[List[Dict[str, Any]]]:
    """
    Try to fetch synced lyrics (LRC format) from LRCLIB.
    Returns a list of dicts with 'time' (float) and 'text' (str) keys.
    Returns None if no synced lyrics found.
    """
    if not query_pairs:
        return None

    # Try exact matches first (most reliable)
    for pair in query_pairs:
        exact_artist = str(pair.get("artist", "") or "").strip()
        exact_title = str(pair.get("title", "") or "").strip()
        if not exact_artist or not exact_title:
            continue
        exact_url = f"{LRCLIB_BASE_URL}/get?{urlencode({'artist_name': exact_artist, 'track_name': exact_title})}"
        exact_data = fetch_json(exact_url)
        if isinstance(exact_data, dict):
            synced_lrc = exact_data.get("syncedLyrics")
            if isinstance(synced_lrc, str) and synced_lrc.strip():
                lines = parse_lrc_to_lines(synced_lrc)
                if lines and len(lines) > 0:
                    return lines

    # Fall back to fuzzy searches across all candidate query variants
    for pair in query_pairs:
        query = " ".join(
            [part for part in [pair.get("artist", ""), pair.get("title", "")] if part]
        ).strip()
        if not query:
            continue

        search_url = f"{LRCLIB_BASE_URL}/search?{urlencode({'q': query})}"
        search_data = fetch_json(search_url)
        if isinstance(search_data, list) and len(search_data) > 0:
            best_item = select_best_lrclib_search_result(search_data, pair)
            if not best_item:
                continue
            synced_lrc = best_item.get("syncedLyrics")
            if not isinstance(synced_lrc, str) or not synced_lrc.strip():
                continue
            lines = parse_lrc_to_lines(synced_lrc)
            if lines and len(lines) > 0:
                return lines

    return None


def fetch_json(url: str, timeout_seconds: float = 6.0) -> Optional[Any]:
    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            if response.status != 200:
                return None
            charset = response.headers.get_content_charset() or "utf-8"
            raw = response.read().decode(charset, errors="replace")
            return json.loads(raw)
    except (URLError, HTTPError, TimeoutError, json.JSONDecodeError, ValueError):
        return None


def parse_lrc_to_lines(lrc: str) -> List[Dict[str, Any]]:
    """
    Parse LRC format (synchronized lyrics) into lines with timestamps.
    LRC format: [MM:SS.CS]Lyric text
    Returns list of dicts with 'time' (float) and 'text' (str) keys, sorted by time.
    """
    if not lrc or not isinstance(lrc, str):
        return []
    
    timestamp_pattern = re.compile(r"\[(\d{1,2}):(\d{2})(?:\.(\d{1,3}))?\]")
    lines: List[Dict[str, Any]] = []

    for raw_line in lrc.splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
            
        matches = list(timestamp_pattern.finditer(raw_line))
        if not matches:
            continue

        text = timestamp_pattern.sub("", raw_line).strip()
        if not text:
            continue

        for match in matches:
            minutes = int(match.group(1))
            seconds = int(match.group(2))
            fraction = match.group(3) or "0"
            
            # Convert fraction to decimal seconds
            if len(fraction) == 1:
                fraction_seconds = int(fraction) / 10
            elif len(fraction) == 2:
                fraction_seconds = int(fraction) / 100
            else:
                fraction_seconds = int(fraction[:3]) / 1000

            total_seconds = round(minutes * 60 + seconds + fraction_seconds, 3)
            lines.append({"time": total_seconds, "text": text})

    # Sort by time and remove duplicates (keep first occurrence)
    lines.sort(key=lambda item: item["time"])
    
    # Remove duplicate times by keeping only the first occurrence
    seen_times = set()
    unique_lines = []
    for line in lines:
        if line["time"] not in seen_times:
            unique_lines.append(line)
            seen_times.add(line["time"])
    
    return unique_lines


def parse_lyrics_to_lines(lyrics: str) -> List[Dict]:
    """Parse plain text lyrics into lines with estimated timing (3.5s per line)"""
    lines = []
    for i, line in enumerate(lyrics.split("\n")):
        text = line.strip()
        if text:
            # Estimate timing: assumes 3.5 seconds per line on average
            # These are NOT real timestamps, just estimates for display
            lines.append({"time": i * 3.5, "text": text})
    return lines


@app.get("/recommendations/{video_id}")
async def get_recommendations(video_id: str, limit: int = 10):
    cache_key = get_cache_key("recs", video_id, limit)
    cached = get_cached(cache_key, max_age_seconds=3600)
    if cached:
        return {"success": True, "data": cached}
    
    try:
        related = ytmusic.get_watch_playlist(videoId=video_id, limit=limit)
        tracks = []
        
        for track in related.get("tracks", []):
            video_info = track.get("videoId")
            if not video_info or video_info == video_id:
                continue
            
            artists = track.get("artists", [])
            artist_name = artists[0].get("name", "") if artists else ""
            
            thumbnails = track.get("thumbnails", [])
            thumbnail = thumbnails[-1].get("url", "") if thumbnails else ""
            
            tracks.append({
                "videoId": video_info,
                "title": track.get("title", ""),
                "artist": artist_name,
                "thumbnail": thumbnail,
                "duration": 0
            })
        
        set_cache(cache_key, tracks)
        return {"success": True, "data": tracks[:limit]}
    
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/artist/{channel_id}")
async def get_artist(channel_id: str):
    try:
        artist = ytmusic.get_artist(channel_id)
        return {"success": True, "data": artist}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/artist/{channel_id}/catalog")
async def get_artist_catalog(channel_id: str):
    cache_key = get_cache_key("artist_catalog", channel_id)
    cached = get_cached(cache_key, max_age_seconds=1800)
    if cached:
        return {"success": True, "data": cached}

    try:
        artist = ytmusic.get_artist(channel_id)
        artist_name = str(artist.get("name", "") or artist.get("title", "") or "Unknown Artist")
        artist_thumbnail = _last_thumbnail(artist) if isinstance(artist, dict) else ""
        subscribers_raw = artist.get("subscribers") if isinstance(artist, dict) else None
        subscribers = str(subscribers_raw).strip() if subscribers_raw is not None else ""
        subscribers_count = parse_subscriber_count(subscribers)
        views_count = parse_view_count(artist.get("views")) if isinstance(artist, dict) else None

        songs_section = artist.get("songs", {}) if isinstance(artist, dict) else {}
        song_items = songs_section.get("results", []) if isinstance(songs_section, dict) else []
        top_songs: List[Dict[str, Any]] = []
        for item in song_items:
            parsed = normalize_track_item(item, artist_name)
            if parsed:
                top_songs.append(parsed)
        top_songs = top_songs[:20]

        def collect_artist_releases(section_key: str, item_type: str) -> List[Dict[str, Any]]:
            section = artist.get(section_key, {}) if isinstance(artist, dict) else {}
            section_items = section.get("results", []) if isinstance(section, dict) else []
            params = str(section.get("params", "") or "").strip() if isinstance(section, dict) else ""

            merged_items: List[Dict[str, Any]] = []
            if isinstance(section_items, list):
                merged_items.extend([entry for entry in section_items if isinstance(entry, dict)])

            # ytmusicapi exposes extra pages through get_artist_albums with section params.
            if params:
                try:
                    fetched = ytmusic.get_artist_albums(channel_id, params)
                    if isinstance(fetched, dict):
                        fetched = fetched.get("results", [])
                    if isinstance(fetched, list):
                        merged_items.extend([entry for entry in fetched if isinstance(entry, dict)])
                except Exception:
                    pass

            deduped: List[Dict[str, Any]] = []
            seen_ids: set[str] = set()
            for entry in merged_items:
                parsed = normalize_collection_item(entry, item_type)
                if not parsed:
                    continue
                dedupe_key = str(parsed.get("browseId") or parsed.get("id") or "").strip()
                if not dedupe_key:
                    dedupe_key = hashlib.md5(f"{item_type}:{parsed.get('title', '')}".encode()).hexdigest()
                if dedupe_key in seen_ids:
                    continue
                seen_ids.add(dedupe_key)
                deduped.append(parsed)
            return deduped

        albums = collect_artist_releases("albums", "album")
        singles = collect_artist_releases("singles", "single")

        payload = {
            "id": channel_id,
            "name": artist_name,
            "description": artist.get("description", "") if isinstance(artist, dict) else "",
            "thumbnail": artist_thumbnail,
            "subscribers": subscribers or None,
            "subscribersCount": subscribers_count,
            "listeners": subscribers or None,
            "listenersCount": subscribers_count,
            "views": views_count,
            "verified": bool(subscribers_count and subscribers_count > 0),
            "topSongs": top_songs,
            "albums": albums,
            "singles": singles,
        }

        set_cache(cache_key, payload)
        return {"success": True, "data": payload}
    except Exception as e:
        print(f"[artist_catalog] error: {e}")
        return {"success": False, "error": "Artist page unavailable"}


@app.get("/album/{browse_id}")
async def get_album(browse_id: str):
    try:
        album = ytmusic.get_album(browse_id)
        album_thumbnail = _last_thumbnail(album if isinstance(album, dict) else {})
        tracks = []
        
        for track in album.get("tracks", []):
            tracks.append({
                "videoId": track.get("videoId"),
                "title": track.get("title"),
                "artist": track.get("artists", [{}])[0].get("name", ""),
                "duration": parse_duration(track.get("duration", "")),
                "thumbnail": album_thumbnail
            })
        
        return {
            "success": True,
            "data": {
                "title": album.get("title"),
                "artist": album.get("artists", [{}])[0].get("name", "") if album.get("artists") else "",
                "year": album.get("year"),
                "releaseDate": album.get("releaseDate") or album.get("release_date") or album.get("year"),
                "thumbnail": album_thumbnail,
                "tracks": tracks
            }
        }
    except Exception as e:
        print(f"[artist_catalog] error: {e}")
        return {"success": False, "error": "Artist page unavailable"}


if __name__ == "__main__":
    requested_host = str(os.getenv("FLARVIFY_API_HOST", "127.0.0.1")).strip().lower()
    host = requested_host if requested_host in {"127.0.0.1", "localhost", "::1"} else "127.0.0.1"
    port = int(os.getenv("FLARVIFY_API_PORT", "5000"))
    uvicorn.run("python_api:app", host=host, port=port)
