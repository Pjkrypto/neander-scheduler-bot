import os
import json
import time
import random
import asyncio
from typing import Any, Dict, Optional, Tuple, List

import httpx

# -----------------------
# ENV
# -----------------------
SCHEDULER_BOT_TOKEN = os.getenv("SCHEDULER_BOT_TOKEN", "").strip()
AUTOPOST_CHAT_ID = os.getenv("AUTOPOST_CHAT_ID", "").strip()  # @channelusername OR numeric chat id

CHAIN = os.getenv("CHAIN", "polygon").strip()

BROS = os.getenv("NEANDERBROS_CONTRACT", "").strip().lower()
GALS = os.getenv("NEANDERGALS_CONTRACT", "").strip().lower()

ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", "").strip()
ALCHEMY_NETWORK = os.getenv("ALCHEMY_NETWORK", "polygon-mainnet").strip()
ALCHEMY_BASE_URL = os.getenv("ALCHEMY_BASE_URL", "").strip().rstrip("/")

OPENSEA_API_KEY = os.getenv("OPENSEA_API_KEY", "").strip()

BRO_WEIGHT = float(os.getenv("BRO_WEIGHT", "0.80"))
GAL_WEIGHT = float(os.getenv("GAL_WEIGHT", "0.20"))

# Rolling cap
MAX_POSTS_PER_24H = int(os.getenv("AUTOPOST_MAX_POSTS_PER_24H", "10"))
WINDOW_HOURS = int(os.getenv("AUTOPOST_WINDOW_HOURS", "24"))

# Even spacing + minimum gap
SCHEDULER_JITTER_PCT = float(os.getenv("SCHEDULER_JITTER_PCT", "0.40"))
SCHEDULER_MIN_GAP_MINUTES = int(os.getenv("SCHEDULER_MIN_GAP_MINUTES", "120"))

# Backoff if APIs fail
FAIL_BACKOFF_MINUTES = int(os.getenv("FAIL_BACKOFF_MINUTES", "15"))

BROS_MIN_TOKEN_ID = int(os.getenv("BROS_MIN_TOKEN_ID", "1"))
GALS_MIN_TOKEN_ID = int(os.getenv("GALS_MIN_TOKEN_ID", "0"))

SUPPLY_REFRESH_SECONDS = int(os.getenv("SUPPLY_REFRESH_SECONDS", str(15 * 60)))
RANDOM_PICK_RETRIES = int(os.getenv("RANDOM_PICK_RETRIES", "6"))

STATE_PATH = os.getenv("SCHEDULER_STATE_PATH", "/opt/bot-state/scheduler_state.json").strip()
MAX_TRAITS = int(os.getenv("MAX_TRAITS", "50"))

if not SCHEDULER_BOT_TOKEN:
    raise SystemExit("Missing SCHEDULER_BOT_TOKEN")
if not AUTOPOST_CHAT_ID:
    raise SystemExit("Missing AUTOPOST_CHAT_ID")
if not ALCHEMY_API_KEY:
    raise SystemExit("Missing ALCHEMY_API_KEY")
if not OPENSEA_API_KEY:
    raise SystemExit("Missing OPENSEA_API_KEY")
if not BROS or not GALS:
    raise SystemExit("Missing NEANDERBROS_CONTRACT/NEANDERGALS_CONTRACT")

DISPLAY_ID_OFFSETS = {BROS: 1, GALS: 0}


def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def _alchemy_root() -> str:
    if ALCHEMY_BASE_URL:
        return f"{ALCHEMY_BASE_URL}/nft/v3/{ALCHEMY_API_KEY}"
    return f"https://{ALCHEMY_NETWORK}.g.alchemy.com/nft/v3/{ALCHEMY_API_KEY}"


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None or isinstance(v, bool):
            return None
        s = str(v).strip()
        if s.lower().startswith("0x"):
            return int(s, 16)
        return int(s)
    except Exception:
        return None


def _norm(s: Any) -> str:
    return str(s).strip().casefold()


def _prevalence_to_pct(prevalence: Any) -> Optional[float]:
    try:
        p = float(prevalence)
    except Exception:
        return None
    if p <= 1.0:
        return p * 100.0
    return p


def _opensea_url(contract: str, token_id: int) -> str:
    return f"https://opensea.io/assets/{CHAIN}/{contract}/{token_id}"


def _collection_label(contract: str) -> str:
    c = (contract or "").lower()
    if c == BROS:
        return "NeanderBros"
    if c == GALS:
        return "NeanderGals"
    return "NFT"


def _display_nft_id(contract: str, token_id: int) -> int:
    c = (contract or "").lower()
    return token_id + DISPLAY_ID_OFFSETS.get(c, 0)


# -----------------------
# STATE
# -----------------------
def _load_state() -> Dict[str, Any]:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _save_state(state: Dict[str, Any]) -> None:
    folder = os.path.dirname(STATE_PATH) or "."
    os.makedirs(folder, exist_ok=True)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_PATH)


def _prune_old_post_times(post_times: List[float], now_ts: float) -> List[float]:
    cutoff = now_ts - (WINDOW_HOURS * 3600)
    return [t for t in post_times if t >= cutoff]


def _seconds_until_cap_clears(post_times: List[float], now_ts: float) -> int:
    oldest = min(post_times)
    window_seconds = WINDOW_HOURS * 3600
    return max(60, int((oldest + window_seconds) - now_ts))


def _compute_next_delay_seconds() -> int:
    base = int((WINDOW_HOURS * 3600) / max(1, MAX_POSTS_PER_24H))
    jitter = max(0.0, min(SCHEDULER_JITTER_PCT, 0.95))
    factor = 1.0 + random.uniform(-jitter, jitter)
    proposed = int(base * factor)
    min_gap = max(60, SCHEDULER_MIN_GAP_MINUTES * 60)
    return max(proposed, min_gap)


# -----------------------
# HTTP (single shared client)
# -----------------------
DEFAULT_HEADERS = {
    "accept": "application/json",
    "user-agent": "Mozilla/5.0 (compatible; NeanderSchedulerBot/1.0)",
}

_client: Optional[httpx.AsyncClient] = None


async def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        timeout = httpx.Timeout(connect=15.0, read=25.0, write=25.0, pool=15.0)
        _client = httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=DEFAULT_HEADERS)
    return _client


async def _get_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    h = dict(DEFAULT_HEADERS)
    if headers:
        h.update(headers)

    try:
        client = await get_client()
        r = await client.get(url, params=params, headers=h)
    except Exception as e:
        return None, f"Network error calling API: {e}"

    if r.status_code == 429:
        return None, "API throttled (429)."
    if r.status_code in (401, 403):
        return None, f"API auth error ({r.status_code})."
    if r.status_code >= 400:
        return None, f"API error {r.status_code}: {(r.text or '')[:200]}"

    try:
        data = r.json()
        if isinstance(data, dict):
            return data, None
        return None, "Bad JSON shape"
    except Exception:
        return None, "Could not parse JSON"


async def _download_bytes(url: str) -> Optional[bytes]:
    try:
        client = await get_client()
        r = await client.get(url, headers={"accept": "*/*"})
        r.raise_for_status()
        return r.content
    except Exception:
        return None


# -----------------------
# API calls
# -----------------------
async def fetch_minted_so_far_alchemy(contract: str) -> Tuple[Optional[int], Optional[str]]:
    url = f"{_alchemy_root()}/getContractMetadata"
    data, err = await _get_json(url, params={"contractAddress": contract})
    if err:
        return None, err
    return _safe_int((data or {}).get("totalSupply")), None


async def fetch_nft_metadata_alchemy(contract: str, token_id: int) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    url = f"{_alchemy_root()}/getNFTMetadata"
    params = {"contractAddress": contract, "tokenId": str(token_id), "refreshCache": "false"}
    return await _get_json(url, params=params)


async def fetch_compute_rarity_alchemy(contract: str, token_id: int) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    url = f"{_alchemy_root()}/computeRarity"
    params = {"contractAddress": contract, "tokenId": str(token_id)}
    return await _get_json(url, params=params)


def _pick_image_url(meta: Dict[str, Any]) -> Optional[str]:
    image = meta.get("image")
    if isinstance(image, dict):
        for k in ("pngUrl", "cachedUrl", "thumbnailUrl", "originalUrl"):
            v = image.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()

    raw = meta.get("raw")
    if isinstance(raw, dict):
        md = raw.get("metadata")
        if isinstance(md, dict):
            v = md.get("image") or md.get("image_url")
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


def _extract_traits(meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = meta.get("raw")
    if isinstance(raw, dict):
        md = raw.get("metadata")
        if isinstance(md, dict):
            attrs = md.get("attributes")
            if isinstance(attrs, list):
                return [a for a in attrs if isinstance(a, dict)]
    return []


def _build_trait_pct_map_from_alchemy(rarity_resp: Dict[str, Any]) -> Dict[Tuple[str, str], float]:
    out: Dict[Tuple[str, str], float] = {}
    rarities = rarity_resp.get("rarities")
    if not isinstance(rarities, list):
        return out

    for item in rarities:
        if not isinstance(item, dict):
            continue
        tt = item.get("trait_type") or item.get("traitType") or item.get("key") or item.get("trait")
        vv = item.get("value")
        if tt is None or vv is None:
            continue
        pct = _prevalence_to_pct(item.get("prevalence") or item.get("frequency") or item.get("pct"))
        if pct is None:
            continue
        out[(_norm(tt), _norm(vv))] = pct
    return out


def _format_trait_line(trait_type: str, value: str, pct: Optional[float]) -> str:
    if pct is None:
        return f"{trait_type}: {value} — n/a"
    return f"{trait_type}: {value} — {pct:.2f}%"


async def fetch_opensea_rank(contract: str, token_id: int) -> Tuple[Optional[int], Optional[str]]:
    url = f"https://api.opensea.io/api/v2/chain/{CHAIN}/contract/{contract}/nfts/{token_id}"
    headers = {"x-api-key": OPENSEA_API_KEY}
    data, err = await _get_json(url, headers=headers)
    if err:
        return None, err

    nft = data.get("nft") if isinstance(data, dict) else None
    if not isinstance(nft, dict):
        nft = data if isinstance(data, dict) else None
    if not isinstance(nft, dict):
        return None, "Unexpected OpenSea response"

    rarity = nft.get("rarity")
    if isinstance(rarity, dict):
        rank = _safe_int(rarity.get("rank"))
        if rank is not None:
            return rank, None
    rank = _safe_int(nft.get("rarity_rank") or nft.get("rarityRank"))
    return rank, None


async def _get_cached_supply(contract: str) -> Optional[int]:
    now_ts = time.time()
    state = _load_state()

    supplies = state.get("supplies")
    if not isinstance(supplies, dict):
        supplies = {}

    entry = supplies.get(contract)
    if isinstance(entry, dict):
        cached_supply = _safe_int(entry.get("supply"))
        cached_at = float(entry.get("ts") or 0.0)
        if cached_supply is not None and (now_ts - cached_at) <= SUPPLY_REFRESH_SECONDS:
            return cached_supply

    supply, err = await fetch_minted_so_far_alchemy(contract)
    if err or supply is None:
        if isinstance(entry, dict):
            cached_supply = _safe_int(entry.get("supply"))
            if cached_supply is not None:
                return cached_supply
        return None

    supplies[contract] = {"supply": int(supply), "ts": now_ts}
    state["supplies"] = supplies
    _save_state(state)
    return int(supply)


async def _build_post(contract: str, token_id: int) -> Tuple[Optional[str], Optional[bytes], Optional[str]]:
    meta, err = await fetch_nft_metadata_alchemy(contract, token_id)
    if err or not isinstance(meta, dict):
        return None, None, err or "Metadata not available"

    rarity_resp, r_err = await fetch_compute_rarity_alchemy(contract, token_id)
    trait_pct_map: Dict[Tuple[str, str], float] = {}
    if not r_err and isinstance(rarity_resp, dict):
        trait_pct_map = _build_trait_pct_map_from_alchemy(rarity_resp)

    minted_so_far = await _get_cached_supply(contract)
    os_rank, _ = await fetch_opensea_rank(contract, token_id)

    coll = _collection_label(contract)
    nft_id = _display_nft_id(contract, token_id)

    header1 = f"<b>{coll} NFT ID #{nft_id}</b>"
    header2 = f"Token ID #{token_id}"
    if minted_so_far and minted_so_far > 0:
        header2 += f" of {minted_so_far}"

    traits = _extract_traits(meta)
    trait_lines: List[str] = []
    for a in traits[:MAX_TRAITS]:
        tt = a.get("trait_type") or a.get("type") or a.get("traitType") or "Trait"
        vv = a.get("value")
        if not isinstance(tt, str) or vv is None:
            continue
        vv_s = str(vv)
        pct = trait_pct_map.get((_norm(tt), _norm(vv_s)))
        trait_lines.append(_format_trait_line(tt, vv_s, pct))

    rarity_lines = ["<b>Rarity (OpenSea)</b>"]
    rarity_lines.append(f"Rank: #{os_rank}" if os_rank is not None else "Rank not available")

    os_url = _opensea_url(contract, token_id)

    caption = (
        f"{header1}\n"
        f"{header2}\n\n"
        f"{'\n'.join(rarity_lines)}\n\n"
        f"<b>Traits</b>\n"
        f"{('\n'.join(trait_lines) if trait_lines else '(No traits returned.)')}\n\n"
        f"<a href=\"{os_url}\">View on OpenSea</a>"
    )

    image_url = _pick_image_url(meta)
    img_bytes = await _download_bytes(image_url) if image_url else None
    return caption, img_bytes, None


# -----------------------
# Telegram send
# -----------------------
async def _telegram_send_photo_or_message(caption: str, img_bytes: Optional[bytes]) -> Tuple[bool, str]:
    base = f"https://api.telegram.org/bot{SCHEDULER_BOT_TOKEN}"
    try:
        client = await get_client()

        if img_bytes:
            files = {"photo": ("nft.png", img_bytes, "image/png")}
            data = {"chat_id": AUTOPOST_CHAT_ID, "caption": caption, "parse_mode": "HTML"}
            r = await client.post(f"{base}/sendPhoto", data=data, files=files)
        else:
            data = {
                "chat_id": AUTOPOST_CHAT_ID,
                "text": caption,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            r = await client.post(f"{base}/sendMessage", data=data)

        if r.status_code >= 400:
            return False, f"Telegram API error {r.status_code}: {(r.text or '')[:400]}"
        return True, "ok"
    except Exception as e:
        return False, f"Telegram send failed: {e}"


async def _startup_test_once() -> None:
    state = _load_state()
    if state.get("startup_test_sent") is True:
        return

    msg = (
        "<b>Neander Scheduler Bot</b> is online.\n"
        "If you see this message, the bot has permission to post to this chat."
    )
    ok, resp = await _telegram_send_photo_or_message(msg, None)
    if ok:
        state["startup_test_sent"] = True
        _save_state(state)
        log("Startup test message sent successfully (and marked as sent).")
    else:
        log(f"Startup test FAILED: {resp}")


def _choose_collection_weighted() -> str:
    return random.choices(
        population=["bro", "gal"],
        weights=[max(BRO_WEIGHT, 0.0), max(GAL_WEIGHT, 0.0)],
        k=1,
    )[0]


def _min_token_id_for(contract: str) -> int:
    c = (contract or "").lower()
    if c == BROS:
        return BROS_MIN_TOKEN_ID
    if c == GALS:
        return GALS_MIN_TOKEN_ID
    return 0


async def run_forever() -> None:
    log("Scheduler starting...")
    log(
        f"AUTOPOST_CHAT_ID={AUTOPOST_CHAT_ID!r} CHAIN={CHAIN} "
        f"CAP={MAX_POSTS_PER_24H}/{WINDOW_HOURS}h "
        f"JITTER={SCHEDULER_JITTER_PCT} MIN_GAP_MIN={SCHEDULER_MIN_GAP_MINUTES} "
        f"FAIL_BACKOFF_MIN={FAIL_BACKOFF_MINUTES}"
    )

    await _startup_test_once()

    while True:
        try:
            now_ts = time.time()
            state = _load_state()

            post_times = state.get("autopost_times", [])
            if not isinstance(post_times, list):
                post_times = []
            post_times = [float(t) for t in post_times if isinstance(t, (int, float))]
            post_times = _prune_old_post_times(post_times, now_ts)

            # enforce min gap since last successful post
            last_post_ts = float(state.get("last_post_ts") or 0.0)
            min_gap_sec = max(60, SCHEDULER_MIN_GAP_MINUTES * 60)
            if last_post_ts > 0 and (now_ts - last_post_ts) < min_gap_sec:
                wait = int(min_gap_sec - (now_ts - last_post_ts))
                log(f"MIN-GAP: waiting {wait}s before next post attempt.")
                await asyncio.sleep(wait)
                continue

            # rolling cap
            if len(post_times) >= MAX_POSTS_PER_24H:
                wait = _seconds_until_cap_clears(post_times, now_ts) + random.randint(60, 10 * 60)
                state["autopost_times"] = post_times
                _save_state(state)
                log(f"CAPPED: {len(post_times)}/{MAX_POSTS_PER_24H}. Sleeping {wait}s.")
                await asyncio.sleep(wait)
                continue

            choice = _choose_collection_weighted()
            contract = BROS if choice == "bro" else GALS

            supply = await _get_cached_supply(contract)
            min_id = _min_token_id_for(contract)
            if supply is None or supply <= min_id:
                wait = FAIL_BACKOFF_MINUTES * 60
                log(f"Supply unavailable (contract={choice}). Sleeping {wait}s.")
                await asyncio.sleep(wait)
                continue

            caption: Optional[str] = None
            img: Optional[bytes] = None
            picked_token: Optional[int] = None

            # Try random tokenIds until metadata resolves
            for _ in range(max(1, RANDOM_PICK_RETRIES)):
                token_id = random.randint(min_id, max(min_id, supply - 1))
                cap, im, err = await _build_post(contract, token_id)
                if err:
                    continue
                caption, img, picked_token = cap, im, token_id
                break

            if not caption:
                wait = FAIL_BACKOFF_MINUTES * 60
                log(f"Failed to build post after retries. Sleeping {wait}s.")
                await asyncio.sleep(wait)
                continue

            ok, resp = await _telegram_send_photo_or_message(caption, img)
            if ok:
                post_times.append(now_ts)
                state["autopost_times"] = post_times
                state["last_post_ts"] = now_ts
                _save_state(state)
                log(f"POSTED ok: {choice} tokenId={picked_token} total_in_window={len(post_times)}/{MAX_POSTS_PER_24H}")
            else:
                log(f"POST FAILED: {choice} tokenId={picked_token} reason={resp}")
                await asyncio.sleep(FAIL_BACKOFF_MINUTES * 60)
                continue

            delay = _compute_next_delay_seconds()
            log(f"Next attempt in {delay}s.")
            await asyncio.sleep(delay)

        except Exception as e:
            log(f"FATAL LOOP ERROR (continuing): {e}")
            await asyncio.sleep(30)


async def _shutdown() -> None:
    global _client
    if _client and not _client.is_closed:
        await _client.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(run_forever())
    finally:
        try:
            asyncio.run(_shutdown())
        except Exception:
            pass
