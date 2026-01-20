import os
import io
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
AUTOPOST_CHAT_ID = os.getenv("AUTOPOST_CHAT_ID", "").strip()  # @NeanderBros or numeric chat id

CHAIN = os.getenv("CHAIN", "polygon").strip()

BROS = os.getenv("NEANDERBROS_CONTRACT", "").strip().lower()
GALS = os.getenv("NEANDERGALS_CONTRACT", "").strip().lower()

ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", "").strip()
ALCHEMY_NETWORK = os.getenv("ALCHEMY_NETWORK", "polygon-mainnet").strip()
ALCHEMY_BASE_URL = os.getenv("ALCHEMY_BASE_URL", "").strip().rstrip("/")

OPENSEA_API_KEY = os.getenv("OPENSEA_API_KEY", "").strip()

BRO_WEIGHT = float(os.getenv("BRO_WEIGHT", "0.80"))
GAL_WEIGHT = float(os.getenv("GAL_WEIGHT", "0.20"))

MIN_MINUTES = int(os.getenv("AUTOPOST_MIN_MINUTES", "30"))
MAX_MINUTES = int(os.getenv("AUTOPOST_MAX_MINUTES", "90"))

MAX_POSTS_PER_24H = int(os.getenv("AUTOPOST_MAX_POSTS_PER_24H", "10"))
WINDOW_HOURS = int(os.getenv("AUTOPOST_WINDOW_HOURS", "24"))

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


def _alchemy_root() -> str:
    if ALCHEMY_BASE_URL:
        return f"{ALCHEMY_BASE_URL}/nft/v3/{ALCHEMY_API_KEY}"
    return f"https://{ALCHEMY_NETWORK}.g.alchemy.com/nft/v3/{ALCHEMY_API_KEY}"


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None or isinstance(v, bool):
            return None
        return int(str(v), 0) if isinstance(v, str) and v.strip().lower().startswith("0x") else int(str(v))
    except Exception:
        return None


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


async def _get_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 25.0,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    h = {"accept": "application/json"}
    if headers:
        h.update(headers)
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            r = await client.get(url, params=params, headers=h)
    except Exception as e:
        return None, f"Network error calling API: {e}"

    if r.status_code == 429:
        return None, "API throttled (429)."
    if r.status_code in (401, 403):
        return None, f"API auth error ({r.status_code})."
    if r.status_code >= 400:
        return None, f"API error {r.status_code}: {r.text[:200]}"

    try:
        data = r.json()
        return data if isinstance(data, dict) else None, None if isinstance(data, dict) else "Bad JSON shape"
    except Exception:
        return None, "Could not parse JSON"


async def fetch_minted_so_far_alchemy(contract: str) -> Tuple[Optional[int], Optional[str]]:
    url = f"{_alchemy_root()}/getContractMetadata"
    params = {"contractAddress": contract}
    data, err = await _get_json(url, params=params)
    if err:
        return None, err
    minted = _safe_int((data or {}).get("totalSupply"))
    return minted, None


async def fetch_nft_metadata_alchemy(contract: str, token_id: int) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    url = f"{_alchemy_root()}/getNFTMetadata"
    params = {"contractAddress": contract, "tokenId": str(token_id), "refreshCache": "false"}
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


async def fetch_opensea_rank(contract: str, token_id: int) -> Tuple[Optional[int], Optional[str]]:
    url = f"https://api.opensea.io/api/v2/chain/{CHAIN}/contract/{contract}/nfts/{token_id}"
    headers = {
        "accept": "application/json",
        "x-api-key": OPENSEA_API_KEY,
        "user-agent": "Mozilla/5.0 (compatible; NeanderSchedulerBot/1.0)",
    }
    data, err = await _get_json(url, headers=headers, timeout=25.0)
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


async def _download_image_bytes(url: str) -> Optional[bytes]:
    try:
        async with httpx.AsyncClient(timeout=25, follow_redirects=True) as client:
            r = await client.get(url, headers={"user-agent": "Mozilla/5.0 (compatible; NeanderSchedulerBot/1.0)"})
            r.raise_for_status()
            return r.content
    except Exception:
        return None


def _load_state() -> Dict[str, Any]:
    try:
        with open(STATE_PATH, "r") as f:
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
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_PATH)


def _prune_old_post_times(post_times: List[float], now_ts: float) -> List[float]:
    cutoff = now_ts - (WINDOW_HOURS * 3600)
    return [t for t in post_times if t >= cutoff]


def _next_delay_if_capped(post_times: List[float], now_ts: float) -> int:
    post_times_sorted = sorted(post_times)
    oldest = post_times_sorted[0]
    window_seconds = WINDOW_HOURS * 3600
    seconds_until_allowed = int((oldest + window_seconds) - now_ts)
    jitter = random.randint(60, 10 * 60)
    return max(60, seconds_until_allowed + jitter)


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

    minted_so_far = await _get_cached_supply(contract)
    os_rank, _ = await fetch_opensea_rank(contract, token_id)

    coll = _collection_label(contract)
    nft_id = _display_nft_id(contract, token_id)

    header1 = f"<b>{coll} NFT ID #{nft_id}</b>"
    header2 = f"Token ID #{token_id}"
    if minted_so_far and minted_so_far > 0:
        header2 += f" of {minted_so_far}"

    traits = _extract_traits(meta or {})
    trait_lines = []
    for a in traits[:MAX_TRAITS]:
        tt = a.get("trait_type") or a.get("type") or a.get("traitType") or "Trait"
        vv = a.get("value")
        if not isinstance(tt, str) or vv is None:
            continue
        trait_lines.append(f"{tt}: {vv}")

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

    image_url = _pick_image_url(meta or {})
    img_bytes = await _download_image_bytes(image_url) if image_url else None
    return caption, img_bytes, None


async def _telegram_send_photo_or_message(caption: str, img_bytes: Optional[bytes]) -> Tuple[bool, str]:
    base = f"https://api.telegram.org/bot{SCHEDULER_BOT_TOKEN}"
    try:
        async with httpx.AsyncClient(timeout=45, follow_redirects=True) as client:
            if img_bytes:
                files = {"photo": ("nft.png", img_bytes, "image/png")}
                data = {
                    "chat_id": AUTOPOST_CHAT_ID,
                    "caption": caption,
                    "parse_mode": "HTML",
                }
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
            return False, f"Telegram API error {r.status_code}: {r.text[:200]}"
        return True, "ok"
    except Exception as e:
        return False, f"Telegram send failed: {e}"


async def run_forever() -> None:
    while True:
        now_ts = time.time()
        state = _load_state()

        post_times = state.get("autopost_times", [])
        if not isinstance(post_times, list):
            post_times = []
        post_times = [float(t) for t in post_times if isinstance(t, (int, float))]
        post_times = _prune_old_post_times(post_times, now_ts)

        if len(post_times) >= MAX_POSTS_PER_24H:
            state["autopost_times"] = post_times
            _save_state(state)
            delay = _next_delay_if_capped(post_times, now_ts)
            await asyncio.sleep(delay)
            continue

        choice = _choose_collection_weighted()
        contract = BROS if choice == "bro" else GALS

        supply = await _get_cached_supply(contract)
        min_id = _min_token_id_for(contract)

        if supply is None or supply <= min_id:
            await asyncio.sleep(random.randint(MIN_MINUTES * 60, MAX_MINUTES * 60))
            continue

        caption = None
        img = None

        for _ in range(max(1, RANDOM_PICK_RETRIES)):
            token_id = random.randint(min_id, supply - 1) if supply > (min_id + 1) else min_id
            cap, im, err = await _build_post(contract, token_id)
            if err:
                continue
            caption, img = cap, im
            break

        if caption:
            ok, msg = await _telegram_send_photo_or_message(caption, img)
            if ok:
                post_times.append(now_ts)
                state["autopost_times"] = post_times
                _save_state(state)

        await asyncio.sleep(random.randint(MIN_MINUTES * 60, MAX_MINUTES * 60))


if __name__ == "__main__":
    asyncio.run(run_forever())

