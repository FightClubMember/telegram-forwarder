import asyncio
import html
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from telethon import TelegramClient
from telethon.sessions import StringSession

STATE_FILE = Path("state.json")


def parse_source_channel_ids(raw: str) -> List[int]:
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def default_channel_state() -> dict:
    return {"last_message_id": 0, "processed_group_ids": []}


def load_state(source_channel_ids: List[int]) -> dict:
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            data = {}
    else:
        data = {}

    if "channels" not in data or not isinstance(data["channels"], dict):
        data = {"channels": {}}

    for channel_id in source_channel_ids:
        key = str(channel_id)
        if key not in data["channels"]:
            data["channels"][key] = default_channel_state()
        data["channels"][key].setdefault("last_message_id", 0)
        data["channels"][key].setdefault("processed_group_ids", [])

    return data


def save_state(state: dict) -> None:
    for channel_data in state.get("channels", {}).values():
        processed = channel_data.get("processed_group_ids", [])
        if len(processed) > 1000:
            channel_data["processed_group_ids"] = processed[-1000:]
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def tg_api_url(bot_token: str, method: str) -> str:
    return f"https://api.telegram.org/bot{bot_token}/{method}"


def tg_request(bot_token: str, method: str, data=None, files=None):
    response = requests.post(
        tg_api_url(bot_token, method),
        data=data,
        files=files,
        timeout=180,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("ok", False):
        raise RuntimeError(f"Telegram API error: {payload}")
    return payload


def escape_html(text: str) -> str:
    return html.escape(text or "", quote=True)


def normalize_url(url: str) -> str:
    url = url.strip()
    if url.startswith("t.me/"):
        url = "https://" + url
    return url.rstrip(").,]}>\"'")


def extract_urls(text: str) -> List[str]:
    if not text:
        return []

    pattern = r"(https?://[^\s]+|t\.me/[^\s]+)"
    matches = re.findall(pattern, text, flags=re.IGNORECASE)

    seen = set()
    result = []
    for item in matches:
        clean = normalize_url(item)
        if clean not in seen:
            seen.add(clean)
            result.append(clean)
    return result


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def smart_trim(text: str, limit: int = 1024) -> str:
    if len(text) <= limit:
        return text
    trimmed = text[: limit - 3]
    last_break = max(trimmed.rfind("\n"), trimmed.rfind(" "), 0)
    if last_break > 700:
        trimmed = trimmed[:last_break]
    return trimmed.rstrip() + "..."


def is_convertible_url(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False

    if host.startswith("www."):
        host = host[4:]

    allowed_hosts = [
        "flipkart.com",
        "fkrt.it",
        "myntra.com",
        "amazon.in",
        "amazon.com",
        "amzn.to",
    ]

    return any(host == allowed or host.endswith("." + allowed) for allowed in allowed_hosts)


def build_footer_only_caption(source_text: str, footer_text: str, footer_link: str) -> str:
    source_text = clean_text(source_text)
    safe_text = escape_html(source_text)
    footer = f'<a href="{escape_html(footer_link)}">{escape_html(footer_text)}</a>'

    if safe_text:
        return f"{safe_text}\n\n{footer}"
    return footer


def file_kind(path: str) -> str:
    ext = Path(path).suffix.lower()
    if ext in [".jpg", ".jpeg", ".png", ".webp"]:
        return "photo"
    if ext in [".mp4", ".mov", ".mkv", ".webm"]:
        return "video"
    return "document"


async def convert_link_via_extrapay(
    client: TelegramClient,
    bot_username: str,
    original_url: str
) -> str:
    try:
        async with client.conversation(bot_username, timeout=60) as conv:
            await conv.send_message(original_url)
            response = await conv.get_response()

            if response.buttons:
                for row in response.buttons:
                    for button in row:
                        button_url = getattr(button, "url", None)
                        if button_url:
                            return button_url

            response_text = response.raw_text or ""
            urls = extract_urls(response_text)
            if urls:
                return urls[0]

    except Exception as exc:
        print(f"[WARN] ExtraPeBot conversion failed for {original_url}: {exc}")

    return original_url


async def rewrite_links_in_text(
    client: TelegramClient,
    bot_username: str,
    text: str
) -> str:
    if not text:
        return text

    urls = extract_urls(text)
    if not urls:
        return text

    replacements: Dict[str, str] = {}
    for old_url in urls:
        if is_convertible_url(old_url):
            new_url = await convert_link_via_extrapay(client, bot_username, old_url)
            replacements[old_url] = new_url

    if not replacements:
        return text

    updated = text
    for old_url, new_url in replacements.items():
        updated = updated.replace(old_url, new_url)

    return updated


async def download_media(client: TelegramClient, message, folder: str) -> Optional[str]:
    os.makedirs(folder, exist_ok=True)
    return await client.download_media(message, file=folder)


def send_text(bot_token: str, chat_id: str, text: str):
    return tg_request(
        bot_token,
        "sendMessage",
        data={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        },
    )


def send_single_file(
    bot_token: str,
    chat_id: str,
    file_path: str,
    caption: str,
):
    kind = file_kind(file_path)
    caption = smart_trim(caption, 1024)

    if kind == "photo":
        with open(file_path, "rb") as f:
            return tg_request(
                bot_token,
                "sendPhoto",
                data={
                    "chat_id": chat_id,
                    "caption": caption,
                    "parse_mode": "HTML",
                },
                files={"photo": f},
            )

    if kind == "video":
        with open(file_path, "rb") as f:
            return tg_request(
                bot_token,
                "sendVideo",
                data={
                    "chat_id": chat_id,
                    "caption": caption,
                    "parse_mode": "HTML",
                    "supports_streaming": True,
                },
                files={"video": f},
            )

    with open(file_path, "rb") as f:
        return tg_request(
            bot_token,
            "sendDocument",
            data={
                "chat_id": chat_id,
                "caption": caption,
                "parse_mode": "HTML",
            },
            files={"document": f},
        )


def send_album(
    bot_token: str,
    chat_id: str,
    media_items: List[Tuple[str, str]],
    caption: str
):
    media = []
    opened_files = []

    try:
        for idx, (file_path, kind) in enumerate(media_items):
            field_name = f"file{idx}"
            file_obj = open(file_path, "rb")
            opened_files.append(file_obj)

            item = {
                "type": kind if kind in ["photo", "video"] else "document",
                "media": f"attach://{field_name}",
            }

            if idx == 0:
                item["caption"] = smart_trim(caption, 1024)
                item["parse_mode"] = "HTML"

            media.append(item)

        files_payload = {f"file{i}": f for i, f in enumerate(opened_files)}

        return tg_request(
            bot_token,
            "sendMediaGroup",
            data={
                "chat_id": chat_id,
                "media": json.dumps(media),
            },
            files=files_payload,
        )

    finally:
        for f in opened_files:
            try:
                f.close()
            except Exception:
                pass


async def resolve_channel_entity(client: TelegramClient, source_channel_id: int):
    dialogs = await client.get_dialogs(limit=None)

    bare_id = abs(source_channel_id)
    if str(bare_id).startswith("100"):
        bare_id = int(str(bare_id)[3:])

    for dialog in dialogs:
        if getattr(dialog, "id", None) == bare_id:
            return dialog.entity

    try:
        return await client.get_entity(source_channel_id)
    except Exception:
        pass

    raise RuntimeError(
        f"Source channel {source_channel_id} not found in this USER_SESSION. "
        f"Join/open it with the same Telegram account first."
    )


async def collect_new_messages(client: TelegramClient, source_entity, min_id: int):
    messages = []
    async for msg in client.iter_messages(source_entity, min_id=min_id, reverse=True):
        if msg:
            messages.append(msg)
    return messages


def build_items(messages):
    singles = []
    albums: Dict[int, List] = {}

    for msg in messages:
        grouped_id = getattr(msg, "grouped_id", None)
        if grouped_id:
            albums.setdefault(grouped_id, []).append(msg)
        else:
            singles.append({"type": "single", "messages": [msg]})

    album_items = []
    for grouped_id, grouped_messages in albums.items():
        grouped_messages = sorted(grouped_messages, key=lambda x: x.id)
        album_items.append(
            {
                "type": "album",
                "group_id": grouped_id,
                "messages": grouped_messages,
            }
        )

    items = singles + album_items
    items.sort(key=lambda x: min(m.id for m in x["messages"]))
    return items


async def process_single(
    client: TelegramClient,
    bot_token: str,
    dest_channel_id: str,
    message,
    extrapay_bot_username: str,
    footer_text: str,
    footer_link: str,
):
    original_text = message.message or ""
    rewritten_text = await rewrite_links_in_text(client, extrapay_bot_username, original_text)
    final_caption = build_footer_only_caption(rewritten_text, footer_text, footer_link)

    temp_dir = tempfile.mkdtemp(prefix="moneyzon_single_")
    try:
        has_media = bool(message.photo or message.video or message.document)

        if has_media:
            file_path = await download_media(client, message, temp_dir)
            if file_path:
                send_single_file(
                    bot_token=bot_token,
                    chat_id=dest_channel_id,
                    file_path=file_path,
                    caption=final_caption,
                )
                print(f"Sent media message {message.id}")
            else:
                send_text(
                    bot_token=bot_token,
                    chat_id=dest_channel_id,
                    text=final_caption,
                )
                print(f"Media missing, sent as text: {message.id}")
        else:
            send_text(
                bot_token=bot_token,
                chat_id=dest_channel_id,
                text=final_caption,
            )
            print(f"Sent text message {message.id}")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


async def process_album(
    client: TelegramClient,
    bot_token: str,
    dest_channel_id: str,
    messages: List,
    extrapay_bot_username: str,
    footer_text: str,
    footer_link: str,
):
    base_text = ""
    for msg in messages:
        if msg.message:
            base_text = msg.message
            break

    rewritten_text = await rewrite_links_in_text(client, extrapay_bot_username, base_text)
    final_caption = build_footer_only_caption(rewritten_text, footer_text, footer_link)

    temp_dir = tempfile.mkdtemp(prefix="moneyzon_album_")
    try:
        media_items = []
        for msg in messages:
            file_path = await download_media(client, msg, temp_dir)
            if not file_path:
                continue
            media_items.append((file_path, file_kind(file_path)))

        if media_items:
            send_album(
                bot_token=bot_token,
                chat_id=dest_channel_id,
                media_items=media_items,
                caption=final_caption,
            )
            print(f"Sent album with {len(media_items)} items")
        else:
            send_text(
                bot_token=bot_token,
                chat_id=dest_channel_id,
                text=final_caption,
            )
            print("Album had no downloadable media, sent as text")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


async def process_channel(
    client: TelegramClient,
    bot_token: str,
    source_channel_id: int,
    dest_channel_id: str,
    extrapay_bot_username: str,
    footer_text: str,
    footer_link: str,
    state: dict,
):
    channel_key = str(source_channel_id)
    channel_state = state["channels"][channel_key]

    last_message_id = int(channel_state.get("last_message_id", 0))
    processed_group_ids = set(str(x) for x in channel_state.get("processed_group_ids", []))

    print(f"Resolving source channel {source_channel_id}")
    source_entity = await resolve_channel_entity(client, source_channel_id)

    print(f"Checking posts in {source_channel_id} after message {last_message_id}")
    new_messages = await collect_new_messages(client, source_entity, last_message_id)

    if not new_messages:
        print(f"No new messages for {source_channel_id}")
        return

    items = build_items(new_messages)
    newest_id = last_message_id

    for item in items:
        item_messages = item["messages"]
        newest_id = max(newest_id, max(m.id for m in item_messages))

        try:
            if item["type"] == "single":
                await process_single(
                    client=client,
                    bot_token=bot_token,
                    dest_channel_id=dest_channel_id,
                    message=item_messages[0],
                    extrapay_bot_username=extrapay_bot_username,
                    footer_text=footer_text,
                    footer_link=footer_link,
                )
            else:
                group_id = f"{source_channel_id}_{item['group_id']}"
                if group_id in processed_group_ids:
                    print(f"Skipping already processed album {group_id}")
                    continue

                await process_album(
                    client=client,
                    bot_token=bot_token,
                    dest_channel_id=dest_channel_id,
                    messages=item_messages,
                    extrapay_bot_username=extrapay_bot_username,
                    footer_text=footer_text,
                    footer_link=footer_link,
                )
                processed_group_ids.add(group_id)

        except Exception as exc:
            print(f"[ERROR] Failed processing item in {source_channel_id}: {exc}")

    channel_state["last_message_id"] = newest_id
    channel_state["processed_group_ids"] = list(processed_group_ids)[-1000:]


async def main():
    api_id = int(os.environ["API_ID"])
    api_hash = os.environ["API_HASH"]
    user_session = os.environ["USER_SESSION"]
    bot_token = os.environ["BOT_TOKEN"]

    source_channel_ids = parse_source_channel_ids(os.environ["SOURCE_CHANNEL_IDS"])
    dest_channel_id = os.environ["DEST_CHANNEL_ID"]
    extrapay_bot_username = os.environ.get("EXTRAPAY_BOT_USERNAME", "ExtraPeBot")

    footer_text = os.environ.get("FOOTER_TEXT", "Powered by Moneyzon")
    footer_link = os.environ.get("FOOTER_LINK", "https://t.me/+Pn4M2jtiFZBhMDhl")

    state = load_state(source_channel_ids)

    client = TelegramClient(StringSession(user_session), api_id, api_hash)

    async with client:
        for source_channel_id in source_channel_ids:
            await process_channel(
                client=client,
                bot_token=bot_token,
                source_channel_id=source_channel_id,
                dest_channel_id=dest_channel_id,
                extrapay_bot_username=extrapay_bot_username,
                footer_text=footer_text,
                footer_link=footer_link,
                state=state,
            )

    save_state(state)
    print("State updated successfully")


if __name__ == "__main__":
    asyncio.run(main())
