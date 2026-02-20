import asyncio
import json
import logging
import os
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ButtonStyle, ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message, InputMediaPhoto, InputMediaVideo, FSInputFile
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from dotenv import load_dotenv


load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("bot")


BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
BOOKING_URL = os.getenv("BOOKING_URL", "")
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0")) if os.getenv("ADMIN_CHAT_ID", "0").isdigit() else 0
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0")) if os.getenv("CHANNEL_ID", "0").lstrip("-").isdigit() else 0
CHANNEL_CATALOG_URL = os.getenv("CHANNEL_CATALOG_URL", "").strip()
STRICT_PHONE_MODE = os.getenv("STRICT_PHONE_MODE", "").lower() in {"1", "true", "yes", "on"}
MANAGER_PHONE = os.getenv("MANAGER_PHONE", "")
MANAGER_TG_USERNAME = os.getenv("MANAGER_TG_USERNAME", "")
MANAGER_WHATSAPP = os.getenv("MANAGER_WHATSAPP", "")
WELCOME_IMAGE = os.getenv("WELCOME_IMAGE", "").strip()
WELCOME_TEXT = os.getenv("WELCOME_TEXT", "").strip()
BUDGET_THRESHOLDS = sorted(
    [int(x.strip()) for x in os.getenv("BUDGET_THRESHOLDS", "").split(",") if x.strip().isdigit()]
)

THROTTLE_SECONDS = 0.6
last_click_at: dict[tuple[int, str], datetime] = {}
user_filters: dict[int, dict[str, Any]] = {}
DB_INSTANCE: "Database | None" = None
user_main_message_ids: dict[int, int] = {}
user_anchor_message_ids: dict[int, int] = {}


class DateRequestState(StatesGroup):
    waiting_text = State()


class EntryDateState(StatesGroup):
    waiting_text = State()


class UserReplyToAdminState(StatesGroup):
    waiting_text = State()


class AdminReplyState(StatesGroup):
    waiting_reply = State()


class AdminPromoState(StatesGroup):
    waiting_custom_code = State()


class AdminBroadcastState(StatesGroup):
    waiting_text = State()
    waiting_confirm = State()


class AdminApartmentState(StatesGroup):
    waiting_add_line = State()
    waiting_field_value = State()


class AdminApartmentWizardState(StatesGroup):
    waiting_text = State()
    waiting_choice = State()
    waiting_media = State()
    preview = State()


class AdminCodesBulkState(StatesGroup):
    waiting_codes = State()


class AdminChannelPostState(StatesGroup):
    waiting_media = State()
    waiting_text = State()
    preview = State()


class AdminCatalogOnlyState(StatesGroup):
    waiting_url = State()


class Database:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def execute(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def fetch(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)


class TouchUserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        db = get_db()
        if user:
            await upsert_user(db, user.id, user.full_name, user.username)
        return await handler(event, data)


def get_db() -> "Database":
    if DB_INSTANCE is None:
        raise RuntimeError("DB не инициализирован")
    return DB_INSTANCE


async def set_user_main_message(bot: Bot, user_id: int, chat_id: int, message_id: int):
    prev_id = user_main_message_ids.get(user_id)
    if prev_id and prev_id != message_id:
        anchor_id = user_anchor_message_ids.get(user_id)
        if prev_id != anchor_id:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=prev_id)
            except Exception:
                pass
    user_main_message_ids[user_id] = message_id


async def send_user_main_message(
    bot: Bot,
    user_id: int,
    chat_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> Message:
    prev_id = user_main_message_ids.get(user_id)
    if prev_id:
        anchor_id = user_anchor_message_ids.get(user_id)
        if prev_id != anchor_id:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=prev_id)
            except Exception:
                pass
    sent = await bot.send_message(chat_id, text, reply_markup=reply_markup)
    user_main_message_ids[user_id] = sent.message_id
    return sent


async def ensure_anchor_welcome_message(bot: Bot, user_id: int, chat_id: int) -> int:
    anchor_id = user_anchor_message_ids.get(user_id)
    if anchor_id:
        return anchor_id

    sent: Message
    if WELCOME_IMAGE:
        try:
            image_source: str | FSInputFile = WELCOME_IMAGE
            if not (WELCOME_IMAGE.startswith("http://") or WELCOME_IMAGE.startswith("https://")):
                resolved = os.path.abspath(WELCOME_IMAGE)
                if os.path.isfile(resolved):
                    image_source = FSInputFile(resolved)
            sent = await bot.send_photo(
                chat_id=chat_id,
                photo=image_source,
                caption=WELCOME_TEXT,
                reply_markup=start_entry_kb(),
            )
        except Exception as e:
            logger.warning("Не удалось отправить WELCOME_IMAGE (%s), отправляем текст: %s", WELCOME_IMAGE, e)
            sent = await bot.send_message(
                chat_id=chat_id,
                text=WELCOME_TEXT,
                reply_markup=start_entry_kb(),
            )
    else:
        sent = await bot.send_message(
            chat_id=chat_id,
            text=WELCOME_TEXT,
            reply_markup=start_entry_kb(),
        )

    user_anchor_message_ids[user_id] = sent.message_id
    user_main_message_ids[user_id] = sent.message_id
    return sent.message_id


def btn(
    text: str,
    callback_data: str,
    style: str | None = None,
    icon_custom_emoji_id: str | None = None,
) -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text=text,
        callback_data=callback_data,
        style=style,
        icon_custom_emoji_id=icon_custom_emoji_id,
    )


def url_btn(
    text: str,
    url: str,
    style: str | None = None,
    icon_custom_emoji_id: str | None = None,
) -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text=text,
        url=url,
        style=style,
        icon_custom_emoji_id=icon_custom_emoji_id,
    )


def nav(back_cb: str = "home") -> list[list[InlineKeyboardButton]]:
    return [[btn("⬅️ Назад", back_cb, style=ButtonStyle.DANGER), btn("🏠 Главная", "home", style=ButtonStyle.PRIMARY)]]


def menu_kb(is_admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [btn("🏠 Квартиры", "catalog:1", style=ButtonStyle.PRIMARY), btn("📅 Бронирование", "booking:main", style=ButtonStyle.SUCCESS)],
        [btn("👤 Кабинет", "cabinet", style=ButtonStyle.PRIMARY), btn("❓ Правила", "rules", style=ButtonStyle.DANGER)],
    ]
    if is_admin:
        rows.append([btn("🛠 Админ-панель", "admin:menu", style=ButtonStyle.DANGER)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def start_entry_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("👋 Перейти в главное меню", "start:menu", style=ButtonStyle.PRIMARY)],
            [btn("🗓 Написать желаемую дату заезда", "start:date", style=ButtonStyle.SUCCESS)],
        ]
    )


def throttle(user_id: int, key: str) -> bool:
    now = datetime.now(timezone.utc)
    composite = (user_id, key)
    last = last_click_at.get(composite)
    if last and (now - last).total_seconds() < THROTTLE_SECONDS:
        return True
    last_click_at[composite] = now
    return False


async def init_db(db: Database):
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id BIGSERIAL PRIMARY KEY,
            tg_user_id BIGINT UNIQUE NOT NULL,
            username TEXT,
            full_name TEXT,
            phone TEXT,
            ref_code TEXT UNIQUE,
            inviter_user_id BIGINT,
            is_blocked BOOLEAN NOT NULL DEFAULT FALSE,
            reminders_opt_out BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_reminder_at TIMESTAMPTZ
        );

        CREATE TABLE IF NOT EXISTS apartments (
            id BIGSERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            address_short TEXT NOT NULL,
            guests_max INTEGER NOT NULL DEFAULT 2,
            amenities TEXT NOT NULL DEFAULT '',
            tags TEXT[] NOT NULL DEFAULT '{}',
            price_from INTEGER NOT NULL DEFAULT 0,
            channel_post_url TEXT NOT NULL,
            map_url TEXT NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS promo_codes (
            id BIGSERIAL PRIMARY KEY,
            code TEXT UNIQUE NOT NULL,
            kind TEXT NOT NULL,
            is_assigned BOOLEAN NOT NULL DEFAULT FALSE,
            assigned_to BIGINT,
            assigned_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS click_events (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            apartment_id BIGINT,
            source TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS date_requests (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            apartment_id BIGINT NOT NULL,
            raw_text TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            admin_id BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            handled_at TIMESTAMPTZ
        );

        CREATE TABLE IF NOT EXISTS entry_date_requests (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            raw_text TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            admin_id BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS giveaway_entries (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT UNIQUE NOT NULL,
            is_winner BOOLEAN NOT NULL DEFAULT FALSE,
            winner_code TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS referrals (
            id BIGSERIAL PRIMARY KEY,
            inviter_user_id BIGINT NOT NULL,
            invitee_user_id BIGINT UNIQUE NOT NULL,
            qualified BOOLEAN NOT NULL DEFAULT FALSE,
            qualified_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS events (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT,
            event_type TEXT NOT NULL,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_users_last_seen ON users(last_seen_at);
        CREATE INDEX IF NOT EXISTS idx_click_events_created ON click_events(created_at);
        CREATE INDEX IF NOT EXISTS idx_events_type_created ON events(event_type, created_at);
        """
    )
    await db.execute(
        """
        ALTER TABLE apartments ADD COLUMN IF NOT EXISTS details_json JSONB NOT NULL DEFAULT '{}'::jsonb;
        ALTER TABLE apartments ADD COLUMN IF NOT EXISTS media_urls TEXT[] NOT NULL DEFAULT '{}';
        ALTER TABLE entry_date_requests ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'new';
        ALTER TABLE entry_date_requests ADD COLUMN IF NOT EXISTS admin_id BIGINT;
        ALTER TABLE entry_date_requests ADD COLUMN IF NOT EXISTS handled_at TIMESTAMPTZ;
        """
    )


async def upsert_user(db: Database, tg_user_id: int, full_name: str | None, username: str | None):
    ref_code = f"u{secrets.token_hex(4)}"
    await db.execute(
        """
        INSERT INTO users(tg_user_id, full_name, username, ref_code, created_at, last_seen_at)
        VALUES($1, $2, $3, $4, NOW(), NOW())
        ON CONFLICT (tg_user_id)
        DO UPDATE SET
            full_name = EXCLUDED.full_name,
            username = EXCLUDED.username,
            last_seen_at = NOW()
        """,
        tg_user_id,
        full_name,
        username,
        ref_code,
    )


async def assign_inviter_if_needed(db: Database, tg_user_id: int, start_arg: str | None):
    if not start_arg or not start_arg.startswith("ref_"):
        return
    ref_code = start_arg.replace("ref_", "", 1)
    me = await db.fetchrow("SELECT tg_user_id, inviter_user_id FROM users WHERE tg_user_id=$1", tg_user_id)
    if not me or me["inviter_user_id"]:
        return
    inviter = await db.fetchrow("SELECT tg_user_id FROM users WHERE ref_code=$1", ref_code)
    if not inviter or inviter["tg_user_id"] == tg_user_id:
        return
    await db.execute("UPDATE users SET inviter_user_id=$1 WHERE tg_user_id=$2", inviter["tg_user_id"], tg_user_id)
    await db.execute(
        """
        INSERT INTO referrals(inviter_user_id, invitee_user_id, qualified)
        VALUES($1, $2, FALSE)
        ON CONFLICT (invitee_user_id) DO NOTHING
        """,
        inviter["tg_user_id"],
        tg_user_id,
    )


async def log_event(db: Database, user_id: int | None, event_type: str, payload: str = "{}"):
    await db.execute("INSERT INTO events(user_id, event_type, payload) VALUES($1,$2,$3::jsonb)", user_id, event_type, payload)


async def maybe_qualify_referral(db: Database, user_id: int):
    ref = await db.fetchrow(
        "SELECT inviter_user_id, qualified FROM referrals WHERE invitee_user_id=$1",
        user_id,
    )
    if not ref or ref["qualified"]:
        return
    if STRICT_PHONE_MODE:
        phone = await db.fetchval("SELECT phone FROM users WHERE tg_user_id=$1", user_id)
        if not phone:
            return
    await db.execute("UPDATE referrals SET qualified=TRUE, qualified_at=NOW() WHERE invitee_user_id=$1", user_id)
    code = await db.fetchrow(
        "SELECT id, code FROM promo_codes WHERE kind='referral_reward' AND is_assigned=FALSE ORDER BY id LIMIT 1"
    )
    if code:
        inviter_id = ref["inviter_user_id"]
        await db.execute(
            "UPDATE promo_codes SET is_assigned=TRUE, assigned_to=$1, assigned_at=NOW() WHERE id=$2",
            inviter_id,
            code["id"],
        )


async def apartment_filters_text(filters: dict[str, Any]) -> str:
    guest_labels = {
        "1-2": "До 2 гостей",
        "3-4": "До 4 гостей",
        "5+": "5 и более гостей",
    }
    guests = guest_labels.get(filters.get("guests"), "любой вариант")
    tags = ", ".join(sorted(filters.get("tags", []))) if filters.get("tags") else "нет"
    return (
        "Текущие фильтры:\n"
        f"• Гости: {guests}\n"
        f"• Теги: {tags}\n\n"
        "Подсказка: фильтр гостей ищет квартиры по вместимости (максимум гостей)."
    )


def filter_menu_kb(filters: dict[str, Any]) -> InlineKeyboardMarkup:
    tags = filters.get("tags", set())
    guests = filters.get("guests")
    rows = [
        [
            btn(f"{'✅ ' if guests == '1-2' else ''}👤 До 2 гостей", "flt:g:1-2"),
            btn(f"{'✅ ' if guests == '3-4' else ''}👨‍👩‍👧 До 4 гостей", "flt:g:3-4"),
        ],
        [btn(f"{'✅ ' if guests == '5+' else ''}👥 5+ гостей", "flt:g:5+")],
    ]
    rows += [
        [btn(f"{'✅' if 'парковка' in tags else '🏷'} парковка", "flt:t:парковка"), btn(f"{'✅' if 'видовая' in tags else '🏷'} видовая", "flt:t:видовая")],
        [btn(f"{'✅' if 'тихо' in tags else '🏷'} тихо", "flt:t:тихо"), btn(f"{'✅' if 'для семьи' in tags else '🏷'} для семьи", "flt:t:для семьи")],
        [btn("✅ Применить", "flt:apply", style=ButtonStyle.SUCCESS), btn("♻️ Сбросить", "flt:reset", style=ButtonStyle.DANGER)],
    ]
    rows += nav("catalog:1")
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def catalog_query(db: Database, filters: dict[str, Any], page: int, page_size: int = 5):
    cond = ["is_active=TRUE"]
    args: list[Any] = []
    idx = 1

    guests = filters.get("guests")
    if guests == "1-2":
        cond.append(f"guests_max >= ${idx}")
        args += [2]
        idx += 1
    elif guests == "3-4":
        cond.append(f"guests_max >= ${idx}")
        args += [4]
        idx += 1
    elif guests == "5+":
        cond.append(f"guests_max >= ${idx}")
        args += [5]
        idx += 1

    tags = [str(tag).strip().lower() for tag in list(filters.get("tags", set())) if str(tag).strip()]
    if tags:
        cond.append(f"EXISTS (SELECT 1 FROM unnest(tags) AS tag WHERE lower(tag) = ANY(${idx}::text[]))")
        args.append(tags)
        idx += 1

    where = " AND ".join(cond)
    total = await db.fetchval(f"SELECT COUNT(*) FROM apartments WHERE {where}", *args)

    offset = (page - 1) * page_size
    args_with_paging = args + [page_size, offset]
    rows = await db.fetch(
        f"SELECT * FROM apartments WHERE {where} ORDER BY sort_order, id LIMIT ${idx} OFFSET ${idx+1}",
        *args_with_paging,
    )
    return rows, int(total)


def catalog_kb(rows, page: int, total: int, page_size: int = 5) -> InlineKeyboardMarkup:
    keyboard = []
    for apartment in rows:
        keyboard.append(
            [btn(f"🏙 {apartment['title']}", f"apt:{apartment['id']}:card:{page}", style=ButtonStyle.PRIMARY)]
        )
    pages = max(1, (total + page_size - 1) // page_size)
    nav_row = []
    if page > 1:
        nav_row.append(btn("◀️", f"catalog:{page-1}"))
    nav_row.append(btn(f"Стр. {page}/{pages}", "noop"))
    if page < pages:
        nav_row.append(btn("▶️", f"catalog:{page+1}"))
    keyboard.append(nav_row)
    keyboard.append([btn("⚙️ Фильтры", "flt:open", style=ButtonStyle.PRIMARY), btn("📅 Забронировать", "booking:catalog", style=ButtonStyle.SUCCESS)])
    keyboard += nav("home")
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def apartment_card_kb(apt_id: int, page: int, channel_post_url: str, map_url: str, has_media: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if has_media:
        rows.append([btn("🖼 Посмотреть фотографии", f"aptmedia:{apt_id}:0:{page}", style=ButtonStyle.PRIMARY)])
    rows.append([url_btn("🗺 Карта", map_url, style=ButtonStyle.PRIMARY), btn("📅 Забронировать", f"book:apt:{apt_id}", style=ButtonStyle.SUCCESS)])
    rows.append([btn("🤔 Не определился", f"dates:{apt_id}", style=ButtonStyle.PRIMARY)])
    rows += nav(f"catalog:{page}")
    return InlineKeyboardMarkup(inline_keyboard=rows)


def booking_kb(back_cb: str, source: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[url_btn("📅 Открыть бронирование", BOOKING_URL, style=ButtonStyle.SUCCESS)]] + nav(back_cb)
    )


def promo_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("🎁 Получить welcome", "promo:welcome"), btn("🔗 Реферальная ссылка", "promo:ref")],
            [btn("📦 Мои промокоды", "promo:mine")],
        ]
        + nav("home")
    )


def giveaway_kb(joined: bool) -> InlineKeyboardMarkup:
    label = "✅ Вы участвуете" if joined else "✅ Участвовать"
    cb = "giveaway:joined" if joined else "giveaway:join"
    return InlineKeyboardMarkup(inline_keyboard=[[btn(label, cb)]] + nav("home"))


def rules_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[btn("📅 Перейти к бронированию", "booking:rules", style=ButtonStyle.SUCCESS)]] + nav("home")
    )


def cabinet_kb() -> InlineKeyboardMarkup:
    rows = [[btn("🔗 Моя реф-ссылка", "promo:ref"), btn("🎁 Мои промокоды", "promo:mine")]]
    rows += nav("home")
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_menu_kb() -> InlineKeyboardMarkup:
    rows = [
        [btn("🏠 Квартиры", "admin:apartments", style=ButtonStyle.PRIMARY), btn("📥 Заявки", "admin:requests", style=ButtonStyle.PRIMARY)],
        [btn("🗓 Даты на старте", "admin:entry_dates", style=ButtonStyle.PRIMARY), btn("📣 Пост в канал", "admin:channel_post", style=ButtonStyle.SUCCESS)],
        [btn("🟢 Закреп: Бронь", "admin:catalog_button", style=ButtonStyle.SUCCESS), btn("📚 Кнопка Каталог", "admin:catalog_only", style=ButtonStyle.PRIMARY)],
        [btn("📊 Статистика", "admin:stats", style=ButtonStyle.PRIMARY)],
    ]
    rows += nav("home")
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_catalog_button_menu_kb() -> InlineKeyboardMarkup:
    rows = [
        [btn("📤 Опубликовать закреп с бронью", "admin:catalog_button:publish", style=ButtonStyle.SUCCESS)],
    ]
    rows += nav("admin:menu")
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_catalog_only_menu_kb() -> InlineKeyboardMarkup:
    rows = [
        [btn("📤 Опубликовать кнопку Каталог", "admin:catalog_only:publish", style=ButtonStyle.SUCCESS)],
        [btn("🔗 Обновить ссылку Каталог", "admin:catalog_only:set_link", style=ButtonStyle.PRIMARY)],
    ]
    rows += nav("admin:menu")
    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_booking_button_kb(booking_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[url_btn("📅 Бронь", booking_url, style=ButtonStyle.SUCCESS)]]
    )


def channel_catalog_button_kb(catalog_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[url_btn("📚 Каталог", catalog_url, style=ButtonStyle.SUCCESS)]]
    )


def admin_channel_post_media_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("✅ Готово, к тексту", "admin:channel_post:done", style=ButtonStyle.SUCCESS)],
            [btn("❌ Отменить", "admin:channel_post:cancel", style=ButtonStyle.DANGER)],
        ]
        + nav("admin:menu")
    )


def admin_channel_post_preview_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("📤 Опубликовать", "admin:channel_post:publish", style=ButtonStyle.SUCCESS)],
            [btn("❌ Отменить", "admin:channel_post:cancel", style=ButtonStyle.DANGER)],
        ]
        + nav("admin:menu")
    )


def channel_custom_buttons_kb(bot_username: str, manager_link: str, whatsapp_link: str) -> InlineKeyboardMarkup:
    rows = [
        [url_btn("🏠 Квартиры", f"https://t.me/{bot_username}", style=ButtonStyle.PRIMARY), url_btn("📅 Бронь", BOOKING_URL, style=ButtonStyle.SUCCESS)],
        [url_btn("👤 Менеджер", manager_link, style=ButtonStyle.DANGER), url_btn("🟢 WhatsApp", whatsapp_link, style=ButtonStyle.SUCCESS)],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_codes_kb() -> InlineKeyboardMarkup:
    rows = [
        [btn("➕ Добавить код", "admin:code:add"), btn("📥 Bulk загрузка", "admin:code:bulk")],
        [btn("🧾 Список по типу", "admin:code:list")],
    ]
    rows += nav("admin:menu")
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_apartments_menu_kb() -> InlineKeyboardMarkup:
    rows = [[btn("➕ Добавить квартиру", "admin:apt:add", style=ButtonStyle.SUCCESS), btn("📋 Список", "admin:apt:list", style=ButtonStyle.PRIMARY)]]
    rows += nav("admin:menu")
    return InlineKeyboardMarkup(inline_keyboard=rows)


def manager_url() -> str | None:
    manager_id = ADMIN_CHAT_ID if ADMIN_CHAT_ID else (min(ADMIN_IDS) if ADMIN_IDS else 0)
    if not manager_id:
        return None
    return f"tg://user?id={manager_id}"


def normalize_phone(phone: str) -> str:
    cleaned = re.sub(r"[^\d+]", "", phone.strip())
    if cleaned.startswith("8"):
        cleaned = "+7" + cleaned[1:]
    if cleaned and not cleaned.startswith("+"):
        cleaned = "+" + cleaned
    return cleaned


def tel_url() -> str | None:
    if not MANAGER_PHONE.strip():
        return None
    normalized = normalize_phone(MANAGER_PHONE)
    if not normalized:
        return None
    return f"tel:{normalized}"


def tg_contact_url() -> str | None:
    username = MANAGER_TG_USERNAME.strip().lstrip("@")
    if username:
        return f"https://t.me/{username}"
    return manager_url()


def whatsapp_url() -> str | None:
    value = MANAGER_WHATSAPP.strip()
    if not value:
        return None
    if value.startswith("http://") or value.startswith("https://"):
        return value
    digits = re.sub(r"\D", "", value)
    if not digits:
        return None
    if digits.startswith("8"):
        digits = "7" + digits[1:]
    return f"https://wa.me/{digits}"


def apartment_post_action_kb(map_url: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [[url_btn("📅 Забронировать", BOOKING_URL, style=ButtonStyle.SUCCESS)]]

    phone_link = tel_url()
    tg_link = tg_contact_url()
    wa_link = whatsapp_url()

    contact_row: list[InlineKeyboardButton] = []
    if phone_link:
        contact_row.append(url_btn("📞 Позвонить", phone_link, style=ButtonStyle.PRIMARY))
    if tg_link:
        contact_row.append(url_btn("✔️ Написать в ТГ", tg_link, style=ButtonStyle.PRIMARY))
    if contact_row:
        rows.append(contact_row)

    if wa_link:
        rows.append([url_btn("🟢 Написать в WhatsApp", wa_link, style=ButtonStyle.SUCCESS)])

    if map_url:
        rows.append([url_btn("🗺 Карта", map_url, style=ButtonStyle.PRIMARY)])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def apartment_post_text_from_record(apt: asyncpg.Record) -> str:
    details = apt.get("details_json") or {}
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except Exception:
            details = {}

    header_text = str(details.get("header_text", apt.get("title") or apt.get("address_short") or "")).strip()
    header_block = f"<b><u>{header_text}</u></b>" if header_text else ""
    short_desc = str(details.get("short_desc", "")).strip()
    quote_desc = str(details.get("quote_desc", "")).strip()
    features_text = str(details.get("features_text", "")).strip()
    tags = ", ".join(apt["tags"]) if apt["tags"] else ""
    tags_line = f"\n\n🏷 Теги: {tags}" if tags else ""
    map_line = f"\n\n🗺 <a href=\"{apt['map_url']}\">Открыть карту</a>" if apt.get("map_url") else ""

    return (
        f"{header_block}\n\n"
        f"{short_desc}\n\n"
        f"<blockquote>{quote_desc}</blockquote>\n\n"
        "<b>Что есть в квартире</b>\n"
        f"<blockquote>{features_text}</blockquote>"
        f"{tags_line}"
        f"{map_line}"
        "\n\n<b>Забронировать эту квартиру</b>\n⬇️ Нажмите кнопку ниже"
    )


def apartment_card_text_from_record(apt: asyncpg.Record) -> str:
    details = apt.get("details_json") or {}
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except Exception:
            details = {}

    tags = ", ".join(apt.get("tags") or [])
    header_text = str(details.get("header_text", apt.get("title") or apt.get("address_short") or "")).strip()
    header_block = f"<b><u>{header_text}</u></b>" if header_text else ""
    short_desc = str(details.get("short_desc", "")).strip()
    quote_desc = str(details.get("quote_desc", "")).strip()
    features_text = str(details.get("features_text", "")).strip()

    tags_line = f"\n\n🏷 {tags}" if tags else ""
    return (
        f"{header_block}\n\n"
        f"{short_desc}\n\n"
        f"<blockquote>{quote_desc}</blockquote>\n\n"
        "<b>Что есть в квартире</b>\n"
        f"<blockquote>{features_text}</blockquote>"
        f"{tags_line}"
        "\n\n<b>Забронировать эту квартиру</b>\n⬇️ Нажмите кнопку ниже"
    )


async def show_apartment_card(call: CallbackQuery, apt: asyncpg.Record, page: int):
    media_items = get_apartment_media_items(apt)
    kb = apartment_card_kb(
        apt["id"],
        int(page),
        apt["channel_post_url"],
        apt["map_url"],
        bool(media_items),
    )
    caption = apartment_card_text_from_record(apt)

    def _is_video_url(url: str) -> bool:
        return any(url.lower().endswith(ext) for ext in [".mp4", ".mov", ".webm", ".mkv"])

    try:
        anchor_id = user_anchor_message_ids.get(call.from_user.id)
        if call.message.message_id != anchor_id:
            await call.message.delete()
    except Exception:
        pass

    if media_items:
        first = media_items[0]
        kind = first.get("type", "url")
        value = first.get("value", "")
        if value:
            try:
                if kind == "photo":
                    sent = await call.bot.send_photo(call.message.chat.id, photo=value, caption=caption, reply_markup=kb)
                    await set_user_main_message(call.bot, call.from_user.id, call.message.chat.id, sent.message_id)
                    return
                if kind == "video":
                    sent = await call.bot.send_video(call.message.chat.id, video=value, caption=caption, reply_markup=kb)
                    await set_user_main_message(call.bot, call.from_user.id, call.message.chat.id, sent.message_id)
                    return
                if kind == "document":
                    sent = await call.bot.send_document(call.message.chat.id, document=value, caption=caption, reply_markup=kb)
                    await set_user_main_message(call.bot, call.from_user.id, call.message.chat.id, sent.message_id)
                    return
                if kind == "url":
                    if _is_video_url(value):
                        sent = await call.bot.send_video(call.message.chat.id, video=value, caption=caption, reply_markup=kb)
                    else:
                        sent = await call.bot.send_photo(call.message.chat.id, photo=value, caption=caption, reply_markup=kb)
                    await set_user_main_message(call.bot, call.from_user.id, call.message.chat.id, sent.message_id)
                    return
            except Exception as e:
                logger.warning("Не удалось отправить первое медиа в карточке квартиры: %s", e)

    sent = await call.bot.send_message(call.message.chat.id, caption, reply_markup=kb)
    await set_user_main_message(call.bot, call.from_user.id, call.message.chat.id, sent.message_id)


async def send_apartment_post_with_media(bot: Bot, chat_id: int, apt: asyncpg.Record):
    media_items = get_apartment_media_items(apt)
    caption = apartment_post_text_from_record(apt)
    markup = apartment_post_action_kb(apt["map_url"])
    if media_items:
        first = media_items[0]
        kind = first.get("type", "photo")
        value = first.get("value", "")
        try:
            if kind == "video":
                await bot.send_video(chat_id=chat_id, video=value, caption=caption, reply_markup=markup)
                return
            await bot.send_photo(chat_id=chat_id, photo=value, caption=caption, reply_markup=markup)
            return
        except Exception as e:
            logger.warning("Не удалось отправить пост с медиа в одном сообщении: %s", e)

    await bot.send_message(chat_id, caption, reply_markup=markup)


APARTMENT_WIZARD_STEPS: list[dict[str, Any]] = [
    {
        "key": "media_items",
        "kind": "media_upload",
        "section": "1/8",
        "label": "Загрузите все фото/видео для карусели",
        "example": "Отправьте 1+ медиа файла. Затем нажмите «✅ Завершить медиа».",
    },
    {
        "key": "header_text",
        "kind": "text",
        "section": "2/8",
        "label": "Заголовок объявления (будет выделен жирным и подчёркнутым)",
        "example": "ЖК Панорама, ул. Героев Сарабеева, 5к1\n1-к квартира для 2-4 гостей",
    },
    {
        "key": "short_desc",
        "kind": "text",
        "section": "3/8",
        "label": "Короткое описание",
        "example": "Уютная квартира рядом с парком и удобным выездом в центр.",
    },
    {
        "key": "quote_desc",
        "kind": "text",
        "section": "4/8",
        "label": "Текст в цитате (рамке) под описанием",
        "example": "Тихий двор, быстрый Wi‑Fi, бесконтактное заселение.",
    },
    {
        "key": "features_text",
        "kind": "text",
        "section": "5/8",
        "label": "Что есть в квартире (тоже в цитате)",
        "example": "Двуспальная кровать, кондиционер, стиральная машина, кухня.",
    },
    {
        "key": "guests_max",
        "kind": "int",
        "section": "6/8",
        "label": "Максимум гостей (цифрой)",
        "example": "4",
    },
    {
        "key": "tags",
        "kind": "text",
        "section": "7/8",
        "label": "Теги для фильтров (через запятую)",
        "example": "парковка,видовая,тихо,для семьи",
    },
    {"key": "map_url", "kind": "url", "section": "8/8", "label": "Ссылка на карту", "example": "https://maps.google.com/?q=55.75,37.61"},
]


def wizard_choice_kb(options: list[tuple[str, str]]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    chunk: list[InlineKeyboardButton] = []
    for label, value in options:
        chunk.append(btn(label, f"aptw:pick:{value}"))
        if len(chunk) == 2:
            rows.append(chunk)
            chunk = []
    if chunk:
        rows.append(chunk)
    rows.append([btn("❌ Отменить мастер", "aptw:cancel")])
    rows += nav("admin:apartments")
    return InlineKeyboardMarkup(inline_keyboard=rows)


def wizard_text_kb() -> InlineKeyboardMarkup:
    rows = [[btn("❌ Отменить мастер", "aptw:cancel")]]
    rows += nav("admin:apartments")
    return InlineKeyboardMarkup(inline_keyboard=rows)


def wizard_media_kb() -> InlineKeyboardMarkup:
    rows = [[btn("✅ Завершить медиа", "aptw:media:done"), btn("❌ Отменить мастер", "aptw:cancel")]]
    rows += nav("admin:apartments")
    return InlineKeyboardMarkup(inline_keyboard=rows)


def wizard_preview_kb() -> InlineKeyboardMarkup:
    rows = [
        [btn("✅ Сохранить", "aptw:preview:save"), btn("🔁 Заполнить заново", "aptw:preview:restart")],
        [btn("❌ Отменить", "aptw:preview:cancel")],
    ]
    rows += nav("admin:apartments")
    return InlineKeyboardMarkup(inline_keyboard=rows)


def format_yes_no(value: str) -> str:
    return "Да" if value == "да" else "Нет"


def parse_guests_max(guests_range: str) -> int:
    numbers = [int(x) for x in re.findall(r"\d+", guests_range)]
    if not numbers:
        return 2
    return max(numbers)


def build_apartment_preview_text(data: dict[str, Any]) -> str:
    media_items = data.get("media_items") or []
    if isinstance(media_items, str):
        media_items = [x.strip() for x in media_items.splitlines() if x.strip()]
    return (
        "🏠 <b>Предпросмотр карточки/поста</b>\n\n"
        "<b>Верхний блок</b>\n"
        f"{data.get('header_text', '')}\n\n"
        "<b>Описание</b>\n"
        f"{data.get('short_desc', '')}\n\n"
        f"<blockquote>{data.get('quote_desc', '')}</blockquote>\n\n"
        "<b>Что есть в квартире</b>\n"
        f"<blockquote>{data.get('features_text', '')}</blockquote>\n\n"
        "<b>Публикация</b>\n"
        f"• Максимум гостей: {data.get('guests_max', '')}\n"
        f"• Теги: {data.get('tags', '')}\n"
        f"• Медиа-файлов/ссылок: {len(media_items)}\n"
        f"• Ссылка на карту: {data.get('map_url', '')}\n"
        f"• Порядок: {data.get('sort_order', '')}"
    )


def apartment_wizard_defaults_from_apartment(apt: asyncpg.Record) -> dict[str, Any]:
    details = apt.get("details_json") or {}
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except Exception:
            details = {}

    tags = ",".join(apt.get("tags") or [])
    media_urls = apt.get("media_urls") or []
    if isinstance(media_urls, str):
        media_urls = [x.strip() for x in media_urls.splitlines() if x.strip()]
    media_items = details.get("media_items", [])
    if not media_items and media_urls:
        media_items = [{"type": "url", "value": u} for u in media_urls]

    return {
        "header_text": details.get("header_text", apt.get("title") or apt.get("address_short") or ""),
        "short_desc": details.get("short_desc", ""),
        "quote_desc": details.get("quote_desc", ""),
        "features_text": details.get("features_text", details.get("apartment_features", "")),
        "guests_max": int(apt.get("guests_max") or 2),
        "tags": tags,
        "media_items": media_items,
        "map_url": apt.get("map_url") or "",
        "sort_order": apt.get("sort_order") or 0,
    }


async def wizard_clear_prev_prompt(target: Message | CallbackQuery, state: FSMContext):
    data = await state.get_data()
    prompt_id = data.get("wizard_prompt_message_id")
    if not prompt_id:
        return
    try:
        bot = target.bot if isinstance(target, CallbackQuery) else target.bot
        chat_id = target.message.chat.id if isinstance(target, CallbackQuery) else target.chat.id
        await bot.delete_message(chat_id=chat_id, message_id=int(prompt_id))
    except Exception:
        pass


async def wizard_send_prompt(target: Message | CallbackQuery, state: FSMContext, text: str, kb: InlineKeyboardMarkup):
    await wizard_clear_prev_prompt(target, state)
    if isinstance(target, CallbackQuery):
        sent = await target.message.answer(text, reply_markup=kb)
    else:
        sent = await target.answer(text, reply_markup=kb)
    await state.update_data(wizard_prompt_message_id=sent.message_id)


def get_apartment_media_items(apt: asyncpg.Record) -> list[dict[str, str]]:
    details = apt.get("details_json") or {}
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except Exception:
            details = {}

    media_items = details.get("media_items", [])
    normalized: list[dict[str, str]] = []
    if isinstance(media_items, list):
        for item in media_items:
            if not isinstance(item, dict):
                continue
            kind = str(item.get("type", "")).strip()
            value = str(item.get("value", "")).strip()
            if kind and value:
                normalized.append({"type": kind, "value": value})

    if normalized:
        return normalized

    media_urls = apt.get("media_urls") or []
    if isinstance(media_urls, str):
        media_urls = [x.strip() for x in media_urls.splitlines() if x.strip()]
    for url in media_urls:
        normalized.append({"type": "url", "value": url})
    return normalized


async def wizard_show_step(target: Message | CallbackQuery, state: FSMContext):
    data = await state.get_data()
    idx = int(data.get("wizard_index", 0))
    step = APARTMENT_WIZARD_STEPS[idx]
    total = len(APARTMENT_WIZARD_STEPS)
    prefix = f"<b>Шаг {idx + 1}/{total}</b>\nРаздел: {step['section']}\n\n"

    if step["kind"] == "choice":
        await state.set_state(AdminApartmentWizardState.waiting_choice)
        text = prefix + f"Сейчас укажите: <b>{step['label']}</b>\nВыберите вариант кнопкой:"
        kb = wizard_choice_kb(step["options"])
    elif step["kind"] == "media_upload":
        await state.set_state(AdminApartmentWizardState.waiting_media)
        media_items = data.get("media_items") or []
        if not isinstance(media_items, list):
            media_items = []
        text = (
            prefix
            + f"Сейчас укажите: <b>{step['label']}</b>\n"
            + "Отправляйте фото/видео файлами прямо в чат.\n"
            + "Можно также отправить ссылки (каждая с новой строки).\n"
            + f"Уже добавлено: <b>{len(media_items)}</b>\n\n"
            + "Когда закончите, нажмите «✅ Завершить медиа»."
        )
        kb = wizard_media_kb()
    else:
        await state.set_state(AdminApartmentWizardState.waiting_text)
        text = prefix + f"Сейчас укажите: <b>{step['label']}</b>\nВведите значение текстом."
        if step.get("key") == "header_text":
            text += "\n\nЭтот блок будет показан в карточке как <b><u>заголовок объявления</u></b>."
        if step.get("example"):
            text += f"\nПример: <b>{step['example']}</b>"
        kb = wizard_text_kb()

    await wizard_send_prompt(target, state, text, kb)


async def wizard_advance(target: Message | CallbackQuery, state: FSMContext):
    data = await state.get_data()
    idx = int(data.get("wizard_index", 0)) + 1
    await state.update_data(wizard_index=idx, custom_step_key=None)
    if idx >= len(APARTMENT_WIZARD_STEPS):
        final_data = await state.get_data()
        await state.set_state(AdminApartmentWizardState.preview)
        await wizard_clear_prev_prompt(target, state)
        sent = await (target.message.answer if isinstance(target, CallbackQuery) else target.answer)(
            build_apartment_preview_text(final_data),
            reply_markup=wizard_preview_kb(),
        )
        await state.update_data(wizard_prompt_message_id=sent.message_id)
        return
    await wizard_show_step(target, state)


async def send_main(message: Message, db: Database, user_id: int | None = None):
    actual_user_id = user_id if user_id is not None else message.from_user.id
    is_admin = actual_user_id in ADMIN_IDS
    await send_user_main_message(
        message.bot,
        actual_user_id,
        message.chat.id,
        "✨ <b>Главное меню</b>\nВыберите нужный раздел:",
        reply_markup=menu_kb(is_admin),
    )


async def render_catalog_for_call(call: CallbackQuery, page: int):
    db = get_db()
    filters = user_filters.get(call.from_user.id, {"tags": set()})
    rows, total = await catalog_query(db, filters, page)
    txt = "Каталог квартир. Выберите вариант ниже."
    await call.message.edit_text(txt, reply_markup=catalog_kb(rows, page, total))


async def answer_or_edit(obj: CallbackQuery | Message, text: str, kb: InlineKeyboardMarkup):
    if isinstance(obj, CallbackQuery):
        await obj.message.edit_text(text, reply_markup=kb)
    else:
        await obj.answer(text, reply_markup=kb)


def parse_start_arg(text: str) -> str | None:
    parts = text.split(maxsplit=1)
    if len(parts) == 2:
        return parts[1].strip()
    return None


async def handle_blocked(db: Database, user_id: int):
    await db.execute("UPDATE users SET is_blocked=TRUE WHERE tg_user_id=$1", user_id)


async def notify_admins(bot: Bot, text: str, kb: InlineKeyboardMarkup | None = None):
    targets = [ADMIN_CHAT_ID] if ADMIN_CHAT_ID else list(ADMIN_IDS)
    for admin_id in targets:
        if not admin_id:
            continue
        try:
            await bot.send_message(admin_id, text, reply_markup=kb)
        except Exception as e:
            logger.warning("Не удалось отправить админу %s: %s", admin_id, e)


async def reminders_loop(bot: Bot):
    db = get_db()
    while True:
        try:
            rows = await db.fetch(
                """
                SELECT tg_user_id FROM users
                WHERE reminders_opt_out=FALSE
                  AND is_blocked=FALSE
                  AND last_seen_at < NOW() - INTERVAL '7 days'
                  AND (last_reminder_at IS NULL OR last_reminder_at < NOW() - INTERVAL '14 days')
                LIMIT 100
                """
            )
            for row in rows:
                user_id = row["tg_user_id"]
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [btn("🔥 Открыть каталог", "catalog:1"), btn("❌ Не получать", "rem:off")],
                        [btn("🏠 Главная", "home")],
                    ]
                )
                try:
                    await bot.send_message(user_id, "Давно не виделись 👋\nПосмотрите актуальные варианты квартир.", reply_markup=kb)
                    await db.execute("UPDATE users SET last_reminder_at=NOW() WHERE tg_user_id=$1", user_id)
                except Exception:
                    await handle_blocked(db, user_id)
        except Exception as e:
            logger.exception("Ошибка цикла напоминаний: %s", e)
        await asyncio.sleep(3600)


async def main():
    global DB_INSTANCE
    required_env = {
        "BOT_TOKEN": BOT_TOKEN,
        "DATABASE_URL": DATABASE_URL,
        "BOOKING_URL": BOOKING_URL,
        "WELCOME_TEXT": WELCOME_TEXT,
        "BUDGET_THRESHOLDS": ",".join(str(x) for x in BUDGET_THRESHOLDS),
    }
    missing = [name for name, value in required_env.items() if not value]
    if missing:
        raise RuntimeError(f"Укажите обязательные переменные окружения: {', '.join(missing)}")

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    db = Database(pool)
    DB_INSTANCE = db
    await init_db(db)

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

    dp = Dispatcher()
    dp.update.middleware(TouchUserMiddleware())

    @dp.message(CommandStart())
    async def cmd_start(message: Message):
        db = get_db()
        await upsert_user(db, message.from_user.id, message.from_user.full_name, message.from_user.username)
        await assign_inviter_if_needed(db, message.from_user.id, parse_start_arg(message.text or ""))
        await log_event(db, message.from_user.id, "start")
        await ensure_anchor_welcome_message(message.bot, message.from_user.id, message.chat.id)

    @dp.callback_query(F.data == "start:menu")
    async def cb_start_menu(call: CallbackQuery):
        db = get_db()
        await send_main(call.message, db, user_id=call.from_user.id)
        await call.answer()

    @dp.callback_query(F.data == "start:date")
    async def cb_start_date(call: CallbackQuery, state: FSMContext):
        await state.set_state(EntryDateState.waiting_text)
        sent = await send_user_main_message(
            call.bot,
            call.from_user.id,
            call.message.chat.id,
            "Напишите желаемую дату заезда в свободном формате.\n"
            "Примеры: <b>с 25 марта на 3 ночи</b>, <b>апрель, 2 взрослых</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("home")),
        )
        await state.update_data(entry_prompt_message_id=sent.message_id)
        await call.answer()

    @dp.message(EntryDateState.waiting_text)
    async def msg_entry_date(message: Message, state: FSMContext):
        db = get_db()
        try:
            await message.delete()
        except Exception:
            pass
        raw_text = (message.text or "").strip()
        if not raw_text:
            await send_user_main_message(
                message.bot,
                message.from_user.id,
                message.chat.id,
                "Не вижу текста. Напишите дату заезда одним сообщением.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("home")),
            )
            return
        req = await db.fetchrow(
            "INSERT INTO entry_date_requests(user_id, raw_text, status) VALUES($1,$2,'new') RETURNING id",
            message.from_user.id,
            raw_text,
        )
        req_id = int(req["id"]) if req else 0
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [btn("✅ Ответить", f"entry:req:reply:{req_id}", style=ButtonStyle.PRIMARY)],
                [btn("🟡 Пометить обработано", f"entry:req:done:{req_id}", style=ButtonStyle.SUCCESS)],
            ]
        )
        await notify_admins(
            message.bot,
            "🗓 Новая дата на старте\n"
            f"Заявка: #{req_id}\n"
            f"Пользователь: {message.from_user.full_name} (@{message.from_user.username or '-'})\n"
            f"ID: {message.from_user.id}\n"
            f"Текст: {raw_text}",
            kb,
        )
        await state.clear()
        await send_user_main_message(
            message.bot,
            message.from_user.id,
            message.chat.id,
            "Супер, дату зафиксировали ✅\n\n✨ <b>Главное меню</b>\nВыберите нужный раздел:",
            reply_markup=menu_kb(message.from_user.id in ADMIN_IDS),
        )

    @dp.message(Command("help"))
    async def cmd_help(message: Message):
        text = (
            "Помощь по боту:\n\n"
            "1) Откройте раздел <b>🏠 Квартиры</b> и выберите вариант.\n"
            "2) В карточке можно посмотреть фото/видео, карту и перейти к бронированию.\n"
            "3) Если хотите, нажмите <b>✍️ Указать даты</b> и отправьте даты + гостей одним сообщением.\n"
            "4) В разделе <b>🎁 Промокод</b> доступны ваши коды и реферальная ссылка.\n\n"
            "Важно: бронирование оформляется на сайте, не внутри Telegram-бота."
        )
        try:
            await message.delete()
        except Exception:
            pass
        await send_user_main_message(
            message.bot,
            message.from_user.id,
            message.chat.id,
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("home")),
        )

    @dp.message(Command("privacy"))
    async def cmd_privacy(message: Message):
        text = (
            "Политика данных:\n"
            "• Сохраняем только данные, нужные для работы бота и обработки заявок.\n"
            "• Вы можете удалить свои данные командой /delete_me."
        )
        try:
            await message.delete()
        except Exception:
            pass
        await send_user_main_message(
            message.bot,
            message.from_user.id,
            message.chat.id,
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("home")),
        )

    @dp.message(Command("delete_me"))
    async def cmd_delete(message: Message):
        db = get_db()
        uid = message.from_user.id
        await db.execute("DELETE FROM referrals WHERE inviter_user_id=$1 OR invitee_user_id=$1", uid)
        await db.execute("DELETE FROM giveaway_entries WHERE user_id=$1", uid)
        await db.execute("DELETE FROM date_requests WHERE user_id=$1", uid)
        await db.execute("DELETE FROM click_events WHERE user_id=$1", uid)
        await db.execute("DELETE FROM promo_codes WHERE assigned_to=$1", uid)
        await db.execute("DELETE FROM events WHERE user_id=$1", uid)
        await db.execute("DELETE FROM users WHERE tg_user_id=$1", uid)
        try:
            await message.delete()
        except Exception:
            pass
        await send_user_main_message(
            message.bot,
            message.from_user.id,
            message.chat.id,
            "Ваши данные удалены.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("home")),
        )

    @dp.callback_query(F.data == "home")
    async def cb_home(call: CallbackQuery):
        await send_user_main_message(
            call.bot,
            call.from_user.id,
            call.message.chat.id,
            "Главное меню. Выберите раздел:",
            reply_markup=menu_kb(call.from_user.id in ADMIN_IDS),
        )
        await call.answer()

    @dp.callback_query(F.data.startswith("catalog:"))
    async def cb_catalog(call: CallbackQuery):
        if throttle(call.from_user.id, "catalog"):
            await call.answer("Слишком быстро, попробуйте ещё раз.", show_alert=False)
            return
        page = int(call.data.split(":")[1])
        await render_catalog_for_call(call, page)
        await call.answer()

    @dp.callback_query(F.data == "flt:open")
    async def cb_filter_open(call: CallbackQuery):
        filters = user_filters.setdefault(call.from_user.id, {"tags": set()})
        text = "Настройте фильтры:\n\n" + await apartment_filters_text(filters)
        await call.message.edit_text(text, reply_markup=filter_menu_kb(filters))
        await call.answer()

    @dp.callback_query(F.data.startswith("flt:g:"))
    async def cb_filter_guests(call: CallbackQuery):
        value = call.data.split(":", 2)[2]
        filters = user_filters.setdefault(call.from_user.id, {"tags": set()})
        filters["guests"] = value
        text = "Фильтр по гостям обновлён.\n\n" + await apartment_filters_text(filters)
        await call.message.edit_text(text, reply_markup=filter_menu_kb(filters))
        await call.answer("Готово")

    @dp.callback_query(F.data.startswith("flt:b:"))
    async def cb_filter_budget(call: CallbackQuery):
        value = call.data.split(":", 2)[2]
        filters = user_filters.setdefault(call.from_user.id, {"tags": set()})
        filters["budget"] = int(value)
        text = "Фильтр по бюджету обновлён.\n\n" + await apartment_filters_text(filters)
        await call.message.edit_text(text, reply_markup=filter_menu_kb(filters))
        await call.answer("Готово")

    @dp.callback_query(F.data.startswith("flt:t:"))
    async def cb_filter_tag(call: CallbackQuery):
        tag = call.data.split(":", 2)[2]
        filters = user_filters.setdefault(call.from_user.id, {"tags": set()})
        tag_set = filters.setdefault("tags", set())
        if tag in tag_set:
            tag_set.remove(tag)
        else:
            tag_set.add(tag)
        text = "Фильтр по тегам обновлён.\n\n" + await apartment_filters_text(filters)
        await call.message.edit_text(text, reply_markup=filter_menu_kb(filters))
        await call.answer("Готово")

    @dp.callback_query(F.data == "flt:reset")
    async def cb_filter_reset(call: CallbackQuery):
        user_filters[call.from_user.id] = {"tags": set()}
        filters = user_filters[call.from_user.id]
        text = "Фильтры сброшены.\n\n" + await apartment_filters_text(filters)
        await call.message.edit_text(text, reply_markup=filter_menu_kb(filters))
        await call.answer("Сброшено")

    @dp.callback_query(F.data == "flt:apply")
    async def cb_filter_apply(call: CallbackQuery):
        await render_catalog_for_call(call, 1)
        await call.answer()

    @dp.callback_query(F.data.startswith("apt:") & F.data.contains(":card:"))
    async def cb_apartment_card(call: CallbackQuery):
        db = get_db()
        _, apt_id, _, page = call.data.split(":")
        apt = await db.fetchrow("SELECT * FROM apartments WHERE id=$1", int(apt_id))
        if not apt:
            await call.answer("Квартира не найдена", show_alert=True)
            return
        await show_apartment_card(call, apt, int(page))
        await log_event(db, call.from_user.id, "view_apartment", '{"apartment_id": %s}' % apt["id"])
        await maybe_qualify_referral(db, call.from_user.id)
        await call.answer()

    @dp.callback_query(F.data.startswith("aptmedia:"))
    async def cb_apartment_media(call: CallbackQuery):
        db = get_db()
        if call.data.startswith("aptmedia:back:"):
            _, _, apt_id_str, page_str = call.data.split(":")
            apt = await db.fetchrow("SELECT * FROM apartments WHERE id=$1", int(apt_id_str))
            if not apt:
                await call.answer("Квартира не найдена", show_alert=True)
                return
            await show_apartment_card(call, apt, int(page_str))
            await call.answer()
            return

        _, apt_id_str, idx_str, page_str = call.data.split(":")
        apt_id = int(apt_id_str)
        idx = int(idx_str)
        page = int(page_str)
        apt = await db.fetchrow("SELECT id, title, media_urls, details_json, channel_post_url FROM apartments WHERE id=$1", apt_id)
        if not apt:
            await call.answer("Квартира не найдена", show_alert=True)
            return

        media_items = get_apartment_media_items(apt)
        if not media_items:
            await call.answer("Медиа пока не добавлены", show_alert=True)
            return

        idx = max(0, min(idx, len(media_items) - 1))
        current = media_items[idx]
        current_type = current.get("type", "url")
        current_value = current.get("value", "")

        nav_row = []
        if len(media_items) > 1:
            prev_idx = (idx - 1) % len(media_items)
            next_idx = (idx + 1) % len(media_items)
            nav_row = [btn("◀️", f"aptmedia:{apt_id}:{prev_idx}:{page}"), btn(f"{idx + 1}/{len(media_items)}", "noop"), btn("▶️", f"aptmedia:{apt_id}:{next_idx}:{page}")]

        keyboard = [[btn("⬅️ К карточке", f"aptmedia:back:{apt_id}:{page}", style=ButtonStyle.DANGER)]]
        if nav_row:
            keyboard.append(nav_row)
        keyboard += nav("home")

        caption = f"🖼 <b>{apt['title']}</b>\nФото {idx + 1}/{len(media_items)}"
        kb = InlineKeyboardMarkup(inline_keyboard=keyboard)

        is_video_url = any(current_value.lower().endswith(ext) for ext in [".mp4", ".mov", ".webm", ".mkv"])
        if current_type == "url":
            try:
                media = InputMediaVideo(media=current_value, caption=caption) if is_video_url else InputMediaPhoto(media=current_value, caption=caption)
                await call.message.edit_media(media=media, reply_markup=kb)
            except Exception:
                text = caption + "\nОткройте медиа по ссылке ниже."
                kb_url = InlineKeyboardMarkup(inline_keyboard=[[url_btn("🔗 Открыть текущее медиа", current_value)]] + keyboard)
                await call.message.edit_text(text, reply_markup=kb_url)
        elif current_type in {"photo", "video"}:
            try:
                media = InputMediaPhoto(media=current_value, caption=caption) if current_type == "photo" else InputMediaVideo(media=current_value, caption=caption)
                await call.message.edit_media(media=media, reply_markup=kb)
            except Exception:
                try:
                    await call.message.delete()
                except Exception:
                    pass
                if current_type == "photo":
                    await call.bot.send_photo(call.message.chat.id, photo=current_value, caption=caption, reply_markup=kb)
                else:
                    await call.bot.send_video(call.message.chat.id, video=current_value, caption=caption, reply_markup=kb)
        else:
            try:
                await call.message.delete()
            except Exception:
                pass
            await call.bot.send_document(call.message.chat.id, document=current_value, caption=caption, reply_markup=kb)
        await call.answer()

    @dp.callback_query(F.data.startswith("book:apt:"))
    async def cb_book_apartment(call: CallbackQuery):
        db = get_db()
        apt_id = int(call.data.split(":")[-1])
        await db.execute(
            "INSERT INTO click_events(user_id, apartment_id, source) VALUES($1, $2, $3)",
            call.from_user.id,
            apt_id,
            "apartment_card",
        )
        await maybe_qualify_referral(db, call.from_user.id)
        await call.message.edit_text(
            "Бронирование проходит на сайте. Нажмите кнопку ниже:",
            reply_markup=booking_kb(f"apt:{apt_id}:card:1", "apartment_card"),
        )
        await set_user_main_message(call.bot, call.from_user.id, call.message.chat.id, call.message.message_id)
        await call.answer("Переходим к бронированию")

    @dp.callback_query(F.data.startswith("booking:"))
    async def cb_booking_screen(call: CallbackQuery):
        db = get_db()
        source = call.data.split(":")[1]
        await db.execute(
            "INSERT INTO click_events(user_id, apartment_id, source) VALUES($1, NULL, $2)",
            call.from_user.id,
            f"booking_{source}",
        )
        await maybe_qualify_referral(db, call.from_user.id)
        text = "Бронирование не внутри бота. Мы откроем сайт с актуальными слотами и ценами."
        await call.message.edit_text(text, reply_markup=booking_kb("home", source))
        await call.answer()

    @dp.callback_query(F.data.startswith("booklog:"))
    async def cb_booklog(call: CallbackQuery):
        db = get_db()
        source = call.data.split(":")[1]
        await db.execute(
            "INSERT INTO click_events(user_id, apartment_id, source) VALUES($1, NULL, $2)",
            call.from_user.id,
            source,
        )
        await maybe_qualify_referral(db, call.from_user.id)
        await call.answer("Учтено ✅")

    @dp.callback_query(F.data.startswith("dates:"))
    async def cb_dates(call: CallbackQuery, state: FSMContext):
        apt_id = int(call.data.split(":")[1])
        await state.set_state(DateRequestState.waiting_text)
        await state.update_data(apartment_id=apt_id)
        text = (
            "Напишите, когда примерно хотите заехать (в свободном формате).\n\n"
            "Как написать:\n"
            "• Период проживания\n"
            "• Сколько взрослых/детей\n"
            "• Доп. пожелания (по желанию)\n\n"
            "Пример: <b>20.03–23.03, 2 взрослых + 1 ребёнок, нужна парковка</b>"
        )
        await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=nav(f"apt:{apt_id}:card:1")))
        await state.update_data(date_prompt_message_id=call.message.message_id)
        await set_user_main_message(call.bot, call.from_user.id, call.message.chat.id, call.message.message_id)
        await call.answer()

    @dp.message(DateRequestState.waiting_text)
    async def msg_dates(message: Message, state: FSMContext):
        db = get_db()
        try:
            await message.delete()
        except Exception:
            pass
        data = await state.get_data()
        apt_id = int(data.get("apartment_id"))
        await db.execute(
            "INSERT INTO date_requests(user_id, apartment_id, raw_text, status) VALUES($1,$2,$3,'new')",
            message.from_user.id,
            apt_id,
            (message.text or "").strip(),
        )
        req = await db.fetchrow(
            "SELECT id FROM date_requests WHERE user_id=$1 AND apartment_id=$2 ORDER BY id DESC LIMIT 1",
            message.from_user.id,
            apt_id,
        )
        apt = await db.fetchrow("SELECT title FROM apartments WHERE id=$1", apt_id)
        text = (
            f"Новая заявка #{req['id']}\n"
            f"Квартира: {apt['title'] if apt else apt_id}\n"
            f"Текст: {message.text}\n"
            f"Пользователь: {message.from_user.full_name} (@{message.from_user.username or '-'})\n"
            f"ID: {message.from_user.id}\n"
            f"Ссылка: tg://user?id={message.from_user.id}"
        )
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [btn("✅ Ответить", f"req:reply:{req['id']}", style=ButtonStyle.PRIMARY)],
                [btn("🎁 Выдать промокод", f"req:promo:{req['id']}", style=ButtonStyle.SUCCESS)],
                [btn("🟡 Пометить обработано", f"req:done:{req['id']}", style=ButtonStyle.SUCCESS)],
            ]
        )
        await notify_admins(message.bot, text, kb)
        await log_event(db, message.from_user.id, "submit_dates", '{"apartment_id": %s}' % apt_id)
        await maybe_qualify_referral(db, message.from_user.id)
        await state.clear()
        await send_user_main_message(
            message.bot,
            message.from_user.id,
            message.chat.id,
            "Спасибо! Передали заявку администратору.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("home")),
        )

    @dp.callback_query(F.data == "promo:menu")
    async def cb_promo_menu(call: CallbackQuery):
        await call.message.edit_text(
            "Раздел промокодов:\n"
            "• <b>🎁 Получить welcome</b> — один раз на пользователя (если есть в пуле).\n"
            "• <b>🔗 Реферальная ссылка</b> — чтобы приглашать друзей.\n"
            "• <b>📦 Мои промокоды</b> — все выданные вам коды.",
            reply_markup=promo_menu_kb(),
        )
        await call.answer()

    @dp.callback_query(F.data == "promo:welcome")
    async def cb_welcome(call: CallbackQuery):
        db = get_db()
        already = await db.fetchval(
            "SELECT COUNT(*) FROM promo_codes WHERE assigned_to=$1 AND kind='welcome'", call.from_user.id
        )
        if already:
            await call.answer("Вы уже получали welcome-код", show_alert=True)
            return
        row = await db.fetchrow(
            "SELECT id, code FROM promo_codes WHERE kind='welcome' AND is_assigned=FALSE ORDER BY id LIMIT 1"
        )
        if not row:
            await call.answer("Сейчас welcome-коды закончились", show_alert=True)
            return
        await db.execute(
            "UPDATE promo_codes SET is_assigned=TRUE, assigned_to=$1, assigned_at=NOW() WHERE id=$2",
            call.from_user.id,
            row["id"],
        )
        await call.message.answer(
            f"Ваш welcome промокод: <b>{row['code']}</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("promo:menu")),
        )
        await call.answer("Готово")

    @dp.callback_query(F.data == "promo:ref")
    async def cb_ref(call: CallbackQuery):
        db = get_db()
        me = await call.bot.get_me()
        ref_code = await db.fetchval("SELECT ref_code FROM users WHERE tg_user_id=$1", call.from_user.id)
        ref_link = f"https://t.me/{me.username}?start=ref_{ref_code}"
        await call.message.answer(
            f"Ваша реферальная ссылка:\n{ref_link}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("promo:menu")),
        )
        await call.answer()

    @dp.callback_query(F.data == "promo:mine")
    async def cb_my_codes(call: CallbackQuery):
        db = get_db()
        rows = await db.fetch(
            "SELECT code, kind, assigned_at FROM promo_codes WHERE assigned_to=$1 ORDER BY assigned_at DESC NULLS LAST",
            call.from_user.id,
        )
        if not rows:
            text = "У вас пока нет выданных промокодов."
        else:
            text = "Ваши промокоды:\n" + "\n".join([f"• {r['code']} ({r['kind']})" for r in rows])
        await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("promo:menu")))
        await call.answer()

    @dp.callback_query(F.data == "cabinet")
    async def cb_cabinet(call: CallbackQuery):
        db = get_db()
        me = await call.bot.get_me()
        ref_code = await db.fetchval("SELECT ref_code FROM users WHERE tg_user_id=$1", call.from_user.id)
        invited = await db.fetchval("SELECT COUNT(*) FROM referrals WHERE inviter_user_id=$1", call.from_user.id)
        qualified = await db.fetchval(
            "SELECT COUNT(*) FROM referrals WHERE inviter_user_id=$1 AND qualified=TRUE", call.from_user.id
        )
        my_codes = await db.fetchval("SELECT COUNT(*) FROM promo_codes WHERE assigned_to=$1", call.from_user.id)
        text = (
            "Личный кабинет:\n"
            f"• Ваша ссылка: https://t.me/{me.username}?start=ref_{ref_code}\n"
            f"• Приглашено: {invited}\n"
            f"• Квалифицировано: {qualified}\n"
            f"• Ваших промокодов: {my_codes}\n\n"
            "Разработчик: @andreuanderson"
        )
        await call.message.edit_text(text, reply_markup=cabinet_kb())
        await call.answer()

    @dp.callback_query(F.data == "giveaway:menu")
    async def cb_giveaway(call: CallbackQuery):
        db = get_db()
        joined = await db.fetchval("SELECT COUNT(*) FROM giveaway_entries WHERE user_id=$1", call.from_user.id)
        txt = "Розыгрыш: нажмите кнопку ниже, чтобы участвовать." if not joined else "Вы уже участвуете в розыгрыше ✅"
        await call.message.edit_text(txt, reply_markup=giveaway_kb(bool(joined)))
        await call.answer()

    @dp.callback_query(F.data == "giveaway:join")
    async def cb_giveaway_join(call: CallbackQuery):
        db = get_db()
        await db.execute(
            "INSERT INTO giveaway_entries(user_id, is_winner) VALUES($1, FALSE) ON CONFLICT (user_id) DO NOTHING",
            call.from_user.id,
        )
        await call.message.edit_text("✅ Вы участвуете", reply_markup=giveaway_kb(True))
        await call.answer("Участие подтверждено")

    @dp.callback_query(F.data == "giveaway:joined")
    async def cb_giveaway_joined(call: CallbackQuery):
        await call.answer("Вы уже в списке участников")

    @dp.callback_query(F.data == "rules")
    async def cb_rules(call: CallbackQuery):
        text = (
            "📘 <b>Правила бронирования</b>\n\n"
            "<blockquote>1) Выберите квартиру и посмотрите фото/видео в карточке.</blockquote>\n"
            "<blockquote>2) Нажмите «📅 Забронировать» — откроется сайт с актуальными датами.</blockquote>\n"
            "<blockquote>3) Если пока не определились, нажмите «🤔 Не определился» и напишите желаемые даты в свободном формате.</blockquote>\n\n"
            "<b>Важно:</b> бронирование оформляется на сайте, а бот помогает быстро подобрать вариант и передать заявку администратору."
        )
        await call.message.edit_text(text, reply_markup=rules_kb())
        await call.answer()

    @dp.callback_query(F.data == "rem:off")
    async def cb_rem_off(call: CallbackQuery):
        db = get_db()
        await db.execute("UPDATE users SET reminders_opt_out=TRUE WHERE tg_user_id=$1", call.from_user.id)
        await call.answer("Напоминания отключены")
        await call.message.edit_text("Напоминания отключены.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("home")))

    @dp.callback_query(F.data == "admin:menu")
    async def cb_admin_menu(call: CallbackQuery):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        await call.message.edit_text("Админ-панель:", reply_markup=admin_menu_kb())
        await call.answer()

    @dp.callback_query(F.data == "admin:catalog_button")
    async def cb_admin_catalog_button(call: CallbackQuery, state: FSMContext):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        await state.clear()
        await call.message.edit_text(
            "🟢 Закреплённый пост с одной кнопкой «Бронь»\n\n"
            "Сценарий:\n"
            "1) Нажмите кнопку ниже — бот отправит пост в канал и закрепит его.\n"
            "2) В этом закрепе будет большая зелёная кнопка «Бронь».\n"
            "3) Саму кнопку «Каталог» оставляйте в основном посте-каталоге.\n\n"
            "Итог: в закрепе — быстрый переход на бронь, в посте — кнопка каталога.",
            reply_markup=admin_catalog_button_menu_kb(),
        )
        await call.answer()

    @dp.callback_query(F.data == "admin:catalog_only")
    async def cb_admin_catalog_only(call: CallbackQuery, state: FSMContext):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        await state.clear()
        await call.message.edit_text(
            "📚 Отдельная кнопка «Каталог»\n\n"
            "Сценарий:\n"
            "1) Нажмите «Опубликовать кнопку Каталог».\n"
            "2) Если нужно, позже обновите ссылку через «Обновить ссылку Каталог».\n\n"
            "Это отдельный пост, не закреплённый. Закреп с кнопкой «Бронь» работает отдельно.",
            reply_markup=admin_catalog_only_menu_kb(),
        )
        await call.answer()

    @dp.callback_query(F.data == "admin:catalog_button:publish")
    async def cb_admin_catalog_button_publish(call: CallbackQuery):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        if not CHANNEL_ID:
            await call.answer("Не задан CHANNEL_ID", show_alert=True)
            return

        db = get_db()
        sent = await call.bot.send_message(
            chat_id=CHANNEL_ID,
            text="Быстрое бронирование квартиры по кнопке ниже 👇",
            reply_markup=channel_booking_button_kb(BOOKING_URL),
        )

        pin_ok = True
        try:
            await call.bot.pin_chat_message(chat_id=CHANNEL_ID, message_id=sent.message_id, disable_notification=True)
        except Exception:
            pin_ok = False

        payload = {
            "channel_id": CHANNEL_ID,
            "message_id": sent.message_id,
            "booking_url": BOOKING_URL,
        }
        await db.execute(
            "INSERT INTO events(user_id, event_type, payload) VALUES($1, $2, $3::jsonb)",
            call.from_user.id,
            "channel_catalog_button",
            json.dumps(payload, ensure_ascii=False),
        )
        await call.message.answer(
            (
                "Закреп с кнопкой «Бронь» опубликован ✅\n"
                + ("И закреплён вверху канала ✅\n" if pin_ok else "Не удалось закрепить автоматически (проверьте права бота на закрепление).\n")
                + "Каталог-кнопку оставляйте в основном посте-каталоге."
            ),
            reply_markup=admin_catalog_button_menu_kb(),
        )
        await call.answer()

    @dp.callback_query(F.data == "admin:catalog_only:publish")
    async def cb_admin_catalog_only_publish(call: CallbackQuery):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        if not CHANNEL_ID:
            await call.answer("Не задан CHANNEL_ID", show_alert=True)
            return

        db = get_db()
        initial_url = CHANNEL_CATALOG_URL or "https://t.me"
        sent = await call.bot.send_message(
            chat_id=CHANNEL_ID,
            text="Откройте каталог квартир по кнопке ниже 👇",
            reply_markup=channel_catalog_button_kb(initial_url),
        )

        payload = {
            "channel_id": CHANNEL_ID,
            "message_id": sent.message_id,
            "catalog_url": initial_url,
        }
        await db.execute(
            "INSERT INTO events(user_id, event_type, payload) VALUES($1, $2, $3::jsonb)",
            call.from_user.id,
            "channel_catalog_only_button",
            json.dumps(payload, ensure_ascii=False),
        )

        await call.message.answer(
            "Пост с кнопкой «Каталог» опубликован ✅\n"
            "Если ссылка изменится — нажмите «Обновить ссылку Каталог».",
            reply_markup=admin_catalog_only_menu_kb(),
        )
        await call.answer()

    @dp.callback_query(F.data == "admin:catalog_only:set_link")
    async def cb_admin_catalog_only_set_link(call: CallbackQuery, state: FSMContext):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        await state.set_state(AdminCatalogOnlyState.waiting_url)
        await call.message.answer(
            "Отправьте ссылку на пост-каталог.\n"
            "Пример: https://t.me/your_channel/123",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:catalog_only")),
        )
        await call.answer()

    @dp.message(AdminCatalogOnlyState.waiting_url)
    async def msg_admin_catalog_only_set_link(message: Message, state: FSMContext):
        if message.from_user.id not in ADMIN_IDS:
            return
        url = (message.text or "").strip()
        if not (url.startswith("http://") or url.startswith("https://")):
            await message.answer("Нужна ссылка, начинающаяся с http:// или https://")
            return

        db = get_db()
        row = await db.fetchrow(
            "SELECT payload FROM events WHERE event_type='channel_catalog_only_button' ORDER BY id DESC LIMIT 1"
        )
        if not row:
            await message.answer(
                "Сначала нажмите «Опубликовать кнопку Каталог».",
                reply_markup=admin_catalog_only_menu_kb(),
            )
            await state.clear()
            return

        payload = row["payload"]
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}

        channel_id = int(payload.get("channel_id") or CHANNEL_ID or 0)
        message_id = int(payload.get("message_id") or 0)
        if not channel_id or not message_id:
            await message.answer(
                "Не нашёл сохранённый пост-кнопку. Опубликуйте его заново.",
                reply_markup=admin_catalog_only_menu_kb(),
            )
            await state.clear()
            return

        try:
            await message.bot.edit_message_reply_markup(
                chat_id=channel_id,
                message_id=message_id,
                reply_markup=channel_catalog_button_kb(url),
            )
        except Exception as e:
            await message.answer(
                f"Не удалось обновить кнопку: {e}",
                reply_markup=admin_catalog_only_menu_kb(),
            )
            await state.clear()
            return

        new_payload = {
            "channel_id": channel_id,
            "message_id": message_id,
            "catalog_url": url,
        }
        await db.execute(
            "INSERT INTO events(user_id, event_type, payload) VALUES($1, $2, $3::jsonb)",
            message.from_user.id,
            "channel_catalog_only_button",
            json.dumps(new_payload, ensure_ascii=False),
        )
        await state.clear()
        await message.answer(
            "Ссылка на кнопку «Каталог» обновлена ✅",
            reply_markup=admin_catalog_only_menu_kb(),
        )

    @dp.callback_query(F.data == "admin:entry_dates")
    async def cb_admin_entry_dates(call: CallbackQuery):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        db = get_db()
        rows = await db.fetch(
            "SELECT id, user_id, raw_text, status, created_at FROM entry_date_requests ORDER BY id DESC LIMIT 20"
        )
        if not rows:
            text = "Пока нет дат, оставленных на входе."
            kb = InlineKeyboardMarkup(inline_keyboard=nav("admin:menu"))
        else:
            text = "🗓 Последние даты на старте:\n\nВыберите заявку для ответа:"
            rows_kb: list[list[InlineKeyboardButton]] = []
            for r in rows:
                icon = "🆕" if (r.get("status") or "new") == "new" else "✅"
                preview = str(r["raw_text"]).replace("\n", " ").strip()
                if len(preview) > 36:
                    preview = preview[:36] + "…"
                rows_kb.append([btn(f"{icon} #{r['id']} user:{r['user_id']} — {preview}", f"entry:req:open:{r['id']}")])
            rows_kb += nav("admin:menu")
            kb = InlineKeyboardMarkup(inline_keyboard=rows_kb)
        await call.message.edit_text(text, reply_markup=kb)
        await call.answer()

    @dp.callback_query(F.data.startswith("entry:req:open:"))
    async def cb_entry_req_open(call: CallbackQuery):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        db = get_db()
        req_id = int(call.data.split(":")[-1])
        req = await db.fetchrow("SELECT * FROM entry_date_requests WHERE id=$1", req_id)
        if not req:
            await call.answer("Заявка не найдена", show_alert=True)
            return
        text = (
            f"Заявка старта #{req['id']}\n"
            f"Статус: {req.get('status') or 'new'}\n"
            f"Пользователь: {req['user_id']}\n"
            f"Текст: {req['raw_text']}"
        )
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [btn("✅ Ответить", f"entry:req:reply:{req_id}", style=ButtonStyle.PRIMARY)],
                [btn("🟡 Пометить обработано", f"entry:req:done:{req_id}", style=ButtonStyle.SUCCESS)],
            ]
            + nav("admin:entry_dates")
        )
        await call.message.edit_text(text, reply_markup=kb)
        await call.answer()

    @dp.callback_query(F.data.startswith("entry:req:reply:"))
    async def cb_entry_req_reply(call: CallbackQuery, state: FSMContext):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        req_id = int(call.data.split(":")[-1])
        await state.set_state(AdminReplyState.waiting_reply)
        await state.update_data(req_id=req_id, req_kind="entry_date", reply_back_cb="admin:entry_dates")
        await call.message.answer(
            "Введите ответ пользователю по заявке «дата на старте» одним сообщением.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:entry_dates")),
        )
        await call.answer()

    @dp.callback_query(F.data.startswith("entry:req:done:"))
    async def cb_entry_req_done(call: CallbackQuery):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        db = get_db()
        req_id = int(call.data.split(":")[-1])
        await db.execute(
            "UPDATE entry_date_requests SET status='handled', admin_id=$1, handled_at=NOW() WHERE id=$2",
            call.from_user.id,
            req_id,
        )
        await call.answer("Помечено обработанным")

    @dp.callback_query(F.data == "admin:channel_post")
    async def cb_admin_channel_post(call: CallbackQuery, state: FSMContext):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        await state.clear()
        await state.set_state(AdminChannelPostState.waiting_media)
        await state.update_data(channel_post_media=[])
        await call.message.answer(
            "📣 Конструктор поста в канал\n\n"
            "Шаг 1/2: отправьте фото/видео (можно несколько сообщений).\n"
            "Когда закончите — нажмите «✅ Готово, к тексту».",
            reply_markup=admin_channel_post_media_kb(),
        )
        await call.answer()

    @dp.message(AdminChannelPostState.waiting_media)
    async def msg_admin_channel_post_media(message: Message, state: FSMContext):
        if message.from_user.id not in ADMIN_IDS:
            return
        data = await state.get_data()
        items = data.get("channel_post_media") or []
        if not isinstance(items, list):
            items = []

        added = 0
        if message.photo:
            items.append({"type": "photo", "value": message.photo[-1].file_id})
            added += 1
        elif message.video:
            items.append({"type": "video", "value": message.video.file_id})
            added += 1
        elif message.document:
            mime = (message.document.mime_type or "").lower()
            if mime.startswith("image/"):
                items.append({"type": "photo", "value": message.document.file_id})
                added += 1
            elif mime.startswith("video/"):
                items.append({"type": "video", "value": message.document.file_id})
                added += 1

        if added == 0:
            await message.answer("Нужны фото/видео. Отправьте файл и продолжайте.", reply_markup=admin_channel_post_media_kb())
            return

        await state.update_data(channel_post_media=items)
        await message.answer(f"Добавлено медиа: {len(items)}", reply_markup=admin_channel_post_media_kb())

    @dp.callback_query(F.data == "admin:channel_post:done")
    async def cb_admin_channel_post_done(call: CallbackQuery, state: FSMContext):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        data = await state.get_data()
        items = data.get("channel_post_media") or []
        if not items:
            await call.answer("Добавьте хотя бы одно фото/видео", show_alert=True)
            return
        await state.set_state(AdminChannelPostState.waiting_text)
        await call.message.answer(
            "Шаг 2/2: отправьте текст поста одним сообщением.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[btn("❌ Отменить", "admin:channel_post:cancel", style=ButtonStyle.DANGER)]] + nav("admin:menu")),
        )
        await call.answer()

    @dp.message(AdminChannelPostState.waiting_text)
    async def msg_admin_channel_post_text(message: Message, state: FSMContext):
        if message.from_user.id not in ADMIN_IDS:
            return
        text = (message.text or "").strip()
        if not text:
            await message.answer("Текст пустой. Отправьте текст поста.")
            return
        await state.update_data(channel_post_text=text)
        await state.set_state(AdminChannelPostState.preview)
        await message.answer(
            f"Предпросмотр текста:\n\n{text}",
            reply_markup=admin_channel_post_preview_kb(),
        )

    @dp.callback_query(F.data == "admin:channel_post:publish")
    async def cb_admin_channel_post_publish(call: CallbackQuery, state: FSMContext):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        if not CHANNEL_ID:
            await call.answer("Не задан CHANNEL_ID", show_alert=True)
            return

        data = await state.get_data()
        media_items = data.get("channel_post_media") or []
        post_text = (data.get("channel_post_text") or "").strip()
        if not media_items or not post_text:
            await call.answer("Не хватает медиа или текста", show_alert=True)
            return

        me = await call.bot.get_me()
        manager_link = f"tg://user?id={ADMIN_CHAT_ID}" if ADMIN_CHAT_ID else (manager_url() or tg_contact_url() or f"https://t.me/{me.username}")
        whatsapp_link = whatsapp_url() or "https://wa.me/"
        markup = channel_custom_buttons_kb(me.username, manager_link, whatsapp_link)

        try:
            if media_items:
                first = media_items[0]
                kind = first.get("type", "photo")
                value = first.get("value", "")
                if kind == "video":
                    await call.bot.send_video(chat_id=CHANNEL_ID, video=value, caption=post_text, reply_markup=markup)
                else:
                    await call.bot.send_photo(chat_id=CHANNEL_ID, photo=value, caption=post_text, reply_markup=markup)
            else:
                await call.bot.send_message(chat_id=CHANNEL_ID, text=post_text, reply_markup=markup)
        except Exception as e:
            logger.warning("Ошибка публикации кастомного поста в канал: %s", e)
            await call.answer("Не удалось опубликовать", show_alert=True)
            return

        await state.clear()
        await call.message.answer("Пост опубликован в канал ✅", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:menu")))
        await call.answer()

    @dp.callback_query(F.data == "admin:channel_post:cancel")
    async def cb_admin_channel_post_cancel(call: CallbackQuery, state: FSMContext):
        await state.clear()
        await call.message.answer("Публикация отменена.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:menu")))
        await call.answer()

    @dp.callback_query(F.data == "admin:apartments")
    async def cb_admin_apartments(call: CallbackQuery):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        await call.message.edit_text(
            "Управление квартирами:\n"
            "• Добавьте новую квартиру\n"
            "• Откройте список и отредактируйте нужные поля\n"
            "• Фото/видео загружайте прямо в мастере, затем публикуйте в канал кнопкой «📤 В канал»",
            reply_markup=admin_apartments_menu_kb(),
        )
        await call.answer()

    @dp.callback_query(F.data == "admin:apt:add")
    async def cb_admin_apt_add(call: CallbackQuery, state: FSMContext):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        await state.clear()
        await state.update_data(wizard_index=0, wizard_mode="add")
        await call.message.answer(
            "🧩 <b>Мастер добавления квартиры</b>\n\n"
            "Короткий сценарий:\n"
            "1) загрузка фото/видео\n"
            "2) заголовок объявления (жирный + подчёркнутый)\n"
            "3) короткое описание\n"
            "4) описание в цитате\n"
            "5) что есть в квартире (цитата)\n"
            "6) максимум гостей\n"
            "7) теги для фильтров (парковка, видовая, тихо, для семьи)\n"
            "8) ссылка на карту\n\n"
            "В конце — красивый предпросмотр и сохранение.",
            reply_markup=wizard_text_kb(),
        )
        await wizard_show_step(call, state)
        await call.answer()

    @dp.callback_query(F.data == "aptw:cancel")
    async def cb_apartment_wizard_cancel(call: CallbackQuery, state: FSMContext):
        await state.clear()
        await call.message.answer("Мастер остановлен.", reply_markup=admin_apartments_menu_kb())
        await call.answer()

    @dp.callback_query(F.data.startswith("aptw:pick:"))
    async def cb_apartment_wizard_pick(call: CallbackQuery, state: FSMContext):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        current = await state.get_state()
        if current != AdminApartmentWizardState.waiting_choice.state:
            await call.answer("Этот выбор сейчас неактивен", show_alert=False)
            return
        value = call.data.split(":", 2)[2]
        data = await state.get_data()
        idx = int(data.get("wizard_index", 0))
        step = APARTMENT_WIZARD_STEPS[idx]
        key = step["key"]

        if value == "custom":
            await state.update_data(custom_step_key=key)
            await state.set_state(AdminApartmentWizardState.waiting_text)
            await wizard_send_prompt(
                call,
                state,
                f"Введите значение для поля <b>{step['label']}</b> вручную.",
                wizard_text_kb(),
            )
            await call.answer()
            return

        await state.update_data(**{key: value})
        await wizard_advance(call, state)
        await call.answer("Сохранено")

    @dp.callback_query(F.data == "aptw:media:done")
    async def cb_apartment_wizard_media_done(call: CallbackQuery, state: FSMContext):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        current = await state.get_state()
        if current != AdminApartmentWizardState.waiting_media.state:
            await call.answer("Сейчас не этап медиа", show_alert=False)
            return
        await wizard_advance(call, state)
        await call.answer("Медиа сохранены")

    @dp.message(AdminApartmentWizardState.waiting_media)
    async def msg_apartment_wizard_media(message: Message, state: FSMContext):
        if message.from_user.id not in ADMIN_IDS:
            return
        data = await state.get_data()
        idx = int(data.get("wizard_index", 0))
        if idx >= len(APARTMENT_WIZARD_STEPS):
            return
        step = APARTMENT_WIZARD_STEPS[idx]
        if step["kind"] != "media_upload":
            await message.answer("Сейчас не этап загрузки медиа.")
            return

        media_items = data.get("media_items") or []
        if not isinstance(media_items, list):
            media_items = []

        added = 0
        if message.photo:
            media_items.append({"type": "photo", "value": message.photo[-1].file_id})
            added += 1
        elif message.video:
            media_items.append({"type": "video", "value": message.video.file_id})
            added += 1
        elif message.document:
            mime = (message.document.mime_type or "").lower()
            if mime.startswith("image/"):
                media_items.append({"type": "photo", "value": message.document.file_id})
            elif mime.startswith("video/"):
                media_items.append({"type": "video", "value": message.document.file_id})
            else:
                media_items.append({"type": "document", "value": message.document.file_id})
            added += 1
        elif message.text:
            urls = [x.strip() for x in message.text.splitlines() if x.strip()]
            valid = [u for u in urls if u.startswith("http://") or u.startswith("https://")]
            for url in valid:
                media_items.append({"type": "url", "value": url})
            added += len(valid)

        if added == 0:
            await message.answer(
                "Пришлите фото/видео файлом или ссылки (по одной в строке).",
                reply_markup=wizard_media_kb(),
            )
            return

        await state.update_data(media_items=media_items)

        try:
            await message.delete()
        except Exception:
            pass

        await wizard_show_step(message, state)

    @dp.message(AdminApartmentWizardState.waiting_text)
    async def msg_apartment_wizard_text(message: Message, state: FSMContext):
        if message.from_user.id not in ADMIN_IDS:
            return
        raw = (message.text or "").strip()
        if not raw:
            await message.answer("Пустое значение. Введите текст или нажмите «Отменить мастер».")
            return

        data = await state.get_data()
        idx = int(data.get("wizard_index", 0))
        step = APARTMENT_WIZARD_STEPS[idx]
        key = step["key"]
        if data.get("custom_step_key"):
            key = data["custom_step_key"]

        kind = step["kind"]
        value: Any = raw

        if kind == "int":
            if not raw.isdigit():
                await message.answer("Нужно ввести целое число. Пример: 3500")
                return
            value = int(raw)
        elif kind == "url":
            if not (raw.startswith("http://") or raw.startswith("https://")):
                await message.answer("Нужна ссылка, начинающаяся с http:// или https://")
                return

        await state.update_data(**{key: value}, custom_step_key=None)
        try:
            await message.delete()
        except Exception:
            pass
        await wizard_advance(message, state)

    @dp.callback_query(F.data.startswith("aptw:preview:"))
    async def cb_apartment_wizard_preview(call: CallbackQuery, state: FSMContext):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        action = call.data.split(":")[-1]

        if action == "cancel":
            await state.clear()
            await call.message.answer("Добавление квартиры отменено.", reply_markup=admin_apartments_menu_kb())
            await call.answer()
            return

        if action == "restart":
            current = await state.get_data()
            mode = current.get("wizard_mode", "add")
            apt_id = current.get("wizard_apartment_id")

            await state.clear()
            if mode == "edit" and apt_id:
                db = get_db()
                apt = await db.fetchrow("SELECT * FROM apartments WHERE id=$1", int(apt_id))
                if apt:
                    defaults = apartment_wizard_defaults_from_apartment(apt)
                    await state.update_data(
                        wizard_index=0,
                        wizard_mode="edit",
                        wizard_apartment_id=int(apt_id),
                        **defaults,
                    )
                else:
                    await state.update_data(wizard_index=0, wizard_mode="add")
            else:
                await state.update_data(wizard_index=0, wizard_mode="add")
            await call.message.answer("Начинаем заново 👌", reply_markup=wizard_text_kb())
            await wizard_show_step(call, state)
            await call.answer()
            return

        data = await state.get_data()
        db = get_db()
        tags = [x.strip().lower() for x in str(data.get("tags", "")).split(",") if x.strip()]
        header_text = str(data.get("header_text", "")).strip()
        short_desc = str(data.get("short_desc", "")).strip()
        quote_desc = str(data.get("quote_desc", "")).strip()
        features_text = str(data.get("features_text", "")).strip()

        title = header_text.splitlines()[0].strip() if header_text else "Квартира"
        if len(title) > 80:
            title = title[:77] + "..."
        address_short = header_text.splitlines()[0].strip() if header_text else "Адрес не указан"
        if len(address_short) > 120:
            address_short = address_short[:117] + "..."

        guests_max_raw = data.get("guests_max")
        if isinstance(guests_max_raw, int):
            guests_max = max(1, guests_max_raw)
        else:
            guests_max_text = str(guests_max_raw or "").strip()
            guests_max = int(guests_max_text) if guests_max_text.isdigit() else 2
        amenities = short_desc[:180]
        sort_order = int(data.get("sort_order", 0))
        media_items_raw = data.get("media_items") or []
        media_items: list[dict[str, str]] = []
        if isinstance(media_items_raw, list):
            for item in media_items_raw:
                if isinstance(item, dict):
                    kind = str(item.get("type", "")).strip()
                    val = str(item.get("value", "")).strip()
                    if kind and val:
                        media_items.append({"type": kind, "value": val})
        elif isinstance(media_items_raw, str):
            for line in media_items_raw.splitlines():
                line = line.strip()
                if line:
                    media_items.append({"type": "url", "value": line})

        media_urls = [x["value"] for x in media_items if x.get("type") == "url"]

        details_payload = {
            "header_text": header_text,
            "short_desc": short_desc,
            "quote_desc": quote_desc,
            "features_text": features_text,
            "media_items": media_items,
        }

        wizard_mode = data.get("wizard_mode", "add")
        wizard_apt_id = data.get("wizard_apartment_id")

        if wizard_mode == "edit" and wizard_apt_id:
            await db.execute(
                """
                UPDATE apartments
                SET title=$1,
                    address_short=$2,
                    guests_max=$3,
                    amenities=$4,
                    tags=$5,
                    price_from=0,
                    channel_post_url=$6,
                    map_url=$7,
                    sort_order=$8,
                    details_json=$9::jsonb,
                    media_urls=$10
                WHERE id=$11
                """,
                title,
                address_short,
                guests_max,
                amenities,
                tags,
                "",
                str(data.get("map_url", "")),
                sort_order,
                json.dumps(details_payload, ensure_ascii=False),
                media_urls,
                int(wizard_apt_id),
            )
            success_text = "Квартира обновлена ✅"
        else:
            await db.execute(
                """
                INSERT INTO apartments(
                    title, address_short, guests_max, amenities, tags,
                    price_from, channel_post_url, map_url, sort_order,
                    details_json, media_urls
                )
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11)
                """,
                title,
                address_short,
                guests_max,
                amenities,
                tags,
                0,
                "",
                str(data.get("map_url", "")),
                sort_order,
                json.dumps(details_payload, ensure_ascii=False),
                media_urls,
            )
            success_text = "Квартира добавлена ✅"

        await state.clear()
        await call.message.answer(
            f"{success_text}\n"
            "Текст предпросмотра можно использовать как готовое описание для поста в канале.",
            reply_markup=admin_apartments_menu_kb(),
        )
        await call.answer("Сохранено")

    @dp.callback_query(F.data == "admin:apt:list")
    async def cb_admin_apt_list(call: CallbackQuery):
        db = get_db()
        rows = await db.fetch("SELECT id, title, is_active FROM apartments ORDER BY sort_order, id")
        keyboard = []
        for r in rows[:30]:
            status = "🟢" if r["is_active"] else "⚫️"
            keyboard.append([btn(f"{status} {r['title']}", f"admin:apt:open:{r['id']}")])
        keyboard += nav("admin:apartments")
        await call.message.edit_text("Список квартир:", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
        await call.answer()

    @dp.callback_query(F.data.startswith("admin:apt:open:"))
    async def cb_admin_apt_open(call: CallbackQuery):
        apt_id = int(call.data.split(":")[-1])
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [btn("🧩 Мастер редактирования", f"admin:apt:wizard:{apt_id}", style=ButtonStyle.PRIMARY)],
                [btn("📣 Предпросмотр поста", f"admin:apt:postpreview:{apt_id}", style=ButtonStyle.PRIMARY), btn("📤 В канал", f"admin:apt:publish:{apt_id}", style=ButtonStyle.SUCCESS)],
                [btn("🔁 Вкл/выкл", f"admin:apt:toggle:{apt_id}", style=ButtonStyle.PRIMARY)],
                [btn("🔗 Пост URL", f"admin:apt:edit:{apt_id}:channel_post_url", style=ButtonStyle.PRIMARY), btn("🗺 Карта URL", f"admin:apt:edit:{apt_id}:map_url", style=ButtonStyle.PRIMARY)],
                [btn("🏷 Теги", f"admin:apt:edit:{apt_id}:tags", style=ButtonStyle.PRIMARY), btn("↕️ Порядок", f"admin:apt:edit:{apt_id}:sort_order", style=ButtonStyle.PRIMARY)],
            ]
            + nav("admin:apt:list")
        )
        await call.message.edit_text(f"Редактирование квартиры #{apt_id}", reply_markup=kb)
        await call.answer()

    @dp.callback_query(F.data.startswith("admin:apt:postpreview:"))
    async def cb_admin_apt_post_preview(call: CallbackQuery):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        apt_id = int(call.data.split(":")[-1])
        db = get_db()
        apt = await db.fetchrow("SELECT * FROM apartments WHERE id=$1", apt_id)
        if not apt:
            await call.answer("Квартира не найдена", show_alert=True)
            return
        post_text = apartment_post_text_from_record(apt)
        await call.message.answer(
            "📣 Предпросмотр поста для канала:\n"
            "(кнопки внизу уже как в публикации)",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav(f"admin:apt:open:{apt_id}")),
        )
        await send_apartment_post_with_media(call.bot, call.message.chat.id, apt)
        await call.answer("Предпросмотр отправлен")

    @dp.callback_query(F.data.startswith("admin:apt:publish:"))
    async def cb_admin_apt_publish(call: CallbackQuery):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        if not CHANNEL_ID:
            await call.answer("Не задан CHANNEL_ID", show_alert=True)
            return
        apt_id = int(call.data.split(":")[-1])
        db = get_db()
        apt = await db.fetchrow("SELECT * FROM apartments WHERE id=$1", apt_id)
        if not apt:
            await call.answer("Квартира не найдена", show_alert=True)
            return
        try:
            await send_apartment_post_with_media(call.bot, CHANNEL_ID, apt)
        except Exception as e:
            await call.answer("Не удалось отправить в канал", show_alert=True)
            logger.warning("Ошибка публикации поста квартиры %s: %s", apt_id, e)
            return
        await call.answer("Опубликовано в канал ✅", show_alert=True)

    @dp.callback_query(F.data.startswith("admin:apt:wizard:"))
    async def cb_admin_apt_wizard(call: CallbackQuery, state: FSMContext):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        apt_id = int(call.data.split(":")[-1])
        db = get_db()
        apt = await db.fetchrow("SELECT * FROM apartments WHERE id=$1", apt_id)
        if not apt:
            await call.answer("Квартира не найдена", show_alert=True)
            return

        defaults = apartment_wizard_defaults_from_apartment(apt)
        await state.clear()
        await state.update_data(
            wizard_index=0,
            wizard_mode="edit",
            wizard_apartment_id=apt_id,
            **defaults,
        )
        await call.message.answer(
            "🧩 <b>Мастер редактирования квартиры</b>\n\n"
            "Откройте шаги и при необходимости измените значения.\n"
            "В конце будет предпросмотр и сохранение в существующую карточку.",
            reply_markup=wizard_text_kb(),
        )
        await wizard_show_step(call, state)
        await call.answer()

    @dp.callback_query(F.data.startswith("admin:apt:toggle:"))
    async def cb_admin_apt_toggle(call: CallbackQuery):
        db = get_db()
        apt_id = int(call.data.split(":")[-1])
        await db.execute("UPDATE apartments SET is_active=NOT is_active WHERE id=$1", apt_id)
        await call.answer("Статус изменён")

    @dp.callback_query(F.data.startswith("admin:apt:edit:"))
    async def cb_admin_apt_edit(call: CallbackQuery, state: FSMContext):
        _, _, _, apt_id, field = call.data.split(":")
        await state.set_state(AdminApartmentState.waiting_field_value)
        await state.update_data(apt_id=int(apt_id), field=field)
        examples = {
            "sort_order": "Пример: 20",
            "channel_post_url": "Пример: https://t.me/your_channel/321",
            "map_url": "Пример: https://maps.google.com/?q=55.75,37.61",
            "tags": "Пример: парковка,видовая,для семьи",
        }
        await call.message.answer(
            f"Редактирование поля <b>{field}</b>.\n"
            f"Отправьте новое значение одним сообщением.\n"
            f"{examples.get(field, '')}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav(f"admin:apt:open:{apt_id}")),
        )
        await call.answer()

    @dp.message(AdminApartmentState.waiting_field_value)
    async def msg_admin_apt_field(message: Message, state: FSMContext):
        if message.from_user.id not in ADMIN_IDS:
            return
        db = get_db()
        data = await state.get_data()
        apt_id = data["apt_id"]
        field = data["field"]
        value = (message.text or "").strip()
        if field in {"sort_order"}:
            await db.execute(f"UPDATE apartments SET {field}=$1 WHERE id=$2", int(value), apt_id)
        elif field == "tags":
            tags = [x.strip().lower() for x in value.split(",") if x.strip()]
            await db.execute("UPDATE apartments SET tags=$1 WHERE id=$2", tags, apt_id)
        elif field in {"channel_post_url", "map_url"}:
            await db.execute(f"UPDATE apartments SET {field}=$1 WHERE id=$2", value, apt_id)
        else:
            await message.answer("Поле не поддерживается")
            return
        await state.clear()
        await message.answer("Обновлено ✅", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav(f"admin:apt:open:{apt_id}")))

    @dp.callback_query(F.data == "admin:codes")
    async def cb_admin_codes(call: CallbackQuery):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        await call.message.edit_text(
            "Управление промокодами:\n"
            "• <b>➕ Добавить код</b> — добавить один код с типом\n"
            "• <b>📥 Bulk загрузка</b> — вставить много кодов списком\n"
            "• <b>🧾 Список по типу</b> — посмотреть остатки",
            reply_markup=admin_codes_kb(),
        )
        await call.answer()

    @dp.callback_query(F.data == "admin:code:add")
    async def cb_admin_code_add(call: CallbackQuery, state: FSMContext):
        await state.set_state(AdminPromoState.waiting_custom_code)
        await state.update_data(mode="single_add")
        await call.message.answer(
            "Добавление одного промокода:\n\n"
            "Отправьте 1 строку в формате <b>КОД|ТИП</b>.\n"
            "Доступные типы: <b>welcome</b>, <b>giveaway</b>, <b>manual</b>, <b>referral_reward</b>.\n\n"
            "Примеры:\n"
            "• WELCOME100|welcome\n"
            "• GIFT-APRIL-10|manual\n"
            "• LUCKY777|giveaway",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:codes")),
        )
        await call.answer()

    @dp.callback_query(F.data == "admin:code:bulk")
    async def cb_admin_code_bulk(call: CallbackQuery, state: FSMContext):
        await state.set_state(AdminCodesBulkState.waiting_codes)
        await call.message.answer(
            "Массовая загрузка промокодов:\n\n"
            "Шаг 1. В первой строке укажите тип.\n"
            "Шаг 2. Ниже вставьте коды — каждый с новой строки.\n\n"
            "Пример сообщения:\n"
            "manual\n"
            "APRIL-100\n"
            "APRIL-200\n"
            "APRIL-300",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:codes")),
        )
        await call.answer()

    @dp.message(AdminCodesBulkState.waiting_codes)
    async def msg_bulk_codes(message: Message, state: FSMContext):
        if message.from_user.id not in ADMIN_IDS:
            return
        db = get_db()
        lines = [x.strip() for x in (message.text or "").splitlines() if x.strip()]
        if len(lines) < 2:
            await message.answer(
                "Нужно минимум 2 строки: тип + хотя бы один код.\n"
                "Пример:\nmanual\nCODE1"
            )
            return
        kind = lines[0]
        codes = lines[1:]
        inserted = 0
        for c in codes:
            try:
                await db.execute("INSERT INTO promo_codes(code, kind) VALUES($1,$2)", c, kind)
                inserted += 1
            except Exception:
                continue
        await state.clear()
        await message.answer(f"Загружено кодов: {inserted}", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:codes")))

    @dp.message(AdminPromoState.waiting_custom_code)
    async def msg_admin_code_add(message: Message, state: FSMContext):
        if message.from_user.id not in ADMIN_IDS:
            return
        db = get_db()
        data = await state.get_data()
        mode = data.get("mode")
        if mode == "single_add":
            parts = [x.strip() for x in (message.text or "").split("|")]
            if len(parts) != 2:
                await message.answer(
                    "Неверный формат.\n"
                    "Используйте: <b>КОД|ТИП</b>\n"
                    "Пример: <b>SAVE10|manual</b>"
                )
                return
            code, kind = parts
            try:
                await db.execute("INSERT INTO promo_codes(code, kind) VALUES($1,$2)", code, kind)
            except Exception as e:
                await message.answer(f"Ошибка: {e}")
                return
            await state.clear()
            await message.answer("Код добавлен ✅", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:codes")))
            return

        req_id = data.get("req_id")
        if not req_id:
            await message.answer("Нет активной заявки")
            return
        code_text = (message.text or "").strip()
        req = await db.fetchrow("SELECT user_id FROM date_requests WHERE id=$1", req_id)
        if not req:
            await message.answer("Заявка не найдена")
            await state.clear()
            return
        user_id = req["user_id"]
        code_row = await db.fetchrow(
            "SELECT id, code FROM promo_codes WHERE code=$1 AND kind='manual' AND is_assigned=FALSE",
            code_text,
        )
        if code_row:
            code = code_row["code"]
            await db.execute(
                "UPDATE promo_codes SET is_assigned=TRUE, assigned_to=$1, assigned_at=NOW() WHERE id=$2",
                user_id,
                code_row["id"],
            )
        else:
            code = code_text
            try:
                await db.execute(
                    "INSERT INTO promo_codes(code, kind, is_assigned, assigned_to, assigned_at) VALUES($1,'manual',TRUE,$2,NOW())",
                    code,
                    user_id,
                )
            except Exception:
                pass
        try:
            await message.bot.send_message(user_id, f"Вам выдан промокод: <b>{code}</b>")
        except Exception:
            await handle_blocked(db, user_id)
        await db.execute(
            "UPDATE date_requests SET status='handled', admin_id=$1, handled_at=NOW() WHERE id=$2",
            message.from_user.id,
            req_id,
        )
        await state.clear()
        await message.answer("Промокод отправлен пользователю ✅", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:requests")))

    @dp.callback_query(F.data == "admin:code:list")
    async def cb_admin_code_list(call: CallbackQuery):
        db = get_db()
        rows = await db.fetch(
            "SELECT kind, COUNT(*) cnt, SUM((NOT is_assigned)::int) free_cnt FROM promo_codes GROUP BY kind ORDER BY kind"
        )
        if not rows:
            text = "Промокодов пока нет."
        else:
            text = "Пулы промокодов:\n" + "\n".join([f"• {r['kind']}: всего {r['cnt']}, свободно {r['free_cnt'] or 0}" for r in rows])
        await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:codes")))
        await call.answer()

    @dp.callback_query(F.data == "admin:giveaway")
    async def cb_admin_giveaway(call: CallbackQuery):
        db = get_db()
        cnt = await db.fetchval("SELECT COUNT(*) FROM giveaway_entries")
        kb = InlineKeyboardMarkup(inline_keyboard=[[btn("🎲 Разыграть", "admin:giveaway:draw")]] + nav("admin:menu"))
        await call.message.edit_text(f"Участников в розыгрыше: {cnt}", reply_markup=kb)
        await call.answer()

    @dp.callback_query(F.data == "admin:giveaway:draw")
    async def cb_draw(call: CallbackQuery):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        db = get_db()
        code = await db.fetchrow(
            "SELECT id, code FROM promo_codes WHERE kind='giveaway' AND is_assigned=FALSE ORDER BY id LIMIT 1"
        )
        if not code:
            await call.answer("Нет свободных giveaway-кодов", show_alert=True)
            return
        participants = await db.fetch("SELECT user_id FROM giveaway_entries WHERE is_winner=FALSE")
        if not participants:
            await call.answer("Нет участников", show_alert=True)
            return
        winner_id = secrets.choice(participants)["user_id"]
        await db.execute(
            "UPDATE promo_codes SET is_assigned=TRUE, assigned_to=$1, assigned_at=NOW() WHERE id=$2",
            winner_id,
            code["id"],
        )
        await db.execute(
            "UPDATE giveaway_entries SET is_winner=TRUE, winner_code=$1, updated_at=NOW() WHERE user_id=$2",
            code["code"],
            winner_id,
        )
        u = await db.fetchrow("SELECT full_name, username FROM users WHERE tg_user_id=$1", winner_id)
        try:
            await call.bot.send_message(
                winner_id,
                f"🏆 Поздравляем! Вы победили в розыгрыше. Ваш промокод: <b>{code['code']}</b>",
            )
        except Exception:
            await handle_blocked(db, winner_id)
        if CHANNEL_ID:
            winner_name = u["full_name"] if u and u["full_name"] else (f"@{u['username']}" if u and u["username"] else str(winner_id))
            try:
                await call.bot.send_message(CHANNEL_ID, f"🏆 Победитель розыгрыша: {winner_name}! Поздравляем! 🎉")
            except Exception as e:
                logger.warning("Не удалось отправить в канал: %s", e)
        await call.answer("Победитель выбран ✅", show_alert=True)

    @dp.callback_query(F.data == "admin:stats")
    async def cb_admin_stats(call: CallbackQuery):
        db = get_db()
        def _window(days: int) -> str:
            return f"NOW() - INTERVAL '{days} days'"

        new7 = await db.fetchval(f"SELECT COUNT(*) FROM users WHERE created_at >= {_window(7)}")
        active7 = await db.fetchval(f"SELECT COUNT(*) FROM users WHERE last_seen_at >= {_window(7)}")
        click7 = await db.fetchval(f"SELECT COUNT(*) FROM click_events WHERE created_at >= {_window(7)}")
        req7 = await db.fetchval(f"SELECT COUNT(*) FROM date_requests WHERE created_at >= {_window(7)}")

        new30 = await db.fetchval(f"SELECT COUNT(*) FROM users WHERE created_at >= {_window(30)}")
        active30 = await db.fetchval(f"SELECT COUNT(*) FROM users WHERE last_seen_at >= {_window(30)}")
        click30 = await db.fetchval(f"SELECT COUNT(*) FROM click_events WHERE created_at >= {_window(30)}")
        req30 = await db.fetchval(f"SELECT COUNT(*) FROM date_requests WHERE created_at >= {_window(30)}")

        top_apts = await db.fetch(
            """
            SELECT apartment_id, COUNT(*) cnt
            FROM click_events
            WHERE apartment_id IS NOT NULL AND created_at >= NOW() - INTERVAL '30 days'
            GROUP BY apartment_id
            ORDER BY cnt DESC
            LIMIT 5
            """
        )
        tops = ", ".join([f"#{r['apartment_id']} ({r['cnt']})" for r in top_apts]) if top_apts else "нет"

        text = (
            "Статистика 7 дней:\n"
            f"• Новые: {new7}\n• Активные: {active7}\n• Клики бронирования: {click7}\n• Заявки: {req7}\n\n"
            "Статистика 30 дней:\n"
            f"• Новые: {new30}\n• Активные: {active30}\n• Клики бронирования: {click30}\n• Заявки: {req30}\n"
            f"• Топ квартир (30д): {tops}"
        )
        await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:menu")))
        await call.answer()

    @dp.callback_query(F.data == "admin:broadcast")
    async def cb_admin_broadcast(call: CallbackQuery):
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [btn("👥 Всем", "admin:bc:all"), btn("🕒 Неактивные 7+", "admin:bc:inactive")],
                [btn("👀 Смотрели, но не бронировали", "admin:bc:view_no_book")],
            ]
            + nav("admin:menu")
        )
        await call.message.edit_text("Выберите сегмент рассылки:", reply_markup=kb)
        await call.answer()

    @dp.callback_query(F.data.startswith("admin:bc:"))
    async def cb_admin_broadcast_segment(call: CallbackQuery, state: FSMContext):
        segment = call.data.split(":")[2]
        await state.set_state(AdminBroadcastState.waiting_text)
        await state.update_data(segment=segment)
        await call.message.answer(
            "Введите текст рассылки одним сообщением.\n"
            "Можно использовать переносы строк и эмодзи.\n\n"
            "Пример:\n"
            "🏠 Новые квартиры уже в каталоге!\n"
            "Откройте раздел «Квартиры» и выберите подходящий вариант.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:broadcast")),
        )
        await call.answer()

    @dp.message(AdminBroadcastState.waiting_text)
    async def msg_broadcast_text(message: Message, state: FSMContext):
        if message.from_user.id not in ADMIN_IDS:
            return
        text = (message.text or "").strip()
        await state.update_data(broadcast_text=text)
        await state.set_state(AdminBroadcastState.waiting_confirm)
        kb = InlineKeyboardMarkup(inline_keyboard=[[btn("✅ Подтвердить", "admin:bc:confirm")]] + nav("admin:broadcast"))
        await message.answer(f"Проверьте текст рассылки:\n\n{text}\n\nОтправить выбранному сегменту?", reply_markup=kb)

    @dp.callback_query(F.data == "admin:bc:confirm")
    async def cb_broadcast_confirm(call: CallbackQuery, state: FSMContext):
        if call.from_user.id not in ADMIN_IDS:
            await call.answer("Нет доступа", show_alert=True)
            return
        db = get_db()
        data = await state.get_data()
        segment = data.get("segment")
        text = data.get("broadcast_text")
        if not segment or not text:
            await call.answer("Нет данных для рассылки", show_alert=True)
            return

        if segment == "all":
            rows = await db.fetch("SELECT tg_user_id FROM users WHERE is_blocked=FALSE")
        elif segment == "inactive":
            rows = await db.fetch(
                "SELECT tg_user_id FROM users WHERE is_blocked=FALSE AND last_seen_at < NOW() - INTERVAL '7 days'"
            )
        else:
            rows = await db.fetch(
                """
                SELECT DISTINCT u.tg_user_id
                FROM users u
                JOIN events e ON e.user_id=u.tg_user_id AND e.event_type='view_apartment'
                LEFT JOIN click_events c ON c.user_id=u.tg_user_id
                WHERE u.is_blocked=FALSE AND c.id IS NULL
                """
            )

        sent = 0
        for r in rows:
            uid = r["tg_user_id"]
            try:
                await call.bot.send_message(uid, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("home")))
                sent += 1
            except Exception:
                await handle_blocked(db, uid)
        await state.clear()
        await call.message.answer(f"Рассылка завершена. Отправлено: {sent}", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:menu")))
        await call.answer()

    @dp.callback_query(F.data == "admin:requests")
    async def cb_admin_requests(call: CallbackQuery):
        db = get_db()
        rows = await db.fetch(
            "SELECT id, status, user_id, apartment_id, created_at FROM date_requests ORDER BY id DESC LIMIT 20"
        )
        keyboard = []
        for r in rows:
            icon = "🆕" if r["status"] == "new" else "✅"
            keyboard.append([btn(f"{icon} #{r['id']} user:{r['user_id']} apt:{r['apartment_id']}", f"admin:req:open:{r['id']}")])
        keyboard += nav("admin:menu")
        await call.message.edit_text("Заявки (последние 20):", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
        await call.answer()

    @dp.callback_query(F.data.startswith("admin:req:open:"))
    async def cb_admin_req_open(call: CallbackQuery):
        db = get_db()
        req_id = int(call.data.split(":")[-1])
        req = await db.fetchrow("SELECT * FROM date_requests WHERE id=$1", req_id)
        if not req:
            await call.answer("Не найдено", show_alert=True)
            return
        text = (
            f"Заявка #{req['id']}\n"
            f"Статус: {req['status']}\n"
            f"Квартира: {req['apartment_id']}\n"
            f"Пользователь: {req['user_id']}\n"
            f"Текст: {req['raw_text']}"
        )
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [btn("✅ Ответить", f"req:reply:{req_id}", style=ButtonStyle.PRIMARY)],
                [btn("🎁 Выдать промокод", f"req:promo:{req_id}", style=ButtonStyle.SUCCESS)],
                [btn("🟡 Пометить обработано", f"req:done:{req_id}", style=ButtonStyle.SUCCESS)],
            ]
            + nav("admin:requests")
        )
        await call.message.edit_text(text, reply_markup=kb)
        await call.answer()

    @dp.callback_query(F.data.startswith("req:reply:"))
    async def cb_req_reply(call: CallbackQuery, state: FSMContext):
        req_id = int(call.data.split(":")[-1])
        await state.set_state(AdminReplyState.waiting_reply)
        await state.update_data(req_id=req_id, req_kind="date_request", reply_back_cb="admin:requests")
        await call.message.answer(
            "Введите ответ пользователю одним сообщением.\n"
            "Пример:\n"
            "Добрый день! Свободно с 20.03 по 23.03.\n"
            "Для подтверждения нажмите «Забронировать» в карточке квартиры.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:requests")),
        )
        await call.answer()

    @dp.message(AdminReplyState.waiting_reply)
    async def msg_req_reply(message: Message, state: FSMContext):
        if message.from_user.id not in ADMIN_IDS:
            return
        db = get_db()
        data = await state.get_data()
        req_id = data.get("req_id")
        req_kind = data.get("req_kind") or "date_request"
        back_cb = data.get("reply_back_cb") or "admin:requests"

        if req_kind == "entry_date":
            req = await db.fetchrow("SELECT user_id FROM entry_date_requests WHERE id=$1", req_id)
        else:
            req = await db.fetchrow("SELECT user_id FROM date_requests WHERE id=$1", req_id)

        if not req:
            await message.answer("Заявка не найдена")
            await state.clear()
            return
        uid = req["user_id"]
        reply_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [btn("💬 Ответить администратору", f"user:reply_admin:{req_kind}:{req_id}:{message.from_user.id}", style=ButtonStyle.PRIMARY)],
                [btn("🏠 Главная", "home", style=ButtonStyle.PRIMARY)],
            ]
        )
        try:
            await message.bot.send_message(uid, f"Ответ администратора:\n{message.text}", reply_markup=reply_kb)
        except Exception:
            await handle_blocked(db, uid)
        if req_kind == "entry_date":
            await db.execute(
                "UPDATE entry_date_requests SET status='handled', admin_id=$1, handled_at=NOW() WHERE id=$2",
                message.from_user.id,
                req_id,
            )
        else:
            await db.execute(
                "UPDATE date_requests SET status='handled', admin_id=$1, handled_at=NOW() WHERE id=$2",
                message.from_user.id,
                req_id,
            )
        await state.clear()
        await message.answer("Ответ отправлен ✅", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav(back_cb)))

    @dp.callback_query(F.data.startswith("user:reply_admin:"))
    async def cb_user_reply_admin(call: CallbackQuery, state: FSMContext):
        parts = call.data.split(":")
        if len(parts) < 6:
            await call.answer("Некорректные данные", show_alert=True)
            return
        _, _, _, req_kind, req_id, admin_id = parts
        await state.set_state(UserReplyToAdminState.waiting_text)
        await state.update_data(
            reply_req_kind=req_kind,
            reply_req_id=int(req_id),
            reply_admin_id=int(admin_id),
        )
        await call.message.answer(
            "Напишите ответ администратору одним сообщением.\n"
            "Это сообщение не удаляется автоматически.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("home")),
        )
        await call.answer()

    @dp.message(UserReplyToAdminState.waiting_text)
    async def msg_user_reply_admin(message: Message, state: FSMContext):
        db = get_db()
        text = (message.text or "").strip()
        if not text:
            await message.answer("Не вижу текста. Напишите ответ одним сообщением.")
            return

        data = await state.get_data()
        req_kind = data.get("reply_req_kind") or "date_request"
        req_id = int(data.get("reply_req_id") or 0)
        admin_id = int(data.get("reply_admin_id") or 0)

        target_admins: list[int] = []
        if admin_id:
            target_admins.append(admin_id)
        if ADMIN_CHAT_ID and ADMIN_CHAT_ID not in target_admins:
            target_admins.append(ADMIN_CHAT_ID)
        for aid in ADMIN_IDS:
            if aid not in target_admins:
                target_admins.append(aid)

        context_line = ""
        if req_kind == "entry_date" and req_id:
            req = await db.fetchrow("SELECT raw_text FROM entry_date_requests WHERE id=$1", req_id)
            if req:
                context_line = f"\nИзначальная заявка (старт): {req['raw_text']}"
        elif req_id:
            req = await db.fetchrow("SELECT raw_text FROM date_requests WHERE id=$1", req_id)
            if req:
                context_line = f"\nИзначальная заявка: {req['raw_text']}"

        admin_text = (
            "💬 Ответ от пользователя\n"
            f"Пользователь: {message.from_user.full_name} (@{message.from_user.username or '-'})\n"
            f"ID: {message.from_user.id}\n"
            f"Тип заявки: {req_kind}\n"
            f"Заявка: #{req_id}\n"
            f"Текст ответа: {text}"
            f"{context_line}"
        )

        for aid in target_admins:
            if not aid:
                continue
            try:
                await message.bot.send_message(aid, admin_text)
            except Exception:
                pass

        await state.clear()
        await message.answer(
            "Спасибо, ваш ответ передан администратору ✅",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("home")),
        )

    @dp.callback_query(F.data.startswith("req:promo:"))
    async def cb_req_promo(call: CallbackQuery, state: FSMContext):
        db = get_db()
        req_id = int(call.data.split(":")[-1])
        rows = await db.fetch(
            "SELECT code FROM promo_codes WHERE kind='manual' AND is_assigned=FALSE ORDER BY id LIMIT 10"
        )
        kb_rows = [[btn(f"🎁 {r['code']}", f"req:promo_pick:{req_id}:{r['code']}")] for r in rows]
        kb_rows.append([btn("⌨️ Ввести вручную", f"req:promo_manual:{req_id}")])
        kb_rows += nav("admin:requests")
        await call.message.edit_text("Выберите код или введите вручную:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
        await call.answer()

    @dp.callback_query(F.data.startswith("req:promo_pick:"))
    async def cb_req_promo_pick(call: CallbackQuery):
        db = get_db()
        _, _, req_id, code = call.data.split(":", 3)
        req = await db.fetchrow("SELECT user_id FROM date_requests WHERE id=$1", int(req_id))
        if not req:
            await call.answer("Заявка не найдена", show_alert=True)
            return
        user_id = req["user_id"]
        row = await db.fetchrow("SELECT id FROM promo_codes WHERE code=$1 AND is_assigned=FALSE", code)
        if not row:
            await call.answer("Код уже занят", show_alert=True)
            return
        await db.execute("UPDATE promo_codes SET is_assigned=TRUE, assigned_to=$1, assigned_at=NOW() WHERE id=$2", user_id, row["id"])
        await db.execute(
            "UPDATE date_requests SET status='handled', admin_id=$1, handled_at=NOW() WHERE id=$2",
            call.from_user.id,
            int(req_id),
        )
        try:
            await call.bot.send_message(user_id, f"Вам выдан промокод: <b>{code}</b>")
        except Exception:
            await handle_blocked(db, user_id)
        await call.message.edit_text("Промокод выдан ✅", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:requests")))
        await call.answer()

    @dp.callback_query(F.data.startswith("req:promo_manual:"))
    async def cb_req_promo_manual(call: CallbackQuery, state: FSMContext):
        req_id = int(call.data.split(":")[-1])
        await state.set_state(AdminPromoState.waiting_custom_code)
        await state.update_data(req_id=req_id)
        await call.message.answer(
            "Введите промокод вручную одним сообщением.\n"
            "Пример: <b>SPRING-15</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("admin:requests")),
        )
        await call.answer()

    @dp.callback_query(F.data.startswith("req:done:"))
    async def cb_req_done(call: CallbackQuery):
        db = get_db()
        req_id = int(call.data.split(":")[-1])
        await db.execute(
            "UPDATE date_requests SET status='handled', admin_id=$1, handled_at=NOW() WHERE id=$2",
            call.from_user.id,
            req_id,
        )
        await call.answer("Помечено обработанным")

    @dp.callback_query(F.data == "noop")
    async def cb_noop(call: CallbackQuery):
        await call.answer()

    @dp.message(F.contact)
    async def msg_contact(message: Message):
        db = get_db()
        if message.contact and message.contact.user_id == message.from_user.id:
            await db.execute("UPDATE users SET phone=$1 WHERE tg_user_id=$2", message.contact.phone_number, message.from_user.id)
            await message.answer("Контакт сохранён ✅", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav("home")))

    @dp.message()
    async def fallback(message: Message):
        if message.text and message.text.startswith("/"):
            await message.answer("Доступные команды: /start /help /privacy /delete_me")
            return
        await message.answer("Используйте кнопки меню ниже.", reply_markup=menu_kb(message.from_user.id in ADMIN_IDS))

    reminder_task = asyncio.create_task(reminders_loop(bot))
    try:
        await dp.start_polling(bot)
    finally:
        reminder_task.cancel()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
