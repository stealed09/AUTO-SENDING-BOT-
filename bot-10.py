import logging
import html
import os
from datetime import datetime, date
from typing import Optional
from zoneinfo import ZoneInfo

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Bot,
    InputMediaPhoto,
    InputMediaVideo,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
    Application,
)
from telegram.error import TelegramError

from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Timezone (IST) ─────────────────────────────────────────────────────────
IST = ZoneInfo("Asia/Kolkata")

def now_ist() -> datetime:
    return datetime.now(IST)

# ─── Bot Token ──────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is not set!")

# ─── Admin System ───────────────────────────────────────────────────────────
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
approved_users: set[int] = set()
pending_users: dict[int, dict] = {}
all_users: dict[int, dict] = {}  # tracks everyone who ever used the bot

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

def is_approved(user_id: int) -> bool:
    return user_id == ADMIN_ID or user_id in approved_users

# ─── In-Memory Storage ─────────────────────────────────────────────────────
user_data_store: dict[int, dict] = {}
user_state: dict[int, Optional[str]] = {}

scheduler: AsyncIOScheduler = None
bot_app: Application = None


# ─── User Data Helper ──────────────────────────────────────────────────────
def get_user_data(user_id: int) -> dict:
    if user_id not in user_data_store:
        user_data_store[user_id] = {
            "channels": [],
            "message": "",
            "times": [],
            "sent_messages": [],
            "forward_message": None,
            "templates": {},        # name -> message text
            "broadcast_history": [], # list of {time, channels, type, results}
        }
    return user_data_store[user_id]


# ─── Admin Menu Keyboard ─────────────────────────────────────────────────────
def full_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    kb = main_menu_keyboard().inline_keyboard
    if is_admin(user_id):
        kb = kb + ([InlineKeyboardButton("🛡 Admin Panel", callback_data="admin_panel")],)
    return InlineKeyboardMarkup(kb)


# ─── Keyboards ──────────────────────────────────────────────────────────────
def main_menu_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("📡 Add Channels", callback_data="add_channels"),
            InlineKeyboardButton("📋 Show Channels", callback_data="show_channels"),
        ],
        [
            InlineKeyboardButton("✉️ Set Message", callback_data="set_message"),
            InlineKeyboardButton("⏰ Set Time", callback_data="set_time"),
        ],
        [
            InlineKeyboardButton("🚀 Instant Broadcast", callback_data="instant_broadcast"),
            InlineKeyboardButton("📨 Instant Forward", callback_data="instant_forward"),
        ],
        [
            InlineKeyboardButton("🗑 Delete All Sent", callback_data="delete_last"),
            InlineKeyboardButton("📜 Manage Sent", callback_data="manage_sent"),
        ],
        [
            InlineKeyboardButton("💾 Templates", callback_data="templates_menu"),
            InlineKeyboardButton("📊 Status", callback_data="show_status"),
        ],
        [
            InlineKeyboardButton("📈 Broadcast History", callback_data="broadcast_history"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")]]
    )


def admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 Pending Requests", callback_data="admin_pending")],
        [InlineKeyboardButton("✅ Approved Users", callback_data="admin_approved")],
        [InlineKeyboardButton("📢 Message All Users", callback_data="admin_broadcast")],
        [InlineKeyboardButton("📊 Bot Stats", callback_data="admin_stats")],
        [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")],
    ])


# ─── /start ─────────────────────────────────────────────────────────────────
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    user_id = user.id

    all_users[user_id] = {
        "name": user.full_name,
        "username": f"@{user.username}" if user.username else "No username",
    }

    if not is_approved(user_id):
        pending_users[user_id] = all_users[user_id]
        if ADMIN_ID:
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=(
                        f"🔔 <b>New Access Request</b>\n\n"
                        f"👤 Name: <b>{html.escape(user.full_name)}</b>\n"
                        f"🆔 ID: <code>{user_id}</code>\n"
                        f"📛 Username: {html.escape(all_users[user_id]['username'])}\n\n"
                        f"Use /admin to manage requests."
                    ),
                    parse_mode="HTML",
                )
            except Exception as e:
                logger.error(f"Failed to notify admin: {e}")
        await update.message.reply_text(
            f"👋 Hello <b>{html.escape(user.full_name)}</b>!\n\n"
            "⛔ You don't have access yet.\n\n"
            "Your request has been sent to the admin. Please wait for approval.",
            parse_mode="HTML",
        )
        return

    get_user_data(user_id)
    await update.message.reply_text(
        f"👋 Hello <b>{html.escape(user.full_name)}</b>!\n\n"
        "I am a <b>Broadcast Bot</b>.\n"
        "I can send scheduled and instant messages to your channels.\n\n"
        "Use the buttons below:",
        parse_mode="HTML",
        reply_markup=full_menu_keyboard(user_id),
    )


# ─── /admin command ──────────────────────────────────────────────────────────
async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ You are not the admin.")
        return
    await update.message.reply_text(
        "🛡 <b>Admin Panel</b>\n\nManage user access:",
        parse_mode="HTML",
        reply_markup=admin_keyboard(),
    )


# ─── Broadcast helpers ───────────────────────────────────────────────────────
async def broadcast_for_user(user_id: int, scheduled_time: str) -> None:
    global bot_app
    ud = get_user_data(user_id)
    message = ud["message"]
    channels = ud["channels"]
    if not message or not channels:
        return

    bot: Bot = bot_app.bot
    results = []
    ok = 0
    fail = 0

    for channel in channels:
        try:
            sent_msg = await bot.send_message(
                chat_id=channel, text=message, parse_mode="HTML",
            )
            ud["sent_messages"].append({
                "channel": channel,
                "message_id": sent_msg.message_id,
                "time": now_ist().strftime("%Y-%m-%d %H:%M IST"),
                "type": "scheduled",
            })
            results.append(f"✅ <code>{html.escape(channel)}</code> — msg#{sent_msg.message_id}")
            ok += 1
        except TelegramError as e:
            results.append(f"❌ <code>{html.escape(channel)}</code> — {html.escape(str(e))}")
            fail += 1
        except Exception as e:
            results.append(f"❌ <code>{html.escape(channel)}</code> — {html.escape(str(e))}")
            fail += 1

    ud["broadcast_history"].append({
        "time": now_ist().strftime("%Y-%m-%d %H:%M IST"),
        "type": "scheduled",
        "channels": len(channels),
        "ok": ok,
        "fail": fail,
    })

    report = "\n".join(results)
    try:
        await bot.send_message(
            chat_id=user_id,
            text=f"⏰ <b>Scheduled Broadcast</b> ({scheduled_time} IST)\n\n{report}\n\n📊 {ok} sent, {fail} failed",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"Report to user {user_id} failed: {e}")


async def do_instant_broadcast(user_id: int, bot: Bot) -> str:
    ud = get_user_data(user_id)
    message = ud["message"]
    channels = ud["channels"]
    if not channels:
        return "❌ No channels configured."
    if not message:
        return "❌ No message set."

    results = []
    ok = 0
    fail = 0

    for channel in channels:
        try:
            sent_msg = await bot.send_message(
                chat_id=channel, text=message, parse_mode="HTML",
            )
            ud["sent_messages"].append({
                "channel": channel,
                "message_id": sent_msg.message_id,
                "time": now_ist().strftime("%Y-%m-%d %H:%M IST"),
                "type": "instant",
            })
            results.append(f"✅ <code>{html.escape(channel)}</code> — msg#{sent_msg.message_id}")
            ok += 1
        except TelegramError as e:
            results.append(f"❌ <code>{html.escape(channel)}</code> — {html.escape(str(e))}")
            fail += 1
        except Exception as e:
            results.append(f"❌ <code>{html.escape(channel)}</code> — {html.escape(str(e))}")
            fail += 1

    ud["broadcast_history"].append({
        "time": now_ist().strftime("%Y-%m-%d %H:%M IST"),
        "type": "instant",
        "channels": len(channels),
        "ok": ok,
        "fail": fail,
    })

    now = now_ist().strftime("%Y-%m-%d %H:%M IST")
    return (
        f"🚀 <b>Instant Broadcast Report</b>\n🕐 {now}\n\n"
        + "\n".join(results)
        + f"\n\n📊 {ok} sent, {fail} failed"
    )


async def do_instant_forward(user_id: int, bot: Bot) -> str:
    ud = get_user_data(user_id)
    channels = ud["channels"]
    fwd = ud.get("forward_message")
    if not channels:
        return "❌ No channels configured."
    if not fwd:
        return "❌ No message stored to forward."

    source_chat = fwd["chat_id"]
    source_msg = fwd["message_id"]
    results = []
    ok = 0
    fail = 0

    for channel in channels:
        try:
            sent_msg = await bot.forward_message(
                chat_id=channel, from_chat_id=source_chat, message_id=source_msg,
            )
            ud["sent_messages"].append({
                "channel": channel,
                "message_id": sent_msg.message_id,
                "time": now_ist().strftime("%Y-%m-%d %H:%M IST"),
                "type": "forward",
            })
            results.append(f"✅ <code>{html.escape(channel)}</code> — msg#{sent_msg.message_id}")
            ok += 1
        except TelegramError as e:
            results.append(f"❌ <code>{html.escape(channel)}</code> — {html.escape(str(e))}")
            fail += 1
        except Exception as e:
            results.append(f"❌ <code>{html.escape(channel)}</code> — {html.escape(str(e))}")
            fail += 1

    ud["broadcast_history"].append({
        "time": now_ist().strftime("%Y-%m-%d %H:%M IST"),
        "type": "forward",
        "channels": len(channels),
        "ok": ok,
        "fail": fail,
    })

    now = now_ist().strftime("%Y-%m-%d %H:%M IST")
    return (
        f"📨 <b>Forward Report</b>\n🕐 {now}\n\n"
        + "\n".join(results)
        + f"\n\n📊 {ok} forwarded, {fail} failed"
    )


async def do_instant_copy(user_id: int, bot: Bot) -> str:
    ud = get_user_data(user_id)
    channels = ud["channels"]
    fwd = ud.get("forward_message")
    if not channels:
        return "❌ No channels configured."
    if not fwd:
        return "❌ No message stored to copy."

    source_chat = fwd["chat_id"]
    source_msg = fwd["message_id"]
    results = []
    ok = 0
    fail = 0

    for channel in channels:
        try:
            sent_msg = await bot.copy_message(
                chat_id=channel, from_chat_id=source_chat, message_id=source_msg,
            )
            ud["sent_messages"].append({
                "channel": channel,
                "message_id": sent_msg.message_id,
                "time": now_ist().strftime("%Y-%m-%d %H:%M IST"),
                "type": "copy",
            })
            results.append(f"✅ <code>{html.escape(channel)}</code> — msg#{sent_msg.message_id}")
            ok += 1
        except TelegramError as e:
            results.append(f"❌ <code>{html.escape(channel)}</code> — {html.escape(str(e))}")
            fail += 1
        except Exception as e:
            results.append(f"❌ <code>{html.escape(channel)}</code> — {html.escape(str(e))}")
            fail += 1

    ud["broadcast_history"].append({
        "time": now_ist().strftime("%Y-%m-%d %H:%M IST"),
        "type": "copy",
        "channels": len(channels),
        "ok": ok,
        "fail": fail,
    })

    now = now_ist().strftime("%Y-%m-%d %H:%M IST")
    return (
        f"📋 <b>Copy Report</b>\n🕐 {now}\n\n"
        + "\n".join(results)
        + f"\n\n📊 {ok} copied, {fail} failed"
    )


# ─── Admin Callbacks ─────────────────────────────────────────────────────────
async def handle_admin_callbacks(query, user_id: int, data: str, context) -> bool:

    if data == "admin_panel":
        if not is_admin(user_id):
            await query.answer("⛔ Not admin.", show_alert=True)
            return True
        await query.edit_message_text(
            "🛡 <b>Admin Panel</b>\n\nManage user access:",
            parse_mode="HTML",
            reply_markup=admin_keyboard(),
        )
        return True

    elif data == "admin_stats":
        if not is_admin(user_id):
            await query.answer("⛔ Not admin.", show_alert=True)
            return True
        total = len(all_users)
        approved = len(approved_users)
        pending = len(pending_users)
        await query.edit_message_text(
            f"📊 <b>Bot Stats</b>\n\n"
            f"👥 Total users ever: <b>{total}</b>\n"
            f"✅ Approved: <b>{approved}</b>\n"
            f"⏳ Pending: <b>{pending}</b>\n"
            f"🕐 Time (IST): <code>{now_ist().strftime('%Y-%m-%d %H:%M')}</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]),
        )
        return True

    elif data == "admin_pending":
        if not is_admin(user_id):
            await query.answer("⛔ Not admin.", show_alert=True)
            return True
        if not pending_users:
            await query.edit_message_text("✅ No pending requests.", reply_markup=admin_keyboard())
            return True
        keyboard = []
        lines = []
        for uid, info in pending_users.items():
            lines.append(f"• <b>{html.escape(info['name'])}</b> {html.escape(info['username'])} — <code>{uid}</code>")
            keyboard.append([
                InlineKeyboardButton(f"✅ {info['name'][:15]}", callback_data=f"approve_{uid}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"reject_{uid}"),
            ])
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="admin_panel")])
        await query.edit_message_text(
            f"🔔 <b>Pending Requests ({len(pending_users)})</b>\n\n" + "\n".join(lines),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return True

    elif data == "admin_approved":
        if not is_admin(user_id):
            await query.answer("⛔ Not admin.", show_alert=True)
            return True
        if not approved_users:
            await query.edit_message_text("👥 No approved users yet.", reply_markup=admin_keyboard())
            return True
        keyboard = []
        lines = []
        for uid in approved_users:
            info = all_users.get(uid, {})
            name = info.get("name", "Unknown")
            username = info.get("username", "")
            lines.append(f"• <b>{html.escape(name)}</b> {html.escape(username)} — <code>{uid}</code>")
            keyboard.append([InlineKeyboardButton(f"🚫 Revoke {name[:15]}", callback_data=f"revoke_{uid}")])
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="admin_panel")])
        await query.edit_message_text(
            f"✅ <b>Approved Users ({len(approved_users)})</b>\n\n" + "\n".join(lines),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return True

    elif data == "admin_broadcast":
        if not is_admin(user_id):
            await query.answer("⛔ Not admin.", show_alert=True)
            return True
        targets = list(approved_users) + [ADMIN_ID]
        if not targets:
            await query.edit_message_text("❌ No users to message.", reply_markup=admin_keyboard())
            return True
        user_state[user_id] = "admin_broadcast_msg"
        await query.edit_message_text(
            f"📢 <b>Message All Users</b>\n\n"
            f"Send your message now.\n"
            f"It will be sent to <b>{len(set(targets))}</b> users.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")]]),
        )
        return True

    elif data.startswith("approve_"):
        if not is_admin(user_id):
            await query.answer("⛔ Not admin.", show_alert=True)
            return True
        try:
            target_id = int(data.replace("approve_", ""))
        except ValueError:
            return True
        approved_users.add(target_id)
        info = all_users.get(target_id, {})
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="✅ <b>Access Granted!</b>\n\nYou can now use the bot. Send /start to begin.",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error(f"Failed to notify user {target_id}: {e}")
        pending_users.pop(target_id, None)
        await query.edit_message_text(
            f"✅ Approved <b>{html.escape(info.get('name', str(target_id)))}</b>",
            parse_mode="HTML",
            reply_markup=admin_keyboard(),
        )
        return True

    elif data.startswith("reject_"):
        if not is_admin(user_id):
            await query.answer("⛔ Not admin.", show_alert=True)
            return True
        try:
            target_id = int(data.replace("reject_", ""))
        except ValueError:
            return True
        info = pending_users.pop(target_id, {})
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="❌ <b>Access Denied.</b>\n\nYour request was rejected by the admin.",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error(f"Failed to notify user {target_id}: {e}")
        await query.edit_message_text(
            f"❌ Rejected <b>{html.escape(info.get('name', str(target_id)))}</b>",
            parse_mode="HTML",
            reply_markup=admin_keyboard(),
        )
        return True

    elif data.startswith("revoke_"):
        if not is_admin(user_id):
            await query.answer("⛔ Not admin.", show_alert=True)
            return True
        try:
            target_id = int(data.replace("revoke_", ""))
        except ValueError:
            return True
        approved_users.discard(target_id)
        info = all_users.get(target_id, {})
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="🚫 <b>Your access has been revoked.</b>\n\nContact admin to request again.",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error(f"Failed to notify user {target_id}: {e}")
        await query.edit_message_text(
            f"🚫 Revoked access for <b>{html.escape(info.get('name', str(target_id)))}</b>",
            parse_mode="HTML",
            reply_markup=admin_keyboard(),
        )
        return True

    return False


# ─── Callback Router ────────────────────────────────────────────────────────
async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id

    if not is_approved(user_id):
        await query.answer("⛔ You don't have access. Send /start to request.", show_alert=True)
        return

    if await handle_admin_callbacks(query, user_id, data, context):
        return

    ud = get_user_data(user_id)

    if data == "back_menu":
        user_state[user_id] = None
        await query.edit_message_text(
            "🏠 <b>Main Menu</b>\n\nChoose an option:",
            parse_mode="HTML",
            reply_markup=full_menu_keyboard(user_id),
        )

    elif data == "add_channels":
        user_state[user_id] = "awaiting_channels"
        await query.edit_message_text(
            "📡 <b>Add Channels</b>\n\n"
            "<b>Public channels:</b> use @username\n"
            "Example: <code>@channel1, @channel2</code>\n\n"
            "<b>Private channels:</b> use Chat ID\n"
            "Example: <code>-1001234567890</code>\n\n"
            "You can mix both, separated by commas.\n\n"
            "⚠️ Bot must be <b>admin</b> in each channel!\n"
            "📌 To get private channel ID, add @getidsbot to the channel.",
            parse_mode="HTML",
            reply_markup=back_keyboard(),
        )

    elif data == "show_channels":
        user_state[user_id] = None
        channels = ud["channels"]
        if not channels:
            text = "📋 <b>Your Channels</b>\n\nNo channels added yet."
        else:
            ch_list = "\n".join(f"  • <code>{html.escape(ch)}</code>" for ch in channels)
            text = f"📋 <b>Your Channels</b> ({len(channels)}):\n\n{ch_list}"
        keyboard = [
            [InlineKeyboardButton("🗑 Clear All Channels", callback_data="clear_channels")],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")],
        ]
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data == "clear_channels":
        ud["channels"] = []
        reschedule_user_jobs(user_id)
        await query.edit_message_text("✅ All channels cleared.", parse_mode="HTML", reply_markup=back_keyboard())

    elif data == "set_message":
        user_state[user_id] = "awaiting_message"
        current = ud["message"]
        preview = f"\n\n📝 Current:\n<i>{html.escape(current[:200])}</i>" if current else "\n\n📝 No message set yet."
        await query.edit_message_text(
            f"✉️ <b>Set Broadcast Message</b>\n\nSend me the text message to broadcast.{preview}",
            parse_mode="HTML",
            reply_markup=back_keyboard(),
        )

    elif data == "set_time":
        user_state[user_id] = "awaiting_times"
        current_times = ud["times"]
        preview = f"\n\n⏰ Current: <code>{', '.join(current_times)}</code>" if current_times else "\n\n⏰ No times set yet."
        await query.edit_message_text(
            f"⏰ <b>Set Broadcast Times (IST)</b>\n\n"
            f"Send times in 24h format, comma separated.\n"
            f"Example: <code>10:00,14:30,23:25</code>{preview}",
            parse_mode="HTML",
            reply_markup=back_keyboard(),
        )

    elif data == "instant_broadcast":
        user_state[user_id] = None
        channels = ud["channels"]
        message = ud["message"]
        if not channels or not message:
            missing = []
            if not channels: missing.append("channels")
            if not message: missing.append("message")
            await query.edit_message_text(
                f"⚠️ <b>Cannot broadcast!</b>\nMissing: <b>{', '.join(missing)}</b>",
                parse_mode="HTML", reply_markup=back_keyboard(),
            )
            return
        ch_list = ", ".join(f"<code>{html.escape(c)}</code>" for c in channels)
        keyboard = [
            [InlineKeyboardButton("✅ Yes, Broadcast Now!", callback_data="confirm_broadcast")],
            [InlineKeyboardButton("❌ Cancel", callback_data="back_menu")],
        ]
        await query.edit_message_text(
            f"🚀 <b>Confirm Broadcast</b>\n\n📡 {ch_list}\n\n✉️ <i>{html.escape(message[:150])}</i>\n\nSend now?",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard),
        )

    elif data == "confirm_broadcast":
        user_state[user_id] = None
        await query.edit_message_text("🚀 <b>Broadcasting...</b>", parse_mode="HTML")
        report = await do_instant_broadcast(user_id, context.bot)
        await context.bot.send_message(chat_id=user_id, text=report, parse_mode="HTML", reply_markup=full_menu_keyboard(user_id))

    elif data == "instant_forward":
        user_state[user_id] = "awaiting_forward_message"
        fwd = ud.get("forward_message")
        extra = f"\n\n📌 Stored msg#{fwd['message_id']} exists. Send new to replace." if fwd else ""
        await query.edit_message_text(
            f"📨 <b>Instant Forward</b>\n\nSend or forward me any message.\n\n"
            f"Then choose Forward (with tag) or Copy (no tag).{extra}",
            parse_mode="HTML", reply_markup=back_keyboard(),
        )

    elif data == "confirm_forward":
        user_state[user_id] = None
        if not ud.get("forward_message"):
            await query.edit_message_text("❌ No message stored.", reply_markup=back_keyboard())
            return
        await query.edit_message_text("📨 <b>Forwarding...</b>", parse_mode="HTML")
        report = await do_instant_forward(user_id, context.bot)
        await context.bot.send_message(chat_id=user_id, text=report, parse_mode="HTML", reply_markup=full_menu_keyboard(user_id))

    elif data == "confirm_copy":
        user_state[user_id] = None
        if not ud.get("forward_message"):
            await query.edit_message_text("❌ No message stored.", reply_markup=back_keyboard())
            return
        await query.edit_message_text("📋 <b>Copying...</b>", parse_mode="HTML")
        report = await do_instant_copy(user_id, context.bot)
        await context.bot.send_message(chat_id=user_id, text=report, parse_mode="HTML", reply_markup=full_menu_keyboard(user_id))

    elif data == "delete_last":
        user_state[user_id] = None
        sent = ud["sent_messages"]
        if not sent:
            await query.edit_message_text("🗑 No sent messages to delete.", parse_mode="HTML", reply_markup=back_keyboard())
            return
        await query.edit_message_text(f"🗑 Deleting <b>{len(sent)}</b> message(s)...", parse_mode="HTML")
        bot: Bot = context.bot
        results = []
        for item in sent:
            try:
                await bot.delete_message(chat_id=item["channel"], message_id=item["message_id"])
                results.append(f"✅ {html.escape(item['channel'])} msg#{item['message_id']}")
            except TelegramError as e:
                results.append(f"❌ {html.escape(item['channel'])} msg#{item['message_id']}: {html.escape(str(e))}")
        ud["sent_messages"] = []
        await bot.send_message(
            chat_id=user_id,
            text="🗑 <b>Delete Results</b>:\n\n" + "\n".join(results),
            parse_mode="HTML", reply_markup=back_keyboard(),
        )

    elif data == "manage_sent":
        user_state[user_id] = None
        sent = ud["sent_messages"]
        if not sent:
            await query.edit_message_text("📜 <b>Sent Messages</b>\n\nNo messages recorded.", parse_mode="HTML", reply_markup=back_keyboard())
            return
        type_icons = {"scheduled": "⏰", "instant": "🚀", "forward": "📨", "copy": "📋"}
        keyboard = []
        for idx, item in enumerate(sent):
            icon = type_icons.get(item.get("type", ""), "💬")
            label = f"🗑 {icon} {item['channel']} #{item['message_id']} | {item['time']}"
            keyboard.append([InlineKeyboardButton(label, callback_data=f"del_msg_{idx}")])
        keyboard.append([InlineKeyboardButton("🗑 Delete ALL", callback_data="delete_last")])
        keyboard.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")])
        await query.edit_message_text("📜 <b>Sent Messages</b>\n\nTap to delete:", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("del_msg_"):
        try:
            idx = int(data.replace("del_msg_", ""))
        except ValueError:
            await query.edit_message_text("❌ Invalid.", reply_markup=back_keyboard())
            return
        sent = ud["sent_messages"]
        if idx < 0 or idx >= len(sent):
            await query.edit_message_text("❌ Not found.", reply_markup=back_keyboard())
            return
        item = sent[idx]
        try:
            await context.bot.delete_message(chat_id=item["channel"], message_id=item["message_id"])
            result_text = f"✅ Deleted <b>#{item['message_id']}</b> from <code>{html.escape(item['channel'])}</code>"
        except TelegramError as e:
            result_text = f"❌ Failed: {html.escape(str(e))}"
        sent.pop(idx)
        keyboard = [
            [InlineKeyboardButton("📜 Back to Sent", callback_data="manage_sent")],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")],
        ]
        await query.edit_message_text(result_text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data == "show_status":
        user_state[user_id] = None
        channels = ud["channels"]
        message = ud["message"]
        times = ud["times"]
        sent_count = len(ud["sent_messages"])
        fwd = ud.get("forward_message")
        templates_count = len(ud.get("templates", {}))

        ch_text = ", ".join(f"<code>{c}</code>" for c in channels) if channels else "<i>None</i>"
        msg_text = f"<i>{html.escape(message[:100])}</i>" if message else "<i>None</i>"
        t_text = ", ".join(f"<code>{t}</code>" for t in times) if times else "<i>None</i>"
        fwd_text = f"msg#{fwd['message_id']} ✅" if fwd else "<i>None</i>"
        ready = bool(channels and message and times)
        status = "🟢 Active" if ready else "🔴 Incomplete"

        await query.edit_message_text(
            f"📊 <b>Status: {status}</b>\n\n"
            f"📡 Channels: {ch_text}\n\n"
            f"✉️ Message: {msg_text}\n\n"
            f"⏰ Times (IST): {t_text}\n\n"
            f"📨 Forward stored: {fwd_text}\n\n"
            f"📬 Tracked sent: <b>{sent_count}</b>\n"
            f"💾 Templates: <b>{templates_count}</b>",
            parse_mode="HTML", reply_markup=back_keyboard(),
        )

    # ── Broadcast History
    elif data == "broadcast_history":
        user_state[user_id] = None
        history = ud.get("broadcast_history", [])
        if not history:
            await query.edit_message_text("📈 No broadcast history yet.", reply_markup=back_keyboard())
            return
        lines = []
        for h in history[-15:]:  # show last 15
            icon = {"scheduled": "⏰", "instant": "🚀", "forward": "📨", "copy": "📋"}.get(h["type"], "💬")
            lines.append(f"{icon} <code>{h['time']}</code> — {h['channels']} ch — ✅{h['ok']} ❌{h['fail']}")
        await query.edit_message_text(
            f"📈 <b>Broadcast History</b> (last {len(lines)})\n\n" + "\n".join(lines),
            parse_mode="HTML", reply_markup=back_keyboard(),
        )

    # ── Templates Menu
    elif data == "templates_menu":
        user_state[user_id] = None
        templates = ud.get("templates", {})
        keyboard = [
            [InlineKeyboardButton("➕ Save Current Message as Template", callback_data="template_save")],
        ]
        if templates:
            for name in list(templates.keys())[:8]:
                keyboard.append([
                    InlineKeyboardButton(f"📝 {name}", callback_data=f"template_use_{name}"),
                    InlineKeyboardButton("🗑", callback_data=f"template_del_{name}"),
                ])
        keyboard.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")])
        text = f"💾 <b>Templates ({len(templates)})</b>\n\nSave messages and reuse them anytime."
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data == "template_save":
        message = ud.get("message", "")
        if not message:
            await query.edit_message_text(
                "❌ No message set yet. Set a message first via ✉️ Set Message.",
                reply_markup=back_keyboard(),
            )
            return
        user_state[user_id] = "awaiting_template_name"
        await query.edit_message_text(
            f"💾 <b>Save Template</b>\n\nSend a name for this template:\n\n<i>{html.escape(message[:150])}</i>",
            parse_mode="HTML", reply_markup=back_keyboard(),
        )

    elif data.startswith("template_use_"):
        name = data.replace("template_use_", "")
        templates = ud.get("templates", {})
        if name in templates:
            ud["message"] = templates[name]
            await query.edit_message_text(
                f"✅ Template <b>{html.escape(name)}</b> loaded as current message!",
                parse_mode="HTML", reply_markup=full_menu_keyboard(user_id),
            )
        else:
            await query.edit_message_text("❌ Template not found.", reply_markup=back_keyboard())

    elif data.startswith("template_del_"):
        name = data.replace("template_del_", "")
        templates = ud.get("templates", {})
        if name in templates:
            del templates[name]
            await query.edit_message_text(
                f"🗑 Template <b>{html.escape(name)}</b> deleted.",
                parse_mode="HTML", reply_markup=back_keyboard(),
            )
        else:
            await query.edit_message_text("❌ Template not found.", reply_markup=back_keyboard())

    elif data == "test_broadcast":
        user_state[user_id] = "awaiting_test_channel"
        await query.edit_message_text(
            "🧪 <b>Test Broadcast</b>\n\nSend one channel username to test:",
            parse_mode="HTML", reply_markup=back_keyboard(),
        )


# ─── Forward Message Store Helper ───────────────────────────────────────────
async def handle_forward_store(update: Update, user_id: int) -> None:
    ud = get_user_data(user_id)
    ud["forward_message"] = {
        "chat_id": update.message.chat_id,
        "message_id": update.message.message_id,
    }
    user_state[user_id] = None

    channels = ud["channels"]
    if not channels:
        await update.message.reply_text(
            "✅ Message stored!\n\n⚠️ No channels added yet.",
            parse_mode="HTML", reply_markup=full_menu_keyboard(user_id),
        )
        return

    ch_list = ", ".join(f"<code>{html.escape(c)}</code>" for c in channels)
    keyboard = [
        [InlineKeyboardButton("📨 Forward (with tag)", callback_data="confirm_forward")],
        [InlineKeyboardButton("📋 Copy (clean, no tag)", callback_data="confirm_copy")],
        [InlineKeyboardButton("❌ Cancel", callback_data="back_menu")],
    ]
    await update.message.reply_text(
        f"✅ Message stored!\n\n📡 Channels: {ch_list}\n\nChoose how to send:",
        parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ─── Text Handler ───────────────────────────────────────────────────────────
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    user = update.effective_user

    all_users[user_id] = {
        "name": user.full_name,
        "username": f"@{user.username}" if user.username else "No username",
    }

    if not is_approved(user_id):
        await update.message.reply_text("⛔ You don't have access. Send /start to request.")
        return

    state = user_state.get(user_id)
    text = update.message.text.strip()
    ud = get_user_data(user_id)

    # ── Admin broadcast to all users
    if state == "admin_broadcast_msg" and is_admin(user_id):
        targets = list(approved_users) + [ADMIN_ID]
        targets = list(set(targets))
        user_state[user_id] = None
        ok = 0
        fail = 0
        for tid in targets:
            try:
                await context.bot.send_message(chat_id=tid, text=f"📢 <b>Message from Admin:</b>\n\n{html.escape(text)}", parse_mode="HTML")
                ok += 1
            except Exception:
                fail += 1
        await update.message.reply_text(
            f"📢 <b>Admin Broadcast Done</b>\n\n✅ Sent to {ok} users\n❌ Failed: {fail}",
            parse_mode="HTML", reply_markup=admin_keyboard(),
        )
        return

    if state is None:
        await update.message.reply_text(
            "🏠 <b>Main Menu</b>\n\nChoose an option:",
            parse_mode="HTML", reply_markup=full_menu_keyboard(user_id),
        )
        return

    if state == "awaiting_channels":
        raw = [ch.strip() for ch in text.split(",") if ch.strip()]
        added = []
        errors = []
        for ch in raw:
            # Accept Chat ID (numbers starting with -100) or @username
            if ch.lstrip("-").isdigit():
                # It's a numeric chat ID — use as-is
                pass
            elif not ch.startswith("@"):
                ch = "@" + ch
            if ch in ud["channels"]:
                errors.append(f"<code>{html.escape(ch)}</code> exists")
            else:
                ud["channels"].append(ch)
                added.append(f"<code>{html.escape(ch)}</code>")
        parts = []
        if added: parts.append(f"✅ Added: {', '.join(added)}")
        if errors: parts.append(f"⚠️ Skipped: {', '.join(errors)}")
        user_state[user_id] = None
        reschedule_user_jobs(user_id)
        await update.message.reply_text(
            "\n".join(parts) + "\n\n📡 Total: " + ", ".join(f"<code>{c}</code>" for c in ud["channels"]),
            parse_mode="HTML", reply_markup=full_menu_keyboard(user_id),
        )

    elif state == "awaiting_message":
        ud["message"] = text
        user_state[user_id] = None
        await update.message.reply_text(
            f"✅ Message set!\n\n<i>{html.escape(text[:300])}</i>",
            parse_mode="HTML", reply_markup=full_menu_keyboard(user_id),
        )

    elif state == "awaiting_times":
        raw = [t.strip() for t in text.split(",") if t.strip()]
        valid = []
        errors = []
        for t in raw:
            try:
                datetime.strptime(t, "%H:%M")
                valid.append(t)
            except ValueError:
                errors.append(t)
        ud["times"] = valid
        user_state[user_id] = None
        parts = []
        if valid: parts.append(f"✅ Times (IST): <code>{', '.join(valid)}</code>")
        if errors: parts.append(f"⚠️ Invalid: <code>{', '.join(errors)}</code>")
        reschedule_user_jobs(user_id)
        await update.message.reply_text("\n".join(parts), parse_mode="HTML", reply_markup=full_menu_keyboard(user_id))

    elif state == "awaiting_template_name":
        name = text[:30]
        if "templates" not in ud:
            ud["templates"] = {}
        ud["templates"][name] = ud.get("message", "")
        user_state[user_id] = None
        await update.message.reply_text(
            f"💾 Template <b>{html.escape(name)}</b> saved!",
            parse_mode="HTML", reply_markup=full_menu_keyboard(user_id),
        )

    elif state == "awaiting_test_channel":
        ch = text.strip()
        if not ch.startswith("@"):
            ch = "@" + ch
        user_state[user_id] = None
        message = ud.get("message", "")
        if not message:
            await update.message.reply_text("❌ No message set.", reply_markup=full_menu_keyboard(user_id))
            return
        try:
            sent = await context.bot.send_message(chat_id=ch, text=message, parse_mode="HTML")
            await update.message.reply_text(
                f"🧪 <b>Test sent!</b>\n\n✅ <code>{html.escape(ch)}</code> — msg#{sent.message_id}",
                parse_mode="HTML", reply_markup=full_menu_keyboard(user_id),
            )
        except TelegramError as e:
            await update.message.reply_text(
                f"❌ Test failed: {html.escape(str(e))}",
                parse_mode="HTML", reply_markup=full_menu_keyboard(user_id),
            )

    elif state == "awaiting_forward_message":
        await handle_forward_store(update, user_id)


# ─── Media Handler ───────────────────────────────────────────────────────────
async def media_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id

    if not is_approved(user_id):
        await update.message.reply_text("⛔ You don't have access. Send /start to request.")
        return

    state = user_state.get(user_id)
    if state != "awaiting_forward_message":
        await update.message.reply_text("Use /start to open the menu.", reply_markup=back_keyboard())
        return

    await handle_forward_store(update, user_id)


# ─── Scheduler ───────────────────────────────────────────────────────────────
def reschedule_user_jobs(user_id: int) -> None:
    global scheduler
    if scheduler is None:
        return

    for job in scheduler.get_jobs():
        if job.id.startswith(f"broadcast_{user_id}_"):
            job.remove()

    ud = get_user_data(user_id)
    if not ud["channels"] or not ud["message"] or not ud["times"]:
        return

    for time_str in ud["times"]:
        try:
            parsed = datetime.strptime(time_str, "%H:%M")
            job_id = f"broadcast_{user_id}_{time_str.replace(':', '')}"
            scheduler.add_job(
                broadcast_for_user,
                trigger="cron",
                hour=parsed.hour,
                minute=parsed.minute,
                timezone=IST,
                args=[user_id, time_str],
                id=job_id,
                replace_existing=True,
                misfire_grace_time=60,
            )
            logger.info(f"Scheduled {job_id} at {time_str} IST for user {user_id}")
        except Exception as e:
            logger.error(f"Schedule error {time_str} user {user_id}: {e}")


# ─── App Lifecycle ───────────────────────────────────────────────────────────
async def post_init(application: Application) -> None:
    global scheduler, bot_app
    bot_app = application
    scheduler = AsyncIOScheduler(timezone=IST)
    scheduler.start()
    logger.info("APScheduler started with IST timezone.")


async def post_shutdown(application: Application) -> None:
    global scheduler
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("APScheduler shut down.")


# ─── Main ────────────────────────────────────────────────────────────────────
def main() -> None:
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .job_queue(None)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("admin", admin_command))
    application.add_handler(CallbackQueryHandler(callback_router))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    application.add_handler(
        MessageHandler(
            (
                filters.PHOTO | filters.VIDEO | filters.Document.ALL
                | filters.AUDIO | filters.VOICE | filters.VIDEO_NOTE
                | filters.Sticker.ALL | filters.ANIMATION
                | filters.CONTACT | filters.LOCATION | filters.FORWARDED
            ) & ~filters.COMMAND,
            media_handler,
        )
    )

    logger.info("Bot starting...")
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
