import logging
import html
from datetime import datetime
from typing import Optional

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Bot,
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

# ─── Bot Token ──────────────────────────────────────────────────────────────
BOT_TOKEN = "ENTER YOUR BOT TOKEN"

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
        }
    return user_data_store[user_id]


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
        ],
        [
            InlineKeyboardButton("📨 Instant Forward", callback_data="instant_forward"),
        ],
        [
            InlineKeyboardButton("🗑 Delete All Sent", callback_data="delete_last"),
            InlineKeyboardButton("📜 Manage Sent", callback_data="manage_sent"),
        ],
        [
            InlineKeyboardButton("📊 Status", callback_data="show_status"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")]]
    )


# ─── /start ─────────────────────────────────────────────────────────────────
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    get_user_data(user.id)
    await update.message.reply_text(
        f"👋 Hello <b>{html.escape(user.first_name)}</b>!\n\n"
        "I am a <b>Broadcast Bot</b>.\n"
        "I can send scheduled and instant messages to your channels.\n\n"
        "Use the buttons below:",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


# ─── Scheduled Broadcast ────────────────────────────────────────────────────
async def broadcast_for_user(user_id: int, scheduled_time: str) -> None:
    global bot_app
    ud = get_user_data(user_id)
    message = ud["message"]
    channels = ud["channels"]
    if not message or not channels:
        return

    bot: Bot = bot_app.bot
    results = []

    for channel in channels:
        try:
            sent_msg = await bot.send_message(
                chat_id=channel,
                text=message,
                parse_mode="HTML",
            )
            ud["sent_messages"].append({
                "channel": channel,
                "message_id": sent_msg.message_id,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "scheduled",
            })
            results.append(
                f"✅ <code>{html.escape(channel)}</code> — "
                f"Success (msg#{sent_msg.message_id})"
            )
        except TelegramError as e:
            results.append(
                f"❌ <code>{html.escape(channel)}</code> — "
                f"Failed: {html.escape(str(e))}"
            )
        except Exception as e:
            results.append(
                f"❌ <code>{html.escape(channel)}</code> — "
                f"Error: {html.escape(str(e))}"
            )

    report = "\n".join(results)
    try:
        await bot.send_message(
            chat_id=user_id,
            text=f"⏰ <b>Scheduled Broadcast Report</b> ({scheduled_time})\n\n{report}",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"Report to user {user_id} failed: {e}")


# ─── Instant Broadcast (text) ───────────────────────────────────────────────
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
                chat_id=channel,
                text=message,
                parse_mode="HTML",
            )
            ud["sent_messages"].append({
                "channel": channel,
                "message_id": sent_msg.message_id,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "instant",
            })
            results.append(
                f"✅ <code>{html.escape(channel)}</code> — "
                f"msg#{sent_msg.message_id}"
            )
            ok += 1
        except TelegramError as e:
            results.append(
                f"❌ <code>{html.escape(channel)}</code> — "
                f"{html.escape(str(e))}"
            )
            fail += 1
        except Exception as e:
            results.append(
                f"❌ <code>{html.escape(channel)}</code> — "
                f"{html.escape(str(e))}"
            )
            fail += 1

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"🚀 <b>Instant Broadcast Report</b>\n"
        f"🕐 {now}\n\n"
        + "\n".join(results)
        + f"\n\n📊 {ok} sent, {fail} failed"
    )


# ─── Instant Forward (with forward tag) ─────────────────────────────────────
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
                chat_id=channel,
                from_chat_id=source_chat,
                message_id=source_msg,
            )
            ud["sent_messages"].append({
                "channel": channel,
                "message_id": sent_msg.message_id,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "forward",
            })
            results.append(
                f"✅ <code>{html.escape(channel)}</code> — "
                f"msg#{sent_msg.message_id}"
            )
            ok += 1
        except TelegramError as e:
            results.append(
                f"❌ <code>{html.escape(channel)}</code> — "
                f"{html.escape(str(e))}"
            )
            fail += 1
        except Exception as e:
            results.append(
                f"❌ <code>{html.escape(channel)}</code> — "
                f"{html.escape(str(e))}"
            )
            fail += 1

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"📨 <b>Forward Report</b>\n"
        f"🕐 {now}\n\n"
        + "\n".join(results)
        + f"\n\n📊 {ok} forwarded, {fail} failed"
    )


# ─── Instant Copy (no forward tag) ──────────────────────────────────────────
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
                chat_id=channel,
                from_chat_id=source_chat,
                message_id=source_msg,
            )
            ud["sent_messages"].append({
                "channel": channel,
                "message_id": sent_msg.message_id,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "copy",
            })
            results.append(
                f"✅ <code>{html.escape(channel)}</code> — "
                f"msg#{sent_msg.message_id}"
            )
            ok += 1
        except TelegramError as e:
            results.append(
                f"❌ <code>{html.escape(channel)}</code> — "
                f"{html.escape(str(e))}"
            )
            fail += 1
        except Exception as e:
            results.append(
                f"❌ <code>{html.escape(channel)}</code> — "
                f"{html.escape(str(e))}"
            )
            fail += 1

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"📋 <b>Copy Report</b>\n"
        f"🕐 {now}\n\n"
        + "\n".join(results)
        + f"\n\n📊 {ok} copied, {fail} failed"
    )


# ─── Callback Router ────────────────────────────────────────────────────────
async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id
    ud = get_user_data(user_id)

    # ── Back to Menu
    if data == "back_menu":
        user_state[user_id] = None
        await query.edit_message_text(
            "🏠 <b>Main Menu</b>\n\nChoose an option:",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard(),
        )

    # ── Add Channels
    elif data == "add_channels":
        user_state[user_id] = "awaiting_channels"
        await query.edit_message_text(
            "📡 <b>Add Channels</b>\n\n"
            "Send channel usernames separated by commas.\n"
            "Example: <code>@channel1,@channel2</code>\n\n"
            "⚠️ Bot must be <b>admin</b> in each channel!",
            parse_mode="HTML",
            reply_markup=back_keyboard(),
        )

    # ── Show Channels
    elif data == "show_channels":
        user_state[user_id] = None
        channels = ud["channels"]
        if not channels:
            text = "📋 <b>Your Channels</b>\n\nNo channels added yet."
        else:
            ch_list = "\n".join(
                f"  • <code>{html.escape(ch)}</code>" for ch in channels
            )
            text = f"📋 <b>Your Channels</b> ({len(channels)}):\n\n{ch_list}"
        keyboard = [
            [InlineKeyboardButton("🗑 Clear All Channels", callback_data="clear_channels")],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")],
        ]
        await query.edit_message_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    # ── Clear Channels
    elif data == "clear_channels":
        ud["channels"] = []
        reschedule_user_jobs(user_id)
        await query.edit_message_text(
            "✅ All channels cleared.",
            parse_mode="HTML",
            reply_markup=back_keyboard(),
        )

    # ── Set Message
    elif data == "set_message":
        user_state[user_id] = "awaiting_message"
        current = ud["message"]
        if current:
            preview = f"\n\n📝 Current:\n<i>{html.escape(current[:200])}</i>"
        else:
            preview = "\n\n📝 No message set yet."
        await query.edit_message_text(
            f"✉️ <b>Set Broadcast Message</b>\n\n"
            f"Send me the text message to broadcast.{preview}",
            parse_mode="HTML",
            reply_markup=back_keyboard(),
        )

    # ── Set Time
    elif data == "set_time":
        user_state[user_id] = "awaiting_times"
        current_times = ud["times"]
        if current_times:
            preview = f"\n\n⏰ Current: <code>{', '.join(current_times)}</code>"
        else:
            preview = "\n\n⏰ No times set yet."
        await query.edit_message_text(
            f"⏰ <b>Set Broadcast Times</b>\n\n"
            f"Send times in 24h format, comma separated.\n"
            f"Example: <code>10:00,14:30,23:25</code>{preview}",
            parse_mode="HTML",
            reply_markup=back_keyboard(),
        )

    # ── Instant Broadcast
    elif data == "instant_broadcast":
        user_state[user_id] = None
        channels = ud["channels"]
        message = ud["message"]
        if not channels or not message:
            missing = []
            if not channels:
                missing.append("channels")
            if not message:
                missing.append("message")
            await query.edit_message_text(
                f"⚠️ <b>Cannot broadcast!</b>\n"
                f"Missing: <b>{', '.join(missing)}</b>",
                parse_mode="HTML",
                reply_markup=back_keyboard(),
            )
            return
        ch_list = ", ".join(f"<code>{html.escape(c)}</code>" for c in channels)
        msg_preview = html.escape(message[:150])
        keyboard = [
            [InlineKeyboardButton("✅ Yes, Broadcast Now!", callback_data="confirm_broadcast")],
            [InlineKeyboardButton("❌ Cancel", callback_data="back_menu")],
        ]
        await query.edit_message_text(
            f"🚀 <b>Instant Broadcast Confirmation</b>\n\n"
            f"📡 Channels: {ch_list}\n\n"
            f"✉️ Preview:\n<i>{msg_preview}</i>\n\n"
            f"Send <b>now</b>?",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    # ── Confirm Broadcast
    elif data == "confirm_broadcast":
        user_state[user_id] = None
        await query.edit_message_text(
            "🚀 <b>Broadcasting...</b>",
            parse_mode="HTML",
        )
        report = await do_instant_broadcast(user_id, context.bot)
        await context.bot.send_message(
            chat_id=user_id,
            text=report,
            parse_mode="HTML",
            reply_markup=main_menu_keyboard(),
        )

    # ── Instant Forward
    elif data == "instant_forward":
        user_state[user_id] = "awaiting_forward_message"
        fwd = ud.get("forward_message")
        if fwd:
            extra = f"\n\n📌 Stored msg#{fwd['message_id']} exists. Send new to replace."
        else:
            extra = ""
        await query.edit_message_text(
            f"📨 <b>Instant Forward</b>\n\n"
            f"Send or forward me <b>any message</b>\n"
            f"(text, photo, video, document, sticker, audio, voice, etc.)\n\n"
            f"Then choose:\n"
            f"• <b>Forward</b> — with forward tag\n"
            f"• <b>Copy</b> — clean, no forward tag\n\n"
            f"to all your channels.{extra}",
            parse_mode="HTML",
            reply_markup=back_keyboard(),
        )

    # ── Confirm Forward
    elif data == "confirm_forward":
        user_state[user_id] = None
        if not ud.get("forward_message"):
            await query.edit_message_text(
                "❌ No message stored.",
                reply_markup=back_keyboard(),
            )
            return
        await query.edit_message_text(
            "📨 <b>Forwarding to all channels...</b>",
            parse_mode="HTML",
        )
        report = await do_instant_forward(user_id, context.bot)
        await context.bot.send_message(
            chat_id=user_id,
            text=report,
            parse_mode="HTML",
            reply_markup=main_menu_keyboard(),
        )

    # ── Confirm Copy
    elif data == "confirm_copy":
        user_state[user_id] = None
        if not ud.get("forward_message"):
            await query.edit_message_text(
                "❌ No message stored.",
                reply_markup=back_keyboard(),
            )
            return
        await query.edit_message_text(
            "📋 <b>Copying to all channels...</b>",
            parse_mode="HTML",
        )
        report = await do_instant_copy(user_id, context.bot)
        await context.bot.send_message(
            chat_id=user_id,
            text=report,
            parse_mode="HTML",
            reply_markup=main_menu_keyboard(),
        )

    # ── Delete All Sent
    elif data == "delete_last":
        user_state[user_id] = None
        sent = ud["sent_messages"]
        if not sent:
            await query.edit_message_text(
                "🗑 No sent messages to delete.",
                parse_mode="HTML",
                reply_markup=back_keyboard(),
            )
            return
        await query.edit_message_text(
            f"🗑 Deleting <b>{len(sent)}</b> message(s)...",
            parse_mode="HTML",
        )
        bot: Bot = context.bot
        results = []
        for item in sent:
            try:
                await bot.delete_message(
                    chat_id=item["channel"],
                    message_id=item["message_id"],
                )
                results.append(
                    f"✅ {html.escape(item['channel'])} "
                    f"msg#{item['message_id']}"
                )
            except TelegramError as e:
                results.append(
                    f"❌ {html.escape(item['channel'])} "
                    f"msg#{item['message_id']}: "
                    f"{html.escape(str(e))}"
                )
        ud["sent_messages"] = []
        await bot.send_message(
            chat_id=user_id,
            text="🗑 <b>Delete Results</b>:\n\n" + "\n".join(results),
            parse_mode="HTML",
            reply_markup=back_keyboard(),
        )

    # ── Manage Sent
    elif data == "manage_sent":
        user_state[user_id] = None
        sent = ud["sent_messages"]
        if not sent:
            await query.edit_message_text(
                "📜 <b>Sent Messages</b>\n\nNo messages recorded.",
                parse_mode="HTML",
                reply_markup=back_keyboard(),
            )
            return
        type_icons = {
            "scheduled": "⏰",
            "instant": "🚀",
            "forward": "📨",
            "copy": "📋",
        }
        keyboard = []
        for idx, item in enumerate(sent):
            icon = type_icons.get(item.get("type", ""), "💬")
            label = (
                f"🗑 {icon} {item['channel']} "
                f"#{item['message_id']} | {item['time']}"
            )
            keyboard.append(
                [InlineKeyboardButton(label, callback_data=f"del_msg_{idx}")]
            )
        keyboard.append(
            [InlineKeyboardButton("🗑 Delete ALL", callback_data="delete_last")]
        )
        keyboard.append(
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")]
        )
        await query.edit_message_text(
            "📜 <b>Sent Messages</b>\n\nTap to delete from channel:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    # ── Delete Individual
    elif data.startswith("del_msg_"):
        try:
            idx = int(data.replace("del_msg_", ""))
        except ValueError:
            await query.edit_message_text(
                "❌ Invalid.",
                reply_markup=back_keyboard(),
            )
            return
        sent = ud["sent_messages"]
        if idx < 0 or idx >= len(sent):
            await query.edit_message_text(
                "❌ Not found.",
                reply_markup=back_keyboard(),
            )
            return
        item = sent[idx]
        try:
            await context.bot.delete_message(
                chat_id=item["channel"],
                message_id=item["message_id"],
            )
            result_text = (
                f"✅ Deleted <b>#{item['message_id']}</b> "
                f"from <code>{html.escape(item['channel'])}</code>"
            )
        except TelegramError as e:
            result_text = (
                f"❌ Failed: {html.escape(str(e))}"
            )
        sent.pop(idx)
        keyboard = [
            [InlineKeyboardButton("📜 Back to Sent", callback_data="manage_sent")],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_menu")],
        ]
        await query.edit_message_text(
            result_text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    # ── Status
    elif data == "show_status":
        user_state[user_id] = None
        channels = ud["channels"]
        message = ud["message"]
        times = ud["times"]
        sent_count = len(ud["sent_messages"])
        fwd = ud.get("forward_message")

        ch_text = (
            ", ".join(f"<code>{c}</code>" for c in channels)
            if channels else "<i>None</i>"
        )
        msg_text = (
            f"<i>{html.escape(message[:100])}</i>"
            if message else "<i>None</i>"
        )
        t_text = (
            ", ".join(f"<code>{t}</code>" for t in times)
            if times else "<i>None</i>"
        )
        fwd_text = (
            f"msg#{fwd['message_id']} ✅"
            if fwd else "<i>None</i>"
        )

        ready = bool(channels and message and times)
        status = "🟢 Active" if ready else "🔴 Incomplete"

        await query.edit_message_text(
            f"📊 <b>Status: {status}</b>\n\n"
            f"📡 Channels: {ch_text}\n\n"
            f"✉️ Message: {msg_text}\n\n"
            f"⏰ Times: {t_text}\n\n"
            f"📨 Forward stored: {fwd_text}\n\n"
            f"📬 Tracked: <b>{sent_count}</b>",
            parse_mode="HTML",
            reply_markup=back_keyboard(),
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
            "✅ Message stored!\n\n"
            "⚠️ No channels added yet. Add channels first.",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard(),
        )
        return

    ch_list = ", ".join(f"<code>{html.escape(c)}</code>" for c in channels)
    keyboard = [
        [InlineKeyboardButton("📨 Forward (with tag)", callback_data="confirm_forward")],
        [InlineKeyboardButton("📋 Copy (clean, no tag)", callback_data="confirm_copy")],
        [InlineKeyboardButton("❌ Cancel", callback_data="back_menu")],
    ]
    await update.message.reply_text(
        f"✅ Message stored!\n\n"
        f"📡 Channels: {ch_list}\n\n"
        f"Choose how to send:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ─── Text Handler ───────────────────────────────────────────────────────────
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    state = user_state.get(user_id)
    text = update.message.text.strip()
    ud = get_user_data(user_id)

    if state is None:
        await update.message.reply_text(
            "Use /start to open the menu.",
            reply_markup=back_keyboard(),
        )
        return

    if state == "awaiting_channels":
        raw = [ch.strip() for ch in text.split(",") if ch.strip()]
        added = []
        errors = []
        for ch in raw:
            if not ch.startswith("@"):
                ch = "@" + ch
            if ch in ud["channels"]:
                errors.append(f"<code>{html.escape(ch)}</code> exists")
            else:
                ud["channels"].append(ch)
                added.append(f"<code>{html.escape(ch)}</code>")
        parts = []
        if added:
            parts.append(f"✅ Added: {', '.join(added)}")
        if errors:
            parts.append(f"⚠️ Skipped: {', '.join(errors)}")
        user_state[user_id] = None
        reschedule_user_jobs(user_id)
        await update.message.reply_text(
            "\n".join(parts)
            + "\n\n📡 Total: "
            + ", ".join(f"<code>{c}</code>" for c in ud["channels"]),
            parse_mode="HTML",
            reply_markup=main_menu_keyboard(),
        )

    elif state == "awaiting_message":
        ud["message"] = text
        user_state[user_id] = None
        await update.message.reply_text(
            f"✅ Message set!\n\n<i>{html.escape(text[:300])}</i>",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard(),
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
        if valid:
            parts.append(f"✅ Times: <code>{', '.join(valid)}</code>")
        if errors:
            parts.append(f"⚠️ Invalid: <code>{', '.join(errors)}</code>")
        reschedule_user_jobs(user_id)
        await update.message.reply_text(
            "\n".join(parts),
            parse_mode="HTML",
            reply_markup=main_menu_keyboard(),
        )

    elif state == "awaiting_forward_message":
        await handle_forward_store(update, user_id)


# ─── Media Handler (photos, videos, docs, stickers, etc.) ───────────────────
async def media_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    state = user_state.get(user_id)

    if state != "awaiting_forward_message":
        await update.message.reply_text(
            "Use /start to open the menu.",
            reply_markup=back_keyboard(),
        )
        return

    await handle_forward_store(update, user_id)


# ─── Scheduler Management ───────────────────────────────────────────────────
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
                args=[user_id, time_str],
                id=job_id,
                replace_existing=True,
                misfire_grace_time=60,
            )
            logger.info(f"Scheduled {job_id} at {time_str} for user {user_id}")
        except Exception as e:
            logger.error(f"Schedule error {time_str} user {user_id}: {e}")


# ─── App Lifecycle ───────────────────────────────────────────────────────────
async def post_init(application: Application) -> None:
    global scheduler, bot_app
    bot_app = application
    scheduler = AsyncIOScheduler()
    scheduler.start()
    logger.info("APScheduler started.")


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
    application.add_handler(CallbackQueryHandler(callback_router))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler)
    )
    application.add_handler(
        MessageHandler(
            (
                filters.PHOTO
                | filters.VIDEO
                | filters.Document.ALL
                | filters.AUDIO
                | filters.VOICE
                | filters.VIDEO_NOTE
                | filters.Sticker.ALL
                | filters.ANIMATION
                | filters.CONTACT
                | filters.LOCATION
                | filters.FORWARDED
            )
            & ~filters.COMMAND,
            media_handler,
        )
    )

    logger.info("Bot starting...")
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
