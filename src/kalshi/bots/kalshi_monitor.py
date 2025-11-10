#!/usr/bin/env python3
"""Kalshi copy trading monitor with enhanced alerts."""

import argparse
import json
import os
import signal
import sys
import time
import traceback
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Deque, List, Optional, Tuple

import requests
from dotenv import load_dotenv

# Import utilities
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.kalshi.clients.kalshi_social_client import KalshiSocialClient
from src.polymarket.utils.log_rotator import LogRotator
from src.polymarket.utils.message_formatter import BetInfo, ConvictionInfo, MessageFormatter
from src.polymarket.utils.message_router import MessageAction, MessageRouter
from src.polymarket.utils.position_tracker_state import NetPosition, PositionTracker
from src.polymarket.utils.state_manager import StateManager
from src.polymarket.utils.telegram_notifier import TelegramNotifier, UpdateStatus

load_dotenv()


@dataclass
class KalshiPosition:
    """Represents a Kalshi position."""

    market_ticker: str
    event_ticker: str
    series_ticker: str
    signed_position: int
    pnl: int
    timestamp: int
    trader_name: str = "Unknown"
    nickname: str = ""

    @property
    def formatted_time(self) -> str:
        try:
            dt = datetime.fromtimestamp(self.timestamp)
            return dt.strftime("%I:%M:%S %p")
        except (ValueError, TypeError, OSError):
            return "Invalid time"

    @property
    def side(self) -> str:
        """BUY or SELL based on position sign."""
        return "BUY" if self.signed_position > 0 else "SELL"

    @property
    def abs_position(self) -> int:
        """Absolute position size."""
        return abs(self.signed_position)

    @property
    def formatted_pnl(self) -> str:
        """Formatted P&L in dollars."""
        pnl_dollars = self.pnl / 100.0  # Convert cents to dollars
        if pnl_dollars >= 0:
            return f"+${pnl_dollars:,.2f}"
        return f"-${abs(pnl_dollars):,.2f}"

    @property
    def market_url(self) -> str:
        """Generate market URL."""
        return f"https://kalshi.com/markets/{self.event_ticker}"

    @property
    def outcome(self) -> str:
        """Extract outcome from market ticker."""
        # Market ticker format: SERIES-DATE-OUTCOME
        parts = self.market_ticker.split("-")
        if len(parts) >= 3:
            return parts[-1]
        return self.market_ticker


class KalshiMonitor:
    """Kalshi copy trading monitor for multiple traders."""

    # Timing Constants
    STALE_MESSAGE_THRESHOLD_SECONDS = 1800  # 30 minutes
    STATE_CLEANUP_INTERVAL_SECONDS = 86400  # 24 hours
    STATE_SAVE_DEBOUNCE_SECONDS = 10  # Debounce state saves
    LOG_ROTATION_CHECK_INTERVAL = 300  # Check for log rotation every 5 minutes

    # Log Rotation Settings
    LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB per log file
    LOG_BACKUP_COUNT = 7  # Keep 7 days of backups
    LOG_ROTATION_TIME_SECONDS = 86400  # Rotate every 24 hours

    # Position Thresholds
    MIN_POSITION_SIZE = 100  # Minimum position size to track (contracts)

    def __init__(
        self,
        traders: List[Tuple[str, str, Optional[int], Optional[str]]],
        poll_interval: int = 60,
        verbose: bool = False,
        log_file: str = "data/kalshi_monitor.log",
        state_file: str = "data/kalshi_monitor_state.json",
        trades_log_file: str = "data/kalshi_trades.jsonl",
        telegram_chat_id: Optional[str] = None,
    ):
        # Store traders with (nickname, name, min_position, profile_url)
        self.traders = traders

        # Create min_position lookup dict
        self.min_position_by_trader: Dict[str, Optional[int]] = {
            nickname.lower(): min_pos for nickname, _, min_pos, _ in traders
        }

        # Create profile_url lookup dict
        self.profile_url_by_trader: Dict[str, Optional[str]] = {
            nickname.lower(): profile_url for nickname, _, _, profile_url in traders
        }

        self.poll_interval = poll_interval
        self.verbose = verbose
        self.log_file = Path(log_file)
        self.state_file = Path(state_file)
        self.trades_log_file = Path(trades_log_file)

        Path("data").mkdir(parents=True, exist_ok=True)

        # Track seen positions by market_ticker
        self.seen_positions: Dict[str, Dict[str, int]] = defaultdict(dict)
        # nickname -> {market_ticker -> signed_position}

        # Initialize PositionTracker utility
        self.position_tracker = PositionTracker(verbose=verbose, logger=self._log)
        self.last_cleanup = time.time()
        self._poll_count = 0

        # Initialize MessageRouter utility
        self.message_router = MessageRouter(
            min_update_pct=5.0,  # 5% change required for update
            min_update_abs=100.0,  # OR $100 absolute change
            stale_threshold_seconds=self.STALE_MESSAGE_THRESHOLD_SECONDS,
            verbose=verbose,
            logger=self._log,
        )

        # Initialize MessageFormatter utility
        self.message_formatter = MessageFormatter(verbose=verbose, logger=self._log)

        self.total_alerts = 0
        self.start_time = time.time()
        self.last_state_cleanup = time.time()
        self.last_log_rotation_check = time.time()
        self.last_weekly_backup_cleanup = time.time()

        # Initialize log rotators
        self.main_log_rotator = LogRotator(
            log_file=self.log_file,
            max_bytes=self.LOG_MAX_BYTES,
            backup_count=self.LOG_BACKUP_COUNT,
            rotation_time_seconds=self.LOG_ROTATION_TIME_SECONDS,
            logger=lambda msg: self._log_raw(msg),
        )

        self.trades_log_rotator = LogRotator(
            log_file=self.trades_log_file,
            max_bytes=self.LOG_MAX_BYTES,
            backup_count=self.LOG_BACKUP_COUNT,
            rotation_time_seconds=self.LOG_ROTATION_TIME_SECONDS,
            logger=lambda msg: self._log_raw(msg),
        )

        # Clean up orphaned log backups on startup
        self.main_log_rotator.cleanup_old_backups()
        self.trades_log_rotator.cleanup_old_backups()

        # Initialize KalshiSocialClient
        self.api_client = KalshiSocialClient(
            verbose=verbose,
            logger=self._log,
        )

        # Initialize TelegramNotifier (with DEV_MODE support from environment)
        dev_mode = os.getenv("DEV_MODE", "false").lower() == "true"

        if dev_mode:
            telegram_bot_token = os.getenv("DEV_TELEGRAM_BOT_TOKEN")
            telegram_chat_id = (
                telegram_chat_id
                or os.getenv("DEV_TELEGRAM_CHAT_ID")
                or os.getenv("TELEGRAM_CHAT_ID")
            )
            self._log("[DEV MODE] Using development Telegram bot for testing")
        else:
            telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
            telegram_chat_id = telegram_chat_id or os.getenv("TELEGRAM_CHAT_ID")

        self.telegram = TelegramNotifier(
            bot_token=telegram_bot_token or "",
            chat_id=telegram_chat_id or "",
            session=self.api_client.session,
            stale_threshold_seconds=self.STALE_MESSAGE_THRESHOLD_SECONDS,
            verbose=verbose,
            logger=self._log,
        )

        mode_indicator = "[DEV MODE] " if dev_mode else ""
        self._log(
            f"{mode_indicator}[OK] Telegram notifications enabled (chat_id: {telegram_chat_id})"
            if self.telegram.enabled
            else f"{mode_indicator}[INFO] Telegram notifications disabled (set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)"
        )

        # Initialize StateManager
        self.state_manager = StateManager(
            state_file=self.state_file,
            debounce_seconds=self.STATE_SAVE_DEBOUNCE_SECONDS,
            verbose=verbose,
            logger=self._log,
        )

        # Validate traders and log configuration
        for nickname, name, min_pos, _ in self.traders:
            if self.api_client.validate_nickname(nickname):
                filter_note = (
                    f" (filtering: {min_pos:,}+ contracts only)" if min_pos else ""
                )
                self._log(f"[OK] Monitoring trader: {name} (@{nickname}){filter_note}")
            else:
                self._log(f"[WARNING] Could not validate trader: {name} (@{nickname})")

        self._load_state()

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _log_raw(self, message: str):
        """Write directly to log without timestamp (used by log rotators)."""
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(f"{message}\n")
        except Exception:
            pass  # Fail silently to avoid recursion

    def _log(self, message: str):
        """Write to log with timestamp and rotation."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"

        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(f"{log_entry}\n")
        except Exception as e:
            print(f"[LOG ERROR] {e}")

    def _load_state(self):
        """Load state from file."""
        data = self.state_manager.load()

        if not data:
            return

        # Load telegram messages into notifier
        telegram_data = data.get("telegram_messages", {})
        if telegram_data:
            self.telegram.load_state_from_persistence(telegram_data)

        # Clean old messages (>7 days)
        cutoff_time = datetime.now() - timedelta(days=7)
        removed = self.telegram.cleanup_old_messages(cutoff_time)
        if removed > 0:
            self._log(f"[CLEANUP] Removed {removed} old entries (>7 days)")
            self._save_state()

        message_count = len(self.telegram.messages)
        if message_count > 0 and self.verbose:
            self._log(f"[OK] Loaded {message_count} telegram message mappings")

        # Load position tracker state
        self.position_tracker.load_from_persistence(data)

        # Load seen positions
        seen_pos_data = data.get("seen_positions", {})
        if seen_pos_data:
            for nickname, positions in seen_pos_data.items():
                self.seen_positions[nickname] = positions
            if self.verbose:
                total_pos = sum(len(pos) for pos in self.seen_positions.values())
                self._log(
                    f"[OK] Loaded {total_pos} seen positions across {len(seen_pos_data)} traders"
                )

    def _save_state(self):
        """Save state to file."""
        position_state = self.position_tracker.export_for_persistence()

        data = {
            "telegram_messages": self.telegram.get_state_for_persistence(),
            **position_state,  # Includes net_positions and threshold_crossed
            "seen_positions": dict(self.seen_positions),
        }
        self.state_manager.save(data)

    def _mark_state_dirty(self):
        """Mark state dirty for debounced save."""
        self.state_manager.mark_dirty()

        # Save if debounce window has passed
        if self.state_manager.should_save():
            self._save_state()

    def _cleanup_old_state(self):
        """Remove message mappings older than 7 days."""
        cutoff_time = datetime.now() - timedelta(days=7)
        removed = self.telegram.cleanup_old_messages(cutoff_time)

        if removed > 0:
            self._log(f"[CLEANUP] Removed {removed} old state entries (>7 days)")
            self._save_state()

    def _cleanup_net_positions(self):
        """Clean up net positions and threshold flags for positions no longer being tracked."""
        tracked_keys = set(self.telegram.messages.keys())
        self.position_tracker.cleanup_orphaned_positions(tracked_keys)

    def fetch_trader_holdings(self, nickname: str) -> List[Dict]:
        """Fetch all holdings for a trader."""
        return self.api_client.fetch_all_holdings(nickname, closed_positions=False)

    def parse_holdings(
        self, holdings_data: List[Dict], nickname: str, trader_name: str
    ) -> List[KalshiPosition]:
        """Parse holdings into KalshiPosition objects."""
        positions = []
        current_time = int(time.time())

        for holding in holdings_data:
            event_ticker = holding.get("event_ticker", "")
            series_ticker = holding.get("series_ticker", "")

            for market_holding in holding.get("market_holdings", []):
                market_ticker = market_holding.get("market_ticker", "")
                signed_position = market_holding.get("signed_open_position", 0)
                pnl = market_holding.get("pnl", 0)

                # Check if position has changed
                last_position = self.seen_positions[nickname].get(market_ticker, 0)

                if signed_position == last_position:
                    continue  # No change, skip

                # Update seen positions
                if signed_position == 0:
                    # Position closed
                    if market_ticker in self.seen_positions[nickname]:
                        del self.seen_positions[nickname][market_ticker]
                else:
                    self.seen_positions[nickname][market_ticker] = signed_position

                # Create position object
                position = KalshiPosition(
                    market_ticker=market_ticker,
                    event_ticker=event_ticker,
                    series_ticker=series_ticker,
                    signed_position=signed_position,
                    pnl=pnl,
                    timestamp=current_time,
                    trader_name=trader_name,
                    nickname=nickname,
                )

                positions.append(position)

        return positions

    def _update_and_check_position(
        self, position: KalshiPosition
    ) -> Tuple[Optional[NetPosition], bool, str]:
        """
        Update position and check if should alert.

        Returns:
            (net_position, should_alert, reason)
        """
        # For Kalshi, we treat each market_ticker as a unique position
        # signed_position tells us the net contracts held
        net_pos = self.position_tracker.update_position(
            wallet=position.nickname,
            market_slug=position.event_ticker,
            outcome=position.outcome,
            side=position.side,
            shares=abs(position.signed_position),
            usdc=0,  # We don't have USDC tracking for Kalshi
        )

        min_position = self.min_position_by_trader.get(position.nickname.lower())
        is_tracked = self.telegram.has_tracked_message(
            self.position_tracker.create_position_key(
                position.nickname, position.event_ticker, position.outcome
            )
        )
        has_crossed = self.position_tracker.has_crossed_threshold(
            position.nickname, position.event_ticker, position.outcome
        )

        should_alert, reason = self.message_router.should_alert_position(
            net_pos=net_pos,
            min_shares=min_position or self.MIN_POSITION_SIZE,
            is_tracked=is_tracked,
            has_crossed_threshold=has_crossed,
        )

        if reason == "threshold_crossed":
            self.position_tracker.mark_threshold_crossed(
                position.nickname, position.event_ticker, position.outcome
            )
            self._log(
                f"[THRESHOLD CROSSED] {position.trader_name}: {abs(net_pos.shares):,.0f} contracts >= {min_position or self.MIN_POSITION_SIZE:,} threshold"
            )

        if not should_alert and self.verbose:
            self._log(f"[FILTERED] {position.trader_name}: {reason}")

        return (net_pos, should_alert, reason)

    def alert_position(self, position: KalshiPosition):
        """Send position alert."""
        net_pos, should_alert, reason = self._update_and_check_position(position)

        if not should_alert or not net_pos:
            return

        self.total_alerts += 1

        alert = (
            f"NEW POSITION FROM {position.trader_name.upper()} | "
            f"{position.side} {position.outcome} | {position.abs_position:,} contracts | "
            f"P&L: {position.formatted_pnl} | {position.event_ticker}"
        )
        self._log(alert)

        # Handle position close vs regular alert
        if reason == "position_closed":
            self._handle_position_close(position, net_pos)
        else:
            self._handle_telegram_notification(position, net_pos)

        self._log_position(position)

    def _handle_position_close(self, position: KalshiPosition, net_pos: NetPosition):
        """Send notification when trader closes position."""
        message_key = self.position_tracker.create_position_key(
            position.nickname, position.event_ticker, position.outcome
        )
        state = self.telegram.get_message_state(message_key)

        if not state:
            if self.verbose:
                self._log(f"[POSITION CLOSED] {position.trader_name} (untracked)")
            return

        profile_url = self.profile_url_by_trader.get(position.nickname.lower())
        bet_info = BetInfo(
            trader_name=position.trader_name,
            outcome=position.outcome,
            market_title=position.event_ticker,
            market_url=position.market_url,
            formatted_price="N/A",
            implied_odds="N/A",
            formatted_time=position.formatted_time,
            side=position.side,
            trader_profile_url=profile_url,
        )

        message = self.message_formatter.format_position_close(
            bet=bet_info,
            net_pos=net_pos,
            original_stake=state.total_usdc,
        )

        max_retries = 3
        for attempt in range(max_retries):
            status = self.telegram.update_message(state.message_id, message)

            if status == UpdateStatus.SUCCESS:
                self._log(f"[CLOSE] {position.trader_name}: {position.outcome}")
                self.telegram.untrack_message(message_key)
                self.position_tracker.reset_threshold(
                    position.nickname, position.event_ticker, position.outcome
                )
                self._mark_state_dirty()
                return

            elif status == UpdateStatus.MESSAGE_DELETED:
                self._log(f"[CLOSE] Message deleted, untracking position")
                self.telegram.untrack_message(message_key)
                self.position_tracker.reset_threshold(
                    position.nickname, position.event_ticker, position.outcome
                )
                self._mark_state_dirty()
                return

            elif status == UpdateStatus.NETWORK_ERROR:
                if attempt < max_retries - 1:
                    retry_delay = 1.0 * (attempt + 1)
                    if self.verbose:
                        self._log(
                            f"[RETRY] Network error, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})"
                        )
                    time.sleep(retry_delay)
                    continue
                else:
                    self._log(
                        f"[WARNING] Failed to update close message after {max_retries} attempts, untracking"
                    )
                    self.telegram.untrack_message(message_key)
                    return

            else:
                self._log(
                    f"[WARNING] Unknown error updating close message, untracking"
                )
                self.telegram.untrack_message(message_key)
                return

    def _handle_telegram_notification(
        self, position: KalshiPosition, net_pos: NetPosition
    ):
        """Unified message handling with MessageRouter and MessageFormatter."""
        if not self.telegram.enabled:
            return

        try:
            message_key = self.position_tracker.create_position_key(
                position.nickname, position.event_ticker, position.outcome
            )

            state = self.telegram.get_message_state(message_key)
            state_dict = None
            if state:
                state_dict = {
                    "total_usdc": state.total_usdc,
                    "first_time": state.first_time,
                    "update_count": state.update_count,
                    "conviction_label": state.conviction_label,
                    "message_id": state.message_id,
                }

            decision = self.message_router.decide_message_action(
                net_pos=net_pos,
                message_state=state_dict,
                current_timestamp=position.timestamp,
            )

            if decision.action == MessageAction.SKIP:
                if self.verbose:
                    self._log(f"[SKIP] {decision.reason}")
                return

            profile_url = self.profile_url_by_trader.get(position.nickname.lower())
            bet_info = BetInfo(
                trader_name=position.trader_name,
                outcome=position.outcome,
                market_title=position.event_ticker,
                market_url=position.market_url,
                formatted_price="N/A",
                implied_odds="N/A",
                formatted_time=position.formatted_time,
                side=position.side,
                trader_profile_url=profile_url,
            )

            # No conviction calculation for Kalshi (no portfolio API)
            conviction = None

            if decision.action == MessageAction.NEW:
                self._send_new_message(bet_info, net_pos, message_key, conviction)
            elif decision.action == MessageAction.UPDATE:
                self._update_message(bet_info, net_pos, message_key, state, conviction)
            elif decision.action == MessageAction.STALE_ADDITION:
                self._send_stale_addition(
                    bet_info, net_pos, message_key, state, conviction
                )

        except requests.RequestException as e:
            self._log(f"[TELEGRAM ERROR] Failed: {e}")
        except Exception as e:
            self._log(f"[TELEGRAM EXCEPTION] {e}")
            if self.verbose:
                self._log(traceback.format_exc())

    def _send_new_message(
        self,
        bet_info: BetInfo,
        net_pos: NetPosition,
        message_key: Tuple[str, str, str],
        conviction: Optional[ConvictionInfo],
    ):
        """Send new Telegram message for position."""
        message = self.message_formatter.format_new_position(
            bet=bet_info,
            net_pos=net_pos,
            portfolio_value=None,
            conviction=conviction,
        )

        display_amount = net_pos.get_display_amount()
        conviction_label = conviction.label if conviction else "MINIMAL"

        msg_id = self.telegram.send_and_track(
            message_key,
            message,
            display_amount,
            datetime.now(),
            conviction_label,
        )

        if msg_id:
            self._mark_state_dirty()
            if self.verbose:
                self._log(
                    f"[TELEGRAM] Sent new message (ID: {msg_id}, contracts: {net_pos.shares:.0f})"
                )

    def _update_message(
        self,
        bet_info: BetInfo,
        net_pos: NetPosition,
        message_key: Tuple[str, str, str],
        state: any,
        conviction: Optional[ConvictionInfo],
    ):
        """Update existing Telegram message with retry logic."""
        message = self.message_formatter.format_position_update(
            bet=bet_info,
            net_pos=net_pos,
            first_time=state.first_time,
            update_count=state.update_count + 1,
            portfolio_value=None,
            conviction=conviction,
        )

        display_amount = net_pos.get_display_amount()
        new_conviction = conviction.label if conviction else "MINIMAL"

        # Retry logic for network errors
        max_retries = 3
        for attempt in range(max_retries):
            status = self.telegram.update_and_track(
                message_key,
                state.message_id,
                message,
                display_amount,
                state.first_time,
                state.update_count + 1,
                new_conviction,
                state.conviction_label,
            )

            if status == UpdateStatus.SUCCESS:
                self._mark_state_dirty()
                if self.verbose:
                    self._log(f"[TELEGRAM] Updated message (ID: {state.message_id})")
                return

            elif status == UpdateStatus.MESSAGE_DELETED:
                # Message was deleted by user, send new message
                if self.verbose:
                    self._log(
                        f"[TELEGRAM] Message {state.message_id} deleted, sending new message"
                    )
                self.telegram.untrack_message(message_key)
                self._send_new_message(bet_info, net_pos, message_key, conviction)
                return

            elif status == UpdateStatus.NETWORK_ERROR:
                if attempt < max_retries - 1:
                    retry_delay = 1.0 * (attempt + 1)
                    if self.verbose:
                        self._log(
                            f"[RETRY] Network error, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})"
                        )
                    time.sleep(retry_delay)
                    continue
                else:
                    # Failed after retries, send new message
                    self._log(
                        f"[WARNING] Failed to update message after {max_retries} attempts, sending new message"
                    )
                    self.telegram.untrack_message(message_key)
                    self._send_new_message(bet_info, net_pos, message_key, conviction)
                    return

            else:  # UNKNOWN_ERROR
                # Don't retry on unknown errors, send new message
                if self.verbose:
                    self._log(
                        f"[TELEGRAM] Unknown error updating message, sending new message"
                    )
                self.telegram.untrack_message(message_key)
                self._send_new_message(bet_info, net_pos, message_key, conviction)
                return

    def _send_stale_addition(
        self,
        bet_info: BetInfo,
        net_pos: NetPosition,
        message_key: Tuple[str, str, str],
        state: any,
        conviction: Optional[ConvictionInfo],
    ):
        """Send new message for stale position addition."""
        message = self.message_formatter.format_stale_addition(
            bet=bet_info,
            net_pos=net_pos,
            first_time=state.first_time,
            previous_total=state.total_usdc,
            portfolio_value=None,
            conviction=conviction,
        )

        display_amount = net_pos.get_display_amount()
        conviction_label = conviction.label if conviction else "MINIMAL"

        msg_id = self.telegram.send_and_track(
            message_key,
            message,
            display_amount,
            datetime.now(),
            conviction_label,
        )

        if msg_id:
            self._mark_state_dirty()
            if self.verbose:
                self._log(
                    f"[TELEGRAM] Sent stale addition message (ID: {msg_id}, contracts: {net_pos.shares:.0f})"
                )

    def _log_position(self, position: KalshiPosition):
        """Log position to JSONL with rotation."""
        try:
            with open(self.trades_log_file, "a", encoding="utf-8") as f:
                log_entry = {
                    "timestamp": datetime.now().isoformat(),
                    "position": asdict(position),
                    "formatted_time": position.formatted_time,
                    "formatted_pnl": position.formatted_pnl,
                }
                f.write(json.dumps(log_entry) + "\n")
        except Exception as e:
            if self.verbose:
                self._log(f"[DEBUG] Position logging failed: {e}")

    def _check_and_rotate_logs(self):
        """Check and rotate logs if needed."""
        current_time = time.time()

        if (
            current_time - self.last_log_rotation_check
            > self.LOG_ROTATION_CHECK_INTERVAL
        ):
            self.main_log_rotator.check_and_rotate()
            self.trades_log_rotator.check_and_rotate()

            # Weekly cleanup of orphaned backups
            time_since_weekly_cleanup = current_time - self.last_weekly_backup_cleanup
            if time_since_weekly_cleanup > 604800:
                self.main_log_rotator.cleanup_old_backups()
                self.trades_log_rotator.cleanup_old_backups()
                self.last_weekly_backup_cleanup = current_time

            self.last_log_rotation_check = current_time

    def _send_startup_message(self):
        """Send startup notification."""
        trader_list = [(name, min_pos) for _, name, min_pos, _ in self.traders]
        self.telegram.send_kalshi_startup_message(
            num_traders=len(self.traders),
            poll_interval=self.poll_interval,
            trader_list=trader_list,
        )

    def _send_shutdown_message(self):
        """Send shutdown notification."""
        uptime_seconds = int(time.time() - self.start_time)
        self.telegram.send_kalshi_shutdown_message(
            uptime_seconds=uptime_seconds, total_alerts=self.total_alerts
        )

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals."""
        self._log("\n[SHUTDOWN] Received signal, stopping bot...")
        self._send_shutdown_message()

        # Final state save on shutdown
        self._log("[SHUTDOWN] Saving final state...")
        try:
            position_state = self.position_tracker.export_for_persistence()

            self.state_manager.force_save(
                {
                    "telegram_messages": self.telegram.get_state_for_persistence(),
                    **position_state,
                    "seen_positions": dict(self.seen_positions),
                }
            )
        except Exception as e:
            self._log(f"[SHUTDOWN ERROR] Failed to save state: {e}")

        self._cleanup_resources()
        self._log("[SHUTDOWN] Cleanup complete, exiting")
        sys.exit(0)

    def _cleanup_resources(self):
        """Clean up resources."""
        try:
            if hasattr(self, "api_client") and self.api_client:
                self.api_client.close()
        except Exception as e:
            self._log(f"[WARNING] Error closing API client: {e}")

    def run(self):
        """Run monitoring loop."""
        self._log(f"Monitoring {len(self.traders)} trader(s)")
        self._log(f"Polling every {self.poll_interval} seconds")
        self._log(f"Log file: {self.log_file}")
        self._log("=" * 64)
        self._log("Loading current positions to establish baseline...")

        for nickname, name, _, _ in self.traders:
            try:
                holdings = self.fetch_trader_holdings(nickname)
                position_count = 0
                for holding in holdings:
                    for market_holding in holding.get("market_holdings", []):
                        market_ticker = market_holding.get("market_ticker", "")
                        signed_position = market_holding.get("signed_open_position", 0)
                        if signed_position != 0:
                            self.seen_positions[nickname][
                                market_ticker
                            ] = signed_position
                            position_count += 1

                self._log(f"  {name}: Loaded {position_count} active position(s)")
            except Exception as e:
                self._log(f"  WARNING: {name}: Could not load baseline: {e}")
                if self.verbose:
                    self._log(traceback.format_exc())

        self._log("=" * 64)
        self._log("MONITORING ACTIVE - Waiting for position changes...")
        print(
            f"\nMonitoring {len(self.traders)} traders. Check {self.log_file} for output.\n"
        )

        self._send_startup_message()

        while True:
            try:
                all_new_positions = []

                for nickname, name, _, _ in self.traders:
                    try:
                        holdings = self.fetch_trader_holdings(nickname)
                        positions = self.parse_holdings(holdings, nickname, name)
                        all_new_positions.extend(positions)
                    except Exception as e:
                        self._log(f"[ERROR] {name}: {e}")
                        if self.verbose:
                            self._log(traceback.format_exc())

                for position in sorted(all_new_positions, key=lambda p: p.timestamp):
                    self.alert_position(position)

                current_time = time.time()

                # Check and rotate logs
                self._check_and_rotate_logs()

                # Cleanup old state
                if (
                    current_time - self.last_state_cleanup
                    > self.STATE_CLEANUP_INTERVAL_SECONDS
                ):
                    self._cleanup_old_state()
                    self.last_state_cleanup = current_time

                # Cleanup positions
                if (
                    current_time - self.last_cleanup
                    > self.STATE_CLEANUP_INTERVAL_SECONDS
                ):
                    self._cleanup_net_positions()
                    self.last_cleanup = current_time

                # Final state save at end of poll if dirty
                if self.state_manager.is_dirty():
                    self._save_state()

                # Reduced logging: only log every 10 polls or when verbose
                self._poll_count += 1

                if self.verbose or self._poll_count % 10 == 0:
                    uptime = int(time.time() - self.start_time)
                    self._log(
                        f"[{datetime.now().strftime('%H:%M:%S')}] Poll complete | "
                        f"Alerts: {self.total_alerts} | Uptime: {uptime}s"
                    )

                time.sleep(self.poll_interval)

            except Exception as e:
                self._log(f"[ERROR] {e}")
                if self.verbose:
                    self._log(traceback.format_exc())
                time.sleep(self.poll_interval)


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description="Monitor Kalshi traders for copy trading",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single trader (legacy mode)
  python -m src.kalshi.bots.kalshi_monitor --nickname trader1 --name "Trader1"

  # Multiple traders from config file
  python -m src.kalshi.bots.kalshi_monitor --config config/kalshi_trader_list.json

  # Config file format:
  {
    "traders": [
      {"nickname": "trader1", "name": "Trader1"},
      {"nickname": "trader2", "name": "Trader2", "min_position": 500}
    ]
  }

  # Optional min_position field: only alert when trader takes positions >= min_position contracts
""",
    )

    parser.add_argument(
        "--config",
        "-c",
        help="JSON config file with multiple traders",
    )

    parser.add_argument(
        "--nickname",
        "-u",
        help="Single trader nickname to monitor (legacy mode)",
    )

    parser.add_argument(
        "--name",
        "-n",
        default="Friend",
        help="Trader name for single nickname (default: Friend)",
    )

    parser.add_argument(
        "--poll-interval",
        "-p",
        type=int,
        default=60,
        help="Seconds between polls (default: 60)",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    parser.add_argument(
        "--log-file",
        "-l",
        default="data/kalshi_monitor.log",
        help="Log file path (default: data/kalshi_monitor.log)",
    )

    parser.add_argument(
        "--telegram-chat-id",
        help="Override Telegram chat ID from .env (for personal vs group routing)",
    )

    parser.add_argument(
        "--state-file",
        default="data/kalshi_monitor_state.json",
        help="State file path (default: data/kalshi_monitor_state.json)",
    )

    parser.add_argument(
        "--trades-log-file",
        default="data/kalshi_trades.jsonl",
        help="Trades log file path (default: data/kalshi_trades.jsonl)",
    )

    args = parser.parse_args()

    traders = []

    if args.config:
        try:
            with open(args.config, "r", encoding="utf-8") as f:
                config = json.load(f)
                for trader_config in config.get("traders", []):
                    nickname = trader_config.get("nickname", "").strip()
                    name = trader_config.get("name", "Unknown").strip()
                    min_position = trader_config.get("min_position")
                    profile_url = trader_config.get("profile_url")

                    # Validate min_position
                    if min_position is not None:
                        if not isinstance(min_position, int):
                            raise ValueError(
                                f"min_position must be integer for {name}, got {type(min_position).__name__}"
                            )
                        if min_position < 0:
                            raise ValueError(
                                f"min_position must be non-negative for {name}, got {min_position}"
                            )

                    if nickname:
                        traders.append((nickname, name, min_position, profile_url))
            print(f"Loaded {len(traders)} trader(s) from {args.config}")
        except Exception as e:
            print(f"ERROR: Failed to load config file: {e}")
            return

    elif args.nickname:
        traders = [(args.nickname, args.name, None, None)]

    else:
        parser.error("Must provide either --config or --nickname")

    if not traders:
        print("ERROR: No traders to monitor")
        return

    monitor = KalshiMonitor(
        traders=traders,
        poll_interval=args.poll_interval,
        verbose=args.verbose,
        log_file=args.log_file,
        state_file=args.state_file,
        trades_log_file=args.trades_log_file,
        telegram_chat_id=args.telegram_chat_id,
    )

    monitor.run()


if __name__ == "__main__":
    main()
