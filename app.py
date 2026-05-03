#!/usr/bin/env python3
"""
CJ Stream 5 — DOUBLER
======================
Monitors Pump.fun WebSocket for new token launches.
Buys within 90 seconds of launch if momentum confirms.
Target: 2x ($2 -> $4). No stop loss. Free ride after 2x.

Usage:
    python stream5_doubler.py

Runs independently alongside cj_bot.py
"""

import os
import sys
import json
import time
import base64
import asyncio
import requests
import traceback
from datetime import datetime, timezone
from colorama import Fore, Style, init
from dotenv import load_dotenv

import websockets
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
import base58

init(autoreset=True)
load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────
WALLET_PRIVATE_KEY  = os.getenv("WALLET_PRIVATE_KEY", "")
HELIUS_RPC          = os.getenv("HELIUS_RPC", "https://mainnet.helius-rpc.com/?api-key=2df091e9-522b-4529-86a9-b934764183e8")
FLUX_RPC            = os.getenv("FLUX_RPC", "https://api.mainnet-beta.solana.com")
RUGCHECK_API_KEY    = os.getenv("RUGCHECK_API_KEY", "")
AUD_TO_USD          = float(os.getenv("AUD_TO_USD", "0.65"))

POSITION_AUD        = 2.0         # Fixed $2 per trade
TARGET_PCT          = 100.0       # 2x target
FREE_RIDE_STOP_PCT  = 20.0        # Trailing stop on free ride
MAX_HOLD_HOURS      = 12          # Max hold time
RUGCHECK_MAX_SCORE  = 20          # Strict — under 20 only
MIN_BUY_PRESSURE    = 55          # Min buy % before entering
CONFIRM_WAIT_SEC    = 30          # Wait 90 seconds for momentum confirm
MAX_OPEN_POSITIONS  = 10          # Max simultaneous $2 positions
DAILY_MAX_SPEND_AUD = 20.0        # Max $20/day on Stream 5

# Pump.fun WebSocket
PUMP_WS_URL = "wss://pumpportal.fun/api/data"

# Jupiter & DexScreener
JUPITER_QUOTE_API = "https://jupiter-prox.onrender.com"
JUPITER_SWAP_API  = "https://jupiter-prox.onrender.com/swap"
DEXSCREENER_BASE  = "https://api.dexscreener.com"
RUGCHECK_BASE     = "https://api.rugcheck.xyz/v1"
SOL_MINT          = "So11111111111111111111111111111111111111112"

STATE_FILE        = "stream5_state.json"
TRADE_LOG_FILE    = "stream5_trades.json"

# ── STATE ─────────────────────────────────────────────────────────────────────
state = {
    "open_positions": {},
    "closed_trades": [],
    "pending_tokens": {},   # Tokens waiting for momentum confirm
    "daily_spent_aud": 0.0,
    "daily_profit_aud": 0.0,
    "daily_loss_aud": 0.0,
    "total_trades": 0,
    "wins": 0,
    "losses": 0,
    "free_rides": 0,
    "tokens_seen": 0,
    "tokens_rugchecked": 0,
    "tokens_bought": 0,
}

# ── WALLET ────────────────────────────────────────────────────────────────────
def load_wallet():
    if not WALLET_PRIVATE_KEY:
        print(f"{Fore.RED}  ✗ No WALLET_PRIVATE_KEY in .env")
        sys.exit(1)
    try:
        key_bytes = base58.b58decode(WALLET_PRIVATE_KEY)
        return Keypair.from_bytes(key_bytes)
    except Exception as e:
        print(f"{Fore.RED}  ✗ Invalid wallet key: {e}")
        sys.exit(1)

# ── FORMATTING ────────────────────────────────────────────────────────────────
def fmt_aud(n):
    return f"${n:.2f} AUD"

def fmt_usd(n):
    if abs(n) >= 1_000_000:
        return f"${n/1_000_000:.2f}M"
    elif abs(n) >= 1_000:
        return f"${n/1_000:.1f}K"
    return f"${n:.8f}"

def color_pct(pct):
    s = f"{pct:+.1f}%"
    if pct >= 50:   return Fore.GREEN + Style.BRIGHT + s + Style.RESET_ALL
    elif pct > 0:   return Fore.GREEN + s + Style.RESET_ALL
    elif pct <= -20: return Fore.RED + Style.BRIGHT + s + Style.RESET_ALL
    elif pct < 0:   return Fore.RED + s + Style.RESET_ALL
    return s

def ts():
    return datetime.now().strftime('%H:%M:%S')

def header():
    wins = state["wins"]
    losses = state["losses"]
    total = wins + losses
    wr = f"{wins/total*100:.0f}%" if total > 0 else "—"
    net = state["daily_profit_aud"] - state["daily_loss_aud"]
    net_str = f"+{fmt_aud(net)}" if net >= 0 else fmt_aud(net)
    print()
    print(Fore.YELLOW + Style.BRIGHT + "═" * 65)
    print(Fore.YELLOW + Style.BRIGHT + "  STREAM 5 — DOUBLER  |  Pump.fun Live Feed")
    print(Fore.YELLOW + Style.BRIGHT + "═" * 65)
    print(f"  Tokens seen: {state['tokens_seen']} | "
          f"Checked: {state['tokens_rugchecked']} | "
          f"Bought: {state['tokens_bought']}")
    print(f"  Win rate: {wr} ({wins}W/{losses}L) | "
          f"Free rides: {state['free_rides']} | "
          f"Net: {net_str}")
    print(f"  Open: {len(state['open_positions'])}/{MAX_OPEN_POSITIONS} | "
          f"Spent today: {fmt_aud(state['daily_spent_aud'])}/{fmt_aud(DAILY_MAX_SPEND_AUD)}")
    print(Fore.YELLOW + "─" * 65)

# ── RUGCHECK ──────────────────────────────────────────────────────────────────
def check_rugcheck(contract):
    """Fast RugCheck — returns (score, passed, reason)"""
    try:
        r = requests.get(
            f"{RUGCHECK_BASE}/tokens/{contract}/report/summary",
            headers={"Accept": "application/json"},
            timeout=8
        )
        if r.status_code == 200:
            data = r.json()
            raw_score = data.get("score", 1000)
            score = raw_score / 10 if raw_score > 100 else raw_score

            # Critical risk flags
            risks = data.get("risks", [])
            for risk in risks:
                name  = risk.get("name", "").lower()
                level = risk.get("level", "").lower()
                if level == "danger" and any(k in name for k in ["rug", "mint", "freeze", "honeypot"]):
                    return score, False, f"Danger: {name}"

            # Holder concentration
            top_holders = data.get("topHolders", [])
            for holder in top_holders[:3]:
                pct   = holder.get("pct", 0)
                owner = holder.get("owner", "")
                if pct > 20 and "pump" not in owner.lower():
                    return score, False, f"Holder {pct:.0f}% too concentrated"

            # Creator still holding
            creator_pct = 0
            for h in top_holders:
                if h.get("isCreator"):
                    creator_pct = h.get("pct", 0)
            if creator_pct > 5:
                return score, False, f"Creator still holds {creator_pct:.0f}%"

            if score > RUGCHECK_MAX_SCORE:
                return score, False, f"Score {score:.0f} > {RUGCHECK_MAX_SCORE} limit"

            return score, True, "Passed"

        elif r.status_code == 429:
            return 999, False, "Rate limited"
        else:
            return 999, False, f"API error {r.status_code}"

    except Exception as e:
        return 999, False, f"Exception: {e}"

# ── MOMENTUM CHECK ────────────────────────────────────────────────────────────
def check_momentum(contract):
    """
    Check if token has buy momentum after 90 seconds.
    Returns (price, buy_pressure_pct, has_momentum)
    """
    try:
        r = requests.get(
            f"{DEXSCREENER_BASE}/tokens/v1/solana/{contract}",
            timeout=8
        )
        if r.status_code == 200:
            data = r.json()
            if not isinstance(data, list) or not data:
                return None, 0, False

            # Get highest liquidity pair
            pairs = [p for p in data if p.get("priceUsd")]
            if not pairs:
                return None, 0, False
            pairs.sort(
                key=lambda p: (p.get("liquidity") or {}).get("usd", 0),
                reverse=True
            )
            pair = pairs[0]

            price   = float(pair.get("priceUsd") or 0)
            txns    = (pair.get("txns") or {}).get("m5") or {}  # Last 5 min
            buys    = txns.get("buys", 0) or 0
            sells   = txns.get("sells", 0) or 0
            total   = buys + sells
            buy_pct = (buys / total * 100) if total > 0 else 0
            liq     = (pair.get("liquidity") or {}).get("usd", 0) or 0
            chg_5m  = (pair.get("priceChange") or {}).get("m5", 0) or 0

            # Momentum conditions — simplified for new pump.fun tokens
            # New tokens start at fixed price so don't require price movement
            has_momentum = (
                liq >= 1_000 and
                buy_pct >= MIN_BUY_PRESSURE
            )

            return price, buy_pct, has_momentum

    except Exception:
        pass
    return None, 0, False

# ── JUPITER HELPERS ───────────────────────────────────────────────────────────
def get_sol_price():
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
            timeout=5
        )
        if r.status_code == 200:
            return r.json().get("solana", {}).get("usd", 140.0)
    except Exception:
        pass
    return 140.0

def aud_to_lamports(amount_aud):
    sol_price = get_sol_price()
    amount_usd = amount_aud * AUD_TO_USD
    sol_amount = amount_usd / sol_price
    return int(sol_amount * 1_000_000_000)

def get_jupiter_quote(input_mint, output_mint, amount, slippage_bps=500):
    try:
        r = requests.get(
            f"{JUPITER_QUOTE_API}/quote",
            params={
                "inputMint": input_mint,
                "outputMint": output_mint,
                "amount": amount,
                "slippageBps": slippage_bps,
            },
            timeout=15
        )
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"{Fore.YELLOW}  Quote error: {e}")
    return None

def execute_swap(keypair, quote_response):
    try:
        wallet_pubkey = str(keypair.pubkey())
        swap_payload = {
            "quoteResponse": quote_response,
            "userPublicKey": wallet_pubkey,
            "wrapAndUnwrapSol": True,
            "dynamicComputeUnitLimit": True,
            "prioritizationFeeLamports": 2000,
        }
        r = requests.post(
            JUPITER_SWAP_API,
            json=swap_payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        if r.status_code != 200:
            print(f"{Fore.RED}  Swap error: {r.status_code}")
            return None

        swap_tx = r.json().get("swapTransaction")
        if not swap_tx:
            return None

        tx_bytes = base64.b64decode(swap_tx)
        tx = VersionedTransaction.from_bytes(tx_bytes)

        # Sign properly
        signature = keypair.sign_message(bytes(tx.message))
        signed_tx = VersionedTransaction.populate(tx.message, [signature])

        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "sendTransaction",
            "params": [
                base64.b64encode(bytes(signed_tx)).decode("utf-8"),
                {"encoding": "base64", "preflightCommitment": "confirmed", "skipPreflight": True}
            ]
        }
        rpc_url = HELIUS_RPC if "HELIUS_RPC" in dir() else FLUX_RPC
        rpc_r = requests.post(rpc_url, json=payload, timeout=30)
        if rpc_r.status_code == 200:
            result = rpc_r.json()
            return result.get("result")

    except Exception as e:
        print(f"{Fore.RED}  Swap exception: {e}")
    return None

# ── BUY ───────────────────────────────────────────────────────────────────────
def buy_token(keypair, contract, symbol, price):
    """Execute $2 buy on a new token."""
    print(f"  {Fore.GREEN + Style.BRIGHT}BUYING {symbol}{Style.RESET_ALL} @ {fmt_usd(price)}")

    lamports = aud_to_lamports(POSITION_AUD)
    quote = get_jupiter_quote(SOL_MINT, contract, lamports, slippage_bps=1000)

    if not quote:
        print(f"  {Fore.RED}✗ No quote available")
        return False

    out_amount = int(quote.get("outAmount", 0))
    if out_amount == 0:
        print(f"  {Fore.RED}✗ Zero output amount")
        return False

    tx_sig = execute_swap(keypair, quote)
    if not tx_sig:
        print(f"  {Fore.RED}✗ Swap failed")
        return False

    tokens = out_amount / 1_000_000

    state["open_positions"][contract] = {
        "symbol":      symbol,
        "contract":    contract,
        "tokens":      tokens,
        "entry_price": price,
        "entry_aud":   POSITION_AUD,
        "opened_at":   datetime.now(timezone.utc).isoformat(),
        "peak_price":  price,
        "free_ride":   False,
        "tx_buy":      tx_sig,
    }
    state["daily_spent_aud"] += POSITION_AUD
    state["total_trades"] += 1
    state["tokens_bought"] += 1
    save_state()

    print(f"  {Fore.GREEN}✓ Bought {symbol} — {fmt_aud(POSITION_AUD)} | TX: {tx_sig[:24]}...")
    print(f"  Target: 2x (+100%) | No stop loss | Free ride after 2x")
    return True

# ── PUMP.FUN FALLBACK SELL ────────────────────────────────────────────────────
def sell_via_pumpfun(keypair, contract, tokens_to_sell):
    """Fallback sell via pump.fun API for pre-graduation tokens."""
    try:
        wallet_pubkey = str(keypair.pubkey())
        token_amount = int(tokens_to_sell * 1_000_000)
        r = requests.post(
            "https://jupiter-prox.onrender.com/pump-trade",
            headers={"Content-Type": "application/json"},
            json={
                "publicKey": wallet_pubkey,
                "action": "sell",
                "mint": contract,
                "amount": token_amount,
                "denominatedInSol": "false",
                "slippage": 15,
                "priorityFee": 0.00002,
                "pool": "pump"
            },
            timeout=15
        )
        if r.status_code != 200:
            return None
        tx_bytes = r.content
        tx = VersionedTransaction.from_bytes(tx_bytes)
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "sendTransaction",
            "params": [
                base64.b64encode(bytes(tx)).decode("utf-8"),
                {"encoding": "base64", "preflightCommitment": "confirmed", "skipPreflight": True}
            ]
        }
        rpc_url = HELIUS_RPC if "HELIUS_RPC" in dir() else FLUX_RPC
        rpc_r = requests.post(rpc_url, json=payload, timeout=30)
        if rpc_r.status_code == 200:
            return rpc_r.json().get("result")
    except Exception as e:
        print(f"{Fore.YELLOW}  Pump.fun sell error: {e}")
    return None


# ── SELL ──────────────────────────────────────────────────────────────────────
def sell_token(keypair, contract, reason, partial=False):
    """Sell a position — full or partial (50%). Jupiter first, pump.fun fallback."""
    if contract not in state["open_positions"]:
        return

    pos    = state["open_positions"][contract]
    symbol = pos["symbol"]

    # Get current price
    try:
        r = requests.get(
            f"{DEXSCREENER_BASE}/tokens/v1/solana/{contract}",
            timeout=8
        )
        if r.status_code == 200:
            data = r.json()
            pairs = [p for p in data if p.get("priceUsd")]
            if pairs:
                pairs.sort(
                    key=lambda p: (p.get("liquidity") or {}).get("usd", 0),
                    reverse=True
                )
                current_price = float(pairs[0]["priceUsd"])
            else:
                current_price = pos["entry_price"]
        else:
            current_price = pos["entry_price"]
    except Exception:
        current_price = pos["entry_price"]

    entry_price = pos["entry_price"]
    entry_aud   = pos["entry_aud"]
    pct_change  = ((current_price - entry_price) / entry_price) * 100

    tokens_to_sell = pos["tokens"] * 0.5 if partial else pos["tokens"]
    token_amount   = int(tokens_to_sell * 1_000_000)

    quote = get_jupiter_quote(contract, SOL_MINT, token_amount, slippage_bps=1000)
    if not quote:
        print(f"  {Fore.YELLOW}⚠ Could not get sell quote for {symbol}")
        return

    tx_sig = execute_swap(keypair, quote)
    if not tx_sig:
        print(f"  {Fore.YELLOW}  Jupiter failed — trying pump.fun direct...")
        tx_sig = sell_via_pumpfun(keypair, contract, tokens_to_sell)
        if not tx_sig:
            print(f"  {Fore.RED}✗ Both sell routes failed for {symbol} — will retry next cycle")
            return
        print(f"  {Fore.GREEN}  Pump.fun fallback sell succeeded")

    # P&L calculation
    sol_received  = int(quote.get("outAmount", 0)) / 1_000_000_000
    proceeds_usd  = sol_received * get_sol_price()
    proceeds_aud  = proceeds_usd / AUD_TO_USD
    cost_basis    = entry_aud * (0.5 if partial else 1.0)
    pnl_aud       = proceeds_aud - cost_basis

    # Update state
    trade = {
        "symbol":      symbol,
        "contract":    contract,
        "entry_price": entry_price,
        "exit_price":  current_price,
        "entry_aud":   cost_basis,
        "exit_aud":    proceeds_aud,
        "pnl_aud":     pnl_aud,
        "pnl_pct":     pct_change,
        "reason":      reason,
        "partial":     partial,
        "tx_sig":      tx_sig,
        "opened_at":   pos["opened_at"],
        "closed_at":   datetime.now(timezone.utc).isoformat(),
    }
    state["closed_trades"].append(trade)

    if pnl_aud > 0:
        state["wins"] += 1
        state["daily_profit_aud"] += pnl_aud
    else:
        state["losses"] += 1
        state["daily_loss_aud"] += abs(pnl_aud)

    if partial:
        # Update remaining position as free ride
        state["open_positions"][contract]["tokens"]      -= tokens_to_sell
        state["open_positions"][contract]["entry_aud"]   -= cost_basis
        state["open_positions"][contract]["free_ride"]   = True
        state["open_positions"][contract]["peak_price"]  = max(
            current_price, pos.get("peak_price", current_price)
        )
        state["free_rides"] += 1
        print(f"  {Fore.GREEN + Style.BRIGHT}🎯 2x HIT — {symbol}! "
              f"{fmt_aud(pnl_aud)} banked. Remainder rides FREE.{Style.RESET_ALL}")
    else:
        del state["open_positions"][contract]
        result = "PROFIT" if pnl_aud >= 0 else "LOSS"
        col    = Fore.GREEN if pnl_aud >= 0 else Fore.RED
        print(f"  {col}{result}: {symbol} {pct_change:+.1f}% | "
              f"{fmt_aud(pnl_aud)} | {reason}{Style.RESET_ALL}")

    save_state()

# ── POSITION MONITOR ──────────────────────────────────────────────────────────
def monitor_positions(keypair):
    """Check all open positions."""
    if not state["open_positions"]:
        return

    for contract, pos in list(state["open_positions"].items()):
        symbol      = pos["symbol"]
        entry_price = pos["entry_price"]
        free_ride   = pos.get("free_ride", False)
        peak_price  = pos.get("peak_price", entry_price)
        opened_at   = datetime.fromisoformat(pos["opened_at"].replace("Z", "+00:00"))
        age_hours   = (datetime.now(timezone.utc) - opened_at).total_seconds() / 3600

        try:
            r = requests.get(
                f"{DEXSCREENER_BASE}/tokens/v1/solana/{contract}",
                timeout=8
            )
            if r.status_code != 200:
                continue
            data = r.json()
            pairs = [p for p in data if p.get("priceUsd")]
            if not pairs:
                continue
            pairs.sort(
                key=lambda p: (p.get("liquidity") or {}).get("usd", 0),
                reverse=True
            )
            current_price = float(pairs[0]["priceUsd"])
        except Exception:
            continue

        pct_change = ((current_price - entry_price) / entry_price) * 100
        peak_price = max(current_price, peak_price)
        state["open_positions"][contract]["peak_price"] = peak_price

        status = "FREE RIDE" if free_ride else "HOLDING"
        col    = Fore.GREEN if free_ride else Fore.CYAN
        print(f"  {col}{symbol}{Style.RESET_ALL} [{status}] "
              f"{color_pct(pct_change)} | "
              f"Age: {age_hours:.1f}h")

        if free_ride:
            # Trailing stop -20% from peak on free ride
            drop_from_peak = ((current_price - peak_price) / peak_price) * 100
            if drop_from_peak <= -FREE_RIDE_STOP_PCT:
                sell_token(keypair, contract,
                          f"Free ride trailing stop -{FREE_RIDE_STOP_PCT}%")
            elif age_hours >= MAX_HOLD_HOURS:
                sell_token(keypair, contract, f"Max hold {MAX_HOLD_HOURS}h")
        else:
            # Hit 2x — partial sell, let rest ride
            if pct_change >= TARGET_PCT:
                sell_token(keypair, contract,
                          f"2x target hit", partial=True)
            # Time exit
            elif age_hours >= MAX_HOLD_HOURS:
                sell_token(keypair, contract, f"Max hold {MAX_HOLD_HOURS}h")

        time.sleep(0.5)

# ── PENDING TOKEN PROCESSOR ───────────────────────────────────────────────────
async def process_pending(keypair):
    """
    Check pending tokens after 90 second wait.
    If momentum confirmed — buy.
    """
    now = time.time()
    for contract, data in list(state["pending_tokens"].items()):
        wait_until = data["wait_until"]
        if now < wait_until:
            continue  # Still waiting

        symbol = data["symbol"]
        del state["pending_tokens"][contract]

        # Already bought or at max positions
        if contract in state["open_positions"]:
            continue
        if len(state["open_positions"]) >= MAX_OPEN_POSITIONS:
            print(f"  {Fore.YELLOW}Max positions open — skipping {symbol}")
            continue
        if state["daily_spent_aud"] >= DAILY_MAX_SPEND_AUD:
            print(f"  {Fore.YELLOW}Daily spend limit reached")
            continue

        print(f"\n  [{ts()}] {Fore.CYAN}Momentum check: {symbol}{Style.RESET_ALL}")

        # Check momentum
        price, buy_pct, has_momentum = check_momentum(contract)
        if not price:
            print(f"  {Fore.YELLOW}✗ No price data yet — skipping")
            continue

        print(f"  Price: {fmt_usd(price)} | Buy pressure: {buy_pct:.0f}%", end="")

        if not has_momentum:
            print(f" {Fore.RED}✗ No momentum{Style.RESET_ALL}")
            continue

        print(f" {Fore.GREEN}✓ Momentum confirmed{Style.RESET_ALL}")

        # RugCheck
        print(f"  RugChecking {contract[:16]}...", end="", flush=True)
        rug_score, rug_passed, rug_reason = check_rugcheck(contract)
        state["tokens_rugchecked"] += 1

        if not rug_passed:
            print(f" {Fore.RED}✗ {rug_reason}{Style.RESET_ALL}")
            continue

        print(f" {Fore.GREEN}✓ Score: {rug_score:.0f}{Style.RESET_ALL}")

        # Buy
        buy_token(keypair, contract, symbol, price)

# ── PUMP.FUN WEBSOCKET ────────────────────────────────────────────────────────
async def pump_listener(keypair):
    """
    Connect to Pump.fun WebSocket and listen for new token launches.
    """
    print(f"\n  {Fore.GREEN}Connecting to Pump.fun live feed...{Style.RESET_ALL}")

    reconnect_delay = 5

    while True:
        try:
            async with websockets.connect(
                PUMP_WS_URL,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                print(f"  {Fore.GREEN}✓ Connected to Pump.fun WebSocket{Style.RESET_ALL}")
                reconnect_delay = 5  # Reset on successful connect

                # Subscribe to new token events
                await ws.send(json.dumps({
                    "method": "subscribeNewToken"
                }))
                print(f"  {Fore.CYAN}Subscribed to new token feed — watching for launches...{Style.RESET_ALL}")

                header()

                async for message in ws:
                    try:
                        data = json.loads(message)

                        # New token event
                        if data.get("txType") == "create" or "mint" in data:
                            contract = data.get("mint", "")
                            symbol   = data.get("symbol", "UNKNOWN")
                            name     = data.get("name", "")

                            if not contract:
                                continue

                            state["tokens_seen"] += 1

                            print(f"\n  [{ts()}] 🆕 {Fore.CYAN + Style.BRIGHT}{symbol}{Style.RESET_ALL} "
                                  f"({name[:20]}) — {contract[:16]}...")

                            # Skip if already seen
                            if (contract in state["open_positions"] or
                                contract in state["pending_tokens"]):
                                continue

                            # Skip if at limits
                            if len(state["open_positions"]) >= MAX_OPEN_POSITIONS:
                                print(f"  {Fore.YELLOW}Max positions — skipping")
                                continue
                            if state["daily_spent_aud"] >= DAILY_MAX_SPEND_AUD:
                                print(f"  {Fore.YELLOW}Daily limit reached — pausing")
                                continue

                            # Queue for momentum check after 90 seconds
                            state["pending_tokens"][contract] = {
                                "symbol":     symbol,
                                "name":       name,
                                "seen_at":    time.time(),
                                "wait_until": time.time() + CONFIRM_WAIT_SEC,
                            }
                            print(f"  ⏳ Queued — momentum check in {CONFIRM_WAIT_SEC}s")

                        # Process pending tokens
                        await process_pending(keypair)

                        # Monitor open positions every 30 events
                        if state["tokens_seen"] % 30 == 0:
                            monitor_positions(keypair)
                            header()

                    except json.JSONDecodeError:
                        pass
                    except Exception as e:
                        print(f"{Fore.YELLOW}  Event error: {e}")

        except websockets.exceptions.ConnectionClosed as e:
            print(f"\n  {Fore.YELLOW}WebSocket disconnected: {e}")
            print(f"  Reconnecting in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

        except Exception as e:
            print(f"\n  {Fore.RED}WebSocket error: {e}")
            print(f"  Reconnecting in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

# ── BACKGROUND MONITOR ────────────────────────────────────────────────────────
async def background_monitor(keypair):
    """Monitor open positions every 2 minutes."""
    while True:
        await asyncio.sleep(120)
        if state["open_positions"]:
            print(f"\n  [{ts()}] {Fore.CYAN}Monitoring {len(state['open_positions'])} positions...{Style.RESET_ALL}")
            monitor_positions(keypair)
        # Also check pending tokens
        await process_pending(keypair)

# ── STATE PERSISTENCE ─────────────────────────────────────────────────────────
def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
        with open(TRADE_LOG_FILE, "w") as f:
            json.dump(state["closed_trades"], f, indent=2)
    except Exception as e:
        print(f"{Fore.YELLOW}  Warning: Could not save state: {e}")

def load_state_from_file():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                saved = json.load(f)
                state["closed_trades"] = saved.get("closed_trades", [])
                state["total_trades"]  = saved.get("total_trades", 0)
                state["wins"]          = saved.get("wins", 0)
                state["losses"]        = saved.get("losses", 0)
                state["free_rides"]    = saved.get("free_rides", 0)
                # Restore open positions from previous session
                state["open_positions"] = saved.get("open_positions", {})
                print(f"  {Fore.CYAN}Restored: {len(state['open_positions'])} open positions, "
                      f"{state['total_trades']} total trades{Style.RESET_ALL}")
        except Exception:
            pass

def print_summary():
    print()
    print(Fore.YELLOW + Style.BRIGHT + "═" * 65)
    print(Fore.YELLOW + Style.BRIGHT + "  STREAM 5 SESSION SUMMARY")
    print(Fore.YELLOW + Style.BRIGHT + "═" * 65)
    total = state["wins"] + state["losses"]
    wr    = f"{state['wins']/total*100:.0f}%" if total > 0 else "—"
    net   = state["daily_profit_aud"] - state["daily_loss_aud"]
    print(f"  Tokens seen:     {state['tokens_seen']}")
    print(f"  RugChecked:      {state['tokens_rugchecked']}")
    print(f"  Bought:          {state['tokens_bought']}")
    print(f"  Win rate:        {wr} ({state['wins']}W/{state['losses']}L)")
    print(f"  Free rides:      {state['free_rides']} 🎯")
    print(f"  Net P&L:         ${net:+.2f} AUD")
    print(f"  Open positions:  {len(state['open_positions'])} (saved for next session)")
    print(Fore.YELLOW + "═" * 65)

# ── MAIN ──────────────────────────────────────────────────────────────────────
async def main():
    print()
    print(Fore.YELLOW + Style.BRIGHT + "═" * 65)
    print(Fore.YELLOW + Style.BRIGHT + "  STREAM 5 — DOUBLER  |  Initialising...")
    print(Fore.YELLOW + Style.BRIGHT + "═" * 65)
    print()
    print(f"  Strategy:    Buy at launch, sell 50% at 2x, free ride rest")
    print(f"  Position:    {fmt_aud(POSITION_AUD)} per trade")
    print(f"  Target:      +{TARGET_PCT:.0f}% (2x)")
    print(f"  Stop loss:   None — accept full {fmt_aud(POSITION_AUD)} loss")
    print(f"  RugCheck:    Under {RUGCHECK_MAX_SCORE} score only")
    print(f"  Entry:       {CONFIRM_WAIT_SEC}s after launch with momentum confirm")
    print(f"  Daily max:   {fmt_aud(DAILY_MAX_SPEND_AUD)}")
    print()

    keypair = load_wallet()
    print(f"  {Fore.GREEN}✓ Wallet loaded: {str(keypair.pubkey())[:20]}...{Style.RESET_ALL}")

    load_state_from_file()

    print(f"  {Fore.RED}Press Ctrl+C to stop{Style.RESET_ALL}")
    print()

    try:
        await asyncio.gather(
            pump_listener(keypair),
            background_monitor(keypair),
        )
    except KeyboardInterrupt:
        print(f"\n  {Fore.YELLOW}Stopped by user.")
        print(f"  Closing free-ride positions...")
        for contract in list(state["open_positions"].keys()):
            pos = state["open_positions"][contract]
            if pos.get("free_ride"):
                sell_token(keypair, contract, "Session end — closing free rides")
            else:
                print(f"  Leaving {pos['symbol']} open — check manually")
        print_summary()
        save_state()

if __name__ == "__main__":
    asyncio.run(main())
