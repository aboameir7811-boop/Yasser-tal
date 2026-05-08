import logging
import asyncio
import random
import time
import os
import json
import unicodedata
import re
import io
import difflib
import requests
import httpx  
import aiohttp
import arabic_reshaper
import math
import traceback
import numpy as np
import pandas as pd
from scipy.stats import linregress
from scipy.signal import find_peaks
from typing import Dict, Union
from aiogram import types
from datetime import datetime, timedelta # 💡 تمت الإضافة هنا
from aiogram.dispatcher.filters import Text 
from pilmoji import Pilmoji 
from PIL import Image, ImageDraw, ImageFont, ImageOps
from bidi.algorithm import get_display
from aiogram import Bot, Dispatcher, types, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from supabase import create_client, Client

# --- [ 1. إعدادات الهوية والاتصال ] ---
ADMIN_ID = 8627110934
OWNER_USERNAME = "@Ya_79k"

# سحب التوكينات من Render (لن يعمل البوت بدونها في الإعدادات)
API_TOKEN = os.getenv('BOT_TOKEN')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')


GROUP_ID = os.getenv('GROUP_ID')

# 2. التحقق ثانياً
if not API_TOKEN or not GROUP_ID:
    logging.error("❌ خطأ: المتغيرات المشفرة مفقودة في إعدادات Render!")
    

# تعريف المحركات
bot = Bot(token=API_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
# 1. في بداية الملف (خارج كل الدوال) قم بتعريف هذا المتغير
bot_username = None 

# ==========================================
# 1. إعدادات الجلسات والقيم الثابتة (Config & State)
# ==========================================
# --- منطقة تعريف المتغيرات العالمية (Global Variables) ---
active_investigations = {}
# 1. تخزين بيانات جلسات التداول المؤقتة لكل مستخدم
trade_sessions = {} 

# 2. إدارة مهام التحديث اللحظي (Tasks) لمنع التكرار والحظر
# يجب أن يكون قاموساً (Dictionary) لكي نتمكن من إلغاء المهمة السابقة لكل مستخدم
active_updates = {} 

# 3. إعدادات الرافعة والنسب والمدد (إذا لم تكن معرفة لديك)
LEVERAGE_LEVELS = [1, 5, 10, 20, 50, 75, 100]
MARGIN_PCT_LEVELS = [10, 25, 50, 75, 100]
# كلمات التحكم الخاصة بك
ADMIN_COMMANDS = ["صفقات اليوم", "لوحتي", "غرفتي"]

async def get_intelligence_report_text():
    """دالة مركزية لجلب البيانات وتنسيقها لتجنب تكرار الكود"""
    res = supabase.table("market_intelligence").select("*").order("pump_score", desc=True).limit(5).execute()
    
    if not res.data:
        return "📭 الرادار لا يرصد فرصاً حالياً.", None

    report = "👁‍🗨 <b>تقرير رادار الأسرار اللحظي:</b>\n\n"
    for item in res.data:
        icon = "🟢" if item['pump_score'] > 70 else "🟡"
        report += f"{icon} <code>#{item['symbol']}</code> | Score: <b>{item['pump_score']}</b> | Trend: {item['trend_status']}\n"
    
    report += "\n<i>البيانات مستخرجة بناءً على 'فراغ السيولة' و 'الاختناق'.</i>"
    return report, get_admin_main_keyboard(ADMIN_ID)
    

    # ==========================================
# --- [ محرك تحليل الحساب المطور ] ---
# ==========================================
async def get_trading_account_snapshot(user_id):
    try:
        user_res = supabase.table("users_global_profile").select("bank_balance").eq("user_id", user_id).execute()
        free_cash = float(user_res.data[0]['bank_balance']) if user_res.data else 0.0
        
        trades = supabase.table("active_trades").select("*").eq("user_id", user_id).eq("is_active", True).execute()
        
        total_used_margin = 0.0
        total_unrealized_pnl = 0.0
        
        for t in trades.data:
            mar = float(t['margin'])
            total_used_margin += mar
            # ... (حساب pnl_pct كما في السابق)
            total_unrealized_pnl += (mar * pnl_pct * float(t['leverage']))

        # 🎯 المنطق الجديد
        total_balance = free_cash + total_used_margin # هذا الـ 1000 في مثالك
        total_equity = total_balance + total_unrealized_pnl # القيمة مع الربح/الخسارة
        
        return {
            "free_cash": round(free_cash, 2),
            "used_margin": round(total_used_margin, 2),
            "total_balance": round(total_balance, 2), # الرصيد الكلي المجموع
            "total_pnl": round(total_unrealized_pnl, 2),
            "total_equity": round(total_equity, 2)
        }
    except Exception as e:
        # التعامل مع الخطأ
        return {"free_cash": 0, "used_margin": 0, "total_balance": 0, "total_pnl": 0, "total_equity": 0}
              
async def trade_reaper():
    """
    رادار التصفية المحصن + منفذ الطلبات المعلقة
    """
    while True:
        try:
            # 1. جلب البيانات مع محاولة المعالجة (Error Handling)
            try:
                trades_res = supabase.table("active_trades").select("*").execute()
                all_trades = trades_res.data
                
                market_res = supabase.table("crypto_market_simulation").select("symbol, current_price").execute()
                market_prices = {item['symbol']: float(item['current_price']) for item in market_res.data}
            except Exception as db_err:
                # إذا حدث خطأ 502 أو مشكلة في الشبكة، انتظر قليلاً وكرر المحاولة بصمت
                logging.warning(f"⚠️ سوبابيس مشغول (502/Network). استراحة 5 ثوانٍ... {db_err}")
                await asyncio.sleep(5)
                continue

            if not all_trades:
                await asyncio.sleep(5)
                continue

            # --- [نفس المنطق الخاص بك في التصنيف] ---
            pending_trades = [t for t in all_trades if not t['is_active']]
            active_trades_by_user = {}
            for t in [x for x in all_trades if x['is_active']]:
                uid = t['user_id']
                if uid not in active_trades_by_user: active_trades_by_user[uid] = []
                active_trades_by_user[uid].append(t)

            # --- [الجزء الثاني: تنفيذ الأوامر المعلقة] ---
            for pt in pending_trades:
                sym = pt['symbol']
                target_p = float(pt['entry_price'])
                current_p = market_prices.get(sym)
                if not current_p: continue

                # منطق التفعيل (رادار القناص)
                if (pt['side'] == 'LONG' and current_p <= target_p) or \
                   (pt['side'] == 'SHORT' and current_p >= target_p):
                    
                    # تحديث الحالة مع التأكد من التنفيذ
                    supabase.table("active_trades").update({"is_active": True}).eq("trade_id", pt['trade_id']).execute()
                    await bot.send_message(pt['user_id'], f"⚡ <b>تـم تـفـعـيـل الـطـلـب!</b>\n#{sym} بدأت العمل الآن 🚀", parse_mode="HTML")

            # --- [الجزء الثالث: رادار الإبادة (التصفية)] ---
            for uid, trades in active_trades_by_user.items():
                try:
                    user_res = supabase.table("users_global_profile").select("bank_balance").eq("user_id", uid).execute()
                    if not user_res.data: continue
                    
                    bank_balance = float(user_res.data[0].get('bank_balance', 0.0))
                    total_used_margin = 0.0
                    total_unrealized_pnl = 0.0
                    
                    for t in trades:
                        sym, entry, margin, lev = t['symbol'], float(t['entry_price']), float(t['margin']), float(t['leverage'])
                        current_p = market_prices.get(sym, entry)
                        total_used_margin += margin
                        pnl_pct = (current_p - entry) / entry if t['side'] == 'LONG' else (entry - current_p) / entry
                        total_unrealized_pnl += (margin * pnl_pct * lev)

                    # 💀 قرار الإعدام: إذا تجاوزت الخسارة الضمان
                    if total_unrealized_pnl <= -(bank_balance + total_used_margin):
                        # تصفير وحذف (تطهير الحساب)
                        supabase.table("users_global_profile").update({"bank_balance": 0.0, "debt_balance": 0.0}).eq("user_id", uid).execute()
                        supabase.table("active_trades").delete().eq("user_id", uid).eq("is_active", True).execute()
                        await bot.send_message(uid, "💀 <b>تـصـفـيـة كـامـلـة!</b>\nتبخرت المحفظة.. ابدأ من جديد يا بطل.", parse_mode="HTML")
                except: continue # تخطي أي مستخدم فيه مشكلة فنية

        except Exception as global_e:
            logging.error(f"❌ Reaper Global Panic: {global_e}")
            
        await asyncio.sleep(8) # وقت مثالي لضمان تحديث سريع وحماية من الحظر



from datetime import datetime

async def intelligence_scanner():
    """
    الرادار v11.1 (عين الصقر + القلعة المحصنة + فلتر الماكد)
    يدمج الدعوم والمقاومات المتعددة الفريمات، يتجاهل العملات الميتة والمزعجة، 
    ويضرب بيد من حديد في مناطق انضغاط السيولة وإبادة البائعين بتأكيد فني صارم.
    """
    print(f"🚀 {datetime.now().strftime('%H:%M:%S')} | الرادار v11.1 يمسح السوق بذكاء الدعوم والمقاومات وتأكيد الماكد...")
    
    try:
        res = supabase.table("crypto_market_simulation").select("*").execute()
        coins = res.data
    
        if not coins: 
            return []
        
        # 🚫 قائمة العملات المزعجة المحظورة (تم التحديث)
        ignored_coins = {
            "EOSUSDT", "GALUSDT", "HNTUSDT", "PLAUSDT", 
            "VOXELUSDT", "DFUSDT", "KLAYUSDT", "BTCDOWNUSDT"
        }
        
        for coin in coins:
            symbol = coin['symbol']
            
            # تجاوز العملات المزعجة فوراً
            if symbol in ignored_coins:
                continue
            
            # ==========================================
            # ⛔ [ 0. فلتر إبادة الأشباح (تجاهل العملات الميتة) ]
            # ==========================================
            vol_15m = float(coin.get('volume_15m') or 0)
            vol_ma_15m = float(coin.get('volume_ma_15m') or 1)
            bbw_15m = float(coin.get('bbw_15m') or 0)
            price = float(coin.get('current_price') or 0)
            
            # إذا كان الفوليوم شبه معدوم، أو العملة متجمدة (لا يوجد سيولة)، تجاوزها فوراً
            if vol_15m < 10000 or price == 0 or bbw_15m <= 0.005:
                continue # تخطي العملة وعدم إضاعة الموارد عليها

            score = 0
            reasons = []
            kill_switch = False
            
            # ==========================================
            # 🧱 [ 1. استخراج ترسانة الدعوم والمقاومات الحقيقية ]
            # ==========================================
            sup_5m = float(coin.get('support_5m') or 0)
            res_5m = float(coin.get('resistance_5m') or 0)
            sup_15m = float(coin.get('support_15m') or 0)
            res_15m = float(coin.get('resistance_15m') or 0)
            sup_1h = float(coin.get('support_1h') or 0)
            res_1h = float(coin.get('resistance_1h') or 0)
            sup_2h = float(coin.get('support_2h') or 0)
            res_2h = float(coin.get('resistance_2h') or 0)
            sup_4h = float(coin.get('support_4h') or 0)
            res_4h = float(coin.get('resistance_4h') or 0)
            sup_1d = float(coin.get('support_1d') or 0)
            res_1d = float(coin.get('resistance_1d') or 0)

            def is_near_level(current_p, level, threshold=0.015):
                if level == 0: return False
                return abs(current_p - level) / level <= threshold

            # ==========================================
            # 🛠️ [ 2. استخراج المؤشرات الفنية الأساسية ]
            # ==========================================
            ema20_1h = float(coin.get('ema_20_1h') or 0)
            ema50_1h = float(coin.get('ema_50_1h') or 0)
            ema100_1h = float(coin.get('ema_100_1h') or 0)
            bb_upper_1h = float(coin.get('bb_upper_1h') or 0)
            bb_mid_1h = float(coin.get('bb_middle_1h') or 1)
                        
            upper = float(coin.get('bb_upper_15m') or 0) 
            lower = float(coin.get('bb_lower_15m') or 0) 
            middle = float(coin.get('bb_middle_15m') or 1) 
            
            kc_upper = float(coin.get('kc_upper_15m') or 0) 
            kc_lower = float(coin.get('kc_lower_15m') or 0) 
            
            ema20 = float(coin.get('ema_20_15m') or 0) 
            ema50 = float(coin.get('ema_50_15m') or 0) 
            ema100 = float(coin.get('ema_100_15m') or 0) 
            rsi_15m = float(coin.get('rsi_15m') or 50) 
            
            # استخراج قيم الماكد (MACD)
            macd_15m = float(coin.get('macd_15m') or 0)
            macd_signal_15m = float(coin.get('macd_signal_15m') or 0)
            macd_hist_15m = float(coin.get('macd_hist_15m') or 0)
            
            obv_slope_15m = float(coin.get('obv_slope_15m') or 0) 
            oi_change = float(coin.get('open_interest_change_24h') or 0) 
            
            funding_rate = float(coin.get('funding_rate') or 0)
            ema20_4h = float(coin.get('ema_20_4h') or 0)
            ema50_4h = float(coin.get('ema_50_4h') or 0)
            rsi_4h = float(coin.get('rsi_4h') or 50)
            
            bbw_prev_15m = float(coin.get('bbw_prev_15m') or 0) 
            expansion_ratio_15m = (bbw_15m / bbw_prev_15m) if bbw_prev_15m > 0 else 1.0 

            bbw_5m = float(coin.get('bbw_5m') or 0) 
            bbw_prev_5m = float(coin.get('bbw_prev_5m') or 0) 
            expansion_ratio_5m = (bbw_5m / bbw_prev_5m) if bbw_prev_5m > 0 else 1.0 

            vol_delta = float(coin.get('volume_delta_15m') or 0)
            adx_val = float(coin.get('adx_15m') or 0)
            stop_loss = float(coin.get('stop_loss_atr') or 0)
            mood = coin.get('market_mood') or 'NEUTRAL'
            orderbook_ratio = float(coin.get('orderbook_imbalance_ratio') or 0)
            whale_detected = coin.get('whale_absorption_detected') or False

            # --- [ استخراج أنماط الشموع ] ---
            patterns = {
                '5m': coin.get('f5m_c1', 'Normal'),
                '15m': coin.get('f15m_c1', 'Normal'),
                '1h': coin.get('f1h_c1', 'Normal'),
                '2h': coin.get('f2h_c1', 'Normal'),
                '4h': coin.get('f4h_c1', 'Normal'),
                '1d': coin.get('f1d_c1', 'Normal')
            }

            # --- [ هندسة السياق ومناطق القيمة العامة ] ---
            is_uptrend = (ema20_1h > ema50_1h > ema100_1h)
            is_downtrend = (ema20_1h < ema50_1h < ema100_1h)

            high_24h = float(coin.get('high_24h') or (price * 1.05)) 
            low_24h = float(coin.get('low_24h') or (price * 0.95)) 
            fib_618 = high_24h - (0.618 * (high_24h - low_24h)) 
            
            is_near_support_general = (price <= lower * 1.015) or (price <= ema50 * 1.015) or (abs(price - fib_618) / fib_618 <= 0.01)
            is_near_resistance_general = (price >= upper * 0.985) or (price >= ema20 * 1.03)
            
            has_volume_confirmation = vol_15m > (vol_ma_15m * 1.2)
            is_sqz = bbw_15m < 0.065

            # ==========================================
            # 💣 [ 3. المحرك الاستخباراتي والسيولة ]
            # ==========================================
            is_squeeze_on = (upper < kc_upper) and (lower > kc_lower)
            is_squeeze_firing = (not is_squeeze_on) and (expansion_ratio_15m > 1.05) and (obv_slope_15m > 0)

            if is_squeeze_firing and oi_change > 5:
                score += 50
                reasons.append(f"🌋 انفجار الانضغاط : B يكسر K مع سيولة قوية (I: +{oi_change}%)")
            elif is_squeeze_on:
                reasons.append("🤫 هدوء : العملة في حالة انضغاط خانق، ننتظر الموجة القادمة.")

            is_short_squeeze = (funding_rate < -0.01) and (price >= lower) and (rsi_15m <= 25)
            if is_short_squeeze and obv_slope_15m > 0:
                score += 30
                reasons.append(f"🩸 إبادة البائعين: التمويل سالب جداً ({funding_rate}%) والسعر يصنع قاعاً مع سيولة خفية.")

            is_liquidity_sweep = (price > lower) and (vol_15m > vol_ma_15m * 2.5) and (rsi_15m < 25)
            if is_liquidity_sweep:
                score += 30
                reasons.append(f"🪤 مصيدة السيولة: الحيتان ضربوا الستوب لوز واشتروا بقوة (الفوليوم: {vol_15m:.1f})")

            if oi_change > 15 and bbw_15m < 0.05:
                score += 30
                reasons.append(f"🪵 تكديس الحطب: السعر ميت ولكن الاهتمام المفتوح يرتفع بجنون (+{oi_change}%)")

            is_4h_bullish = (ema20_4h > ema50_4h) and (rsi_4h > 50)
            if is_4h_bullish and score > 0:
                score += 80
                reasons.append("🛡️ غطاء مالي (4H): الاتجاه العام صاعد ويدعم الموجة القادمة")

            # ==========================================
            # 📊 [ 4. التأكيد الفني الصارم (MACD) ]
            # ==========================================
            is_macd_bullish = (macd_15m > macd_signal_15m) and (macd_hist_15m > 0)
            is_macd_bearish = (macd_15m < macd_signal_15m) and (macd_hist_15m < 0)

            if is_macd_bullish and has_volume_confirmation:
                score += 40
                reasons.append("📈 تأكيد الماكد (MACD): تقاطع إيجابي وزخم صاعد مدعوم بالسيولة (+40)")
            elif is_macd_bearish:
                score -= 50  # عقوبة قوية للاتجاه السلبي الواضح
                reasons.append("📉 رفض الماكد (MACD): تقاطع سلبي يضغط على السعر (-50)")

            # ==========================================
            # 🕯️ [ 5. محرك الشموع v11.1 (المدعوم بالدعوم والمقاومات) ]
            # ==========================================
            tf_levels = {
                '5m': {'sup': sup_5m, 'res': res_5m},
                '15m': {'sup': sup_15m, 'res': res_15m},
                '1h': {'sup': sup_1h, 'res': res_1h},
                '2h': {'sup': sup_2h, 'res': res_2h},
                '4h': {'sup': sup_4h, 'res': res_4h},
                '1d': {'sup': sup_1d, 'res': res_1d}
            }

            for tf, pattern in patterns.items():
                if pattern in ["Normal", "Not enough data", "Neutral_Doji", "Spinning_Top", None]:
                    continue

                clean_name = pattern.replace("_", " ")
                is_bullish = "صاعد" in pattern
                is_bearish = "هابط" in pattern
                
                tf_sup = tf_levels[tf]['sup']
                tf_res = tf_levels[tf]['res']
                
                is_at_tf_support = is_near_level(price, tf_sup) or is_near_support_general
                is_at_tf_resistance = is_near_level(price, tf_res) or is_near_resistance_general

                # فريم 1D
                if tf == '1d':
                    weight = 100
                    if has_volume_confirmation: 
                        if is_bullish and (is_at_tf_support or is_uptrend):
                            score += weight
                            reasons.append(f"🏛️ا [1D - استراتيجي] {clean_name}: سيولة مؤسساتية عند دعم {tf_sup if tf_sup > 0 else 'مؤكد'} (+{weight})")
                        elif is_bearish and (is_at_tf_resistance or is_downtrend):
                            score -= int(weight * 1.5)
                            reasons.append(f"🔴 ا [1D - تحذير] {clean_name}: تصريف مؤسساتي عند مقاومة (-{int(weight * 1.5)})")

                # فريم 4H
                elif tf == '4h' and any(x in pattern for x in ["نجمة", "ثلاثة", "الساندوتش", "مطرقة", "المشنوق", "الشهاب"]):
                    weight = 60
                    if has_volume_confirmation:
                        if is_bullish and is_at_tf_support:
                            score += weight
                            reasons.append(f"🛡️ا [4H - سوينج] {clean_name}: ارتداد قوي من دعم {tf_sup if tf_sup > 0 else 'مؤكد'} (+{weight})")
                        elif is_bearish and is_at_tf_resistance:
                            score -= int(weight * 1.5)
                            reasons.append(f"🔴 ا [4H - فخ] {clean_name}: رفض سعري عنيف عند المقاومة (-{int(weight * 1.5)})")

                # فريم 2H
                elif tf == '2h' and any(x in pattern for x in ["تاسوكي", "التقدم", "ابتلاع", "الراكل", "الحزام"]):
                    weight = 50
                    if is_bullish and (is_uptrend or is_at_tf_support) and vol_delta >= 0:
                        score += weight
                        reasons.append(f"🎯 ا [2H - زخم] {clean_name}: تأكيد قوة شرائية مع الاتجاه (+{weight})")
                    elif is_bearish and (is_downtrend or is_at_tf_resistance) and vol_delta <= 0:
                        score -= int(weight * 1.5)
                        reasons.append(f"🔴 ا [2H - بيع] {clean_name}: سيطرة بيعية واضحة (-{int(weight * 1.5)})")

                # فريم 1H
                elif tf == '1h' and any(x in pattern for x in ["هارامي", "الثاقب", "السحابة", "الملقط", "التلاقي", "الانفصال"]):
                    weight = 30
                    if has_volume_confirmation:
                        if is_bullish and is_at_tf_support:
                            score += weight
                            reasons.append(f"⏱️ ا [1H - يومي] {clean_name}: ارتداد تكتيكي من دعم {tf_sup if tf_sup > 0 else 'مدعوم بسيولة'} (+{weight})")
                        elif is_bearish and is_at_tf_resistance:
                            score -= int(weight * 1.5)
                            reasons.append(f"🔴 ا [1H - يومي] {clean_name}: ضغط بيعي عند مقاومة (-{int(weight * 1.5)})")

                # فريم 15m
                elif tf == '15m' and any(x in pattern for x in ["على_الرقبة", "في_الرقبة", "دفع", "نجمة_دوجي"]):
                    weight = 15
                    if is_bullish and rsi_15m <= 35 and is_uptrend:
                        score += weight
                        reasons.append(f"⚡ ا [15m - مضاربة] {clean_name}: نهاية تصحيح (RSI={rsi_15m:.0f}) (+{weight})")
                    elif is_bearish and rsi_15m >= 65 and is_downtrend:
                        score -= int(weight * 1.2)
                        reasons.append(f"🔴 ا [15m - مضاربة] {clean_name}: ذروة شراء في ترند هابط (-{int(weight * 1.2)})")

                # فريم 5m
                elif tf == '5m' and "النجوم_الثلاثة" in pattern:
                    if is_sqz:
                        weight = 10
                        score += weight if is_bullish else -weight
                        reasons.append(f"🔍 ا [5m - انضغاط] {clean_name}: إشارة حيرة تسبق الانفجار ({'+' if is_bullish else '-'}{weight})")

            # ==========================================
            # 🛡️ [ 6. الغطاء الجوي ومعززات الاتجاه ]
            # ==========================================
            is_1h_ready = (
                (price > ema20_1h) and             
                (price < bb_upper_1h) and            
                (ema20_1h > bb_mid_1h) and          
                (ema20_1h > ema50_1h > ema100_1h)   
            )

            if is_1h_ready:
                score += 50
                reasons.append("🛡️ ا غطاء جوي (1H): ترتيب هجومي مثالي يدعم الانفجار")
                is_1h_confirmed = True
            else:
                reasons.append("⚠️ ا تنبيه: الانفجار محلي بدون غطاء جوي 1H")
                is_1h_confirmed = False
                
            is_crawling_up = (
                (price >= ema20) and  
                (price >= upper * 0.995) and 
                (ema20 > middle) and 
                (ema20 > ema50 > ema100) and 
                (expansion_ratio_15m > 1.10) 
            )

            is_5m_spark = expansion_ratio_5m > 1.20 
            is_volume_spike = vol_ma_15m > 0 and vol_15m > (vol_ma_15m * 2) 
            is_yusr_detected = mood == "YUSR_EXPLOSION"
            intel_report = f"إشارة {mood} مرصودة بدقة"

            if is_crawling_up:
                score += 50 
                intel_report = "🚀 زحف الإعصار: السعر يركب الخط العلوي بقوة هجومية." if mood != "YUSR_EXPLOSION" else intel_report
                reasons.append(f"🚀 زحف الإعصار: قوة هجومية مع توسع ({expansion_ratio_15m:.1%})") 
                mood = "NUCLEAR_CRAWL" if mood != "YUSR_EXPLOSION" else mood

            if is_5m_spark:
                score += 50 
                reasons.append(f"🔥 شرارة الانفجار: توسع عنيف 5m ({expansion_ratio_5m:.1%})") 

            if is_volume_spike:
                score += 50 
                reasons.append(f"📊 فوليوم مضاعف: السيولة تتجاوز 200% من المتوسط") 

            if (upper > kc_upper) and expansion_ratio_15m > 1.05: 
                score += 50 
                reasons.append("🌋 كسر الانضغاط (k): تحرر بقوة هائلة") 

            if oi_change > 5 and (is_crawling_up or is_yusr_detected): 
                score += 50 
                reasons.append(f"🐳 وقود الحيتان: الاهتمام المفتوح يرتفع (+{oi_change}%)") 

            if adx_val > 25 and is_crawling_up:
                score += 50
                reasons.append(f"🌪️ قوة الاتجاه (A): مسار انفجاري مؤكد ({adx_val})")

            # ==========================================
            # 🛡️ [ 7. فلاتر الحماية الصارمة ]
            # ==========================================
            if rsi_4h < 40 and ema20_4h < ema50_4h:
                score -= 30
                reasons.append("⚠️ الفريم الأكبر (4H) منهار، تم إبطال الهجوم الشرائي.")
                
            if (price > upper or is_crawling_up) and (obv_slope_15m < 0 or expansion_ratio_15m < 0.95 or vol_delta < 0): 
                score -= 80  
                intel_report = "⚠️ فخ تلاعب: صعود وهمي وتصريف مخفي للسيولة!"
                reasons.append("🚫 حماية مطلقة: سيولة بيعية سالبة خلف الصعود الوهمي.") 

            # ==========================================
            # 📐 [ 8. دمج القنوات السعرية، الترند، والنماذج الكلاسيكية - الرادار الشامل ]
            # ==========================================

            # --- [ استخراج بيانات الترند والقنوات ] ---
            trend_1h = coin.get('1h_trend_direction') or 'عرضي'
            trend_touches_1h = int(coin.get('1h_trend_touches') or 0)
            trend_angle_1h = float(coin.get('1h_trend_angle') or 0.0)
            is_body_close = int(coin.get('is_body_close') or 2) # 1: نعم، 2: لا

            channel_1h_status = coin.get('1h_channel_status') or 'NONE'
            channel_touches_1h = int(coin.get('1h_channel_touches') or 0)
            channel_weakness = coin.get('channel_weakness') or 'NONE' 

            # --- [ استخراج بيانات النماذج الفنية والدايفرجنز ] ---
            pattern_15m = coin.get('15m_pattern_name') or 'لا يوجد'
            pattern_class_15m = coin.get('15m_pattern_class') or 'لايوجد'
            pattern_4h = coin.get('4h_pattern_name') or 'لايوجد'

            pattern_retracement_pct = float(coin.get('pattern_retracement_pct') or 0.0)
            pattern_apex_progress = float(coin.get('pattern_apex_progress') or 0.0)
            is_marubozu = int(coin.get('is_marubozu_breakout') or 2) # 1: نعم، 2: لا
            divergence_4h = coin.get('rsi_divergence_4h') or 'NONE'
            harmonic_d_confluence = int(coin.get('harmonic_d_confluence') or 2) # 1: نعم، 2: لا

            # --- [ فلتر الأمان للفريمات الكبيرة 4H & 1D ] ---
            trend_4h = coin.get('4h_trend_direction') or 'عرضي'
            is_huge_resistance = price >= float(coin.get('resistance_1d', price * 1.5))
            is_huge_support = price <= float(coin.get('support_1d', price * 0.5))                        


            # ==========================================
            # أ. قوة الترند العام (Trend Alignment & Health)
            # ==========================================
            trend_multiplier = 1.0

            if 30 <= abs(trend_angle_1h) <= 45 and trend_touches_1h >= 3:
                trend_multiplier = 1.5 # ترند صحي ومستدام
            elif abs(trend_angle_1h) >= 70 or abs(trend_angle_1h) <= 15:
                trend_multiplier = 0.5 # ترند حاد جداً معرض للكسر، أو مسطح ضعيف العزم
            elif trend_touches_1h >= 3:
                trend_multiplier = 1.2

            if (trend_4h == "UP" and trend_1h == "صاعد") or (trend_4h == "DOWN" and trend_1h == "هابط"): 
                trend_multiplier += 0.3 

            if trend_1h == "صاعد" and is_uptrend:
                if not is_huge_resistance:
                    score += (40 * trend_multiplier)
                    reasons.append(f"📈 توافق الترند (1H/4H): زحف إيجابي صحي ({trend_touches_1h} لمسات) (+{int(40 * trend_multiplier)})")
            elif trend_1h == "هابط" and is_downtrend:
                if not is_huge_support:
                    score -= (40 * trend_multiplier)
                    reasons.append(f"📉 ضغط الترند (1H/4H): مسار هابط صحي ({trend_touches_1h} لمسات) (-{int(40 * trend_multiplier)})")

            # ==========================================
            # ب. القنوات السعرية (Price Channels) والإنذار المبكر
            # ==========================================
            channel_power = 1.3 if channel_touches_1h >= 3 else 1.0

            if trend_1h == "صاعد" and channel_weakness == "BULLISH_EXHAUSTION":
                score -= 30
                reasons.append("⚠️ إنذار مبكر: إرهاق شرائي - فشل السعر في الوصول لسقف القناة الصاعدة (-30)")
            elif trend_1h == "هابط" and channel_weakness == "BEARISH_EXHAUSTION":
                score += 30
                reasons.append("⚠️ إنذار مبكر: إرهاق بيعي - فشل السعر في الوصول لقاع القناة الهابطة (+30)")

            if channel_1h_status == "BREAKOUT_UP" and is_body_close == 1:
                score += (70 * channel_power)
                reasons.append(f"🚀 اختراق قناة (1H): انفجار سعري موثق بجسم الشمعة (+{int(70 * channel_power)})")
            elif channel_1h_status == "BREAKOUT_DOWN" and is_body_close == 1:
                score -= (70 * channel_power)
                reasons.append(f"🩸 كسر قناة (1H): انهيار سفلي موثق بجسم الشمعة (-{int(70 * channel_power)})")

            # ==========================================
            # ج. النماذج الاستمرارية والانعكاسية (Strict Validation)
            # ==========================================

            # قوائم النماذج المخصصة
            bullish_harmonics = ["سايفر شرائي", "قرش شرائي", "جارتلي شرائي", "خفاش شرائي"]
            bearish_harmonics = ["سايفر بيعي", "قرش بيعي", "جارتلي بيعي", "خفاش بيعي"]
            
            bullish_reversals = ["قاع مزدوج", "رأس وكتفين مقلوب", "قاع ثلاثي"]
            bearish_reversals = ["قمة مزدوجة", "رأس وكتفين", "قمة ثلاثية"]

            # 1. النماذج الاستمرارية (الأعلام وصناديق دارفاس)
            if pattern_15m in ["علم صاعد", "صندوق دارفاس صاعد"]:
                if pattern_retracement_pct <= 61.8 and is_marubozu == 1:
                    score += 60
                    reasons.append(f"🎯 استمراري صاعد ({pattern_15m}): تصحيح صحي لم يتجاوز 61.8% واختراق زخم (+60)")
                elif pattern_retracement_pct > 61.8:
                    score -= 40
                    reasons.append(f"⚠️ فشل {pattern_15m}: التصحيح تجاوز 61.8% وتحول لنموذج انعكاسي (-40)")

            elif pattern_15m in ["علم هابط", "صندوق دارفاس هابط"]:
                if pattern_retracement_pct <= 61.8 and is_marubozu == 1:
                    score -= 60
                    reasons.append(f"🩸 استمراري هابط ({pattern_15m}): تصحيح ضعيف واستئناف الهبوط بقوة (-60)")

            # 2. المثلثات والأوتاد ونموذج البوق
            if pattern_15m == "مثلث متماثل":
                if 50 <= pattern_apex_progress <= 75 and is_body_close == 1:
                    if rsi_15m < 78:
                        score += 50
                        reasons.append("📐 اختراق مثلث متماثل: في المساحة الذهبية (50-75%) بجسم الشمعة (+50)")
                elif pattern_apex_progress > 75:
                    reasons.append("⚠️ مثلث متماثل: السعر وصل للرأس وفقد الزخم (حركة عشوائية)")

            if pattern_15m == "وتد هابط" and divergence_4h == "BULLISH_DIVERGENCE":
                score += 70
                reasons.append("🚀 وتد هابط انعكاسي: مدعوم بدايفرجنز إيجابي وتضيق سعري (+70)")
            elif pattern_15m == "وتد صاعد" and divergence_4h == "BEARISH_DIVERGENCE":
                score -= 70
                reasons.append("⚠️ وتد صاعد انعكاسي: ضعف مشترين مدعوم بدايفرجنز سلبي (-70)")

            if pattern_4h == "بوق متسع" and is_body_close == 1:
                score += 80
                reasons.append("🌋 انفجار سعري (4H): اختراق نموذج بوق متسع بعد تذبذب عالٍ (+80)")

            # 3. النماذج الانعكاسية الكلاسيكية والهارمونيك الاحترافي (4H+)
            if is_body_close == 1:
                # مسار الهارمونيك الشرائي
                if pattern_4h in bullish_harmonics and pattern_class_15m == "هارمونيك - احترافي":
                    if harmonic_d_confluence == 1 and divergence_4h == "BULLISH_DIVERGENCE":
                        score += 100
                        reasons.append(f"💠 {pattern_4h} (نخبوي): النقطة D تتوافق مع دعم تاريخي ودايفرجنز إيجابي (+100)")
                    elif harmonic_d_confluence == 1:
                        score += 85
                        reasons.append(f"💠 {pattern_4h}: ارتداد قوي من النقطة D مع توافق دعوم (+85)")

                # مسار الانعكاس الكلاسيكي الشرائي
                elif pattern_4h in bullish_reversals:
                    if divergence_4h == "BULLISH_DIVERGENCE":
                        score += 80
                        reasons.append(f"🔄 انعكاس صاعد (4H): نموذج {pattern_4h} مكتمل ومؤكد بدايفرجنز (+80)")
                    elif pattern_4h == "قاع ثلاثي":
                        score += 75
                        reasons.append("🔄 قاع ثلاثي (4H): فشل البائعين في الكسر للمرة الثالثة (+75)")

                # مسار الهارمونيك البيعي
                if pattern_4h in bearish_harmonics and pattern_class_15m == "هارمونيك - احترافي":
                    if harmonic_d_confluence == 1 and divergence_4h == "BEARISH_DIVERGENCE":
                        score -= 100
                        reasons.append(f"💠 {pattern_4h} (نخبوي): النقطة D تتوافق مع مقاومة قوية ودايفرجنز سلبي (-100)")
                    elif harmonic_d_confluence == 1:
                        score -= 85
                        reasons.append(f"💠 {pattern_4h}: انعكاس هابط من النقطة D مع توافق مقاومات (-85)")

                # مسار الانعكاس الكلاسيكي البيعي
                elif pattern_4h in bearish_reversals:
                    if divergence_4h == "BEARISH_DIVERGENCE":
                        score -= 80
                        reasons.append(f"🔄 انعكاس هابط (4H): نموذج {pattern_4h} مكتمل ومؤكد بدايفرجنز (-80)")
                    elif pattern_4h == "قمة ثلاثية":
                        score -= 75
                        reasons.append("🔄 قمة ثلاثية (4H): فشل المشترين في الاختراق للمرة الثالثة (-75)")
                               
            # ==========================================
            # 🎯 [ 8. قرار الإطلاق النهائي ]
            # ==========================================
            sc_crawling = 1 if is_crawling_up else 0 
            sc_spark = 1 if is_5m_spark else 0 
            sc_volume = 1 if is_volume_spike else 0 
            sc_keltner = 1 if (upper > kc_upper and expansion_ratio_15m > 1.05) else 0 
            sc_whale = 1 if (oi_change > 5 and is_crawling_up) else 0 

            if is_crawling_up and is_5m_spark and is_volume_spike: 
                score += 60  

            signal_type = "NONE"
            
            if score >= 250:
                if is_near_support_general or is_uptrend or is_at_tf_support:
                    signal_type = "LONG"
                else:
                    reasons.append("🚫 تم الإلغاء: السكور عالٍ لكن المكان عشوائي (معلق بالهواء)")

            elif score <= -256:
                if is_near_resistance_general or is_downtrend or is_at_tf_resistance:
                    signal_type = "SHORT"
                else:
                    reasons.append("🚫 تم الإلغاء: السكور منخفض لكن المكان عشوائي")

            if signal_type != "NONE":  
                supabase.table("market_intelligence").upsert({ 
                    "symbol": symbol, 
                    "current_price": price, 
                    "avg_volume": vol_ma_15m, 
                    "volume_24h": vol_15m, 
                    "rsi_value": rsi_15m, 
                    "pump_score": int(score), 
                    "signal_direction": signal_type,
                    "global_obv_status": "SQUEEZE_FIRE" if is_squeeze_firing else ("MOMENTUM_EXPLOSION" if signal_type == "LONG" else "BEARISH_DUMP"), 
                    "multi_frame_liquidity_score": obv_slope_15m, 
                    "fib_golden_ratio": fib_618, 
                    "trend_status": mood, 
                    "is_1h_confirmed": True, 
                    "score_crawling": sc_crawling, 
                    "score_spark": sc_spark, 
                    "score_volume": sc_volume, 
                    "score_keltner": sc_keltner, 
                    "score_whale": sc_whale,
                    "is_squeezed": is_sqz,
                    "intelligence_report": intel_report,
                    "dynamic_sl_atr": stop_loss,
                    "market_emotion_rsi": mood,
                    "orderbook_imbalance_ratio": orderbook_ratio,
                    "whale_support_detected": whale_detected,
                    "is_kill_switch_active": kill_switch,
                    "is_fake_move": (signal_type == "LONG" and vol_delta < 0) or (signal_type == "SHORT" and vol_delta > 0),
                    "last_updated": "now()" 
                }).execute() 

                await trigger_golden_signal(symbol, score, reasons, fib_618, price, signal_type) 
                
    except Exception as e: 
        import logging 
        logging.error(f"❌ خطأ داخلي في الرادار القناص v11.1: {e}") 

    print("✅ تم الانتهاء من المسح الاستخباراتي ورصد الأنماط (v11.1) بنجاح.")


# تحديث دالة التنبيه لتقبل السعر الحالي والاتجاه (v10.4)
async def trigger_golden_signal(symbol, score, reasons, fib_618, price, direction="LONG"):
    # تخصيص المظهر بناءً على الاتجاه
    is_long = direction == "LONG"
    emoji_main = "🚀" if is_long else "📉"
    trade_label = "شراء (LONG)" if is_long else "بيع (SHORT)"
    color_circle = "🟢" if is_long else "🔴"
    
    text = (
        f"🚨 <b>إشعار مهم: فرصة {trade_label}!</b> {emoji_main}\n\n"
        f"🪙 <b>العملة:</b> <code>{symbol}</code>\n"
        f"💵 <b>السعر لحظة الرصد:</b> <code>{price}</code>\n"
        f"🔥 <b>درجة الانفجار:</b> <code>{score}/100</code> {color_circle}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🕵️‍♂️ <b>الأسرار المرصودة:</b>\n"
    )
    
    for reason in reasons:
        text += f"- {reason}\n"
        
    text += (
        f"\n📐 <b>المستويات الفنية:</b>\n"
        f"👈 النسبة الذهبية (0.618): <code>{fib_618:,.4f}</code>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<i>⚠️ هذه البيانات مرسلة لك فقط.</i>"
    )

    # أزرار التحكم الديناميكية
    keyboard = types.InlineKeyboardMarkup()
    
    # زر إصدار التوصية (سيرسل نوع الاتجاه أيضاً للـ Callback)
    callback_vip = f"vip_signal:{ADMIN_ID}:{symbol}:{direction}"
    keyboard.add(types.InlineKeyboardButton(f"⚡ إصدار توصية VIP ({trade_label})", callback_data=callback_vip))
    
    # زر عرض الشارت
    keyboard.add(types.InlineKeyboardButton(f"📊 عرض شارت {symbol}", callback_data=f"coin_view:{ADMIN_ID}:{symbol}:15m"))

    try:
        await bot.send_message(chat_id=ADMIN_ID, text=text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        import logging
        logging.error(f"❌ HTML Parse Error: {e}")
        # نسخة احتياطية في حال خطأ التنسيق لضمان عدم ضياع الصفقة
        clean_text = f"إشارة {trade_label} لعملة {symbol}\nالسعر: {price}\nالسكور: {score}"
        await bot.send_message(chat_id=ADMIN_ID, text=f"⚠️ خطأ في التنسيق، إليك البيانات الأساسية:\n\n{clean_text}")
# ==========================================
# 🛠️ 1. دوال القوالب (صناعة القالب بناءً على الأعمدة)
# ==========================================
def build_coin_template(coin):
    """دالة تبني القالب الاستخباراتي بدقة بناءً على الأرقام المرفوعة من الرادار"""
    symbol = coin.get('symbol', 'UNKNOWN')
    price = coin.get('current_price', 0)
    total_score = coin.get('pump_score', 0)
    fib_618 = coin.get('fib_golden_ratio', 0)
    
    # قراءة أرقام الأسرار من الأعمدة الجديدة
    sc_crawling = coin.get('score_crawling', 0)
    sc_spark = coin.get('score_spark', 0)
    sc_volume = coin.get('score_volume', 0)
    sc_keltner = coin.get('score_keltner', 0)
    sc_whale = coin.get('score_whale', 0)
    is_squeezed = coin.get('is_squeezed', False)

    reasons = ""
    if sc_crawling > 0:
        reasons += f"- 🚀 زحف الإعصار: السعر يركب الخط العلوي بقوة هجومية.\n"
    if sc_spark > 0:
        reasons += f"- 🔥 شرارة الانفجار: توسع عنيف جداً في فريم 5m.\n"
    if sc_volume > 0:
        reasons += f"- 📊 فوليوم مضاعف: السيولة الحالية تتجاوز 200% من المتوسط.\n"
    if sc_keltner > 0:
        reasons += f"- 🌋 انفجار الانضغاط: البولنجر يكسر كيلتنر مع دخول سيولة.\n"
    if sc_whale > 0:
        reasons += f"- 🐳 وقود الحيتان: الاهتمام المفتوح يرتفع بالتزامن مع الصعود.\n"
    if is_squeezed:
        reasons += f"- 🤫 هدوء البحر: العملة في حالة انضغاط خانق (تجميع سيولة).\n"
        
    if not reasons:
         reasons = "- ⚡ رصد إيجابي: العملة في مسار صاعد وتجمع زخماً.\n"

    # تجميع القالب النهائي
    template = (
        f"🚨 **إشعار مهم: فرصة ذهبية!** 🚨\n\n"
        f"🪙 **العملة:** {symbol}\n"
        f"💵 **السعر لحظة الرصد:** {price}\n"
        f"🔥 **درجة الانفجار:** {total_score}/100 🟢\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🕵️‍♂️ **الأسرار المرصودة:**\n"
        f"{reasons}"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📐 **المستويات الفنية:**\n"
        f"👈 النسبة الذهبية (0.618): {fib_618:.4f}\n\n"
        f"⚠️ هذه البيانات سرية ومرسلة لك فقط."
    )
    return template

# ==========================================
# 1. الدوال الحسابية الأساسية (Math Core)
# ==========================================
async def get_user_bank_balance(user_id):
    """جلب رصيد البنك بدقة الكسور العشرية"""
    try:
        res = supabase.table("users_global_profile").select("bank_balance").eq("user_id", user_id).execute()
        if res.data:
            return float(res.data[0]['bank_balance'])
        return 0.0
    except Exception as e:
        logging.error(f"Error getting bank balance: {e}")
        return 0.0

def calculate_liquidation(entry_price, leverage, side, margin_amount=None, quantity=None):
    """
    حساب سعر التصفية الديناميكي.
    إذا تم تمرير الهامش والكمية، سيحسب السعر بدقة بناءً على المبلغ المدفوع.
    """
    entry = float(entry_price)
    lev = float(leverage)
    
    # إذا لم تتوفر بيانات الهامش، نستخدم المعادلة التقليدية (تصفية نظرية)
    if margin_amount is None or quantity is None or quantity == 0:
        if side == 'LONG':
            return round(entry * (1 - (1.0 / lev)), 6)
        else: 
            return round(entry * (1 + (1.0 / lev)), 6)

    # الحساب الاحترافي بناءً على "المخاطرة الفعلية":
    # السعر الذي تخسر فيه "الهامش" بالكامل
    if side == 'LONG':
        # سعر التصفية = سعر الدخول - (الهامش / الكمية)
        liq_price = entry - (float(margin_amount) / float(quantity))
    else:
        # سعر التصفية = سعر الدخول + (الهامش / الكمية)
        liq_price = entry + (float(margin_amount) / float(quantity))
    
    return max(0, round(liq_price, 6))
    
# بديل لـ numpy إذا كنت لا تريد استخدامه لتوليد 4 مناطق
def get_zones(low, high, count=4):
    step = (high - low) / (count - 1)
    return [low + (step * i) for i in range(count)]


def generate_candle_chart(direction):
    """تمثيل مرئي بسيط لاتجاه الحركة الحالية"""
    if direction == 'UP':
        return "📉 ⇠ |---🟩---|\n⇠ 🚀 صعود إيجابي"
    else:
        return "📈 ⇠ |---🟥---|\n⇠ 🩸 هبوط سلبي"

# ==========================================
# 2. إدارة الأمان المالي (Financial Health)
# ==========================================

async def get_user_data(user_id):
    """جلب الملف الشخصي الكامل للمستخدم"""
    try:
        res = supabase.table("users_global_profile").select("*").eq("user_id", user_id).execute()
        return res.data[0] if res.data else None
    except:
        return None

async def check_financial_health(user_id, amount, action="WITHDRAW"):
    """
    محرك الحماية: يمنع التلاعب بالرصيد في حال وجود:
    1. ديون نشطة.
    2. صفقات مفتوحة تحجز الهامش (Margin Lock).
    """
    data = await get_user_data(user_id)
    if not data: return False, "❌ حسابك غير مسجل في النظام."
    
    bank_bal = float(data.get('bank_balance', 0.0))
    debt = float(data.get('debt_balance', 0.0))
    
    # 🔍 حساب الهامش المحجوز فعلياً في السوق الآن
    trades_res = supabase.table("active_trades").select("margin").eq("user_id", user_id).eq("is_active", True).execute()
    locked_margin = sum(float(t['margin']) for t in trades_res.data) if trades_res.data else 0.0
    
    # الكاش المتاح للسحب = رصيد البنك - الهامش المحجوز
    available_cash = max(0.0, bank_bal - locked_margin)

    if action == "WITHDRAW":
        # منع السحب في حال وجود دين
        if debt > 0:
            return False, f"⚠️ لا يمكنك السحب! لديك دين مستحق بقيمة <code>{debt:,.2f} $</code>.\nيجب سداد الدين أولاً."
        
        # منع سحب المبالغ التي تُستخدم حالياً كضمان لصفقات مفتوحة
        if amount > available_cash:
            return False, (
                f"⚠️ عذراً، السيولة غير كافية للسحب.\n"
                f"• المتاح فعلياً: <code>{available_cash:,.2f} $</code>\n"
                f"• المحجوز في الصفقات: <code>{locked_margin:,.2f} $</code>"
            )
    
    elif action == "BORROW":
        # شروط الاقتراض: لا دين سابق + حد أدنى للرصيد
        if debt > 0:
            return False, f"⚠️ لديك قرض نشط بقيمة <code>{debt:,.2f} $</code>. سدده لتتمكن من الاقتراض مجدداً."
        if bank_bal < 10.0:
            return False, "⚠️ رصيدك أقل من 10$، لا تملك الأهلية الائتمانية الكافية للقرض."
            
    return True, "Success"

# ==========================================
# 3. إدارة الصفقات النشطة (دعم الفواصل العشرية)
# ==========================================
async def get_active_trades_report(user_id):
    try:
        # 1. جلب بيانات الحساب الشاملة (Snapshot)
        account = await get_trading_account_snapshot(user_id)
        
        # السيولة الفعلية المحركة للحساب
        equity = account['total_equity'] 
        # مجموع أرباح وخسائر الصفقات المفتوحة حالياً
        total_pnl_all = account['total_pnl'] 
        
        res = supabase.table("active_trades").select("*").eq("user_id", int(user_id)).eq("is_active", True).execute()
        trades = res.data
        
        if not trades:
            return None, "📋 <b>لا توجد صفقات مفتوحة حالياً.</b>"

        pnl_all_emoji = "🟢" if total_pnl_all >= 0 else "🔴"
        
        # 2. الهيدر المختصر (صافي القيمة والارباح فقط)
        report_text = f"📋 | <b>مـراكز الـتداول الـنشطة</b>\n"
        report_text += "━━━━━━━━━━━━━━━━━━\n"

        # 3. عرض تفاصيل الصفقات
        for trade in trades:
            symbol = trade['symbol']
            side = trade['side']
            entry = float(trade['entry_price'])
            lev = float(trade['leverage'])
            margin = float(trade['margin'])
            quantity = float(trade.get('quantity', 0))
            
            # جلب سعر السوق اللحظي
            coin_res = supabase.table("crypto_market_simulation").select("current_price").eq("symbol", symbol).execute()
            current_price = float(coin_res.data[0]['current_price']) if coin_res.data else entry

            # حساب PNL الصفقة المنفردة
            pnl_pct = (current_price - entry) / entry if side == 'LONG' else (entry - current_price) / entry
            pnl_amount = margin * pnl_pct * lev
            pnl_emoji = "💰" if pnl_amount >= 0 else "📉"

            fmt = lambda p: f"{p:,.4f}" if p < 1 else f"{p:,.2f}"
            side_str = "🟢 LONG" if side == 'LONG' else "🔴 SHORT"

            report_text += f"<b>#{symbol} | {side_str} {int(lev)}x</b>\n"
            report_text += f"• الـكمية: <code>{quantity:,.4f}</code>\n"
            report_text += f"• المـبلغ الـمستخدم: <code>{margin:,.2f} $</code>\n"
            report_text += f"• سـعر الـدخول: <code>{fmt(entry)}</code>\n"
            report_text += f"• الـسعر الحالي: <code>{fmt(current_price)}</code>\n"
            report_text += f"{pnl_emoji} الـربح/الخسارة: <b>{pnl_amount:+.2f} $</b>\n"
            report_text += "━━━━━━━━━━━━━━━━━━\n"
            
        return trades, report_text

    except Exception as e:
        import logging
        logging.error(f"Error in trade report: {e}")
        return None, "❌ حدث خطأ أثناء جلب التقرير."
        
# --- دالة حساب السعر المستهدف (دعم الكسور العشرية) ---
def calc_price(base_price, roe_pct, is_tp, side, lev):
    """
    تحسب السعر المطلوب للوصول لنسبة ROE معينة.
    ROE = (Target - Entry) / Entry * Lev * 100
    """
    base_price = float(base_price)
    lev = float(lev)
    # تحويل نسبة الـ ROE إلى نسبة تحرك السعر
    move_pct = (roe_pct / 100.0) / lev
    
    if side == "LONG":
        target = base_price * (1 + move_pct) if is_tp else base_price * (1 - move_pct)
    else:
        target = base_price * (1 - move_pct) if is_tp else base_price * (1 + move_pct)
    
    # نرجع السعر بـ 6 أرقام عشرية لضمان الدقة في كل العملات
    return round(target, 6)
    
# ==========================================
# --- [ توليد واجهة الإعدادات المطورة ] ---
# ==========================================
def get_trade_settings_view(trade, current_price, expand_section=None):
    symbol = trade['symbol']
    # 🟢 تعديل: جلب البيانات كـ float لضمان الدقة
    entry = float(trade['entry_price'])
    liq = float(trade['liquidation_price'])
    t_id = str(trade['trade_id'])
    u_id = str(trade['user_id'])
    c_price = float(current_price)
    
    # دالة تنسيق السعر الذكية (4 أرقام للأجزاء، 2 للعملات الكبيرة)
    fmt = lambda p: f"{p:,.4f}" if p < 1 else f"{p:,.2f}"

    text = f"⚙️ <b>لوحة تحكم المركز: #{symbol}</b>\n"
    text += f"━━━━━━━━━━━━━━━━━━\n"
    text += f"• سعر الدخول: <code>{fmt(entry)}</code>\n"
    text += f"• السعر الحالي: <code>{fmt(c_price)}</code>\n"
    text += f"• التصفية: <pre>{fmt(liq)}</pre> ⚠️\n"
    text += f"━━━━━━━━━━━━━━━━━━\n"

    markup = InlineKeyboardMarkup(row_width=1)
    
    # --- [ القائمة الرئيسية ] ---
    if not expand_section:
        markup.add(
            InlineKeyboardButton("✂️ إغلاق جزئي", callback_data=f"exp_cl_{u_id}_{t_id}"),
            InlineKeyboardButton("🎯 أهداف الربح والخسارة", callback_data=f"exp_risk_{u_id}_{t_id}"),
            InlineKeyboardButton("🔙 العودة", callback_data=f"active_trades_view:{u_id}")
        )
    
    # --- [ قسم الإغلاق الجزئي ] ---
    elif expand_section == "cl":
        text += "\n<b>💡 اختر نسبة الإغلاق من حجم العقد:</b>"
        btns = [InlineKeyboardButton(f"{p}%", callback_data=f"conf_cl_{p}_{u_id}_{t_id}") for p in [10, 25, 50, 75]]
        markup.row(*btns)
        markup.add(InlineKeyboardButton("🛑 إغلاق 100% (تأكيد)", callback_data=f"conf_cl_100_{u_id}_{t_id}"))
        markup.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"manage_trade:{t_id}"))

    # --- [ قسم إدارة المخاطر SL/TP ] ---
    elif expand_section == "risk":
        side = trade['side']
        lev = float(trade['leverage'])
        qty = float(trade.get('quantity', 0)) 
        # جلب صافي الربح الحالي/الخسارة للحساب (إذا لم يتوفر نضع 100 كافتراضي)
        net_balance = float(trade.get('margin', 100)) 
        
        text += f"\n<b>⚙️ نظام إدارة المخاطر الذكي:</b>\n"
        text += f"• الرافعة: {int(lev)}x | الدخول: {fmt(entry)}\n"

        def get_price_by_pnl(amount_to_lose_or_gain, is_profit=False):
            if qty <= 0: return entry
            price_change = amount_to_lose_or_gain / (qty * (lev / lev)) # معادلة بسيطة للتغير
            # تحسين الحساب بناءً على الرافعة والكمية
            move_needed = (amount_to_lose_or_gain / (margin * lev)) * entry
            
            if side == "LONG":
                res = entry + move_needed if is_profit else entry - move_needed
            else:
                res = entry - move_needed if is_profit else entry + move_needed
            return res

        # --- توليد مستويات وقف الخسارة (SL) ---
        text += "\n<b>🛑 مستويات وقف الخسارة المقترحة:</b>"
        is_in_profit = (side == "LONG" and c_price > entry) or (side == "SHORT" and c_price < entry)
        
        targets = []
        if is_in_profit:
            # إذا كان رابحاً: خيارات تأمين الربح
            targets = [
                (entry, "الدخول (BE)"),
                (calc_price(entry, 10, True, side, lev), "+10%"), 
                (calc_price(entry, 25, True, side, lev), "+25%"), 
                (calc_price(c_price, 5, False, side, lev), "Trailing 5%")
            ]
        else:
            # إذا كان خاسراً: مستويات وقف خسارة من الهامش
            for p in [20, 40, 60, 80]:
                targets.append((calc_price(entry, p, False, side, lev), f"SL {p}%"))

        # بناء الأزرار (مع فحص التصفية)
        row = []
        for opt_price, label in targets:
            # التحقق أن الـ SL ليس خلف التصفية
            valid = (side == "LONG" and opt_price > liq) or (side == "SHORT" and opt_price < liq)
            if valid or is_in_profit:
                btn_label = f"{label} ({fmt(opt_price)})"
                # نرسل السعر خام في الـ callback ليعالجه الـ handler بدقة
                row.append(InlineKeyboardButton(btn_label, callback_data=f"pr_sl_{u_id}_{t_id}_{opt_price:.6f}"))
                if len(row) == 2:
                    markup.row(*row)
                    row = []
        if row: markup.row(*row)

        # --- توليد أهداف جني الأرباح (TP) ---
        text += "\n\n<b>💰 أهداف جني الأرباح (ROE):</b>"
        tp_levels = [(50, "M1"), (100, "M2"), (200, "M3"), (500, "L1"), (1000, "L2")]
        
        tp_row = []
        for roe, lab in tp_levels:
            target_p = calc_price(entry, roe, True, side, lev)
            tp_row.append(InlineKeyboardButton(f"{lab} +{roe}% ({fmt(target_p)})", callback_data=f"pr_tp_{u_id}_{t_id}_{target_p:.6f}"))
            if len(tp_row) == 2:
                markup.row(*tp_row)
                tp_row = []
        if tp_row: markup.row(*tp_row)

        markup.add(InlineKeyboardButton("🔙 رجوع للإعدادات", callback_data=f"manage_trade:{t_id}"))

    return text, markup

async def close_trade_manually(trade_id, current_price):
    """إغلاق الصفقة وتصفية الحساب وإرجاع الرصيد للبنك بدقة float"""
    try:
        # 1. جلب بيانات الصفقة
        res = supabase.table("active_trades").select("*").eq("trade_id", str(trade_id)).execute()
        if not res.data: 
            return False, "⚠️ الصفقة غير موجودة أو تم إغلاقها مسبقاً."
        
        trade = res.data[0]
        user_id = int(trade['user_id'])
        
        # 🟢 استخدام float لضمان دقة العملات والكميات
        entry = float(trade['entry_price'])
        margin = float(trade['margin'])
        lev = float(trade['leverage'])
        side = trade['side']
        cur_price = float(current_price) 
        
        # 2. حساب الربح/الخسارة (PNL)
        if entry > 0:
            pnl_pct = (cur_price - entry) / entry if side == 'LONG' else (entry - cur_price) / entry
        else:
            pnl_pct = 0.0
            
        # الربح الفعلي = الهامش * نسبة التغير * الرافعة
        pnl_amount = margin * pnl_pct * lev
        total_return = margin + pnl_amount 
        
        # 🛡️ حماية التصفية: لا يمكن خسارة أكثر من الهامش الموضوع
        if total_return < 0: 
            total_return = 0.0 
        
        # 3. تحديث رصيد البنك (إضافة الهامش + الربح/الخسارة)
        user_res = supabase.table("users_global_profile").select("bank_balance").eq("user_id", user_id).execute()
        if user_res.data:
            current_bank = float(user_res.data[0]['bank_balance'])
            new_bank = max(0.0, current_bank + total_return) # ضمان عدم نزول البنك تحت الصفر
            
            supabase.table("users_global_profile").update({
                "bank_balance": new_bank
            }).eq("user_id", user_id).execute()
        
        # 4. تجميد الصفقة (إيقاف النشاط)
        # ملاحظة: تم الاكتفاء بـ is_active لعدم وجود أعمدة pnl/close_price في جدولك حالياً
        supabase.table("active_trades").update({
            "is_active": False
        }).eq("trade_id", str(trade_id)).execute()
        
        return True, pnl_amount

    except Exception as e:
        import logging
        logging.error(f"Error in close_trade_manually: {e}")
        return False, "❌ حدث خطأ فني أثناء تصفية الصفقة."
         
# ==========================================
# 3. قوالب واجهات المستخدم (Secured Keyboards)
# ==========================================

def get_market_keyboard(user_id):
    markup = InlineKeyboardMarkup(row_width=3)
    
    # تصحيح: إضافة الفواصل بين الأزرار وحذف المراجع النصية التي تسبب الخطأ
    markup.row(
        InlineKeyboardButton("🔥 الرائجة", callback_data=f"market_tab:{user_id}:trending"),
        InlineKeyboardButton("📈 الرابحة", callback_data=f"market_tab:{user_id}:gainers"),
        InlineKeyboardButton("📉 الخاسرة", callback_data=f"market_tab:{user_id}:losers")
    )
    
    # إضافة الأزرار الرئيسية في صفوف منفصلة
    markup.add(InlineKeyboardButton("🏦 محفظتي الماليـة", callback_data=f"wallet_view:{user_id}"))
    markup.add(InlineKeyboardButton("📋 صفقاتي المفتوحة", callback_data=f"active_trades_view:{user_id}"))

    return markup

    
    # ==========================================
# 3. قوالب واجهات المستخدم المصححة
# ==========================================
async def is_authorized(callback_query: types.CallbackQuery):
    """🛡️ الحارس الشخصي للتأكد من ملكية الأزرار"""
    data_parts = callback_query.data.split(':')
    if len(data_parts) > 1 and data_parts[1].isdigit():
        owner_id = int(data_parts[1])
        if callback_query.from_user.id != owner_id:
            await callback_query.answer("🚫 هذي ليست محفظتك! العب بعيد يا مبعسس 🤫", show_alert=True)
            return False
    return True

# ==========================================
# 3. قوالب واجهات المستخدم
# ==========================================

# --- [ 1. دالة الكيبورد التفاعلي للفريمات ] ---
def get_coin_keyboard(user_id, symbol, current_tf="15m"):
    markup = InlineKeyboardMarkup(row_width=5)
    
    # صف الفريمات (تحديد الفريم النشط)
    tfs = ['15m', '1h', '2h', '4h', '1d']
    tf_buttons = []
    for tf in tfs:
        text = f"🔘 {tf}" if tf == current_tf else tf
        tf_buttons.append(InlineKeyboardButton(text, callback_data=f"coin_view:{user_id}:{symbol}:{tf}"))
    markup.row(*tf_buttons)
    
    # صف توصية VIP
    markup.row(InlineKeyboardButton("💎 تـوصـيـة VIP حـصـريـة 💎", callback_data=f"vip_signal:{user_id}:{symbol}"))
    
    # صف الأوامر السريعة
    markup.row(
        InlineKeyboardButton("🟢 شـراء (LONG)", callback_data=f"setup_trade:{user_id}:{symbol}:LONG"),
        InlineKeyboardButton("🔴 بـيـع (SHORT)", callback_data=f"setup_trade:{user_id}:{symbol}:SHORT")
    )
    
    # زر الرجوع المخصص
    markup.row(InlineKeyboardButton("🔙 رجـوع", callback_data=f"market_tab:{user_id}:trending"))
    return markup

def get_trade_setup_keyboard(user_id):
    session = trade_sessions.get(user_id)
    if not session: return None
    
    sym = session['symbol']
    side = session['side']
    show_zones = session.get('show_zones', False) # هل عرضنا مناطق الدخول؟
    selected_price = session.get('selected_entry_price', None)

    markup = InlineKeyboardMarkup(row_width=3)
    
    # صف الرافعة والنسبة
    markup.row(
        InlineKeyboardButton(f"⚖️ {session['leverage']}x", callback_data=f"trade_cycle:{user_id}:leverage"),
        InlineKeyboardButton(f"💼 {session['margin_pct']}%", callback_data=f"trade_cycle:{user_id}:margin")
    )
    
    # زر مناطق الدخول (يتحول عند الضغط)
    if not show_zones:
        markup.add(InlineKeyboardButton("🎯 تحديد منطقة الدخول", callback_data=f"trade_zones:{user_id}:show"))
    else:
        # توليد مناطق الدخول
        c_price = session['market_price']
        high = session['high_24h']
        low = session['low_24h']        
        # داخل الكيبورد استبدل سطر zones بـ:    
        zones = []
        if side == 'LONG':
            # مناطق بين الأدنى والسعر الحالي
            zones = get_zones(low, c_price)
        else:
            # مناطق بين الحالي والأعلى
            zones = get_zones(c_price, high)
        
        zone_buttons = []
        for z in zones:
            is_sel = "✅" if selected_price and abs(selected_price - z) < 0.0001 else ""
            txt = f"{is_sel} {z:,.4f}" if z < 1 else f"{is_sel} {z:,.2f}"
            zone_buttons.append(InlineKeyboardButton(txt, callback_data=f"set_zone:{user_id}:{z}"))
        
        markup.row(*zone_buttons[:2])
        markup.row(*zone_buttons[2:])
        markup.add(InlineKeyboardButton("⚡ العودة للسعر المباشر (Market)", callback_data=f"set_zone:{user_id}:market"))

    # زر التأكيد والإلغاء
    confirm_text = "🚀 تأكيد الشراء" if side == 'LONG' else "🩸 تأكيد البيع"
    markup.add(InlineKeyboardButton(confirm_text, callback_data=f"trade_confirm:{user_id}:{sym}"))
    markup.add(InlineKeyboardButton("❌ إلغاء", callback_data=f"coin_view:{user_id}:{sym}"))
    
    return markup
    

async def update_trade_ui(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id not in trade_sessions: return
    
    # --- 🛡️ نظام منع التكرار والحظر ---
    # إذا كانت هناك حلقة تعمل بالفعل لهذا المستخدم، نقوم بإلغائها لبدء واحدة جديدة بالقيم الجديدة
    if user_id in active_updates:
        active_updates[user_id].cancel()
    
    # إنشاء مهمة (Task) جديدة للحلقة وحفظها في القاموس
    task = asyncio.create_task(run_ui_loop(callback_query, user_id))
    active_updates[user_id] = task

async def run_ui_loop(callback_query, user_id):
    """هذه الدالة الفرعية هي التي تدير الحلقة لمنع تداخل الكود"""
    try:
        for _ in range(15):
            if user_id not in trade_sessions: break
            
            session = trade_sessions[user_id]
            sym = session['symbol']
            
            # 1. جلب السعر اللحظي
            res = supabase.table("crypto_market_simulation").select("*").eq("symbol", sym).execute()
            if not res.data: break
            
            market_price = float(res.data[0]['current_price'])
            session['market_price'] = market_price
            
            # 2. تحديد نوع السعر (Market vs Limit)
            is_limit = session.get('selected_entry_price') is not None
            price = session['selected_entry_price'] if is_limit else market_price
            session['entry_price'] = price
            
            status_tag = "🕒 سـعر معلق (Limit)" if is_limit else "⚡ سـعر الـسوق (مباشر)"
            icon = "📌" if is_limit else "🔄"

            # 3. الحسابات المالية
            margin_amount = session['balance'] * (session['margin_pct'] / 100.0)
            quantity = (margin_amount * session['leverage']) / price
            liq_price = calculate_liquidation(price, session['leverage'], session['side'], margin_amount, quantity)
            
            # 4. بناء النص
            text = (
                f"⚙️ | <b>إعـداد صـفـقـة: #{sym}</b>\n"
                f"الـنوع: {'🟢 LONG' if session['side'] == 'LONG' else '🔴 SHORT'} | {status_tag}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"💵 سـعـر الـدخول: <code>{price:,.4f} $</code> {icon}\n"
                f"⚖️ الـرافـعـة: <b>{session['leverage']}x</b>\n"
                f"💼 الـمبلغ: <b>{margin_amount:,.2f} $</b> ({session['margin_pct']}%)\n"
                f"⚠️ الـتصفية: <code>{liq_price:,.4f} $</code>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"<i>البيانات تتحدث تلقائياً..</i>"
            )

            try:
                await callback_query.message.edit_text(
                    text, 
                    reply_markup=get_trade_setup_keyboard(user_id), 
                    parse_mode="HTML"
                )
            except Exception: pass

            # 🛑 إذا كان السعر معلقاً، نكتفي بتحديث واحد فقط وننهي الحلقة فوراً
            if is_limit: break
            
            await asyncio.sleep(4) # وقت أمان لمنع حظر تليجرام
            
    except asyncio.CancelledError:
        pass # تم إلغاء المهمة لبدء واحدة جديدة
    finally:
        # مسح المهمة من السجل عند الانتهاء
        if active_updates.get(user_id) == asyncio.current_task():
            active_updates.pop(user_id, None)
            


def get_wallet_keyboard(user_id, debt):
    markup = InlineKeyboardMarkup(row_width=2)
    
    # صف الإيداع والسحب
    markup.row(
        InlineKeyboardButton("📥 إيداع للتداول", callback_data=f"transfer_flow:{user_id}:to_bank"),
        InlineKeyboardButton("📤 سحب للمحفظة", callback_data=f"transfer_flow:{user_id}:to_wallet")
    )
    
    # زر القرض أو التسديد
    if debt > 0:
        # إذا كان عليه دين، يظهر زر التسديد باللون الأحمر (إيموجي)
        markup.add(InlineKeyboardButton("🔴 تسديد القرض المستحق", callback_data=f"repay_loan:{user_id}"))
    else:
        # إذا كان سليم، يظهر زر طلب القرض
        markup.add(InlineKeyboardButton("💰 طلب قرض سريع", callback_data=f"loan_menu:{user_id}"))
        
    # صف السوق والصفقات
    markup.row(
        # تم حذف الشرطة السفلية _ قبل النقطتين : لتطابق المعالج
        InlineKeyboardButton("📋 صفقاتي", callback_data=f"active_trades_view:{user_id}"),
        InlineKeyboardButton("🛒 السوق", callback_data=f"market_tab:{user_id}:trending")
    )
    return markup
    

def get_trades_keyboard(user_id, trades):
    markup = InlineKeyboardMarkup(row_width=1) 
    for trade in trades:
        # تحويل المعرف لسلسلة نصية
        t_id_str = str(trade.get('trade_id'))
        symbol = trade.get('symbol', 'COIN')
        
        # 1. زر إعدادات الصفقة (للتعديل على SL/TP)
        # 2. زر عرض الشارت (ينقله لواجهة التحليل coin_view)
        markup.row(
            InlineKeyboardButton(f"⚙️ إعدادات {symbol}", callback_data=f"manage_trade:{t_id_str}"),
            InlineKeyboardButton(f"📊 عرض الشارت", callback_data=f"coin_view:{user_id}:{symbol}")
        )        
        
    # أزرار التنقل الإضافية
    markup.add(InlineKeyboardButton("⏳ الطلبات المعلقة", callback_data=f"pending_trades_view:{user_id}"))
    markup.add(InlineKeyboardButton("🔙 العودة للسوق", callback_data=f"market_tab:{user_id}:trending"))
    return markup
    
    
class BankTransfer(StatesGroup):
    waiting_for_amount = State()      # انتظار مبلغ التحويل/الإيداع
    waiting_for_account = State()     # انتظار رقم الحساب (في حال التحويل لشخص)
# ==========================================
# 4. مستمعات المحفظة (متوافق مع Trade_ID)
# ==========================================         
@dp.message_handler(Text(equals=["محفظتي", "المحفظة"], ignore_case=True), state="*")
async def message_wallet_view(message: types.Message):
    await process_wallet_logic(message.from_user.id, message.from_user.first_name, message=message)

async def process_wallet_logic(user_id, first_name, message=None, callback=None):
    try:
        # 1. جلب بيانات المستخدم
        res = supabase.table("users_global_profile").select("*").eq("user_id", user_id).execute()
        data = res.data[0] if res.data else None

        if not data:
            return # التعامل مع الخطأ كما في كودك السابق

        bank_bal = float(data.get('bank_balance', 0.0))    # الكاش المتاح حالياً
        wallet_bal = float(data.get('wallet', 0.0))        # المحفظة الرئيسية (خارج التداول)
        debt = float(data.get('debt_balance', 0.0))
        flag = data.get('country_flag', '🇾🇪')

        # 2. تحليل الصفقات النشطة
        trades_res = supabase.table("active_trades").select("*").eq("user_id", user_id).eq("is_active", True).execute()
        active_trades = trades_res.data if trades_res.data else []
        
        long_count = 0
        short_count = 0
        total_locked_margin = 0.0  # المبالغ المستخدمة في الصفقات
        unrealized_pnl = 0.0       # الأرباح والخسائر العائمة

        for trade in active_trades:
            if trade['side'] == 'LONG': long_count += 1
            else: short_count += 1
            
            margin = float(trade['margin'])
            total_locked_margin += margin
            
            # حساب الـ PnL (نفس منطقك السابق)
            symbol = trade['symbol']
            coin_res = supabase.table("crypto_market_simulation").select("current_price").eq("symbol", symbol).execute()
            if coin_res.data:
                current_price = float(coin_res.data[0]['current_price'])
                entry = float(trade['entry_price'])
                lev = float(trade['leverage'])
                if entry > 0:
                    pnl_pct = (current_price - entry) / entry if trade['side'] == 'LONG' else (entry - current_price) / entry
                    unrealized_pnl += (margin * pnl_pct * lev)

        # 🎯 3. الحسبة اللي طلبتها يا أثر:
        # إجمالي رصيد التداول (الرأس مال الكلي) = الكاش المتاح + المبالغ المستخدمة
        total_trading_balance = bank_bal + total_locked_margin
        
        # صافي القيمة (السيولة الفعلية مع الأرباح)
        equity = total_trading_balance + unrealized_pnl
        
        pnl_color = "🟢" if unrealized_pnl >= 0 else "🔴"

        # 4. التنسيق بستايل بينانس (إظهار الجمع)
        text = (
            f"🏦 | <b>مـركـز إدارة الأصـول</b>\n"
            f"   ━━━━━━━━━━━━━━━━━━\n"
            f"👤 الـمـستخدم: <b>{first_name}</b> {flag}\n\n"
            f"💳 <b>إجمالي الرصيد (Total):</b> <code>{total_trading_balance:,.2f} $</code>\n"
            f"   <i>(كاش: {bank_bal:,.2f} + مستخدم: {total_locked_margin:,.2f})</i>\n\n"
            f"💎 <b>صافي القيمة (Equity):</b> <code>{equity:,.2f} $</code>\n"
            f"💰 <b>المحفظة الفورية:</b> <code>{wallet_bal:,.2f} $</code>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>إحصائيات المـراكز:</b>\n"
            f"🟢 شراء: <b>{long_count}</b> | 🔴 بيع: <b>{short_count}</b>\n"
            f"{pnl_color} <b>الأرباح العائمة:</b> <b>{unrealized_pnl:+.2f} $</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
        )
        
        if debt > 0:
            # إذا وصلت السيولة (Equity) للصفر، الديون ستظل موجودة لكن الحساب سيتجمد
            text += f"⚠️ <b>الـديون الـمستحقة:</b> <code>{debt:,.2f} $</code>\n"
        else:
            text += "✅ <b>حالة الائتمان:</b> ممتاز (لا يوجد دين)\n"
        
        text += "   ━━━━━━━━━━━━━━━━━━"

        # 6. استدعاء الكيبورد وتحديث الواجهة
        # نمرر قيمة debt للكيبورد لكي تظهر أزرار "تسديد الدين" إذا كان هناك دين
        markup = get_wallet_keyboard(user_id, debt)

        if message:
            await message.answer(text, reply_markup=markup, parse_mode="HTML")
        elif callback:
            # تعديل النص في الرسالة الحالية (تحديث لحظي للسعر)
            try:
                await callback.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
            except Exception:
                # لتجنب خطأ "Message is not modified" إذا لم يتغير السعر
                pass

    except Exception as e:
        import logging
        logging.error(f"❌ Wallet Error for user {user_id}: {e}")
        if message: 
            await message.answer("⚠️ فشل في تحديث بيانات المحفظة.")

 # ==========================================
# --- [ مستمع السوق ] ---
# ==========================================            
@dp.message_handler(Text(equals=["تداول", "السوق", "التداول"], ignore_case=True))
async def listener_market(message: types.Message):
    user_id = message.from_user.id
    
    # جلب العملات من السوق (Binance Mode)
    res = supabase.table("crypto_market_simulation").select("*").order("volume_24h", desc=True).limit(0).execute()
    coins = res.data
    
    text = "📊 | <b>سـوق الـعـمـلات (Binance Mode)</b>\n"
    text += "━━━━━━━━━━━━━━━━━━\n"
    text += "🔥 <b>الأكثر رواجاً حالياً:</b>\n\n"
    
    markup = get_market_keyboard(user_id)
    
    if not coins:
        text += "⚠️ لا توجد بيانات في السوق حالياً."
    else:
        for c in coins:
            sym = c['symbol']
            price = float(c['current_price'])
            chg = float(c['change_24h'])
            icon = "🟢" if chg >= 0 else "🔴"
            text += f"{icon} <b>{sym}</b> : <code>{price:,.2f} $</code> ({chg:+.2f}%)\n"
            # إضافة أزرار العملات تحت الرسالة
            markup.add(InlineKeyboardButton(f"عرض {sym} 🪙", callback_data=f"coin_view:{user_id}:{sym}"))

    await message.answer(text, reply_markup=markup, parse_mode="HTML")

    # --- 2. المستمع (الذي لا يستجيب) ---
@dp.message_handler(Text(equals=["صفقاتي", "الصفقات"], ignore_case=True), state="*")
async def listener_trades(message: types.Message):
    user_id = int(message.from_user.id)
    try:
        trades, text = await get_active_trades_report(user_id)
        
        if not trades:
            # تأكد أن دالة get_market_keyboard لا تحتوي على أخطاء أيضاً
            return await message.answer(text, reply_markup=get_market_keyboard(user_id), parse_mode="HTML")
        
        # استدعاء الكيبورد المصحح
        await message.answer(text, reply_markup=get_trades_keyboard(user_id, trades), parse_mode="HTML")
    except Exception as e:
        logging.error(f"Listener Error: {e}")
        await message.answer(f"⚠️ عذراً، حدث خطأ أثناء جلب صفقاتك: {e}")

# ==========================================
# 🎛️ 2. المستمع الرئيسي (صفقات اليوم، فلب، ترند)
# ==========================================
@dp.message_handler(Text(equals=["صفقات اليوم", "فلب", "ترند"], ignore_case=True), state="*")
async def main_deals_menu(message: types.Message):
    text = "☢️ **غرفة العمليات الاستخباراتية**\n\nاختر نوع الصفقات المرصودة من الرادار v9.0:"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🔥 صفقات VIP (الانفجار النووي)", callback_data="cat_vip"),
        InlineKeyboardButton("⚡ صفقات متوسطة (زخم تصاعدي)", callback_data="cat_mid"),
        InlineKeyboardButton("🤫 صفقات تجميع (انضغاط السيولة)", callback_data="cat_squeeze")
    )
    
    await message.reply(text, reply_markup=keyboard)


# ==========================================
# 🗂️ 3. مستمع تصنيفات الصفقات (الأقسام)
# ==========================================
@dp.callback_query_handler(Text(startswith="cat_"), state="*")
async def category_handler(call: types.CallbackQuery):
    category = call.data
    
    # جلب البيانات من سوبابيس
    res = supabase.table("market_intelligence").select("*").execute()
    coins = res.data
    
    if not coins:
        await call.answer("⚠️ لا توجد صفقات مرصودة حالياً!", show_alert=True)
        return

    filtered_coins = []
    category_title = ""

    # تصنيف ذكي للعملات
    for coin in coins:
        score = coin.get('pump_score', 0)
        is_squeezed = coin.get('is_squeezed', False)
        
        if category == "cat_vip" and score >= 180:
            filtered_coins.append(coin)
            category_title = "🔥 صفقات VIP"
        elif category == "cat_mid" and 130 <= score < 180:
            filtered_coins.append(coin)
            category_title = "⚡ صفقات متوسطة الزخم"
        elif category == "cat_squeeze" and is_squeezed:
            filtered_coins.append(coin)
            category_title = "🤫 صفقات تجميع السيولة"

    if not filtered_coins:
        await call.answer("⚠️ الرادار لم يرصد عملات في هذا القسم حالياً.", show_alert=True)
        return

    keyboard = InlineKeyboardMarkup(row_width=2)
    buttons = []
    
    # صناعة أزرار العملات بديناميكية
    for coin in filtered_coins:
        symbol = coin['symbol']
        # نحفظ القسم القديم في الكول باك لنتمكن من الرجوع إليه
        buttons.append(InlineKeyboardButton(f"🪙 {symbol}", callback_data=f"coo_{symbol}_{category}"))
    
    keyboard.add(*buttons) # إضافة الأزرار صفين صفين
    keyboard.add(InlineKeyboardButton("🔙 رجوع للقائمة الرئيسية", callback_data="back_to_main"))
    
    await call.message.edit_text(
        f"📊 **{category_title}**\n\nاختر العملة لعرض التحليل الاستخباري:",
        reply_markup=keyboard
    )


# ==========================================
# 🪙 4. مستمع عرض قالب العملة المختار
# ==========================================
@dp.callback_query_handler(Text(startswith="coo_"), state="*")
async def coin_detail_handler(call: types.CallbackQuery):
    # تفكيك الكول باك (مثال: coin_ORDIUSDT_cat_vip)
    parts = call.data.split("_")
    symbol = parts[1]
    prev_category = f"{parts[2]}_{parts[3]}" 
    
    # جلب بيانات العملة المحددة من سوبابيس
    res = supabase.table("market_intelligence").select("*").eq("symbol", symbol).execute()
    coin_data = res.data
    
    if not coin_data:
        await call.answer("⚠️ حدث خطأ: لا توجد بيانات لهذه العملة.", show_alert=True)
        return
        
    coin = coin_data[0]
    
    # بناء القالب باستخدام الدالة المخصصة
    template = build_coin_template(coin)
    
    # زر الرجوع للقسم المحدد
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("🔙 رجوع للقائمة السابقة", callback_data=prev_category))
    
    await call.message.edit_text(template, reply_markup=keyboard, parse_mode="Markdown")


# ==========================================
# 🔙 5. مستمع الرجوع للقائمة الرئيسية
# ==========================================
@dp.callback_query_handler(Text(equals="back_to_main"), state="*")
async def back_to_main_handler(call: types.CallbackQuery):
    text = "☢️ **غرفة العمليات الاستخباراتية**\n\nاختر نوع الصفقات المرصودة من الرادار v9.0:"
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🔥 صفقات VIP (الانفجار النووي)", callback_data="cat_vip"),
        InlineKeyboardButton("⚡ صفقات متوسطة (زخم تصاعدي)", callback_data="cat_mid"),
        InlineKeyboardButton("🤫 صفقات تجميع (انضغاط السيولة)", callback_data="cat_squeeze")
    )
    
    await call.message.edit_text(text, reply_markup=keyboard)
    
 # ==========================================
# 6. معالجات الأزرار الأساسية (Secured Callbacks)
# ==========================================
# --- 🖱️ تحديث معالج الكولباك ليستخدم نفس الدالة الموحدة ---
@dp.callback_query_handler(lambda c: c.data == 'view_intel_report')
async def show_intelligence_report(callback_query: types.CallbackQuery):
    if callback_query.from_user.id != ADMIN_ID:
        return await callback_query.answer("❌ عذراً، هذا القسم مخصص للمالك فقط.", show_alert=True)

    report_text, markup = await get_intelligence_report_text()
    
    try:
        await callback_query.message.edit_text(
            report_text, 
            reply_markup=markup, 
            parse_mode="HTML"
        )
    except:
        # في حال لم يتغير النص أو حدث خطأ في التعديل
        await callback_query.answer("تم تحديث البيانات")
       
@dp.callback_query_handler(lambda c: c.data and c.data.startswith('wallet_view:'), state="*")
async def callback_wallet_view(callback_query: types.CallbackQuery):
    user_id = int(callback_query.data.split(':')[1])
    if callback_query.from_user.id != user_id:
        return await callback_query.answer("❌ هذه المحفظة ليست لك!", show_alert=True)
    await process_wallet_logic(user_id, callback_query.from_user.first_name, callback=callback_query)


@dp.callback_query_handler(Text(startswith='market_tab:'), state="*")
async def callback_market_tabs(callback_query: types.CallbackQuery):
    # 🔐 القفل الأمني
    data_parts = callback_query.data.split(':')
    owner_id = int(data_parts[1])
    visitor_id = callback_query.from_user.id

    if visitor_id != owner_id:
        return await callback_query.answer("⚠️ هذه القائمة ليست لك!", show_alert=True)

    if not await is_authorized(callback_query): return
    
    try:
        tab_type = data_parts[2]
        # استخراج الصفحة الحالية (إذا لم توجد نبدأ من 0)
        page = int(data_parts[3]) if len(data_parts) > 3 else 0
        per_page = 24 # عدد العملات في كل صفحة
        start = page * per_page
        end = start + per_page - 1
        
        # جلب البيانات بناءً على التبويب مع تحديد النطاق (Range)
        query = supabase.table("crypto_market_simulation").select("*")
        
        if tab_type == 'gainers':
            res = query.order("change_24h", desc=True).range(start, end).execute()
            header = "📈 <b>الأعلى ربحاً (24h):</b>"
        elif tab_type == 'losers':
            res = query.order("change_24h", desc=False).range(start, end).execute()
            header = "📉 <b>الأكثر خسارة (24h):</b>"
        else: # trending
            res = query.order("volume_24h", desc=True).range(start, end).execute()
            header = "🔥 <b>الأكثر رواجاً (السيولة):</b>"
            
        if not res.data:
            return await callback_query.answer("⚠️ لا توجد عملات إضافية في هذا التبويب.", show_alert=True)

        text = f"📊 | <b>سـوق الـعـمـلات (Binance Mode)</b>\n"
        text += f"━━━━━━━━━━━━━━━━━━\n"
        text += f"{header} (صفحة {page + 1})\n\n"
        
        markup = InlineKeyboardMarkup(row_width=2)
        
        for c in res.data:
            sym = c['symbol'].replace("USDT", "")
            price = float(c.get('current_price', 0))
            chg = float(c.get('change_24h', 0))
            
            icon = "🟢" if chg >= 0 else "🔴"
            price_format = f"{price:,.4f}" if price < 1 else f"{price:,.2f}"
            
            text += f"{icon} <b>{sym}</b> : <code>{price_format}$</code> ({chg:+.2f}%)\n"
            markup.insert(InlineKeyboardButton(f"🪙 {sym}", callback_data=f"coin_view:{owner_id}:{c['symbol']}"))

        # --- [ صف الأزرار الوظيفية (التنقل) ] ---
        nav_buttons = []
        # زر "السابق": يظهر فقط إذا لم نكن في الصفحة الأولى
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"market_tab:{owner_id}:{tab_type}:{page - 1}"))
        
        # زر "التالي": يظهر دائماً طالما أن الصفحة الحالية ممتلئة (مما يعني وجود المزيد غالباً)
        if len(res.data) == per_page:
            nav_buttons.append(InlineKeyboardButton("التالي ➡️", callback_data=f"market_tab:{owner_id}:{tab_type}:{page + 1}"))
        
        if nav_buttons:
            markup.row(*nav_buttons)

        # أزرار التبويبات الرئيسية
        markup.row(
            InlineKeyboardButton("🔥 الرائجة", callback_data=f"market_tab:{owner_id}:trending:0"),
            InlineKeyboardButton("📈 الرابحة", callback_data=f"market_tab:{owner_id}:gainers:0"),
            InlineKeyboardButton("📉 الخاسرة", callback_data=f"market_tab:{owner_id}:losers:0")
        )
        markup.add(InlineKeyboardButton("🔙 عودة للمحفظة", callback_data=f"wallet_view:{owner_id}"))
        
        await callback_query.message.edit_text(text, reply_markup=markup, parse_mode="HTML")

    except Exception as e:
        logging.error(f"Error in market_tab: {e}")
        await callback_query.answer("⚠️ فشل تحديث بيانات السوق.", show_alert=True)
        

# --- 3. الكولباك (الذي لا يستجيب للضغط + حماية وتنظيف) --
@dp.callback_query_handler(Text(startswith='active_trades_view:'), state="*")
async def callback_view_trades(callback_query: types.CallbackQuery):
    await callback_query.answer()
    
    # تفكيك البيانات باستخدام النقطتين :
    # البيانات المتوقعة: active_trades_view:123456
    data = callback_query.data.split(':') 
    user_id = int(data[1]) # الآيدي سيكون في الخانة الثانية [1]
    
    # 🛡️ الجدار الناري
    if callback_query.from_user.id != user_id:
        return await callback_query.answer("⚠️ ليس لديك صلاحية للوصول إلى لوحة غيرك!", show_alert=True)
    

    try:
        trades, text = await get_active_trades_report(user_id)
        
        # دالة حذف الرسالة في الخلفية
        async def delete_message_later(msg, delay=600):
            await asyncio.sleep(delay)
            try:
                await msg.delete()
            except:
                pass # تجاهل الخطأ لو المستخدم حذفها يدوياً
                
        if not trades:
            msg = await callback_query.message.edit_text(
                text, 
                reply_markup=get_market_keyboard(user_id), 
                parse_mode="HTML"
            )
        else:
            msg = await callback_query.message.edit_text(
                text, 
                reply_markup=get_trades_keyboard(user_id, trades), 
                parse_mode="HTML"
            )
            
        # تشغيل المؤقت (5 دقائق = 300 ثانية)
        asyncio.create_task(delete_message_later(callback_query.message, 600))
        
    except Exception as e:
        logging.error(f"Callback View Error: {e}")
        await callback_query.message.answer(f"❌ فشل عرض الصفقات.")             

# --- [ 2. هاندلر عرض الشارت التفاعلي ] ---
@dp.callback_query_handler(Text(startswith='coin_view:'), state="*")
async def process_coin_view(callback_query: types.CallbackQuery):
    try:
        data_parts = callback_query.data.split(':')
        owner_id = int(data_parts[1])
        symbol = data_parts[2]
        tf = data_parts[3] if len(data_parts) > 3 else "15m"
        visitor_id = callback_query.from_user.id

        if visitor_id != owner_id:
            return await callback_query.answer("⚠️ هذه البيانات ليست لك!", show_alert=True)

        res = supabase.table("crypto_market_simulation").select("*").eq("symbol", symbol).execute()
        if not res.data:
            return await callback_query.answer("⚠️ العملة غير موجودة!", show_alert=True)
            
        coin = res.data[0]
        price = float(coin.get('current_price', 0))
        high = float(coin.get('high_24h', 0))
        low = float(coin.get('low_24h', 0))
        change = float(coin.get('change_24h', 0))
        
        # --- [ استدعاء البيانات الاستخباراتية المحدثة ] ---
        # بيانات السيولة (OBV)
        vol_current = float(coin.get(f'volume_{tf}', 0))
        obv_now = float(coin.get(f'obv_{tf}', 0))
        obv_prev = float(coin.get(f'obv_prev_{tf}', 0))
        obv_slope = float(coin.get(f'obv_slope_{tf}', 0))
        
        # بيانات عرض القناة (BBW) - "فم التمساح"
        bbw_now = float(coin.get(f'bbw_{tf}', 0))
        bbw_prev = float(coin.get(f'bbw_prev_{tf}', 0))
        
        # حساب نسبة الانفجار (Expansion Ratio)
        expansion = (bbw_now / bbw_prev * 100) if bbw_prev > 0 else 100

        # مؤشرات الشارت
        ema20 = float(coin.get(f'ema_20_{tf}', price))
        ema50 = float(coin.get(f'ema_50_{tf}', price))
        ema100 = float(coin.get(f'ema_100_{tf}', price))
        bb_up = float(coin.get(f'bb_upper_{tf}', price))
        bb_mid = float(coin.get(f'bb_middle_{tf}', price))
        bb_low = float(coin.get(f'bb_lower_{tf}', price))
        rsi = float(coin.get(f'rsi_{tf}', 50))

        def f_num(val): return f"{val:,.4f}" if val < 1 else f"{val:,.2f}"
        
        # أيقونات ذكية للحالة
        expansion_icon = "🔥" if expansion > 110 else "💤"
        obv_icon = "🌊" if obv_slope > 0 else "📉"

        # ترتيب الشارت الديناميكي
        chart_elements = [
            {"name": "البولنجر العلوي", "val": bb_up, "icon": "🟡"},
            {"name": "البولنجر الأوسط", "val": bb_mid, "icon": "⚪"},
            {"name": "البولنجر السفلي", "val": bb_low, "icon": "🟡"},
            {"name": "خط EMA 100", "val": ema100, "icon": "🔵"},
            {"name": "خط EMA 50", "val": ema50, "icon": "🟢"},
            {"name": "خط EMA 20", "val": ema20, "icon": "🔴"},
            {"name": "سعر العملة الحالي", "val": price, "icon": "💵"}
        ]
        chart_elements.sort(key=lambda x: x["val"], reverse=True)

        # 📝 [ بناء الرسالة النهائية الاستخباراتية ]
        text = f"<b>{symbol.replace('USDT', '')} / USDT</b> | ⏱ {tf}\n"
        text += f"💰 السعر: <code>{f_num(price)}</code> ({change:+.2f}%)\n"
        text += f"🔝 أعلى: <code>{f_num(high)}</code> | 🔙 أدنى: <code>{f_num(low)}</code>\n"
        
        text += "----------------------\n"
        text += f"💎 <b>قسم استخبارات السيولة (OBV):</b>\n"
        text += f"• الحالي: <code>{obv_now:,.0f}</code>\n"
        text += f"• السابق: <code>{obv_prev:,.0f}</code>\n"
        text += f"{obv_icon} الميل (Slope): <code>{obv_slope:,.0f}</code>\n"
        
        text += "----------------------\n"
        text += f"🐊 <b>قوة الانفجار (BBW):</b>\n"
        text += f"• عرض القناة: <code>{bbw_now:.4f}</code>\n"
        text += f"{expansion_icon} نسبة التوسع: <code>{expansion:.1f}%</code>\n"
        
        text += "----------------------\n"
        for el in chart_elements:
            text += f"{el['icon']}: {el['name']} {{ <code>{f_num(el['val'])}</code> }}\n"
            
        text += "----------------------\n"
        text += f"📈 RSI 14: <b>{rsi:.1f}</b> | 🧭 OBV/V: <code>{vol_current:,.0f}</code>\n"
        text += "⚠️ <i>إعداداتك الذهبية: RSI (22 / 78)</i>\n"
        text += "===================="

        await callback_query.message.edit_text(
            text, 
            reply_markup=get_coin_keyboard(owner_id, symbol, tf), 
            parse_mode="HTML"
        )
        await callback_query.answer()
    except Exception as e:
        print(f"Error: {e}")
        await callback_query.answer("❌ حدث خطأ في معالجة البيانات.")
        

# --- [ 3. هاندلر توصية VIP (قالب العنود / الدخول الهجومي) ] ---
# --- [ 3. هاندلر توصية VIP (قالب العنود / الدخول الهجومي) ] ---
# 🛠️ [ أداة تحليل المخاطر المحسنة - جدار الحماية ]
def evaluate_reversal_risk(current_price, support_1d, resistance_1d, direction):
    try:
        if direction == "LONG":
            distance_to_res = (resistance_1d - current_price) / current_price
            risk_score = 99 if distance_to_res < 0.01 else max(10, 100 - (distance_to_res * 1000))
            return min(risk_score, 99)
        elif direction == "SHORT":
            distance_to_sup = (current_price - support_1d) / current_price
            risk_score = 99 if distance_to_sup < 0.01 else max(10, 100 - (distance_to_sup * 1000))
            return min(risk_score, 99)
    except ZeroDivisionError:
        return 50

# 🚀 [ غرفة العمليات الـ VIP - خوارزمية كشف النوايا والانفجار ]
@dp.callback_query_handler(Text(startswith='vip_signal:'), state="*")
async def process_vip_signal(callback_query: types.CallbackQuery):
    def f_num(val): 
        if val is None or val == 0: return "0.00"
        return f"{val:.5f}".rstrip('0').rstrip('.') if val < 1 else f"{val:.4f}"

    try:
        data_parts = callback_query.data.split(':')
        owner_id = int(data_parts[1])
        symbol = data_parts[2]

        if callback_query.from_user.id != owner_id:
            return await callback_query.answer("⚠️ مستوى أمني غير كافٍ!", show_alert=True)

        res = supabase.table("crypto_market_simulation").select("*").eq("symbol", symbol).execute()
        if not res.data: 
            return await callback_query.answer("❌ لا توجد بيانات كافية.", show_alert=True)
        
        c = res.data[0]
        price = float(c['current_price'])
        
        # --- 1️⃣ سحب البيانات الأساسية ---
        obv_slope_15m = float(c.get('obv_slope_15m', 0))
        orderbook_imb = float(c.get('orderbook_imbalance_ratio', 1.0))
        whale_absorption = c.get('whale_absorption_detected', False)
        
        bb_up_15m = float(c.get('bb_upper_15m', price * 1.01))
        bb_low_15m = float(c.get('bb_lower_15m', price * 0.99))
        kc_up_15m = float(c.get('kc_upper_15m', price * 1.02))
        kc_low_15m = float(c.get('kc_lower_15m', price * 0.98))
        bbw_15m = float(c.get('bbw_15m', 0.05))
        bbw_prev_15m = float(c.get('bbw_prev_15m', 0.05))
        
        is_squeezed = (bb_up_15m < kc_up_15m) and (bb_low_15m > kc_low_15m)
        is_expanding = bbw_15m > bbw_prev_15m
        
        ema20_15m = float(c.get('ema_20_15m', price))
        ema50_15m = float(c.get('ema_50_15m', price))
        rsi_15m = float(c.get('rsi_15m', 50))
        macd_15m = float(c.get('macd_15m', 0))
        macd_sig_15m = float(c.get('macd_signal_15m', 0))
        atr_15m = float(c.get('atr_15m', price * 0.01))

        support_1h = float(c.get('support_1h', price * 0.98))
        res_1h = float(c.get('resistance_1h', price * 1.02))
        support_1d = float(c.get('support_1d', price * 0.85))
        res_1d = float(c.get('resistance_1d', price * 1.15))

        # --- 📐 سحب بيانات البرايس أكشن والترند ---
        trend_1h = c.get('1h_trend_direction', 'SIDEWAY')
        channel_1h_status = c.get('1h_channel_status', 'NONE')
        pattern_15m = c.get('15m_pattern_name', 'NONE')
        pattern_class = c.get('15m_pattern_class', 'NONE')

        # --- 🧠 2️⃣ محرك القرار المتقدم (نظام النقاط الشامل) ---
        bull_score = 0
        bear_score = 0
        
        # أ. تقييم السيولة والحيتان (Weight: 30)
        if orderbook_imb > 1.05: bull_score += 15
        elif orderbook_imb < 0.95: bear_score += 15
        
        if obv_slope_15m > 0: bull_score += 15
        elif obv_slope_15m < 0: bear_score += 15
        
        # ب. تقييم المؤشرات الفنية (Weight: 30)
        if price > ema50_15m: bull_score += 10
        else: bear_score += 10
            
        if macd_15m > macd_sig_15m: bull_score += 10
        else: bear_score += 10
            
        if rsi_15m > 55 and rsi_15m < 78: bull_score += 10
        elif rsi_15m < 45 and rsi_15m > 22: bear_score += 10

        # ج. تقييم الحيتان (Weight: 10)
        if whale_absorption and orderbook_imb > 1: bull_score += 10
        elif whale_absorption and orderbook_imb < 1: bear_score += 10

        # د. تقييم البرايس أكشن والترند والنماذج (Weight: 30)
        if trend_1h == "UP": bull_score += 10
        elif trend_1h == "DOWN": bear_score += 10

        if channel_1h_status in ["BREAKOUT_UP", "RETEST_UP"]: bull_score += 10
        elif channel_1h_status in ["BREAKOUT_DOWN", "RETEST_DOWN"]: bear_score += 10

        bullish_patterns = ["Bullish Flag", "Bullish Pennant", "Symmetrical Triangle", "Ascending Triangle", "Falling Wedge", "Double Bottom", "Inverted Head and Shoulders"]
        bearish_patterns = ["Bearish Flag", "Bearish Pennant", "Descending Triangle", "Rising Wedge", "Double Top", "Head and Shoulders"]

        if pattern_15m in bullish_patterns and rsi_15m < 78: bull_score += 10
        elif pattern_15m in bearish_patterns and rsi_15m > 22: bear_score += 10

        # --- 📊 3️⃣ تحديد الاتجاه النهائي بناءً على المنتصر ---
        total_score = bull_score + bear_score
        if total_score == 0: total_score = 1
        
        if bull_score >= bear_score:
            trade_direction = "LONG"
            direction_text = "شراء (LONG) 🟢"
            emoji_trend = "🚀"
            confidence_rate = min((bull_score / 100) * 100 * 1.2, 99) # Boost confidence slightly if elements align
        else:
            trade_direction = "SHORT"
            direction_text = "بيع (SHORT) 🔴"
            emoji_trend = "📉"
            confidence_rate = min((bear_score / 100) * 100 * 1.2, 99)

        risk_percentage = evaluate_reversal_risk(price, support_1d, res_1d, trade_direction)
        
        # --- ⏳ 4️⃣ تحديد التوقيت الزمني للحركة ---
        if channel_1h_status in ["BREAKOUT_UP", "BREAKOUT_DOWN"]:
            time_estimate = "الآن (انفجار سيولة 🌊)"
            move_when = "تم كسر القناة السعرية بقوة"
        elif channel_1h_status in ["RETEST_UP", "RETEST_DOWN"]:
            time_estimate = "جاهز للانطلاق 🎯"
            move_when = "نهاية إعادة الاختبار (قنص الارتداد)"
        elif is_expanding:
            time_estimate = "الآن (بدأ تدفق السيولة 🌊)"
            move_when = "السعر يتحرك في هذه اللحظة"
        elif is_squeezed:
            time_estimate = "خلال 15 - 45 دقيقة ⏳"
            move_when = "بعد كسر الانضغاط السعري (Squeeze Breakout)"
        else:
            time_estimate = "خلال 1 - 4 ساعات 🕰️"
            move_when = "حركة اعتيادية متدرجة"

        # --- 🎯 5️⃣ تحديد الأهداف ونقاط الدخول (دخول هجومي متقدم) ---
        if trade_direction == "LONG":
            entry_1 = price
            # دخول هجومي على دعم قوي مثل EMA20 أو بعد إعادة اختبار القناة
            entry_2 = ema20_15m if ema20_15m < price else price * 0.995
            dca = ema50_15m
            sl = ema50_15m - (atr_15m * 1.5)
            
            tp1 = res_1h if (res_1h - price) > (atr_15m * 1.2) else price + (atr_15m * 1.5)
            tp2 = tp1 + (atr_15m * 2.0)
            tp3 = min(res_1d, tp2 + (atr_15m * 3.5))
        else:
            entry_1 = price
            entry_2 = ema20_15m if ema20_15m > price else price * 1.005
            dca = ema50_15m
            sl = ema50_15m + (atr_15m * 1.5)
            
            tp1 = support_1h if (price - support_1h) > (atr_15m * 1.2) else price - (atr_15m * 1.5)
            tp2 = tp1 - (atr_15m * 2.0)
            tp3 = max(support_1d, tp2 - (atr_15m * 3.5))

        stars = "⭐" * int(confidence_rate / 20) if confidence_rate >= 20 else "⭐"

        # تجهيز نصوص البرايس أكشن للعرض
        trend_display = "صاعد 📈" if trend_1h == "UP" else "هابط 📉" if trend_1h == "DOWN" else "عرضي ↔️"
        pattern_display = f"نموذج {pattern_15m} ({'إيجابي' if pattern_15m in bullish_patterns else 'سلبي'})" if pattern_15m != "NONE" else "لا يوجد"
        
        channel_display = "مستقرة داخل النطاق"
        if "BREAKOUT" in channel_1h_status: channel_display = "🔥 اختراق قوي للقناة السعرية"
        elif "RETEST" in channel_1h_status: channel_display = "🎯 إعادة اختبار ناجحة (فرصة قنص)"

        # --- 📝 6️⃣ القالب النهائي (VIP) ---
        signal_text = f"🔥 <b> القنص المتقدم :</b> #{symbol} {emoji_trend}\n"
        signal_text += f"ــــــــــــــــــــــــــــــــــــــــــــــــــ\n\n"
        
        signal_text += f"📊 <b>الوضع الفني والبرايس أكشن :</b>\n"
        signal_text += f"• القرار: <b>{direction_text}</b>\n"
        signal_text += f"• جودة الصفقة: {stars} ({confidence_rate:.0f}%)\n"
        signal_text += f"• الترند العام (1H): <b>{trend_display}</b>\n"
        signal_text += f"• حالة القناة: <b>{channel_display}</b>\n"
        if pattern_15m != "NONE":
            signal_text += f"• النماذج الفنية: <b>{pattern_display}</b>\n"
        signal_text += f"• نسبة المخاطرة: <b>{risk_percentage:.0f}%</b> {'🟢' if risk_percentage < 40 else '🟡' if risk_percentage < 70 else '🔴'}\n\n"

        signal_text += f"⏳ <b>التوقيت الزمني للحركة:</b>\n"
        signal_text += f"• متى سيتحرك؟: <b>{move_when}</b>\n"
        signal_text += f"• المدة المتوقعة: <b>{time_estimate}</b>\n\n"
        
        signal_text += f"📐 <b>خطة الهجوم الموصى بها:</b>\n"
        signal_text += f"🎯 مناطق الدخول: <code>{f_num(entry_2)}</code> - <code>{f_num(entry_1)}</code>\n"
        signal_text += f"🛡️ نقطة التبريد (DCA): <code>{f_num(dca)}</code>\n"
        signal_text += f"🚫 وقف الخسارة (SL): <code>{f_num(sl)}</code>\n\n"
        
        signal_text += f"💰 <b>محطات جني الأرباح:</b>\n"
        signal_text += f"1️⃣ الهدف الأول: <code>{f_num(tp1)}</code> ⚡\n"
        signal_text += f"2️⃣ الهدف الثاني: <code>{f_num(tp2)}</code> 🚀\n"
        signal_text += f"3️⃣ الهدف الثالث: <code>{f_num(tp3)}</code> 🐋\n"

        back_kb = InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 رجوع", callback_data=f"coin_view:{owner_id}:{symbol}:15m"))
        await callback_query.message.edit_text(signal_text, reply_markup=back_kb, parse_mode="HTML")

    except Exception as e:
        print(f"VIP Error: {e}")
        await callback_query.answer("❌ تعذر التوليد. حدث خطأ أثناء تحليل البيانات.", show_alert=True)
        
# ==========================================
# 7. معالجات دورة الصفقة (المطورة لدعم الفواصل والأمان)
# ==========================================
@dp.callback_query_handler(Text(startswith='setup_trade:'), state="*")
async def process_setup_trade(callback_query: types.CallbackQuery):
    data_parts = callback_query.data.split(':')
    owner_id = int(data_parts[1])
    if callback_query.from_user.id != owner_id:
        return await callback_query.answer("⚠️ المتصفح ليس لك!", show_alert=True)

    symbol = data_parts[2]
    side = data_parts[3]
    
    try:
        # جلب السعر والمستويات (High/Low)
        coin_res = supabase.table("crypto_market_simulation").select("*").eq("symbol", symbol).execute()
        if not coin_res.data:
            return await callback_query.answer("⚠️ العملة غير متوفرة.", show_alert=True)
            
        coin = coin_res.data[0]
        price = float(coin['current_price'])
        balance = await get_user_bank_balance(owner_id)
        
        # تخزين الجلسة مع إضافة بيانات الـ High و Low ومفتاح للمناطق
        trade_sessions[owner_id] = {
            'symbol': symbol,
            'side': side,
            'market_price': price,        # السعر المباشر
            'entry_price': price,         # السعر المعتمد (قد يتغير لو اختار منطقة)
            'selected_entry_price': None, # لحفظ السعر المختار يدوياً
            'high_24h': float(coin.get('high_24h', price)),
            'low_24h': float(coin.get('low_24h', price)),
            'leverage': 10,
            'margin_pct': 25,
            'balance': float(balance),
            'show_zones': False           # لإظهار/إخفاء أزرار المناطق
        }
        
        # حذفنا المدة كما طلبت، وسنبدأ التحديث اللحظي
        await update_trade_ui(callback_query)
        
    except Exception as e:
        print(f"Error: {e}")
        await callback_query.answer("⚠️ خطأ في التجهيز.")
        

@dp.callback_query_handler(Text(startswith='trade_cycle:'), state="*")
async def process_trade_cycle(callback_query: types.CallbackQuery):
    data_parts = callback_query.data.split(':')
    owner_id = int(data_parts[1])
    
    if callback_query.from_user.id != owner_id:
        return await callback_query.answer("⚠️ المتصفح ليس لك!", show_alert=True)

    if owner_id not in trade_sessions:
        return await callback_query.answer("⚠️ انتهت الجلسة.")
    
    action = data_parts[2]
    session = trade_sessions[owner_id]
    
    # تحديث القيم في الجلسة
    if action == 'leverage':
        idx = LEVERAGE_LEVELS.index(session['leverage'])
        session['leverage'] = LEVERAGE_LEVELS[(idx + 1) % len(LEVERAGE_LEVELS)]
    elif action == 'margin':
        idx = MARGIN_PCT_LEVELS.index(session['margin_pct'])
        session['margin_pct'] = MARGIN_PCT_LEVELS[(idx + 1) % len(MARGIN_PCT_LEVELS)]
    
    # الإجابة على الكولباك لمنع ظهور الساعة الرملية
    await callback_query.answer(f"تم تحديث {action}")
    
    # استدعاء التحديث (الدالة ستحمي نفسها من التكرار)
    await update_trade_ui(callback_query)

@dp.callback_query_handler(Text(startswith='trade_zones:'), state="*")
async def handle_trade_zones_activation(callback_query: types.CallbackQuery):
    data = callback_query.data.split(':')
    user_id = int(data[1])
    
    if user_id not in trade_sessions:
        return await callback_query.answer("⚠️ الجلسة منتهية.")

    # تفعيل عرض المناطق
    trade_sessions[user_id]['show_zones'] = True
    
    await callback_query.answer("🎯 جاري استخراج مناطق الدخول...")
    
    # التحديث فوراً
    await update_trade_ui(callback_query)

@dp.callback_query_handler(Text(startswith='set_zone:'), state="*")
async def handle_set_zone(callback_query: types.CallbackQuery):
    data = callback_query.data.split(':')
    user_id = int(data[1])
    value = data[2]

    if user_id not in trade_sessions:
        return await callback_query.answer("⚠️ انتهت الجلسة.")

    if value == "market":
        trade_sessions[user_id]['selected_entry_price'] = None
    else:
        # تحديد السعر المختار يدوياً (Limit Order)
        trade_sessions[user_id]['selected_entry_price'] = float(value)
        trade_sessions[user_id]['entry_price'] = float(value)

    await callback_query.answer("📍 تم تحديد سعر الدخول")
    await update_trade_ui(callback_query)
        

@dp.callback_query_handler(Text(startswith='trade_confirm:'), state="*")
async def process_trade_confirm(callback_query: types.CallbackQuery):
    data_parts = callback_query.data.split(':')
    owner_id = int(data_parts[1])
    if callback_query.from_user.id != owner_id:
        return await callback_query.answer("⚠️ لا يمكنك تأكيد صفقة غيرك!", show_alert=True)

    if owner_id not in trade_sessions:
        return await callback_query.answer("⚠️ انتهت الجلسة.", show_alert=True)
        
    session = trade_sessions[owner_id]
    
    # حساب الهامش
    margin_amount = session['balance'] * (session['margin_pct'] / 100.0)
    
    # فحص هل هي صفقة "معلقة" (Limit) أم "فورية" (Market)
    is_limit = session.get('selected_entry_price') is not None
    is_active_status = not is_limit  # إذا كان ليميت تكون False
    
    # السعر المعتمد للتنفيذ
    exec_price = session['entry_price']

    try:
        # الحسابات الدقيقة
        quantity = (margin_amount * session['leverage']) / exec_price
        liq_price = calculate_liquidation(exec_price, session['leverage'], session['side'])
        
        new_balance = session['balance'] - margin_amount
        
        # 1. تحديث الرصيد (يتم خصم المبلغ بمجرد فتح الطلب سواء معلق أو فوري لضمان الجدية)
        supabase.table("users_global_profile").update({
            "bank_balance": float(new_balance) 
        }).eq("user_id", owner_id).execute()
        
        # 2. إدخال البيانات في active_trades
        trade_data = {
            "user_id": owner_id,
            "symbol": session['symbol'],
            "side": session['side'],
            "entry_price": exec_price,
            "leverage": session['leverage'],
            "margin": margin_amount,
            "quantity": quantity,
            "liquidation_price": liq_price,
            "is_active": is_active_status, # التعديل الجوهري هنا ✅
            "created_at": datetime.now().isoformat()
        }
        
        supabase.table("active_trades").insert(trade_data).execute()
        
        # 3. عرض رسالة النجاح
        status_text = "⚡ صفقة فورية نشطة" if is_active_status else "⏳ طلب معلق (Limit)"
        
        text = f"✅ <b>تم تنفيذ العملية بنجاح!</b>\n\n"
        text += f"الحالة: {status_text}\n"
        text += f"العملة: #{session['symbol']}\n"
        text += f"سعر الدخول: <code>{exec_price:,.4f} $</code>\n"
        text += f"المبلغ المحجوز: <code>{margin_amount:,.2f} $</code>\n"
        text += f"الرصيد المتبقي: <code>{new_balance:,.2f} $</code>"
        
        # تنظيف الجلسة
        del trade_sessions[owner_id]
        
        markup = InlineKeyboardMarkup()
        btn_text = "📋 صفقاتي النشطة" if is_active_status else "⏳ طلباتي المعلقة"
        markup.add(InlineKeyboardButton(btn_text, callback_data=f"active_trades_view:{owner_id}"))
        markup.add(InlineKeyboardButton("🔙 العودة للسوق", callback_data=f"market_tab:{owner_id}:trending"))
        
        await callback_query.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
          
    except Exception as e:
        print(f"Trade Confirmation Error: {e}")
        await callback_query.answer("❌ فشل تنفيذ الصفقة.")
        
@dp.callback_query_handler(Text(startswith='pending_trades_view:'), state="*")
async def pending_trades_view(callback_query: types.CallbackQuery):
    data_parts = callback_query.data.split(':')
    owner_id = int(data_parts[1])
    
    # التأكد من هوية المستخدم
    if callback_query.from_user.id != owner_id:
        return await callback_query.answer("⚠️ هذه القائمة ليست لك!", show_alert=True)

    try:
        # جلب الصفقات غير النشطة (is_active = False)
        res = supabase.table("active_trades")\
            .select("*")\
            .eq("user_id", owner_id)\
            .eq("is_active", False)\
            .order("created_at", desc=True).execute()
        
        trades = res.data
        
        if not trades:
            text = "⏳ <b>لا توجد لديك طلبات معلقة حالياً.</b>\n\n<i>بمجرد تحديد منطقة دخول، ستظهر طلباتك هنا حتى يلمسها السعر.</i>"
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("📋 عرض الصفقات النشطة", callback_data=f"active_trades_view:{owner_id}"))
            markup.add(InlineKeyboardButton("🔙 العودة للمحفظة", callback_data=f"wallet_tab:{owner_id}"))
            return await callback_query.message.edit_text(text, reply_markup=markup, parse_mode="HTML")

        text = "⏳ <b>قائمة الطلبات المعلقة (Limit Orders)</b>\n"
        text += "━━━━━━━━━━━━━━━━━━\n\n"
        
        markup = InlineKeyboardMarkup(row_width=2)
        
        for t in trades:
            side_icon = "🟢" if t['side'] == 'LONG' else "🔴"
            entry_p = float(t['entry_price'])
            margin = float(t['margin'])
            symbol = t['symbol']
            trade_id = t['trade_id']
            
            p_fmt = f"{entry_p:,.4f}" if entry_p < 1 else f"{entry_p:,.2f}"
            
            text += f"{side_icon} <b>#{symbol}</b> ({t['leverage']}x)\n"
            text += f"🎯 سعر الدخول المطلوب: <code>{p_fmt} $</code>\n"
            text += f"💰 الهامش المحجوز: <code>{margin:,.2f} $</code>\n"
            text += f"🗓 التاريخ: <code>{t['created_at'][:16].replace('T', ' ')}</code>\n"
            text += "━━━━━━━━━━━━━━━━━━\n"
            
            # زر إلغاء الطلب لكل صفقة
            markup.add(InlineKeyboardButton(f"❌ إلغاء طلب {symbol}", callback_data=f"cancel_limit:{owner_id}:{trade_id}"))

        # أزرار التنقل السفلية
        markup.row(
            InlineKeyboardButton("🔙 صفقاتي النشطة", callback_data=f"active_trades_view:{owner_id}"),
            InlineKeyboardButton("🏠 الرئيسية", callback_data=f"wallet_tab:{owner_id}")
        )

        await callback_query.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
        
    except Exception as e:
        print(f"Error viewing pending trades: {e}")
        await callback_query.answer("⚠️ حدث خطأ أثناء جلب البيانات.")
        
@dp.callback_query_handler(Text(startswith='cancel_limit:'), state="*")
async def cancel_limit_order(callback_query: types.CallbackQuery):
    data_parts = callback_query.data.split(':')
    owner_id = int(data_parts[1])
    trade_id = data_parts[2]
    
    if callback_query.from_user.id != owner_id:
        return await callback_query.answer("⚠️ لا يمكنك إلغاء طلب غيرك!", show_alert=True)

    try:
        # 1. جلب بيانات الصفقة للتأكد من مبلغ الهامش (Margin)
        trade_res = supabase.table("active_trades").select("*").eq("trade_id", trade_id).execute()
        if not trade_res.data:
            return await callback_query.answer("⚠️ الطلب غير موجود أو تم تنفيذه بالفعل.", show_alert=True)
            
        trade = trade_res.data[0]
        refund_amount = float(trade['margin'])
        
        # 2. جلب رصيد المستخدم الحالي لإعادة المال
        balance = await get_user_bank_balance(owner_id)
        new_balance = float(balance) + refund_amount
        
        # 3. تنفيذ العمليات (تحديث الرصيد وحذف الصفقة)
        # تحديث الرصيد
        supabase.table("users_global_profile").update({"bank_balance": new_balance}).eq("user_id", owner_id).execute()
        
        # حذف الطلب المعلق
        supabase.table("active_trades").delete().eq("trade_id", trade_id).execute()
        
        await callback_query.answer(f"✅ تم إلغاء الطلب وإعادة {refund_amount:,.2f}$ لمحفظتك.", show_alert=True)
        
        # تحديث القائمة بعد الحذف
        await pending_trades_view(callback_query)
        
    except Exception as e:
        print(f"Cancel Error: {e}")
        await callback_query.answer("❌ فشل إلغاء الطلب.")
        
# ==========================================
# --- [ المعالجات Handlers المحدثة ] ---
# ==========================================

# 1. معالج اختيار الهدف والتأكيد (دعم الفواصل العشرية)
@dp.callback_query_handler(Text(startswith=('pr_sl_', 'pr_tp_')), state="*")
async def handle_automated_risk_selection(callback_query: types.CallbackQuery):
    try:
        data = callback_query.data.split('_') # الهيكلية: pr_sl_uid_tid_price
        risk_type = data[1]
        btn_user_id = int(data[2])
        trade_id = data[3]
        # 🟢 تعديل: تحويل السعر لـ float بدلاً من int لدعم العملات الرخيصة
        target_price = float(data[4]) 

        if callback_query.from_user.id != btn_user_id:
            return await callback_query.answer("⚠️ هذه الصلاحية ليست لك! 🚫", show_alert=True)

        res = supabase.table("active_trades").select("*").eq("trade_id", trade_id).execute()
        if not res.data:
            return await callback_query.answer("⚠️ الصفقة مغلقة.")
        
        trade = res.data[0]
        # 🟢 تعديل: جلب القيم كـ float لضمان دقة الحسابات
        entry = float(trade['entry_price'])
        liq = float(trade['liquidation_price'])
        side = trade['side']
        lev = int(trade['leverage'])
        margin = float(trade['margin'])

        # فحص التصفية (Liquidation Check)
        if risk_type == "sl":
            if (side == "LONG" and target_price <= liq) or (side == "SHORT" and target_price >= liq):
                p_fmt = f"{target_price:,.4f}" if target_price < 1 else f"{target_price:,.2f}"
                return await callback_query.answer(f"⚠️ السعر {p_fmt} خلف التصفية!", show_alert=True)

        # حسابات الربح والخسارة المتوقعة بدقة
        diff = (target_price - entry) if side == "LONG" else (entry - target_price)
        pnl_pct = (diff / entry) * lev * 100
        expected_cash = margin * (pnl_pct / 100)

        label = "إيقاف الخسارة (SL)" if risk_type == "sl" else "جني الأرباح (TP)"
        status_icon = "✅ حماية" if pnl_pct > 0 else "📉 مخاطرة"
        
        # تنسيق السعر للعرض
        p_fmt = f"{target_price:,.4f}" if target_price < 1 else f"{target_price:,.2f}"

        text = f"⚖️ <b>تأكيد مستهدف {label}</b>\n"
        text += f"━━━━━━━━━━━━━━\n"
        text += f"• السعر المختار: <code>{p_fmt} $</code>\n"
        text += f"• الحالة: <b>{status_icon}</b>\n"
        text += f"• النسبة المتوقعة: <b>{pnl_pct:+.2f}%</b>\n"
        text += f"• الربح/الخسارة: <b>{expected_cash:+.2f} $</b>\n\n"
        text += "هل تريد اعتماد هذا المستهدف وحفظه؟"

        # حفظ الكولباك (ملاحظة: تليجرام لده حد 64 بايت، لذا نرسل السعر كما هو)
        save_callback = f"c_{risk_type}_{btn_user_id}_{trade_id}_{data[4]}"
        
        markup = InlineKeyboardMarkup(row_width=1).add(
            InlineKeyboardButton("✅ نعم، تأكيد الحفظ", callback_data=save_callback),
            InlineKeyboardButton("❌ تراجع (العودة)", callback_data=f"exp_risk_{btn_user_id}_{trade_id}")
        )

        await callback_query.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
        await callback_query.answer()

    except Exception as e:
        import logging
        logging.error(f"Error in automated risk: {e}")
        await callback_query.answer("⚠️ خطأ في المعالجة.")

# 2. معالج الحفظ النهائي (دعم numeric)
@dp.callback_query_handler(Text(startswith=('c_sl_', 'c_tp_')), state="*")
async def commit_risk_to_db(callback_query: types.CallbackQuery):
    try:
        data = callback_query.data.split('_')
        risk_type = data[1]
        btn_user_id = int(data[2])
        t_id = data[3]
        # 🟢 تعديل: حفظ السعر كـ float
        new_price = float(data[4]) 

        if callback_query.from_user.id != btn_user_id:
            return await callback_query.answer("⚠️ عذراً، لا تملك الصلاحية! 🚫", show_alert=True)

        column_name = "stop_loss" if risk_type == "sl" else "take_profit"
        label = "وقف الخسارة" if risk_type == "sl" else "جني الأرباح"

        # التحديث في سوبابيس (numeric يقبل float)
        supabase.table("active_trades").update({
            column_name: new_price
        }).eq("trade_id", t_id).execute()
        
        await callback_query.answer(f"✅ تم حفظ {label} بنجاح!", show_alert=True)
        
        # إعادة التوجيه للوحة الإدارة
        callback_query.data = f"manage_trade:{t_id}"
        await callback_manage_trade_handler(callback_query)
        
    except Exception as e:
        import logging
        logging.error(f"Error in commit_risk: {e}")
        await callback_query.answer("❌ خطأ في الحفظ.")

# 3. معالج التوسع (دعم الفواصل في الأسعار الحالية)
@dp.callback_query_handler(Text(startswith='exp_'), state="*")
async def handle_expansion_protected(callback_query: types.CallbackQuery):
    try:
        data = callback_query.data.split('_') 
        section = data[1]
        btn_user_id = int(data[2])
        t_id = data[3]        
        
        if callback_query.from_user.id != btn_user_id:
            return await callback_query.answer("⚠️ مبعسس! هذه الأزرار ليست لك. 🚫", show_alert=True)

        res = supabase.table("active_trades").select("*").eq("trade_id", t_id).execute()
        if not res.data:
            return await callback_query.answer("⚠️ الصفقة غير موجودة.")
        
        trade = res.data[0]
        
        # 🟢 جلب سعر السوق الحالي بالفواصل
        coin_res = supabase.table("crypto_market_simulation").select("current_price").eq("symbol", trade['symbol']).execute()
        current_price = float(coin_res.data[0]['current_price']) if coin_res.data else float(trade['entry_price'])

        # استدعاء دالة العرض (تأكد أن get_trade_settings_view تدعم float)
        text, markup = get_trade_settings_view(trade, current_price, expand_section=section)
        
        await callback_query.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
        await callback_query.answer()

    except Exception as e:
        import logging
        logging.error(f"Expansion Error: {e}")
        await callback_query.answer("❌ حدث خطأ داخلي.")
       

# 4. معالج فتح لوحة الإعدادات (Main Gate)
@dp.callback_query_handler(Text(startswith='manage_trade:'), state="*")
async def callback_manage_trade_handler(callback_query: types.CallbackQuery):
    try:
        t_id = callback_query.data.split(':')[1]
        res = supabase.table("active_trades").select("*").eq("trade_id", t_id).execute()
        
        if not res.data:
            return await callback_query.answer("⚠️ الصفقة غير موجودة أو أغلقت.", show_alert=True)
        
        trade = res.data[0]
        # 🛡️ التأكد من صاحب الصفقة
        if callback_query.from_user.id != int(trade['user_id']):
            return await callback_query.answer("⚠️ لا يمكنك إدارة صفقات الآخرين!", show_alert=True)

        # جلب السعر الحالي بالفواصل العشرية
        coin_res = supabase.table("crypto_market_simulation").select("current_price").eq("symbol", trade['symbol']).execute()
        current_price = float(coin_res.data[0]['current_price']) if coin_res.data else float(trade['entry_price'])

        # إرسال البيانات لدالة العرض (تأكد أن الدالة get_trade_settings_view تقبل float)
        text, markup = get_trade_settings_view(trade, current_price)
        await callback_query.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
        await callback_query.answer()
    except Exception as e:
        logging.error(f"Error in manage_trade: {e}")
        await callback_query.answer("❌ خطأ في فتح الإعدادات.")

 # ==========================================
# --- [ بوابة تأكيد التنفيذ ] ---
# ==========================================
@dp.callback_query_handler(Text(startswith='conf_'), state="*")
async def security_gate_protected(callback_query: types.CallbackQuery):
    try:
        # تفكيك البيانات: conf_action_percent_uid_tid
        _, action, percent, u_id, t_id = callback_query.data.split('_')
        
        if callback_query.from_user.id != int(u_id):
            return await callback_query.answer("⚠️ لا تتدخل في صفقات غيرك! 🚫", show_alert=True)

        res = supabase.table("active_trades").select("symbol").eq("trade_id", t_id).execute()
        if not res.data: 
            return await callback_query.message.edit_text("⚠️ الصفقة مغلقة أو غير موجودة.")
        
        symbol = res.data[0]['symbol']
        act_name = "إغلاق جزء من المركز" if percent != "100" else "إغلاق المركز بالكامل"
        
        text = f"🛡️ <b>تأكيـد التنفيذ: #{symbol}</b>\n"
        text += f"━━━━━━━━━━━━━━━━━━\n"
        text += f"• الإجراء: <b>{act_name}</b>\n"
        text += f"• النسبة: <b>{percent}%</b>\n\n"
        text += "⚠️ <b>سيتم التنفيذ فوراً بسعر السوق الحالي، هل أنت متأكد؟</b>"
        
        markup = InlineKeyboardMarkup(row_width=2).add(
            InlineKeyboardButton("✅ نعم، تنفيذ", callback_data=f"exe_{action}_{percent}_{u_id}_{t_id}"),
            InlineKeyboardButton("❌ تراجع", callback_data=f"manage_trade:{t_id}")
        )
        
        await callback_query.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
        await callback_query.answer()
    except Exception as e:
        logging.error(f"Security Gate Error: {e}")
        await callback_query.answer("❌ خطأ في بوابة التأكيد.")
        

# ==========================================
# --- [ محرك التنفيذ الموحد: الإغلاق فقط ] ---
# ==========================================
@dp.callback_query_handler(Text(startswith='exe_'), state="*")
async def universal_execution_engine(callback_query: types.CallbackQuery):
    try:
        _, action, percent_str, u_id, t_id = callback_query.data.split('_')
        percent = int(percent_str)
        user_id = int(u_id)

        if callback_query.from_user.id != user_id:
            return await callback_query.answer("⚠️ لا تتدخل في صفقات غيرك!", show_alert=True)

        # جلب بيانات المستخدم والصفقة
        account = await get_trading_account_snapshot(user_id)
        res = supabase.table("active_trades").select("*").eq("trade_id", t_id).execute()
        if not res.data: return await callback_query.message.edit_text("❌ الصفقة غير موجودة.")
        
        trade = res.data[0]
        coin_res = supabase.table("crypto_market_simulation").select("current_price").eq("symbol", trade['symbol']).execute()
        # 🟢 استخدام float للسعر الحالي
        cur_price = float(coin_res.data[0]['current_price'])
        
        success_text = ""

        if action == 'cl':
            # حساب الكميات المغلقة بدقة float
            m_to_close = float(trade['margin']) * (percent / 100.0)
            q_to_close = float(trade['quantity']) * (percent / 100.0)
            
            # 🟢 حساب PNL الدقيق
            entry_price = float(trade['entry_price'])
            if trade['side'] == 'LONG':
                pnl_pct = (cur_price - entry_price) / entry_price
            else:
                pnl_pct = (entry_price - cur_price) / entry_price
                
            pnl_amt = m_to_close * pnl_pct * float(trade['leverage'])
            ret_to_bank = m_to_close + pnl_amt

            # تحديث البنك (بدون int لضمان حفظ السنتات)
            new_bank = max(0.0, float(account['free_cash']) + ret_to_bank)
            supabase.table("users_global_profile").update({"bank_balance": new_bank}).eq("user_id", user_id).execute()

            if percent >= 100:
                supabase.table("active_trades").delete().eq("trade_id", t_id).execute()
                success_text = f"✅ <b>تم إغلاق المركز بالكامل: #{trade['symbol']}</b>\n"
            else:
                # تحديث الصفقة (طرح الهامش والكمية المغلقة)
                supabase.table("active_trades").update({
                    "margin": float(trade['margin']) - m_to_close,
                    "quantity": float(trade['quantity']) - q_to_close
                }).eq("trade_id", t_id).execute()
                success_text = f"✂️ <b>تم إغلاق جزئي {percent}%: #{trade['symbol']}</b>\n"

            pnl_emoji = "🟢" if pnl_amt >= 0 else "🔴"
            # تنسيق عرض الأسعار
            e_fmt = f"{entry_price:,.4f}" if entry_price < 1 else f"{entry_price:,.2f}"
            c_fmt = f"{cur_price:,.4f}" if cur_price < 1 else f"{cur_price:,.2f}"
            
            success_text += f"• سعر الدخول: <b>{e_fmt} $</b>\n• سعر الإغلاق: <b>{c_fmt} $</b>\n"
            success_text += f"• الربح/الخسارة: <b>{pnl_amt:+.2f} $</b> {pnl_emoji}\n"
            success_text += f"• العائد للبنك: <b>{ret_to_bank:,.2f} $</b>"

            msg = await callback_query.message.edit_text(success_text, parse_mode="HTML")
            await asyncio.sleep(10)
            try: await msg.delete()
            except: pass

            # تحديث العرض للمستخدم
            trades_left = supabase.table("active_trades").select("trade_id").eq("user_id", user_id).execute()
            if not trades_left.data:
                from bot_handlers import send_main_portfolio
                await send_main_portfolio(callback_query.message, user_id)
            else:
                callback_query.data = f"active_trades_view:{user_id}"
                from bot_handlers import callback_view_trades
                await callback_view_trades(callback_query)

    except Exception as e:
        logging.error(f"Logic Error: {e}")
        await callback_query.answer("❌ حدث خطأ في الحسابات.")
# ==========================================
# 9. زر العودة للوحة التحكم الرئيسية للصفقة (Back Button)
# ==========================================
@dp.callback_query_handler(Text(startswith='back_ts_'), state="*")
async def back_to_settings_protected(callback_query: types.CallbackQuery):
    try:
        data = callback_query.data.split('_') # الهيكلية: back_ts_uid_tid
        btn_user_id = int(data[2])
        t_id = data[3]
        
        if callback_query.from_user.id != btn_user_id:
            return await callback_query.answer("⚠️ الصلاحية منتهية.")

        res = supabase.table("active_trades").select("*").eq("trade_id", t_id).execute()
        if not res.data: 
            return await callback_query.answer("⚠️ الصفقة مغلقة.")
        
        trade = res.data[0]
        coin_res = supabase.table("crypto_market_simulation").select("current_price").eq("symbol", trade['symbol']).execute()
        current_price = int(float(coin_res.data[0]['current_price'])) if coin_res.data else int(float(trade['entry_price']))

        # إرجاع لوحة التحكم الرئيسية بدون توسيع أي قسم
        text, markup = get_trade_settings_view(trade, current_price)
        await callback_query.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
        await callback_query.answer("🔙 تم الرجوع")
        
    except Exception as e:
        import logging
        logging.error(f"Error in Back TS: {e}")
        await callback_query.answer("❌ خطأ في الرجوع للقائمة.")

# ==========================================
# --- [ نظام التحويلات المالية المطور ] ---
# ==========================================

@dp.callback_query_handler(Text(startswith='transfer_flow:'), state="*")
async def transfer_init(callback_query: types.CallbackQuery, state: FSMContext):
    data = callback_query.data.split(':')
    user_id = int(data[1])
    direction = data[2] # to_bank أو to_wallet
    
    # 🔐 القفل الأمني
    if callback_query.from_user.id != user_id:
        return await callback_query.answer("❌ لا يمكنك التحكم بأموال غيرك!", show_alert=True)
    
    await state.update_data(trans_direction=direction)
    await BankTransfer.waiting_for_amount.set()
    
    # رسائل واضحة تدعم مفهوم الكسور
    prompt = "📥 <b>إيداع للتداول</b>\nأرسل المبلغ المراد تحويله (مثال: 10.50):" if direction == "to_bank" else \
             "📤 <b>سحب للمحفظة</b>\nأرسل المبلغ المراد سحبه (مثال: 5.25):"
    
    await callback_query.message.answer(prompt, parse_mode="HTML")
    await callback_query.answer()

# --- [ 2. معالجة المبلغ وتنفيذ التحديث بدقة float ] ---
@dp.message_handler(state=BankTransfer.waiting_for_amount)
async def process_transfer_amount(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    # 🟢 تحويل المدخل إلى float لدعم الكسور العشرية
    try:
        # تنظيف النص من أي رموز وإدخاله كـ float
        amount_text = message.text.replace(',', '.').replace('$', '').strip()
        amount = round(float(amount_text), 2) # تقريب لرقمين عشريين (سنتات)
        if amount <= 0: raise ValueError
    except:
        return await message.reply("⚠️ يرجى إرسال مبلغ صحيح (أرقام فقط)، مثال: 10.50")

    state_data = await state.get_data()
    direction = state_data.get('trans_direction')
    
    # جلب بيانات المستخدم (استخدام float للأرصدة)
    user_data = await get_user_data(user_id)
    if not user_data: return await state.finish()

    # 🟢 قراءة الأرصدة كـ float
    wallet_bal = float(user_data.get('wallet', 0) or 0)
    bank_bal = float(user_data.get('bank_balance', 0) or 0)

    try:
        if direction == "to_bank":
            if amount > wallet_bal:
                return await message.reply(f"❌ رصيد المحفظة غير كافٍ.\nالمتاح: <code>{wallet_bal:,.2f} $</code>")
            
            # تحديث سوبابيس (بيانات float متوافقة مع numeric)
            supabase.table("users_global_profile").update({
                "wallet": wallet_bal - amount,
                "bank_balance": bank_bal + amount
            }).eq("user_id", user_id).execute()
            
        else: # سحب للمحفظة
            # فحص الهامش المتاح (Margin Check) إذا كان لديه صفقات مفتوحة
            is_safe, health_msg = await check_financial_health(user_id, amount, "WITHDRAW")
            if not is_safe: return await message.reply(health_msg)
            
            if amount > bank_bal:
                return await message.reply(f"❌ رصيد التداول غير كافٍ.\nالمتاح: <code>{bank_bal:,.2f} $</code>")

            supabase.table("users_global_profile").update({
                "bank_balance": bank_bal - amount,
                "wallet": wallet_bal + amount
            }).eq("user_id", user_id).execute()

        await message.answer(f"✅ تم تحويل <b>{amount:,.2f} $</b> بنجاح!", parse_mode="HTML")
        await state.finish()
        
        # تحديث واجهة المحفظة فوراً
        await process_wallet_logic(user_id, message.from_user.first_name, message=message)

    except Exception as e:
        import logging
        logging.error(f"Transfer DB Error: {e}")
        await message.reply("❌ حدث خطأ أثناء التحديث في قاعدة البيانات.")
        await state.finish()
        
# --- قسم القروض ---
@dp.callback_query_handler(Text(startswith='repay_loan:'), state="*")
async def repay_loan_handler(callback_query: types.CallbackQuery):
    try:
        # 🔐 القفل الأمني
        data_parts = callback_query.data.split(':')
        owner_id = int(data_parts[1])
        if callback_query.from_user.id != owner_id:
            return await callback_query.answer("⚠️ لا يمكنك سداد ديون غيرك!", show_alert=True)

        # جلب البيانات مباشرة (float لدعم الكسور)
        res = supabase.table("users_global_profile").select("bank_balance, debt_balance").eq("user_id", owner_id).execute()
        
        if not res.data:
            return await callback_query.answer("❌ لم يتم العثور على بياناتك.", show_alert=True)
            
        user_data = res.data[0]
        debt = float(user_data.get('debt_balance', 0) or 0)
        bank_bal = float(user_data.get('bank_balance', 0) or 0)
        
        if debt <= 0:
            return await callback_query.answer("✅ ليس لديك أي ديون مستحقة حالياً!", show_alert=True)
            
        if bank_bal < debt:
            missing = debt - bank_bal
            return await callback_query.answer(f"❌ رصيد التداول ({bank_bal:,.2f}$) غير كافٍ.\nتحتاج لجمع {missing:,.2f}$ إضافية للسداد.", show_alert=True)
        
        # تنفيذ عملية الخصم (دقة float)
        new_bank_balance = bank_bal - debt
        
        supabase.table("users_global_profile").update({
            "bank_balance": float(new_bank_balance),
            "debt_balance": 0.0
        }).eq("user_id", owner_id).execute()
        
        await callback_query.answer(f"✅ تم سداد القرض بالكامل ({debt:,.2f}$).\nرصيدك الحالي: {new_bank_balance:,.2f}$", show_alert=True)
        
        # تحديث واجهة المحفظة
        await process_wallet_logic(owner_id, callback_query.from_user.first_name, callback=callback_query)

    except Exception as e:
        logging.error(f"❌ Error in repay_loan: {e}")
        await callback_query.answer("⚠️ حدث خطأ فني أثناء السداد.", show_alert=True)
        
@dp.callback_query_handler(Text(startswith='loan_menu:'), state="*")
async def loan_menu(callback_query: types.CallbackQuery):
    # 🔐 القفل الأمني
    owner_id = int(callback_query.data.split(':')[1])
    if callback_query.from_user.id != owner_id:
        return await callback_query.answer("⚠️ اطلب قائمة القروض من محفظتك الخاصة!", show_alert=True)
    
    user_data = await get_user_data(owner_id)
    if not user_data: return
    
    current_debt = float(user_data.get('debt_balance', 0) or 0)
    
    if current_debt > 0:
        return await callback_query.answer(f"⚠️ لديك قرض نشط بقيمة {current_debt:,.2f}$، سدده أولاً!", show_alert=True)

    loan_amount = 10000.0  # مبلغ القرض المتاح
    
    markup = InlineKeyboardMarkup()
    # نمرر owner_id في الكولباك للحماية في الخطوة التالية
    markup.add(InlineKeyboardButton(f"💰 اقتراض {loan_amount:,.0f} $ (مرة واحدة)", callback_data=f"exec_loan:{owner_id}:{loan_amount}"))
    markup.add(InlineKeyboardButton("🔙 عودة للمحفظة", callback_data=f"wallet_view:{owner_id}"))
    
    text = (
        f"🏦 | <b>مـركـز الائـتـمـان والـقـروض</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💵 الـمبلغ الـمتاح لك: <b>{loan_amount:,.2f} $</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<i>* ملاحظة: القروض تساعدك على بدء التداول عند تصفير المحفظة.</i>"
    )

    await callback_query.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
    
@dp.callback_query_handler(Text(startswith='exec_loan:'), state="*")
async def exec_loan_handler(callback_query: types.CallbackQuery):
    data = callback_query.data.split(':')
    owner_id = int(data[1])
    loan_amount = float(data[2])
    
    # 🔐 تأكيد الهوية
    if callback_query.from_user.id != owner_id:
        return await callback_query.answer("❌ خطأ في التحقق من الهوية!", show_alert=True)
    
    user_data = await get_user_data(owner_id)
    if not user_data: return

    # حساب القيم الجديدة بدقة float
    new_bank = float(user_data.get('bank_balance', 0) or 0) + loan_amount
    new_debt = float(user_data.get('debt_balance', 0) or 0) + loan_amount

    try:
        # تحديث سوبابيس (بيانات float متوافقة مع numeric)
        supabase.table("users_global_profile").update({
            "bank_balance": new_bank,
            "debt_balance": new_debt
        }).eq("user_id", owner_id).execute()
        
        await callback_query.answer(f"✅ تم منحك قرض بقيمة {loan_amount:,.2f} $ بنجاح!", show_alert=True)
        
        # تحديث واجهة المحفظة فوراً
        await process_wallet_logic(owner_id, callback_query.from_user.first_name, callback=callback_query)
        
    except Exception as e:
        logging.error(f"❌ Loan Error: {e}")
        await callback_query.answer("❌ فشل في تحديث قاعدة البيانات، حاول لاحقاً.", show_alert=True)

import asyncio
import aiohttp
import math
import logging  # تمت الإضافة لحل خطأ الـ logging
from datetime import datetime


# تمت إضافة الدالة المفقودة هنا
async def async_manual_upsert(table_name, records):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    endpoint = f"{SUPABASE_URL}/rest/v1/{table_name}"
    
    # ⏱️ وضع حد زمني ذكي (15 ثانية للاتصال، 30 ثانية للرفع)
    timeout = aiohttp.ClientTimeout(total=45, connect=15)
    
    try:
        # يفضل لاحقاً جعل الـ session عامة (Global)، لكن الآن سنصلحها هكذا:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(endpoint, json=records, headers=headers) as response:
                if response.status in [200, 201, 204]:
                    return True
                else:
                    error_text = await response.text()
                    logging.error(f"❌ فشل الرفع إلى {table_name}! الحالة: {response.status}")
                    logging.error(f"📝 رسالة الخطأ: {error_text}")
                    return False
    except asyncio.TimeoutError:
        logging.error("⏳ نفد الوقت (Timeout) سوبابيس لم ترد، سيتم التخطي لإكمال الباقي.")
        return False
    except Exception as e:
        logging.error(f"⚠️ خطأ تقني أثناء محاولة الرفع: {str(e)}")
        return False
        
# ==========================================
# --- [ دوال الحساب الرياضي ] ---
# ==========================================
def calculate_ema(data, period):
    if len(data) < period: return data[-1]
    alpha = 2 / (period + 1)
    ema = sum(data[:period]) / period
    for price in data[period:]:
        ema = (price * alpha) + (ema * (1 - alpha))
    return ema

def calculate_rsi(data, period=14):
    if len(data) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(data)):
        diff = data[i] - data[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    if avg_loss == 0: return 100.0
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_bollinger(data, period=20):
    if len(data) < period: return data[-1], data[-1], data[-1]
    recent = data[-period:]
    sma = sum(recent) / period
    variance = sum((x - sma) ** 2 for x in recent) / period
    std_dev = math.sqrt(variance)
    return sma + (std_dev * 2), sma, sma - (std_dev * 2)


def calculate_volume(volumes):
    """
    تعيد حجم التداول للشمعة الحالية (العمود الأخير)
    هذا هو المحرك الذي يكشف دخول السيولة المفاجئ.
    """
    if not volumes: return 0.0
    
    # جلب حجم تداول الشمعة الأخيرة (آخر عمود في الشارت)
    current_volume = float(volumes[-1])
    
    return current_volume
    
def calculate_obv(closes, volumes):
    """
    حساب مؤشر حجم التداول المتوازن (OBV)
    يعتمد على العلاقة بين سعر الإغلاق وحجم التداول
    """
    if len(closes) < 2: return 0.0
    
    obv = 0.0
    # نبدأ الحساب بمقارنة كل شمعة بالتي قبلها
    for i in range(1, len(closes)):
        if closes[i] > closes[i-1]:
            # إغلاق صاعد: أضف الفوليوم
            obv += volumes[i]
        elif closes[i] < closes[i-1]:
            # إغلاق هابط: اطرح الفوليوم
            obv -= volumes[i]
        # إذا تساوى الإغلاق يبقى الـ OBV كما هو دون تغيير
            
    return obv

def calculate_bbw(upper, lower, middle):
    """
    تحسب عرض نطاق البولنجر (BBW).
    المعادلة: (الخط العلوي - الخط السفلي) / الخط الأوسط
    """
    try:
        if middle > 0:
            return (upper - lower) / middle
        return 0
    except Exception:
        return 0
  

def calculate_keltner_channels(highs, lows, closes, ema_period=20, atr_period=10, multiplier=2):
    if len(closes) < max(ema_period, atr_period) + 1:
        return closes[-1], closes[-1], closes[-1]
    mid = calculate_ema(closes, ema_period)
    atr_v = calculate_atr(highs, lows, closes, atr_period)
    return mid + (multiplier * atr_v), mid, mid - (multiplier * atr_v)
    
# ==========================================
# --- [ دوال الأدوات المحرمة - قلعة أثر ] ---
# ==========================================

def calculate_atr(highs, lows, closes, period=14):
    """
    نسخة قلعة أثر المعتمدة (Wilder's ATR)
    أدق في حساب الستوب لوز ومنع ضربه بالذيول العشوائية.
    """
    if len(closes) < period + 1: return 0.0
    
    tr_list = []
    for i in range(1, len(closes)):
        # حساب المدى الحقيقي (True Range)
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        )
        tr_list.append(tr)
    
    # حساب أول قيمة كمتوسط بسيط (SMA) لتبدأ منه
    atr = sum(tr_list[:period]) / period
    
    # تطبيق التنعيم (Smoothing) لبقية القيم - هذا هو "سر" الاستقرار
    for i in range(period, len(tr_list)):
        atr = (atr * (period - 1) + tr_list[i]) / period
        
    return round(atr, 6)

def calculate_adx(highs, lows, closes, period=14):
    """
    قاعدة (المرصاد): حساب مؤشر ADX
    لمعرفة هل العملة في "انفجار" (ADX > 25) أم "تذبذب" (ADX < 20).
    """
    if len(closes) < period * 2: return 0.0
    
    plus_dm = []
    minus_dm = []
    tr_list = []
    
    for i in range(1, len(closes)):
        up_move = highs[i] - highs[i-1]
        down_move = lows[i-1] - lows[i]
        
        plus_dm.append(max(up_move, 0) if up_move > down_move else 0)
        minus_dm.append(max(down_move, 0) if down_move > up_move else 0)
        
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        tr_list.append(tr)

    # حساب الـ DI والـ DX (تبسيطاً للمحرك اليدوي)
    # ملاحظة: هذه نسخة مختصرة لتناسب الأداء السريع في البوت
    avg_tr = sum(tr_list[-period:]) / period
    avg_plus_dm = sum(plus_dm[-period:]) / period
    avg_minus_dm = sum(minus_dm[-period:]) / period
    
    plus_di = 100 * (avg_plus_dm / avg_tr) if avg_tr != 0 else 0
    minus_di = 100 * (avg_minus_dm / avg_tr) if avg_tr != 0 else 0
    
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di) if (plus_di + minus_di) != 0 else 0
    return round(dx, 2)

def calculate_volume_delta(buy_volumes, total_volumes):
    """
    قاعدة (فَتَبَيَّنُوا): حساب صافي السيولة (Volume Delta)
    يميز بين "الزبد" (فوليوم وهمي) و"ما ينفع الناس" (شراء حقيقي).
    """
    if not buy_volumes or not total_volumes: return 0.0
    
    # صافي السيولة = حجم الشراء - حجم البيع (البيع هو الإجمالي ناقص الشراء)
    current_buy = float(buy_volumes[-1])
    current_total = float(total_volumes[-1])
    current_sell = current_total - current_buy
    
    delta = current_buy - current_sell
    return round(delta, 2)

def get_market_mood(rsi_value):
    """
    سيكولوجية (هَلُوعًا ومَنُوعًا): بناءً على مستويات أثر 78/22
    """
    if rsi_value >= 78: return "GREED (MANOU'A)"
    if rsi_value <= 22: return "FEAR (HALOU'A)"
    if rsi_value >= 50: return "BULLISH_BIAS"
    return "BEARISH_BIAS"
    
    
# ==========================================
# 1. دالة تحليل أنماط الشموع اليابانية
# ==========================================
import numpy as np

def detect_all_pdf_patterns(df):
    if len(df) < 5:
        return "Not enough data"

    # تحويل البيانات لمصفوفات Numpy للسرعة
    op = df['open'].values
    hi = df['high'].values
    lo = df['low'].values
    cl = df['close'].values
    
    # حسابات أساسية
    body = np.abs(cl - op)
    upper_wick = hi - np.maximum(op, cl)
    lower_wick = np.minimum(op, cl) - lo
    candle_range = hi - lo
    
    # تجنب القسمة على صفر
    candle_range = np.where(candle_range == 0, 0.00001, candle_range)
    direction = np.where(cl > op, 1, -1)
    # استبدل السطر القديم بهذا:
    window = min(20, len(body)) # يأخذ 20 أو أقل إذا كانت الشموع قليلة
    avg_body = np.convolve(body, np.ones(window)/window, mode='same')
    
    # نسبة التسامح للقمم والقيعان المتساوية
    tolerance = avg_body * 0.1 

    # المؤشرات المكانية (لآخر 5 شموع لتحديد النماذج المعقدة)
    curr, prev, pprev, p3, p4 = -1, -2, -3, -4, -5

    # --- 1. الشروط الأساسية للفحص السريع ---
    is_doji = body <= (candle_range * 0.1)
    is_marubozu = body >= (candle_range * 0.95)
    is_dragon_doji = is_doji & (lower_wick >= body * 3) & (upper_wick <= candle_range * 0.1)
    is_gravestone_doji = is_doji & (upper_wick >= body * 3) & (lower_wick <= candle_range * 0.1)
    is_hammer_type = (lower_wick >= body * 2) & (upper_wick <= candle_range * 0.1) & (body > candle_range * 0.1)
    is_star_type = (upper_wick >= body * 2) & (lower_wick <= candle_range * 0.1) & (body > candle_range * 0.1)
    is_spinning_top = (body < avg_body * 0.8) & (upper_wick > body) & (lower_wick > body)
    is_long_body = body > avg_body * 1.5

    # الفجوات السعرية (Gaps)
    gap_up = lo[curr] > hi[prev]
    gap_down = hi[curr] < lo[prev]

    # حساب الاتجاه البسيط (مقارنة الإغلاق السابق بـ 4 شموع قبلها) - يُستخدم للأنماط الفردية
    prev_trend = 1 if cl[prev] > cl[p4] else -1 

    res = "Normal"
    
    # ==========================================
    # --- 2. الأنماط الخماسية والرباعية (5 & 4 Candles) ---
    # ==========================================

    # Rising Three Methods (صاعد)
    if direction[p4] == 1 and is_long_body[p4] and \
       direction[p3] == -1 and direction[pprev] == -1 and direction[prev] == -1 and \
       direction[curr] == 1 and cl[curr] > hi[p4] and \
       max(hi[p3], hi[pprev], hi[prev]) < hi[p4] and min(lo[p3], lo[pprev], lo[prev]) > lo[p4]:
        res = "طرق_الارتفاع_الثلاثة_صاعد"

    # Falling Three Methods (هابط)
    elif direction[p4] == -1 and is_long_body[p4] and \
         direction[p3] == 1 and direction[pprev] == 1 and direction[prev] == 1 and \
         direction[curr] == -1 and cl[curr] < lo[p4] and \
         max(hi[p3], hi[pprev], hi[prev]) < hi[p4] and min(lo[p3], lo[pprev], lo[prev]) > lo[p4]:
        res = "طرق_الانخفاض_الثلاثة_هابط"

    # Concealing Baby Swallow (ابتلاع الطفل الرضيع - هابط يتحول لصاعد)
    elif direction[p3] == -1 and is_marubozu[p3] and direction[pprev] == -1 and is_marubozu[pprev] and \
         direction[prev] == -1 and is_star_type[prev] and gap_down and \
         direction[curr] == -1 and cl[curr] > cl[prev] and op[curr] > hi[prev]:
        res = "ابتلاع_الطفل_الرضيع_صاعد"

    # Mat Hold (القبضة المحكمة - صاعد)
    elif direction[p4] == 1 and is_long_body[p4] and \
         direction[p3] == -1 and lo[p3] > hi[p4] and \
         direction[pprev] == -1 and direction[prev] == -1 and \
         min(lo[p3], lo[pprev], lo[prev]) > lo[p4] and \
         direction[curr] == 1 and cl[curr] > hi[p3]:
        res = "القبضة_المحكمة_صاعد"

    # ==========================================
    # --- 3. الأنماط الثلاثية (3 Candles) ---
    # ==========================================

    # Abandoned Baby (الطفل المهجور)
    elif direction[pprev] == -1 and is_doji[prev] and lo[pprev] > hi[prev] and direction[curr] == 1 and lo[curr] > hi[prev]:
        res = "الطفل_المهجور_صاعد"
    elif direction[pprev] == 1 and is_doji[prev] and hi[pprev] < lo[prev] and direction[curr] == -1 and hi[curr] < lo[prev]:
        res = "الطفل_المهجور_هابط"

    # Morning / Evening Stars
    elif direction[pprev] == -1 and direction[curr] == 1 and cl[curr] > (op[pprev] + cl[pprev])/2 and op[prev] < cl[pprev] and cl[prev] < op[curr]:
        res = "نجمة_الصباح_دوجي_صاعد" if is_doji[prev] else "نجمة_الصباح_صاعد"
    elif direction[pprev] == 1 and direction[curr] == -1 and cl[curr] < (op[pprev] + cl[pprev])/2 and op[prev] > cl[pprev] and cl[prev] > op[curr]:
        res = "نجمة_المساء_دوجي_هابط" if is_doji[prev] else "نجمة_المساء_هابط"

    # Three White Soldiers / Three Black Crows
    elif direction[pprev] == 1 and direction[prev] == 1 and direction[curr] == 1 and cl[curr] > cl[prev] > cl[pprev] and op[curr] > op[prev] > op[pprev]:
        res = "الجنود_الثلاثة_البيض_صاعد"
    elif direction[pprev] == -1 and direction[prev] == -1 and direction[curr] == -1 and cl[curr] < cl[prev] < cl[pprev] and op[curr] < op[prev] < op[pprev]:
        res = "الغربان_الثلاثة_السود_هابط"

    # Three Inside Up / Down
    elif direction[pprev] == -1 and direction[prev] == 1 and op[prev] > cl[pprev] and cl[prev] < op[pprev] and direction[curr] == 1 and cl[curr] > cl[prev]:
        res = "ثلاثة_للداخل_صاعد"
    elif direction[pprev] == 1 and direction[prev] == -1 and op[prev] < cl[pprev] and cl[prev] > op[pprev] and direction[curr] == -1 and cl[curr] < cl[prev]:
        res = "ثلاثة_للداخل_هابط"

    # Three Outside Up / Down
    elif direction[pprev] == -1 and direction[prev] == 1 and op[prev] < cl[pprev] and cl[prev] > op[pprev] and direction[curr] == 1 and cl[curr] > cl[prev]:
        res = "ثلاثة_للخارج_صاعد"
    elif direction[pprev] == 1 and direction[prev] == -1 and op[prev] > cl[pprev] and cl[prev] < op[pprev] and direction[curr] == -1 and cl[curr] < cl[prev]:
        res = "ثلاثة_للخارج_هابط"

    # Upside / Downside Tasuki Gap
    elif direction[pprev] == 1 and direction[prev] == 1 and lo[prev] > hi[pprev] and direction[curr] == -1 and op[curr] < cl[prev] and cl[curr] < op[prev] and cl[curr] > hi[pprev]:
        res = "فجوة_تاسوكي_صاعدة"
    elif direction[pprev] == -1 and direction[prev] == -1 and hi[prev] < lo[pprev] and direction[curr] == 1 and op[curr] > cl[prev] and cl[curr] > op[prev] and cl[curr] < lo[pprev]:
        res = "فجوة_تاسوكي_هابطة"

    # Tri-Star (النجوم الثلاثة)
    elif is_doji[pprev] and is_doji[prev] and is_doji[curr]:
        res = "صاعد_النجوم_الثلاثة_تغير اتجاه صعود او هبوط "

    # Advance Block (التقدم المعاق - هابط)
    elif direction[pprev] == 1 and direction[prev] == 1 and direction[curr] == 1 and \
         op[prev] > op[pprev] and op[prev] < cl[pprev] and \
         op[curr] > op[prev] and op[curr] < cl[prev] and \
         body[curr] < body[prev] < body[pprev] and \
         upper_wick[curr] > upper_wick[prev]:
        res = "التقدم_المعاق_هابط"

    # Stalled Pattern / Deliberation (نموذج التروي - هابط)
    elif direction[pprev] == 1 and is_long_body[pprev] and \
         direction[prev] == 1 and is_long_body[prev] and \
         direction[curr] == 1 and body[curr] < (avg_body[curr] * 0.5) and \
         op[curr] >= (cl[prev] - tolerance[curr]):
        res = "نموذج_التروي_هابط"

    # Upside Gap Two Crows (غرابان بفجوة صاعدة - هابط)
    elif direction[pprev] == 1 and is_long_body[pprev] and \
         direction[prev] == -1 and lo[prev] > hi[pprev] and \
         direction[curr] == -1 and op[curr] > op[prev] and cl[curr] < cl[prev] and cl[curr] > op[pprev]:
        res = "غرابان_بفجوة_صاعدة_هابط"

    # Unique Three River Bottom (نهر الثلاثة الفريد - صاعد)
    elif direction[pprev] == -1 and is_long_body[pprev] and \
         direction[prev] == -1 and lower_wick[prev] >= (body[prev] * 2) and cl[prev] > lo[pprev] and \
         direction[curr] == 1 and body[curr] < avg_body[curr] and cl[curr] < cl[prev]:
        res = "نهر_الثلاثة_الفريد_صاعد"

    # Stick Sandwich (الساندوتش - صاعد)
    elif direction[pprev] == -1 and direction[prev] == 1 and direction[curr] == -1 and \
         op[curr] > cl[prev] and cl[curr] < op[prev] and \
         abs(cl[curr] - cl[pprev]) <= tolerance[curr]:
        res = "الساندوتش_صاعد"

    # ==========================================
    # --- 4. الأنماط الثنائية (2 Candles) ---
    # ==========================================

    # Engulfing (الابتلاع)
    elif direction[prev] == -1 and direction[curr] == 1 and op[curr] <= cl[prev] and cl[curr] >= op[prev]:
        res = "ابتلاع_صاعد"
    elif direction[prev] == 1 and direction[curr] == -1 and op[curr] >= cl[prev] and cl[curr] <= op[prev]:
        res = "ابتلاع_هابط"

    # Harami & Harami Cross (الهارامي والهارامي الصليب)
    elif direction[prev] == -1 and direction[curr] == 1 and op[curr] > cl[prev] and cl[curr] < op[prev]:
        res = "هارامي_صليب_صاعد" if is_doji[curr] else "هارامي_صاعد"
    elif direction[prev] == 1 and direction[curr] == -1 and op[curr] < cl[prev] and cl[curr] > op[prev]:
        res = "هارامي_صليب_هابط" if is_doji[curr] else "هارامي_هابط"

    # Piercing Line & Dark Cloud Cover
    elif direction[prev] == -1 and direction[curr] == 1 and op[curr] < cl[prev] and cl[curr] > (op[prev] + cl[prev])/2 and cl[curr] < op[prev]:
        res = "الخط_الثاقب_صاعد"
    elif direction[prev] == 1 and direction[curr] == -1 and op[curr] > cl[prev] and cl[curr] < (op[prev] + cl[prev])/2 and cl[curr] > op[prev]:
        res = "السحابة_القاتمة_هابط"

    # Kicker
    elif direction[prev] == -1 and direction[curr] == 1 and op[curr] >= op[prev] and lo[curr] > hi[prev]:
        res = "الراكل_صاعد"
    elif direction[prev] == 1 and direction[curr] == -1 and op[curr] <= op[prev] and hi[curr] < lo[prev]:
        res = "الراكل_هابط"

    # Meeting Lines (خطوط التلاقي)
    elif direction[prev] == -1 and direction[curr] == 1 and abs(cl[curr] - cl[prev]) <= tolerance[curr]:
        res = "خطوط_التلاقي_صاعد"
    elif direction[prev] == 1 and direction[curr] == -1 and abs(cl[curr] - cl[prev]) <= tolerance[curr]:
        res = "خطوط_التلاقي_هابط"

    # Separating Lines (خطوط الانفصال)
    elif direction[prev] == -1 and direction[curr] == 1 and abs(op[curr] - op[prev]) <= tolerance[curr]:
        res = "خطوط_الانفصال_صاعد"
    elif direction[prev] == 1 and direction[curr] == -1 and abs(op[curr] - op[prev]) <= tolerance[curr]:
        res = "خطوط_الانفصال_هابط"

    # Matching Low (القيعان المتطابقة)
    elif direction[prev] == -1 and direction[curr] == -1 and abs(cl[curr] - cl[prev]) <= tolerance[curr]:
        res = "القيعان_المتطابقة_صاعد"

    # Homing Pigeon (الحمامة الزاجلة)
    elif direction[prev] == -1 and direction[curr] == -1 and op[curr] < op[prev] and cl[curr] > cl[prev]:
        res = "الحمامة_الزاجلة_صاعد"

    # On Neck / In Neck
    elif direction[prev] == -1 and direction[curr] == 1 and abs(cl[curr] - lo[prev]) <= tolerance[curr]:
        res = "على_الرقبة_هابط"
    elif direction[prev] == -1 and direction[curr] == 1 and cl[curr] > cl[prev] and cl[curr] < (op[prev] + cl[prev])/2:
        res = "في_الرقبة_هابط"

    # Tweezer Top / Bottom
    elif direction[prev] == 1 and direction[curr] == -1 and abs(hi[curr] - hi[prev]) <= tolerance[curr]:
        res = "قمة_الملقط_هابط"
    elif direction[prev] == -1 and direction[curr] == 1 and abs(lo[curr] - lo[prev]) <= tolerance[curr]:
        res = "قاع_الملقط_صاعد"

    # Thrusting Line (خط الدفع - هابط)
    elif direction[prev] == -1 and direction[curr] == 1 and \
         op[curr] < lo[prev] and cl[curr] > cl[prev] and cl[curr] < (op[prev] + cl[prev])/2:
        res = "خط_الدفع_هابط"

    # Doji Star (نجمة الدوجي - بداية انعكاس)
    elif is_long_body[prev] and is_doji[curr]:
        if direction[prev] == 1 and lo[curr] > hi[prev]:
            res = "نجمة_دوجي_هابط"
        elif direction[prev] == -1 and hi[curr] < lo[prev]:
            res = "نجمة_دوجي_صاعد"

    # ==========================================
    # --- 5. الأنماط الفردية (1 Candle) ---
    # ==========================================
    
    # تم وضع شروط المطرقة ضمن سلسلة الـ elif لمنعها من الكتابة فوق الأنماط الثنائية أو الثلاثية
    elif is_hammer_type[curr]:
        res = "مطرقة_صاعد" if prev_trend == -1 else "الرجل_المشنوق_هابط"
    elif is_star_type[curr]:
        res = "مطرقة_مقلوبة_صاعد" if prev_trend == -1 else "نجمة_الشهاب_هابط"
        
    # Belt Hold (الحزام الممسوك)
    elif direction[curr] == 1 and is_long_body[curr] and lower_wick[curr] <= tolerance[curr]:
        res = "الحزام_الممسوك_صاعد"
    elif direction[curr] == -1 and is_long_body[curr] and upper_wick[curr] <= tolerance[curr]:
        res = "الحزام_الممسوك_هابط"
    
    elif is_dragon_doji[curr]: res = "دوجي_التنين_صاعد"
    elif is_gravestone_doji[curr]: res = "دوجي_شاهد_القبر_هابط"
    elif is_doji[curr]: res = "Neutral_Doji"
    elif is_marubozu[curr]: res = "ماروبوزو_صاعد" if direction[curr] == 1 else "ماروبوزو_هابط"
    elif is_spinning_top[curr]: res = "Spinning_Top"

    return res
    

# ==========================================
# --- [ 📡 الرادار الذكي: قناص الفجوات والسيولة ] ---
# ==========================================
def extract_smart_money_concepts(df):
    if len(df) < 25:
        return {"fvg": "None", "volume_anomaly": False, "strict_pattern": "None"}
    
    # 1. الفجوات العادلة (تحويل صريح لـ bool)
    bullish_fvg = bool(df['low'].iloc[-1] > df['high'].iloc[-3])
    bearish_fvg = bool(df['high'].iloc[-1] < df['low'].iloc[-3])
    
    # 2. انفجار السيولة (هنا غالباً يقع الخطأ بسبب المتوسط)
    vol_sma_20 = df['volume'].iloc[-21:-1].mean()
    current_vol = df['volume'].iloc[-1]
    volume_anomaly = bool(current_vol >= (vol_sma_20 * 2))
    
    # 3. الأنماط الصارمة
    body = abs(df['close'].iloc[-1] - df['open'].iloc[-1])
    upper_wick = df['high'].iloc[-1] - max(df['open'].iloc[-1], df['close'].iloc[-1])
    lower_wick = min(df['open'].iloc[-1], df['close'].iloc[-1]) - df['low'].iloc[-1]
    
    strict_pattern = "None"
    if lower_wick >= (2 * body) and upper_wick <= (0.2 * body) and body > 0:
        strict_pattern = "Strict_Hammer"
    elif upper_wick >= (2 * body) and lower_wick <= (0.2 * body) and body > 0:
        strict_pattern = "Strict_Shooting_Star"

    fvg_status = "Bullish_FVG" if bullish_fvg else "Bearish_FVG" if bearish_fvg else "None"
    
    return {
        "fvg": fvg_status,
        "volume_anomaly": volume_anomaly,
        "strict_pattern": strict_pattern
    }


def detect_divergence(prices, indicators):
    """
    🕵️‍♂️ كاشف الانحرافات (Divergence Detector)
    يقارن بين قمم السعر وقمم المؤشر (RSI/OBV) لكشف التلاعب أو ضعف الاتجاه.
    """
    if len(prices) < 5 or len(indicators) < 5:
        return "Normal"

    try:
        # قمة السعر الحالية مقارنة بالسابقة
        price_higher_high = prices[-1] > prices[-5]
        price_lower_low = prices[-1] < prices[-5]

        # قمة المؤشر الحالية مقارنة بالسابقة
        ind_higher_high = indicators[-1] > indicators[-5]
        ind_lower_low = indicators[-1] < indicators[-5]

        # 1. انحراف سلبي (Bearish Divergence): السعر يصعد والمؤشر يهبط
        if price_higher_high and not ind_higher_high:
            return "Bearish Divergence"

        # 2. انحراف إيجابي (Bullish Divergence): السعر يهبط والمؤشر يصعد
        if price_lower_low and not ind_lower_low:
            return "Bullish Divergence"

        return "Normal"
    except Exception:
        return "Normal"


def calculate_macd_values(closes, fast=12, slow=26, signal=9):
    try:
        # تحويل القائمة إلى Series من بانداز
        s = pd.Series(closes)
        
        # 1. حساب المتوسطات الأسية
        ema_fast = s.ewm(span=fast, adjust=False).mean()
        ema_slow = s.ewm(span=slow, adjust=False).mean()
        
        # 2. حساب خط الماكد الرئيسي
        macd_line = ema_fast - ema_slow
        
        # 3. حساب خط الإشارة (Signal Line)
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        
        # 4. حساب الهستغرام
        histogram = macd_line - signal_line
        
        return {
            "macd": float(macd_line.iloc[-1]),
            "signal": float(signal_line.iloc[-1]),
            "hist": float(histogram.iloc[-1])
        }
    except Exception as e:
        print(f"❌ خطأ في الحساب اليدوي للماكد: {e}")
        return {"macd": 0.0, "signal": 0.0, "hist": 0.0}
        

# ==========================================
# 1. دالة استخراج الدعوم والمقاومات (المحدثة)
# ==========================================

def calculate_price_action_sr(highs, lows, return_swings=False):
    """
    تستخرج أحدث دعم وأحدث مقاومة، مع إمكانية إرجاع كافة القمم والقيعان (Swings)
    لدعم حساب القنوات السعرية في مشروع Trade Reaper.
    """
    supports = []     # ستخزن الآن: (الفهرس، السعر)
    resistances = []  # ستخزن الآن: (الفهرس، السعر)

    # استخراج القيعان والقمم الحقيقية (شمعتين يمين وشمعتين يسار للفلترة الصارمة)
    for i in range(2, len(highs) - 2):
        # القاع الحقيقي
        if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
            supports.append((i, lows[i])) # حفظ الفهرس والسعر كزوج (tuple)
            
        # القمة الحقيقية
        if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
            resistances.append((i, highs[i])) # حفظ الفهرس والسعر كزوج (tuple)

    # --- الجزء المضاف لحل مشكلة القنوات السعرية ---
    if return_swings:
        # نعيد القوائم كاملة (الفهرس والسعر) لكي تستطيع دالة القنوات رسم التوازي
        return resistances, supports 

    # استخراج الأحدث (للمحافظة على عمل الكود القديم وتحديث قاعدة البيانات)
    # نأخذ القيمة السعرية فقط [1] من آخر عنصر سجلناه
    latest_support = supports[-1][1] if supports else None
    latest_resistance = resistances[-1][1] if resistances else None

    return latest_support, latest_resistance


def get_imbalance_ratio(depth_data):
    """
    تحويل بيانات دفتر الأوامر الخام إلى نسبة اختلال السيولة.
    depth_data: هي النتيجة القادمة من exchange.fetch_order_book
    """
    try:
        # تحويل القوائم إلى مصفوفات Numpy لمعالجة سريعة جداً
        # نأخذ أول 20 مستوى (أهم مستويات السيولة القريبة من السعر)
        bids = np.array(depth_data['bids'][:20]) 
        asks = np.array(depth_data['asks'][:20])
        
        # جمع كميات الشراء (العمود الثاني في المصفوفة)
        total_bids_volume = np.sum(bids[:, 1])
        
        # جمع كميات البيع (العمود الثاني في المصفوفة)
        total_asks_volume = np.sum(asks[:, 1])
        
        # حساب النسبة النهائية
        if total_asks_volume > 0:
            ratio = total_bids_volume / total_asks_volume
        else:
            ratio = 1.0 # قيمة افتراضية في حال تعطل البيانات
            
        return float(ratio)
    except Exception as e:
        return 1.0

# ==========================================
# 1. دوال المساعدة، المؤشرات، والدقة الرياضية المتقدمة
# ==========================================

def calculate_log_fib_accuracy(actual_price: float, target_price: float, atr_value: float) -> float:
    """
    حساب الدقة باستخدام المقياس اللوغاريتمي ونسبة سماح تعتمد على معدل التذبذب (ATR)
    بدلاً من نسبة مئوية ثابتة.
    """
    if target_price == 0: return 0.0
    
    # حساب الانحراف اللوغاريتمي
    log_deviation = abs(np.log(actual_price / target_price))
    
    # التسامح الديناميكي بناءً على تقلبات السوق (ATR)
    dynamic_tolerance = atr_value / target_price 
    
    # حماية من القسمة على صفر في حالة انعدام السيولة التام
    if dynamic_tolerance == 0:
        return 100.0 if log_deviation == 0 else 0.0
        
    if log_deviation > dynamic_tolerance * 2:
        return 0.0 # بعيد جداً عن منطقة الانعكاس
        
    # دالة غاوسية (Gaussian) لحساب الدقة بحيث تكون 100% في المركز وتقل بانحناء طبيعي
    accuracy = np.exp(-0.5 * (log_deviation / (dynamic_tolerance / 2))**2)
    return round(accuracy * 100, 2)

def calculate_statistical_trend(x_coords: np.ndarray, y_coords: np.ndarray):
    """
    استخدام الانحدار الخطي (OLS) لإيجاد خط الترند الأقوى رياضياً،
    وحساب R-squared لمعرفة مدى "مثالية" هذا الترند.
    """
    if len(x_coords) < 3:
        return {"slope": 0, "intercept": 0, "r_squared": 0, "is_valid": False}
        
    slope, intercept, r_value, p_value, std_err = linregress(x_coords, y_coords)
    r_squared = r_value ** 2 # معامل التحديد
    
    # نعتبر الترند قوياً إذا كان R-squared أكبر من 0.85
    is_valid = True if r_squared >= 0.85 else False
    
    return {
        "slope": slope,
        "intercept": intercept,
        "r_squared": round(r_squared, 4),
        "is_valid": is_valid
    }

def is_near_ratio(value: float, target: float, tolerance: float = 0.02) -> bool:
    return abs(value - target) <= tolerance


def calculate_exact_accuracy(actual: float, target: float) -> float:
    """حساب الدقة المئوية لنسبة الفيبوناتشي"""
    if target == 0: return 0.0
    acc = 1.0 - (abs(actual - target) / target)
    return round(max(0, acc) * 100, 2)


def calculate_rsi(series, period: int = 14):
    """حساب مؤشر القوة النسبية (RSI) بشكل آمن"""
    # تحويل البيانات إلى Series إذا كانت قائمة (List) لتجنب خطأ 'list' object has no attribute 'diff'
    if isinstance(series, list):
        series = pd.Series(series)
    
    delta = series.diff()
    
    # حساب المكاسب والخسائر
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    
    # التعامل مع حالة القسمة على صفر إذا كانت الخسائر صفراً
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi


def detect_rsi_divergence_4h(df_4h: pd.DataFrame) -> str:
    """اكتشاف الدايفرجنس باستخدام فريم 4 ساعات ومستويات السيولة العميقة (78/22)"""
    if df_4h is None or len(df_4h) < 50:
        return "NONE"
    
    df = df_4h.copy()
    if 'rsi' not in df.columns:
        df['rsi'] = calculate_rsi(df['close'])
        
    highs, lows = df['high'].values, df['low'].values
    rsis = df['rsi'].values
    
    price_peaks, _ = find_peaks(highs, distance=5)
    price_troughs, _ = find_peaks(-lows, distance=5)
    
    if len(price_peaks) >= 2 and len(price_troughs) >= 2:
        p1, p2 = price_peaks[-2], price_peaks[-1]
        if highs[p2] > highs[p1] and rsis[p2] < rsis[p1] and (rsis[p1] >= 78 or rsis[p2] >= 78):
            return "BEARISH_DIVERGENCE"
            
        t1, t2 = price_troughs[-2], price_troughs[-1]
        if lows[t2] < lows[t1] and rsis[t2] > rsis[t1] and (rsis[t1] <= 22 or rsis[t2] <= 22):
            return "BULLISH_DIVERGENCE"
            
    return "NONE"


def calculate_marubozu_status(open_p: float, high_p: float, low_p: float, close_p: float) -> int:
    """تحديد ما إذا كانت شمعة الاختراق ماروبوزو (1: نعم، 2: لا)"""
    body = abs(close_p - open_p)
    wick = high_p - low_p
    if wick == 0: return 2
    return 1 if (body / wick) >= 0.85 else 2

def check_ema_confluence(df: pd.DataFrame, target_price: float, tolerance_pct: float = 0.005) -> int:
    """تأكيد التوافق (Confluence) مع متوسط متحرك صارم (EMA 50)"""
    if len(df) < 50: return 2
    ema_50 = df['close'].ewm(span=50, adjust=False).mean().iloc[-1]
    return 1 if abs(target_price - ema_50) / ema_50 <= tolerance_pct else 2

# ==========================================
# 2. محرك الهارمونيك المطور (EliteTradingEngine)
# ==========================================

def calculate_harmonic_targets(
    pattern_name: str, direction: str, point_d: float, point_a: float, point_x: float,
    accuracy: float = 0.0, confluence: int = 2, pattern_type: str = "standard"
) -> Dict[str, Union[str, float, int]]:
    
    ad_length = abs(point_a - point_d)
    ratio = 0.618 if pattern_type == "standard" else 0.50 
    
    if direction == "شراء":
        target = point_d + (ad_length * ratio) 
        sl = point_x - (point_x * 0.002) 
    else:
        target = point_d - (ad_length * ratio)
        sl = point_x + (point_x * 0.002)

    return {
        "name": pattern_name,
        "class": "هارمونيك - احترافي",
        "breakout": round(point_d, 5), 
        "target": round(target, 5),
        "sl": round(sl, 5),
        "status": "مكتمل",
        "harmonic_fib_accuracy": accuracy,
        "harmonic_d_confluence": confluence
    }
    

def detect_elite_patterns(
    df: pd.DataFrame, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, 
    current_price: float, tolerance: float = 0.02
) -> Dict[str, Union[str, float, int]]:
    
    default_pattern = {
        "name": "لا يوجد", "class": "لا يوجد", "breakout": 0.0, "target": 0.0, 
        "sl": 0.0, "status": "بحث مستمر", "harmonic_fib_accuracy": 0.0, "harmonic_d_confluence": 2
    }

    # 1. حساب ATR لضبط دقة الفيبوناتشي اللوغاريتمية
    atr_value = (df['high'] - df['low']).rolling(window=14).mean().iloc[-1]

    peaks, _ = find_peaks(highs, distance=5)
    troughs, _ = find_peaks(-lows, distance=5)
    
    if len(peaks) < 3 or len(troughs) < 3:
        return default_pattern

    p, t = highs[peaks], lows[troughs]
    p1, p2, p3 = p[-3], p[-2], p[-1]
    t1, t2, t3 = t[-3], t[-2], t[-1]
    
    # ---------------------------------------------------------
    # 1. السايفر (Cypher)
    # ---------------------------------------------------------
    if len(p) >= 2 and len(t) >= 2:
        # شرائي
        X, A, B, C = t[-2], p[-2], t[-1], p[-1]
        xa = A - X
        if xa != 0:
            b_ret, c_ext = (A - B) / xa, (C - X) / xa
            if (0.382 <= b_ret <= 0.618) and (1.272 <= c_ext <= 1.414):
                d_target = C - (abs(C - B) * 0.786)
                acc = calculate_log_fib_accuracy(current_price, d_target, atr_value)
                conf = check_ema_confluence(df, d_target)
                return calculate_harmonic_targets("سايفر شرائي", "شراء", d_target, C, X, acc, conf, "standard")

        # بيعي
        X, A, B, C = p[-2], t[-2], p[-1], t[-1]
        xa = X - A
        if xa != 0:
            b_ret, c_ext = (B - A) / xa, (X - C) / xa
            if (0.382 <= b_ret <= 0.618) and (1.272 <= c_ext <= 1.414):
                d_target = C + (abs(B - C) * 0.786)
                acc = calculate_log_fib_accuracy(current_price, d_target, atr_value)
                conf = check_ema_confluence(df, d_target)
                return calculate_harmonic_targets("سايفر بيعي", "بيع", d_target, C, X, acc, conf, "standard")

    # ---------------------------------------------------------
    # 2. القرش (Shark)
    # ---------------------------------------------------------
    # شرائي
    if len(t) >= 3 and len(p) >= 2:
        O, X, A, B, C = t[-3], p[-2], t[-2], p[-1], t[-1]
        ox, xa, ab, bc = (X-O), (X-A), (B-A), (B-C)
        if xa != 0 and 1.13 <= (ab/xa) <= 1.618:
            if ox != 0 and is_near_ratio(bc/ox, 0.886, tolerance):
                acc = calculate_log_fib_accuracy(current_price, C, atr_value)
                conf = check_ema_confluence(df, C)
                return calculate_harmonic_targets("قرش شرائي", "شراء", C, B, O, acc, conf, "shark")

    # بيعي
    if len(p) >= 3 and len(t) >= 2:
        O, X, A, B, C = p[-3], t[-2], p[-2], t[-1], p[-1]
        ox, xa, ab, bc = (O-X), (A-X), (A-B), (C-B)
        if xa != 0 and 1.13 <= (ab/xa) <= 1.618:
            if ox != 0 and is_near_ratio(bc/ox, 0.886, tolerance):
                acc = calculate_log_fib_accuracy(current_price, C, atr_value)
                conf = check_ema_confluence(df, C)
                return calculate_harmonic_targets("قرش بيعي", "بيع", C, B, O, acc, conf, "shark")

    # ---------------------------------------------------------
    # 3. الجارتلي والخفاش (Gartley & Bat)
    # ---------------------------------------------------------
    # شرائي
    if len(t) >= 3 and len(p) >= 2:
        X, A, B, C, D = t[-3], p[-2], t[-2], p[-1], t[-1]
        xa, ab, ad = (A-X), (A-B), (A-D)
        if xa != 0:
            actual_ab, actual_ad = ab/xa, ad/xa
            if is_near_ratio(actual_ab, 0.618, tolerance) and is_near_ratio(actual_ad, 0.786, tolerance):
                acc = calculate_log_fib_accuracy(current_price, D, atr_value)
                conf = check_ema_confluence(df, D)
                return calculate_harmonic_targets("جارتلي شرائي", "شراء", D, A, X, acc, conf)
            if (0.382 <= actual_ab <= 0.5) and is_near_ratio(actual_ad, 0.886, tolerance):
                acc = calculate_log_fib_accuracy(current_price, D, atr_value)
                conf = check_ema_confluence(df, D)
                return calculate_harmonic_targets("خفاش شرائي", "شراء", D, A, X, acc, conf)

    # بيعي
    if len(p) >= 3 and len(t) >= 2:
        X, A, B, C, D = p[-3], t[-2], p[-2], t[-1], p[-1]
        xa, ab, ad = (X-A), (B-A), (D-A)
        if xa != 0:
            actual_ab, actual_ad = ab/xa, ad/xa
            if is_near_ratio(actual_ab, 0.618, tolerance) and is_near_ratio(actual_ad, 0.786, tolerance):
                acc = calculate_log_fib_accuracy(current_price, D, atr_value)
                conf = check_ema_confluence(df, D)
                return calculate_harmonic_targets("جارتلي بيعي", "بيع", D, A, X, acc, conf)
            if (0.382 <= actual_ab <= 0.5) and is_near_ratio(actual_ad, 0.886, tolerance):
                acc = calculate_log_fib_accuracy(current_price, D, atr_value)
                conf = check_ema_confluence(df, D)
                return calculate_harmonic_targets("خفاش بيعي", "بيع", D, A, X, acc, conf)

 
    # ---------------------------------------------------------
    # 4. النماذج الهيكلية والأنماط الكلاسيكية
    # ---------------------------------------------------------
    # البوق المتسع (Megaphone)
    if p3 > p2 > p1 and t3 < t2 < t1:
        megaphone_height = p3 - t3
        if current_price > p3:
            return {"name": "بوق متسع", "class": "انفجار سعري", "breakout": round(p3, 5), "target": round(p3 + (megaphone_height * 0.8), 5), "sl": round(p3 - (megaphone_height * 0.3), 5), "status": "مكتمل", "harmonic_fib_accuracy": 0.0, "harmonic_d_confluence": 2}
        elif current_price < t3:
            return {"name": "بوق متسع", "class": "انهيار سعري", "breakout": round(t3, 5), "target": round(t3 - (megaphone_height * 0.8), 5), "sl": round(t3 + (megaphone_height * 0.3), 5), "status": "مكتمل", "harmonic_fib_accuracy": 0.0, "harmonic_d_confluence": 2}

    # القمة الثلاثية
    if is_near_ratio(p1/p2, 1, tolerance) and is_near_ratio(p2/p3, 1, tolerance):
        neckline = min(lows[peaks[-3]:peaks[-1]])
        if current_price < neckline:
            height = max(p1, p2, p3) - neckline
            return {"name": "قمة ثلاثية", "class": "انعكاسي هابط", "breakout": round(neckline, 5), "target": round(neckline - height, 5), "sl": round(neckline + (height * 0.3), 5), "status": "مكتمل", "harmonic_fib_accuracy": 0.0, "harmonic_d_confluence": 2}

    # القاع الثلاثي
    if is_near_ratio(t1/t2, 1, tolerance) and is_near_ratio(t2/t3, 1, tolerance):
        neckline = max(highs[troughs[-3]:troughs[-1]])
        if current_price > neckline:
            height = neckline - min(t1, t2, t3)
            return {"name": "قاع ثلاثي", "class": "انعكاسي صاعد", "breakout": round(neckline, 5), "target": round(neckline + height, 5), "sl": round(neckline - (height * 0.3), 5), "status": "مكتمل", "harmonic_fib_accuracy": 0.0, "harmonic_d_confluence": 2}

    # صندوق دارفاس
    if is_near_ratio(p2, p3, p2*tolerance) and is_near_ratio(t2, t3, t2*tolerance):
        box_high = max(p2, p3)
        box_low = min(t2, t3)
        box_height = box_high - box_low
        if current_price > box_high:
            return {"name": "صندوق دارفاس صاعد", "class": "استمراري/اختراق", "breakout": round(box_high, 5), "target": round(box_high + box_height, 5), "sl": round(box_high - (box_height * 0.5), 5), "status": "مكتمل", "harmonic_fib_accuracy": 0.0, "harmonic_d_confluence": 2}
        elif current_price < box_low:
            return {"name": "صندوق دارفاس هابط", "class": "استمراري/كسر", "breakout": round(box_low, 5), "target": round(box_low - box_height, 5), "sl": round(box_low + (box_height * 0.5), 5), "status": "مكتمل", "harmonic_fib_accuracy": 0.0, "harmonic_d_confluence": 2}

    return default_pattern

# ==========================================
# 3. دوال تحليل الترند والزاوية والقنوات
# ==========================================

def find_swing_points(df, window=5):
    highs, lows = df['high'].values, df['low'].values
    swing_highs, swing_lows = [], []
    for i in range(window, len(df) - window):
        if highs[i] == max(highs[i - window : i + window + 1]):
            swing_highs.append((i, highs[i]))
        if lows[i] == min(lows[i - window : i + window + 1]):
            swing_lows.append((i, lows[i]))
    return swing_highs, swing_lows


def calculate_trendline_angle(x1, y1, x2, y2, avg_price):
    dx = x2 - x1
    if dx == 0: return 90
    dy = ((y2 - y1) / avg_price) * 100  
    angle_rad = math.atan(dy / dx)
    return abs(math.degrees(angle_rad))


def validate_strict_trendline(df, x1, y1, x2, y2, trend_type="UP"):
    slope = (y2 - y1) / (x2 - x1)
    intercept = y1 - (slope * x1)
    for i in range(x1 + 1, len(df)):
        trend_price = (slope * i) + intercept 
        body_bottom = min(df['open'].iloc[i], df['close'].iloc[i])
        body_top = max(df['open'].iloc[i], df['close'].iloc[i])
        if trend_type == "UP" and body_bottom < trend_price: return False 
        elif trend_type == "DOWN" and body_top > trend_price: return False                
    return True
    

def generate_trend_data(df, min_distance=10):
    swings_high, swings_low = find_swing_points(df, window=5)
    avg_price = df['close'].mean()
    touch_tolerance = avg_price * 0.001 
    
    best_trend = {
        "direction": "عرضي", "angle": 0.0, "touches": 0, 
        "current_line_price": 0.0, "is_valid": 2, "slope": 0.0, 
        "intercept": 0.0, "r_squared": 0.0 # إضافة معامل التحديد هنا
    }
    
    last_idx = len(df) - 1

    # --- 1. البحث عن ترند صاعد (عبر القيعان Swings Low) ---
    if len(swings_low) >= 2:
        for i in range(len(swings_low)-1, 0, -1):
            for j in range(i-1, -1, -1):
                x2, y2 = swings_low[i]
                x1, y1 = swings_low[j]
                if (x2 - x1) < min_distance or y2 <= y1: continue
                
                slope = (y2 - y1) / (x2 - x1)
                intercept = y1 - (slope * x1)
                
                if validate_strict_trendline(df, x1, y1, x2, y2, "UP"):
                    angle = calculate_trendline_angle(x1, y1, x2, y2, avg_price)
                    
                    if 15 <= angle <= 65:
                        # --- [هنا يبدأ التبني!] ---
                        # جمع النقاط التي تلمس الخط فعلياً لاختبارها إحصائياً
                        pts_x, pts_y = [], []
                        for sx, sy in swings_low:
                            expected_y = (slope * sx) + intercept
                            if abs(sy - expected_y) <= touch_tolerance:
                                pts_x.append(sx)
                                pts_y.append(sy)
                        
                        # استدعاء الدالة اليتيمة لتحليل الترند إحصائياً
                        stats = calculate_statistical_trend(np.array(pts_x), np.array(pts_y))
                        
                        # تحديث أفضل ترند إذا كان إحصائياً أقوى (R-squared أعلى)
                        if len(pts_x) >= 2 and stats["r_squared"] > best_trend["r_squared"]:
                            best_trend.update({
                                "direction": "صاعد",
                                "angle": round(angle, 2),
                                "touches": len(pts_x),
                                "is_valid": 1 if (len(pts_x) >= 3 and stats["is_valid"]) else 2,
                                "slope": slope,
                                "intercept": intercept,
                                "current_line_price": round((slope * last_idx) + intercept, 5),
                                "r_squared": stats["r_squared"]
                            })

    # --- 2. البحث عن ترند هابط (عبر القمم Swings High) ---
    if len(swings_high) >= 2:
        for i in range(len(swings_high)-1, 0, -1):
            for j in range(i-1, -1, -1):
                x2, y2 = swings_high[i]
                x1, y1 = swings_high[j]
                if (x2 - x1) < min_distance or y2 >= y1: continue
                
                slope = (y2 - y1) / (x2 - x1)
                intercept = y1 - (slope * x1)
                
                if validate_strict_trendline(df, x1, y1, x2, y2, "DOWN"):
                    angle = calculate_trendline_angle(x1, y1, x2, y2, avg_price)
                    
                    if 15 <= angle <= 65:
                        # --- [هنا يبدأ التبني!] ---
                        pts_x, pts_y = [], []
                        for sx, sy in swings_high:
                            expected_y = (slope * sx) + intercept
                            if abs(sy - expected_y) <= touch_tolerance:
                                pts_x.append(sx)
                                pts_y.append(sy)
                        
                        stats = calculate_statistical_trend(np.array(pts_x), np.array(pts_y))

                        if len(pts_x) >= 2 and stats["r_squared"] > best_trend["r_squared"]:
                            best_trend.update({
                                "direction": "هابط",
                                "angle": round(angle, 2),
                                "touches": len(pts_x),
                                "is_valid": 1 if (len(pts_x) >= 3 and stats["is_valid"]) else 2,
                                "slope": slope,
                                "intercept": intercept,
                                "current_line_price": round((slope * last_idx) + intercept, 5),
                                "r_squared": stats["r_squared"]
                            })

    return best_trend, swings_high, swings_low
    

def calculate_price_channel(df, best_trend, swings_high, swings_low):
    """
    تحديث احترافي: يقوم ببناء قناة سعرية موازية للترند المكتشف،
    ويحسب عدد اللمسات على الخط المقابل لضمان صحة القناة كلاسيكياً.
    """
    # 1. الإعدادات الأولية
    channel_data = {
        "channel_upper": 0.0, 
        "channel_lower": 0.0, 
        "channel_direction": best_trend.get("direction", "عرضي"),
        "channel_touches": 0,
        "channel_status": "NONE",
        "channel_weakness": "NONE"
    }

    # التأكد من وجود ترند صحيح لبناء القناة عليه
    if best_trend.get("is_valid") != 1 or best_trend.get("slope") == 0:
        return channel_data

    m = best_trend["slope"]
    b_base = best_trend.get("intercept", 0.0)
    avg_price = df['close'].mean()
    touch_tolerance = avg_price * 0.0015  # نسبة سماح 0.15% لتغطية ذيول الشموع
    last_x = len(df) - 1

    # 2. منطق البحث عن الخط الموازي (السقف أو القاع المقابل)
    best_intercept_opp = None
    max_opp_touches = 0
    
    # إذا كان الترند صاعداً (قاعدته القيعان)، نبحث عن أفضل سقف يمر بالقمم
    if best_trend["direction"] == "صاعد":
        target_swings = swings_high
        # السعر السفلي للقناة (الترند الأساسي)
        channel_data["channel_lower"] = round((m * last_x) + b_base, 5)
        
        for sx, sy in target_swings:
            current_b_opp = sy - (m * sx)
            # حساب كم نقطة تلمس هذا الخط الموازي
            touches = sum(1 for tx, ty in target_swings if abs(ty - ((m * tx) + current_b_opp)) <= touch_tolerance)
            if touches > max_opp_touches:
                max_opp_touches = touches
                best_intercept_opp = current_b_opp
        
        if best_intercept_opp is not None:
            channel_data["channel_upper"] = round((m * last_x) + best_intercept_opp, 5)

    # إذا كان الترند هابطاً (قاعدته القمم)، نبحث عن أفضل قاع يمر بالقيعان
    elif best_trend["direction"] == "هابط":
        target_swings = swings_low
        # السعر العلوي للقناة (الترند الأساسي)
        channel_data["channel_upper"] = round((m * last_x) + b_base, 5)
        
        for sx, sy in target_swings:
            current_b_opp = sy - (m * sx)
            touches = sum(1 for tx, ty in target_swings if abs(ty - ((m * tx) + current_b_opp)) <= touch_tolerance)
            if touches > max_opp_touches:
                max_opp_touches = touches
                best_intercept_opp = current_b_opp
                
        if best_intercept_opp is not None:
            channel_data["channel_lower"] = round((m * last_x) + best_intercept_opp, 5)

    # 3. تقييم جودة القناة (اللمسات والحالة)
    total_touches = best_trend.get("touches", 2) + max_opp_touches
    channel_data["channel_touches"] = total_touches
    
    if total_touches >= 5: # 3 من جهة و 2 من جهة أخرى مثلاً
        channel_data["channel_status"] = "STRONG_CONFIRMED"
    elif total_touches >= 3:
        channel_data["channel_status"] = "VALID"
    else:
        channel_data["channel_status"] = "WEAK"

    # 4. دمج تحليل الضعف (RSI) من كودك القديم كفلتر إضافي
    if 'rsi' not in df.columns:
        df['rsi'] = calculate_rsi(df['close'])
    current_rsi = df['rsi'].iloc[-1]
    
    if best_trend["direction"] == "صاعد" and current_rsi < 45:
        channel_data["channel_weakness"] = "BULLISH_EXHAUSTION" # إرهاق شرائي
    elif best_trend["direction"] == "هابط" and current_rsi > 55:
        channel_data["channel_weakness"] = "BEARISH_EXHAUSTION" # إرهاق بيعي

    return channel_data

# ==========================================
# 4. دمج المحرك والمخرجات الشاملة
# ==========================================

def calculate_continuation_logic(pattern_name: str, prior_trend: str, breakout_point: float, pattern_high: float, pattern_low: float) -> dict:
    height = pattern_high - pattern_low
    target = breakout_point + height if prior_trend == "شراء" else breakout_point - height
    sl = breakout_point - (height * 0.5) if prior_trend == "شراء" else breakout_point + (height * 0.5)
    return {"name": pattern_name, "class": "استمراري", "breakout": round(breakout_point, 5), "target": round(target, 5), "sl": round(sl, 5), "pattern_high": pattern_high, "pattern_low": pattern_low}


def detect_patterns_and_calculate(
    df_tf: pd.DataFrame, symbol: str, tf: str, df_4h: pd.DataFrame = None, 
    min_bars: int = 20, trend_threshold: float = 0.03, tolerance: float = 0.02, strict_breakout: bool = True
) -> Dict[str, Union[str, float, int]]:
    
    final_output = {
        "name": "لا يوجد", "class": "لا يوجد", "breakout": 0.0, "target": 0.0, "sl": 0.0, "status": "بحث مستمر",
        "is_body_close": 2,
        "channel_weakness": "NONE",
        "pattern_retracement_pct": 0.0,
        "pattern_apex_progress": 0.0,
        "is_marubozu_breakout": 2,
        "rsi_divergence_4h": "NONE",
        "harmonic_fib_accuracy": 0.0,
        "harmonic_d_confluence": 2,
        "1h_trend_angle": 0.0
    }

    if df_tf is None or len(df_tf) < min_bars: return final_output

    highs, lows, closes = df_tf['high'].values, df_tf['low'].values, df_tf['close'].values
    opens = df_tf['open'].values
    current_price = closes[-2] if strict_breakout else closes[-1]
    current_candle_idx = -2 if strict_breakout else -1
    
    # 1. الترند والزاوية والقنوات
    trend_data, s_highs, s_lows = generate_trend_data(df_tf)
    final_output["1h_trend_angle"] = trend_data["angle"]
    
    channel_data = calculate_price_channel(df_tf, trend_data, s_highs, s_lows)
    final_output["channel_weakness"] = channel_data["channel_weakness"]

    final_output["rsi_divergence_4h"] = detect_rsi_divergence_4h(df_4h)

    # 2. محرك النماذج (Elite Patterns)
    detected_pattern = detect_elite_patterns(df_tf, highs, lows, closes, current_price, tolerance)
    
    # 3. النماذج الكلاسيكية والاستمرارية (في حال لم يتم العثور على هارمونيك/هيكلي)
    if detected_pattern["name"] == "لا يوجد":
        peaks, _ = find_peaks(highs, distance=5)
        troughs, _ = find_peaks(-lows, distance=5)
        
        # الأعلام والرايات (Continuation)
        lookback = 15 
        price_change = ((current_price - closes[-lookback]) / closes[-lookback]) if closes[-lookback] != 0 else 0.0
        recent_high = highs[-10:].max() if len(highs) >= 10 else highs.max()
        recent_low = lows[-10:].min() if len(lows) >= 10 else lows.min()
        consolidation_height = recent_high - recent_low

        if price_change > trend_threshold and current_price > (recent_high - (consolidation_height * 0.2)):
            pole_height = recent_high - lows[-lookback:].min()
            if current_price > recent_high:
                detected_pattern = calculate_continuation_logic("علم صاعد", "شراء", recent_high, recent_high, recent_high - pole_height)
        elif price_change < -trend_threshold and current_price < (recent_low + (consolidation_height * 0.2)):
            pole_height = highs[-lookback:].max() - recent_low
            if current_price < recent_low:
                detected_pattern = calculate_continuation_logic("علم هابط", "بيع", recent_low, recent_low + pole_height, recent_low)

        # باقي النماذج الكلاسيكية
        if detected_pattern["name"] == "لا يوجد" and len(peaks) >= 3 and len(troughs) >= 3:
            p1, p2, p3 = highs[peaks[-3]], highs[peaks[-2]], highs[peaks[-1]]
            t1, t2, t3 = lows[troughs[-3]], lows[troughs[-2]], lows[troughs[-1]]
            
            # قمة مزدوجة
            if is_near_ratio(p2/p3, 1, tolerance):
                neckline = t2
                if current_price < neckline:
                    height = max(p2, p3) - neckline
                    detected_pattern = {"name": "قمة مزدوجة", "class": "انعكاسي هابط", "breakout": round(neckline, 5), "target": round(neckline - height, 5), "sl": round(neckline + (height * 0.3), 5)}
            
            # قاع مزدوج
            elif is_near_ratio(t2/t3, 1, tolerance):
                neckline = p2
                if current_price > neckline:
                    height = neckline - min(t2, t3)
                    detected_pattern = {"name": "قاع مزدوج", "class": "انعكاسي صاعد", "breakout": round(neckline, 5), "target": round(neckline + height, 5), "sl": round(neckline - (height * 0.3), 5)}
            
            # رأس وكتفين
            elif p2 > p1 and p2 > p3 and is_near_ratio(p1/p3, 1, tolerance*2):
                neckline = (t1 + t2) / 2
                if current_price < neckline:
                    height = p2 - neckline
                    detected_pattern = {"name": "رأس وكتفين", "class": "انعكاسي هابط", "breakout": round(neckline, 5), "target": round(neckline - height, 5), "sl": round(p3, 5)}
            
            # رأس وكتفين مقلوب
            elif t2 < t1 and t2 < t3 and is_near_ratio(t1/t3, 1, tolerance*2):
                neckline = (p1 + p2) / 2
                if current_price > neckline:
                    height = neckline - t2
                    detected_pattern = {"name": "رأس وكتفين مقلوب", "class": "انعكاسي صاعد", "breakout": round(neckline, 5), "target": round(neckline + height, 5), "sl": round(t3, 5)}
            
            # المثلثات والأوتاد
            elif p3 < p2 and t3 > t2:
                if current_price > p3: 
                    detected_pattern = calculate_continuation_logic("مثلث متماثل", "شراء", p3, p2, t2)
                elif current_price < t3: 
                    detected_pattern = calculate_continuation_logic("مثلث متماثل", "بيع", t3, p2, t2)
                
                if detected_pattern.get("name", "لا يوجد") != "لا يوجد":
                    start_idx = min(peaks[-2], troughs[-2])
                    est_apex = start_idx + (len(df_tf) - start_idx) * 1.5
                    if est_apex > start_idx:
                        progress = (len(df_tf) - start_idx) / (est_apex - start_idx)
                        final_output["pattern_apex_progress"] = round(min(1.0, progress) * 100, 2)
            
            # وتد هابط / صاعد
            elif p3 < p2 < p1 and t3 < t2 < t1:
                if current_price > p3:
                    detected_pattern = calculate_continuation_logic("وتد هابط", "شراء", current_price, p2, t3)
            elif p3 > p2 > p1 and t3 > t2 > t1:
                if current_price < t3:
                    detected_pattern = calculate_continuation_logic("وتد صاعد", "بيع", current_price, p3, t2)

    # 4. الدمج والتأكيدات النهائية
    if detected_pattern.get("name", "لا يوجد") != "لا يوجد":
        final_output.update({k: v for k, v in detected_pattern.items() if k in final_output or k in ["name", "class", "breakout", "target", "sl"]})
        
        breakout_price = final_output["breakout"]
        is_buy_setup = final_output["target"] > breakout_price
        
        if is_buy_setup and closes[current_candle_idx] > breakout_price:
            final_output["is_body_close"] = 1
        elif not is_buy_setup and closes[current_candle_idx] < breakout_price:
            final_output["is_body_close"] = 1
            
        final_output["is_marubozu_breakout"] = calculate_marubozu_status(
            opens[current_candle_idx], highs[current_candle_idx], lows[current_candle_idx], closes[current_candle_idx]
        )
        
        if "pattern_high" in detected_pattern and "pattern_low" in detected_pattern:
            p_height = detected_pattern["pattern_high"] - detected_pattern["pattern_low"]
            if p_height > 0:
                ret = (detected_pattern["pattern_high"] - current_price) / p_height if is_buy_setup else (current_price - detected_pattern["pattern_low"]) / p_height
                final_output["pattern_retracement_pct"] = round(max(0, ret) * 100, 2)

    return final_output
    
    
async def fetch_klines(session, symbol, interval, limit=100):
    url = f"https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        async with session.get(url, timeout=10) as res:
            if res.status == 200: return await res.json()
    except: return None


async def update_crypto_market_data():
    print(f"\n🚀 {datetime.now().strftime('%H:%M:%S')} | بدء جلب بيانات Binance Vision (شاملة فلترة العملات والأنماط الفنية)...")
    
    async with aiohttp.ClientSession() as session:
        # ✨ [الحل الجذري]: جلب حالة العملات الحية أولاً لتصفية الميتة والمتوقفة ✨
        valid_symbols = set()
        try:
            async with session.get("https://data-api.binance.vision/api/v3/exchangeInfo", timeout=10) as ex_res:
                if ex_res.status == 200:
                    ex_data = await ex_res.json()
                    for s in ex_data.get('symbols', []):
                        if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT':
                            valid_symbols.add(s['symbol'])
        except Exception as e:
            logging.error(f"❌ فشل جلب ExchangeInfo: {e}")
            return

        try:
            async with session.get("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10) as res:
                if res.status != 200: return
                ticker_data = await res.json()
                if not isinstance(ticker_data, list): return
        except Exception as e:
            logging.error(f"❌ فشل الاتصال بـ API التيكر: {e}")
            return

        STABLE_COINS = {
            "USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "BUSDUSDT", 
            "DAIUSDT", "EURUSDT", "AEURUSDT", "USDPUSDT", "USDDUSDT",
            "PYUSDUSDT", "EURIUSDT"
        }

        top_coins = []
        for c in ticker_data:
            if not isinstance(c, dict): continue
            
            symbol = c.get('symbol', '')
            
            if symbol not in valid_symbols: continue
            if not symbol.endswith('USDT'): continue
            if symbol in STABLE_COINS: continue 
            if symbol.endswith('UPUSDT') or symbol.endswith('DOWNUSDT'): continue 
            
            last_price = float(c.get('lastPrice', 0))
            quote_volume = float(c.get('quoteVolume', 0))
            high_price = float(c.get('highPrice', 0))
            low_price = float(c.get('lowPrice', 0))
            trades_count = int(c.get('count', 0))

            if last_price < 0.0001: continue
            
            if 0.98 <= last_price <= 1.02 and low_price > 0:
                price_volatility = (high_price - low_price) / low_price
                if price_volatility < 0.015: 
                    continue 
                    
            if trades_count < 1000: continue
            if quote_volume < 100000: continue
            if high_price == low_price: continue
            
            top_coins.append(c)
        
        top_coins = sorted(top_coins, key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)[:600]
        
        timeframes = ['5m', '15m', '1h', '2h', '4h', '1d', '1w', '1M']
        final_records = []

        for coin in top_coins:
            symbol = coin.get('symbol')
            try:
                price = float(coin.get('lastPrice', 0))
                change_percent = float(coin.get('priceChangePercent', 0))
                
                # جلب عمق السوق
                orderbook_url = f"https://data-api.binance.vision/api/v3/depth?symbol={symbol}&limit=20"
                imbalance_ratio = 1.0 
                
                try:
                    async with session.get(orderbook_url, timeout=5) as ob_res:
                        if ob_res.status == 200:
                            depth = await ob_res.json()
                            bids_vol = sum([float(bid[1]) for bid in depth.get('bids', [])])
                            asks_vol = sum([float(ask[1]) for ask in depth.get('asks', [])])
                            if asks_vol > 0:
                                imbalance_ratio = bids_vol / asks_vol
                except Exception as e:
                    logging.warning(f"⚠️ فشل جلب عمق السوق لـ {symbol}: {e}")

                record = {
                    "symbol": symbol,
                    "name": symbol.replace("USDT", ""),
                    "current_price": price,
                    "orderbook_imbalance_ratio": round(imbalance_ratio, 4),
                    "open_price_24h": float(coin.get('openPrice', 0)),
                    "high_24h": float(coin.get('highPrice', 0)),
                    "low_24h": float(coin.get('lowPrice', 0)),
                    "volume_24h": float(coin.get('volume', 0)),
                    "change_24h": change_percent,
                    "last_tick_direction": "UP" if change_percent >= 0 else "DOWN",
                    "updated_at": "now()",
                    "last_api_update_ms": int(datetime.now().timestamp() * 1000)
                }
                
                tasks = [fetch_klines(session, symbol, tf) for tf in timeframes]
                results = await asyncio.gather(*tasks)

                # 🌟 [ تعديل: استخراج df_4h مسبقاً لتمريره للمحرك لكشف الدايفرجنس ] 🌟
                df_4h_data = None
                idx_4h = timeframes.index('4h') if '4h' in timeframes else -1
                if idx_4h != -1 and results[idx_4h] and isinstance(results[idx_4h], list):
                    df_4h_data = pd.DataFrame(results[idx_4h], columns=[
                        'timestamp', 'open', 'high', 'low', 'close', 'volume',
                        'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'
                    ])
                    for col in ['open', 'high', 'low', 'close', 'volume']:
                        df_4h_data[col] = df_4h_data[col].astype(float)

                for i, tf in enumerate(timeframes):
                    if results[i] and isinstance(results[i], list):
                        df_tf = pd.DataFrame(results[i], columns=[
                            'timestamp', 'open', 'high', 'low', 'close', 'volume',
                            'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'
                        ])
                        
                        for col in ['open', 'high', 'low', 'close', 'volume']:
                            df_tf[col] = df_tf[col].astype(float)

                        highs = df_tf['high'].tolist()
                        lows = df_tf['low'].tolist()
                        closes = df_tf['close'].tolist()
                        volumes = df_tf['volume'].tolist()
                        trend_info, s_highs, s_lows = generate_trend_data(df_tf)                        
                        adx_val = calculate_adx(highs, lows, closes)

                        # حساب القناة السعرية
                        swings_high, swings_low = calculate_price_action_sr(highs, lows, return_swings=True) 
                        # تأكد أنه هكذا (استخدام المتغيرات الجديدة التي فككناها من الـ Tuple)
                        channel_info = calculate_price_channel(df_tf, trend_info, s_highs, s_lows)                        
                        # 🌟 [ تعديل: تمرير df_4h لمحرك الأنماط ] 🌟
                        pattern_data = detect_patterns_and_calculate(df_tf, symbol, tf, df_4h=df_4h_data)

                        # 🌟 [ حقن بيانات النماذج في الـ Record لجميع الفريمات ] 🌟
                        record.update({
                            f"{tf}_pattern_name": pattern_data.get("name", "لا يوجد"),
                            f"{tf}_pattern_class": pattern_data.get("class", "لا يوجد"),
                            f"{tf}_pattern_breakout": float(pattern_data.get("breakout", 0.0)),
                            f"{tf}_pattern_target": float(pattern_data.get("target", 0.0)),
                            f"{tf}_pattern_sl": float(pattern_data.get("sl", 0.0))
                        })

                        # 🌟 [ الإضافة الجديدة: حقن الأعمدة الجديدة الخاصة بالمحرك عند فريم الساعة كمرجع رئيسي ] 🌟
                        if tf == '1h':
                            record.update({
                                "is_body_close": int(pattern_data.get("is_body_close", 2)),
                                "channel_weakness": pattern_data.get("channel_weakness", "NONE"),
                                "pattern_retracement_pct": float(pattern_data.get("pattern_retracement_pct", 0.0)),
                                "pattern_apex_progress": float(pattern_data.get("pattern_apex_progress", 0.0)),
                                "is_marubozu_breakout": int(pattern_data.get("is_marubozu_breakout", 2)),
                                "rsi_divergence_4h": pattern_data.get("rsi_divergence_4h", "NONE"),
                                "harmonic_fib_accuracy": float(pattern_data.get("harmonic_fib_accuracy", 0.0)),
                                "harmonic_d_confluence": int(pattern_data.get("harmonic_d_confluence", 2)),
                                "1h_trend_angle": float(pattern_data.get("1h_trend_angle", 0.0))
                            })
                        
                        if tf in ['1w', '1M']:
                            record.update({
                                f"{tf}_trend_direction": trend_info.get("direction", "عرضي"),
                                f"{tf}_trend_slope_angle": trend_info.get("angle", 0.0),
                                f"{tf}_trend_touches": trend_info.get("touches", 0),
                                f"{tf}_trend_current_price": trend_info.get("current_line_price", 0.0),
                                f"{tf}_is_valid_trend": trend_info.get("is_valid", 2),
                                f"{tf}_channel_upper": channel_info.get("channel_upper", 0.0),
                                f"{tf}_channel_lower": channel_info.get("channel_lower", 0.0),
                                f"{tf}_channel_direction": channel_info.get("channel_direction", "NONE"),
                                f"{tf}_channel_touches": channel_info.get("channel_touches", 0),
                                f"{tf}_channel_status": channel_info.get("channel_status", "NONE"),
                                f"adx_{tf}": adx_val
                            })
                        else:
                            patterns = []
                            for j in range(5):
                                sub_df = df_tf if j == 0 else df_tf.iloc[:-j]
                                pattern_name = detect_all_pdf_patterns(sub_df)
                                patterns.append(pattern_name if pattern_name else "Normal")

                            last_candle_open_ts = datetime.fromtimestamp(int(results[i][-1][0]) / 1000).isoformat()
                            taker_buy_vols = [float(k[9]) for k in results[i]] 
                            
                            upper, mid, lower = calculate_bollinger(closes)
                            bbw_value = (upper - lower) / mid if mid > 0 else 0
                            atr_val = calculate_atr(highs, lows, closes)
                            kc_up, kc_mid, kc_low = calculate_keltner_channels(highs, lows, closes)
                            obv_val = calculate_obv(closes, volumes)
                            obv_prev_val = calculate_obv(closes[:-1], volumes[:-1]) if len(closes) > 1 else 0.0

                            v_delta = calculate_volume_delta(taker_buy_vols, volumes) 
                            rsi_series = calculate_rsi(closes)
                            rsi_val = float(rsi_series.iloc[-1]) if hasattr(rsi_series, 'iloc') and len(rsi_series) > 0 else 50.0
                            mood = get_market_mood(rsi_val) 
                            
                            tf_support, tf_resistance = calculate_price_action_sr(highs, lows)
                            macd_data = calculate_macd_values(closes)

                            if tf == '15m':
                                record["entry_zone_start"] = round(price * 0.998, 6)
                                record["entry_zone_end"] = round(price * 1.002, 6)
                                record["dca_protection_price"] = round(price - (atr_val * 1.5), 6)
                                record["target_1"] = round(price + (atr_val * 1.2), 6)
                                record["target_2"] = round(price + (atr_val * 2.5), 6)
                                record["stop_loss_atr"] = round(price - (atr_val * 2.2), 6)
                                record["market_mood"] = mood

                            record.update({
                                f"f{tf}_c1": patterns[0],
                                f"f{tf}_c2": patterns[1],
                                f"f{tf}_c3": patterns[2],
                                f"f{tf}_c4": patterns[3],
                                f"f{tf}_c5": patterns[4],
                                f"last_f{tf}_ts": last_candle_open_ts,
                                
                                f"macd_{tf}": macd_data['macd'],
                                f"macd_signal_{tf}": macd_data['signal'],
                                f"macd_hist_{tf}": macd_data['hist'],                            
                                
                                f"{tf}_trend_direction": trend_info.get("direction", "عرضي"),
                                f"{tf}_trend_slope_angle": trend_info.get("angle", 0.0),
                                f"{tf}_trend_touches": trend_info.get("touches", 0),
                                f"{tf}_trend_current_price": trend_info.get("current_line_price", 0.0),
                                f"{tf}_is_valid_trend": trend_info.get("is_valid", 2),
                                f"adx_{tf}": adx_val,

                                f"ema_20_{tf}": calculate_ema(closes, 20),
                                f"ema_50_{tf}": calculate_ema(closes, 50),
                                f"ema_100_{tf}": calculate_ema(closes, 100),
                                f"rsi_{tf}": rsi_val,
                                f"bb_upper_{tf}": upper, 
                                f"bb_middle_{tf}": mid, 
                                f"bb_lower_{tf}": lower,
                                f"bbw_{tf}": bbw_value,
                                f"atr_{tf}": atr_val,
                                f"volume_delta_{tf}": v_delta,
                                f"kc_upper_{tf}": kc_up,
                                f"kc_middle_{tf}": kc_mid,
                                f"kc_lower_{tf}": kc_low,
                                f"volume_{tf}": float(volumes[-1]),
                                f"volume_ma_{tf}": sum(volumes[-20:]) / 20 if len(volumes) >= 20 else sum(volumes)/len(volumes),
                                f"obv_{tf}": obv_val,
                                f"obv_prev_{tf}": obv_prev_val,
                                f"obv_slope_{tf}": obv_val - obv_prev_val,
                                
                                f"support_{tf}": tf_support,
                                f"resistance_{tf}": tf_resistance,
                                
                                f"{tf}_channel_upper": channel_info.get("channel_upper", 0.0),
                                f"{tf}_channel_lower": channel_info.get("channel_lower", 0.0),
                                f"{tf}_channel_direction": channel_info.get("channel_direction", "NONE"),
                                f"{tf}_channel_touches": channel_info.get("channel_touches", 0),
                                f"{tf}_channel_status": channel_info.get("channel_status", "NONE"),
                                
                                "market_mood": mood if tf == '15m' else record.get("market_mood", "STABLE"),
                                "stop_loss_atr": price - (atr_val * 1.5) if tf == '15m' else record.get("stop_loss_atr", 0)
                            })
# --- نهاية حلقة معالجة العملات ---
        # هنا يتم إضافة السجل للقائمة (إزاحة 16 فراغاً)
                final_records.append(record)
                print(f"🔹 [فحص] تم تجهيز {symbol}") # رادار للتأكد من المعالجة
            except Exception as e: 
                logging.error(f"❌ خطأ في معالجة {symbol}: {e}")
                continue

        # --- بعد خروجنا من حلقة الـ for (إزاحة 8 أو 12 فراغاً حسب الكود لديك) ---
        print(f"📊 إجمالي العملات الجاهزة للرفع: {len(final_records)}")

        if final_records:
            print(f"📦 جاري رفع {len(final_records)} عملة إلى سوبابيس...")
            for i in range(0, len(final_records), 50): 
                batch = final_records[i:i + 50]
                success = await async_manual_upsert("crypto_market_simulation", batch)
                
                if success:
                    logging.info(f"✅ تم حقن الدفعة {i//50 + 1} بنجاح")
                else:
                    logging.error(f"⚠️ فشل في حقن الدفعة {i//50 + 1}")
                
                # 🚨 السطر السحري: استراحة لمنع حظر سوبابيس
                await asyncio.sleep(1)

        # --- [ دمج السطر هنا: نهاية العملية بالكامل ] ---
        print(f"🏁 {datetime.now().strftime('%H:%M:%S')} | انتهت دورة التحديث بالكامل.")


async def async_manual_upsert1(table_name, records):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    endpoint = f"{SUPABASE_URL}/rest/v1/{table_name}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(endpoint, json=records, headers=headers, timeout=30) as response:
                if response.status in [200, 201, 204]:
                    return True
                else:
                    # طباعة الخطأ القادم من سوبابيس بالتفصيل
                    error_text = await response.text()
                    print(f"❌ فشل الرفع إلى {table_name}!")
                    print(f"📊 الحالة: {response.status}")
                    print(f"📝 رسالة الخطأ من سوبابيس: {error_text}")
                    return False
    except Exception as e:
        print(f"⚠️ خطأ تقني أثناء محاولة الرفع: {str(e)}")
        return False       
        
def get_trading_session(timestamp_ms):
    try:
        # بما أنك استوردت datetime مباشرة، نستخدمها هكذا:
        ts = timestamp_ms / 1000
        
        # استخدام utcfromtimestamp لأنه أبسط ويتوافق مع استيرادك
        dt_object = datetime.utcfromtimestamp(ts)
        
        hour = dt_object.hour
        day = dt_object.strftime('%A')
        
        if 0 <= hour < 8:
            session = "Asian (Tokyo/Sydney)"
        elif 8 <= hour < 16:
            session = "European (London)"
        else:
            session = "American (New York)"
            
        return session, day
    except Exception as e:
        logging.error(f"❌ خطأ في دالة الزمن: {e}")
        return "Unknown Session", "Unknown Day"
        
async def fetch_klines1(session, symbol, interval, limit=500): # تم رفع الحد إلى 300 لحساب EMA 200 بأمان
    url = f"https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        async with session.get(url, timeout=10) as res:
            if res.status == 200: 
                return await res.json()
    except Exception as e:
        logging.error(f"❌ خطأ في جلب بيانات {symbol} فريم {interval}: {e}")
    return None

def clean_nans(d):
    """دالة سحرية لتنظيف أي NaN وتحويله إلى None لكي يقبله سوبابيس بدون أخطاء"""
    cleaned = {}
    for k, v in d.items():
        if isinstance(v, float) and math.isnan(v):
            cleaned[k] = None
        elif isinstance(v, dict):
            cleaned[k] = clean_nans(v)
        else:
            cleaned[k] = v
    return cleaned

async def update_live_status(symbol, current_price, current_change):
    try:
        response = supabase.table("forensic_reports") \
            .select("id") \
            .eq("symbol", symbol) \
            .order("trigger_candle_timestamp_ms", desc=True) \
            .limit(1) \
            .execute()

        if response.data:
            record_id = response.data[0]['id']
            supabase.table("forensic_reports") \
                .update({
                    "price_after_event": float(current_price),
                    "price_change_percent_final": float(current_change)
                }) \
                .eq("id", record_id) \
                .execute()
    except Exception as e:
        logging.error(f"⚠️ فشل تحديث مسار {symbol}: {str(e)}")


async def run_forensic_autopsy(symbol, change_percent):
    """
    🕵️‍♂️ وحدة التحقيق الجنائي المتقدمة (المحقق كونان v3.1 - إصدار أثير للتحليل العميق)
    تمت إضافة رادار سيولة الحيتان والفجوات العادلة (FVG) وكشف التلاعب.
    """
    try:
        # 🛡️ فلتر الأمان: التأكد من أن العملة ضمن نطاق التحقيق المطلوب
        if change_percent >= 30:
            event_type = "PUMP"
        elif change_percent <= -20:
            event_type = "DUMP"
        else:
            return  # تجاهل إذا لم تكن مطابقة للشروط الصارمة

        print(f"\n🕵️‍♂️ [المحقق كونان] فتح ملف تحقيق شامل للعملة {symbol} | الحدث: {event_type} ({change_percent}%)")
        
        timeframes = ['1h', '2h', '4h', '1d']
        klines_data = {}
        
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_klines1(session, symbol, tf, limit=300) for tf in timeframes]
            results = await asyncio.gather(*tasks)
            
            for i, tf in enumerate(timeframes):
                if results[i]: klines_data[tf] = results[i]
                
        if '1h' not in klines_data or len(klines_data['1h']) < 30:
            print(f"⚠️ [المحقق كونان] الأدلة غير كافية لعملة {symbol}. إغلاق الملف.")
            return

        # ==========================================
        # 🕵️‍♂️ 1. تحديد "ساعة الصفر" من فريم الساعة (1H)
        # ==========================================
        df_1h = pd.DataFrame(klines_data['1h'], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'])
        for col in ['open', 'high', 'low', 'close', 'volume', 'taker_base_vol', 'timestamp']:
            df_1h[col] = df_1h[col].astype(float)
            
        df_1h['body_size'] = abs(df_1h['close'] - df_1h['open']) / df_1h['open'] * 100
        point_zero_idx = df_1h['body_size'].idxmax()
        
        if point_zero_idx < 25:
            print(f"⚠️ [المحقق كونان] الحدث حصل مبكراً جداً ولا يوجد تاريخ كافي لما قبل الكارثة. {symbol}")
            return

        point_zero_timestamp = int(df_1h.iloc[point_zero_idx]['timestamp'])
        current_timestamp = int(df_1h.iloc[-1]['timestamp'])

        # ==========================================
        # 🦈 [ إضافة بصمات الحيتان والأموال الذكية ]
        # ==========================================
        
        # 1. حساب صافي سيولة الحيتان (Whale Net Flow)
        tbv_before = float(df_1h.iloc[point_zero_idx - 1]['taker_base_vol']) # شراء السوق
        total_vol_before = float(df_1h.iloc[point_zero_idx - 1]['volume'])
        tsv_before = total_vol_before - tbv_before # بيع السوق
        taker_buy_ratio = (tbv_before / tsv_before) if tsv_before > 0 else 1.0
        whale_net_flow = tbv_before - tsv_before # 👈 الدليل القاطع على اتجاه السيولة
        
        # 2. كاشف الفجوات العادلة (FVG Size) قبل الانفجار
        fvg_size_pct = 0.0
        if point_zero_idx >= 2:
            c1_high = float(df_1h.iloc[point_zero_idx - 2]['high'])
            c1_low = float(df_1h.iloc[point_zero_idx - 2]['low'])
            c3_high = float(df_1h.iloc[point_zero_idx]['high'])
            c3_low = float(df_1h.iloc[point_zero_idx]['low'])
            
            if c1_high < c3_low:  # Bullish FVG
                fvg_size_pct = ((c3_low - c1_high) / c1_high) * 100
            elif c1_low > c3_high: # Bearish FVG
                fvg_size_pct = ((c1_low - c3_high) / c3_high) * 100

        # ==========================================
        # 🕯️ 2. دالة كشف أنماط الشموع ما قبل الكارثة
        # ==========================================
        def extract_past_patterns(tf_data):
            past_data = [k for k in tf_data if int(k[0]) < point_zero_timestamp]
            if len(past_data) < 25: return "No Pattern"
            
            df_past = pd.DataFrame(past_data[-25:], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'ct', 'qav', 'nt', 'tbv', 'tqv', 'ig'])
            for col in ['open', 'high', 'low', 'close']:
                df_past[col] = df_past[col].astype(float)
                
            try:
                detected = detect_all_pdf_patterns(df_past)
                if isinstance(detected, list):
                    valid_patterns = [p for p in detected if p and isinstance(p, str)]
                    return ", ".join(set(valid_patterns)) if valid_patterns else "Normal"
                elif isinstance(detected, str) and detected.strip():
                    return detected
                return "Normal"
            except Exception as e:
                print(f"⚠️ [المحقق كونان] خطأ أثناء فحص الشموع: {e}")
                return "Neutral"
                  
        patterns_1h = extract_past_patterns(klines_data.get('1h', []))
        patterns_2h = extract_past_patterns(klines_data.get('2h', []))
        patterns_4h = extract_past_patterns(klines_data.get('4h', []))
        patterns_1d = extract_past_patterns(klines_data.get('1d', []))

        # ==========================================
        # 🧬 3. دالة تشريح الفريمات (بيانات ما قبل الكارثة)
        # ==========================================
        def dissect_timeframe(tf_data, tf_name):
            past_data = [k for k in tf_data if int(k[0]) < point_zero_timestamp]
            if len(past_data) < 25: return None
            
            highs = [float(k[2]) for k in past_data]
            lows = [float(k[3]) for k in past_data]
            closes = [float(k[4]) for k in past_data]
            volumes = [float(k[5]) for k in past_data]
            
            upper, mid, lower = calculate_bollinger(closes) if len(closes) >= 20 else (None, None, None)
            bbw_val = (upper - lower) / mid if (mid and mid > 0) else 0
            kc_up, kc_mid, kc_low = calculate_keltner_channels(highs, lows, closes) if len(closes) >= 20 else (None, None, None)
            
            obv_val = calculate_obv(closes, volumes)
            obv_prev_val = calculate_obv(closes[:-1], volumes[:-1]) if len(closes) > 1 else 0.0
            
            return {
                f"ema_20_{tf_name}": calculate_ema(closes, 20) if len(closes) >= 20 else None,
                f"ema_50_{tf_name}": calculate_ema(closes, 50) if len(closes) >= 50 else None,
                f"ema_100_{tf_name}": calculate_ema(closes, 100) if len(closes) >= 100 else None,
                f"ema_200_{tf_name}": calculate_ema(closes, 200) if len(closes) >= 200 else None,
                f"rsi_{tf_name}": calculate_rsi(closes) if len(closes) >= 14 else None,
                f"obv_{tf_name}": obv_val,
                f"obv_slope_{tf_name}": obv_val - obv_prev_val if obv_val else None,
                f"atr_{tf_name}": calculate_atr(highs, lows, closes) if len(closes) >= 14 else None,
                f"adx_{tf_name}": calculate_adx(highs, lows, closes) if len(closes) >= 14 else None,
                f"bb_upper_{tf_name}": upper,
                f"bb_middle_{tf_name}": mid,
                f"bb_lower_{tf_name}": lower,
                f"bbw_{tf_name}": bbw_val,
                f"was_squeezed_{tf_name}": bool(bbw_val < 0.07) if bbw_val else None,
                f"kc_upper_{tf_name}": kc_up,
                f"kc_middle_{tf_name}": kc_mid,
                f"kc_lower_{tf_name}": kc_low,
                "last_volume": volumes[-1],
                "avg_volume_20": sum(volumes[-20:]) / 20 if len(volumes) >= 20 else (sum(volumes)/len(volumes) if volumes else 0),
                "last_close": closes[-1]
            }

        report_1h = dissect_timeframe(klines_data.get('1h', []), '1h')
        report_2h = dissect_timeframe(klines_data.get('2h', []), '2h')
        report_4h = dissect_timeframe(klines_data.get('4h', []), '4h')
        report_1d = dissect_timeframe(klines_data.get('1d', []), '1d')

        if not report_1h: 
            return

        # 🧮 حسابات الحركة السعرية والانحرافات
        price_before = float(report_1h['last_close'])
        price_after = float(df_1h.iloc[-1]['close'])
        actual_change_percent = ((price_after - price_before) / price_before) * 100
        duration_mins = int((current_timestamp - point_zero_timestamp) / 60000)
        vol_spike_ratio = report_1h['last_volume'] / report_1h['avg_volume_20'] if report_1h.get('avg_volume_20', 0) > 0 else 1

        closes_1h = df_1h['close'].iloc[:point_zero_idx].tolist()
        rsi_1h_vals = [calculate_rsi(closes_1h[:i+1]) for i in range(max(0, len(closes_1h)-10), len(closes_1h))]
        obv_1h_vals = [calculate_obv(closes_1h[:i+1], df_1h['volume'].iloc[:i+1].tolist()) for i in range(max(0, len(closes_1h)-10), len(closes_1h))]
        
        rsi_div = detect_divergence(closes_1h[-10:], [r for r in rsi_1h_vals if r is not None])
        obv_div = detect_divergence(closes_1h[-10:], [o for o in obv_1h_vals if o is not None])

        session, day_of_week = get_trading_session(point_zero_timestamp)

        # ==========================================
        # 📑 4. تجميع التقرير النهائي (تغذية الأعمدة الجديدة)
        # ==========================================
        raw_record = {
            "symbol": symbol,
            "event_type": event_type,
            "price_change_percent": float(change_percent),
            "price_before_event": price_before,
            "volume_before_event": float(report_1h['last_volume']),
            "price_after_event": price_after,
            "price_change_percent_final": float(actual_change_percent),
            "event_duration_minutes": duration_mins,
            
            # 🔥 بصمات السيولة والحيتان (تمت إضافتها)
            "taker_buy_ratio_1h": float(taker_buy_ratio),
            "whale_net_flow_volume": float(whale_net_flow), # 👈 العمود الجديد
            "fvg_gap_size": float(fvg_size_pct), # 👈 العمود الجديد
            "rsi_divergence_1h": rsi_div,
            "obv_divergence_1h": obv_div,
            
            # 🔥 بيانات المشتقات والبيئة (قيم مبدئية حتى نربط API الفيوتشرز)
            "oi_surge_rate": None, # سنقوم بجلبه لاحقاً
            "funding_bias": None,  # سنقوم بجلبه لاحقاً
            "btc_correlation": None, # سنقوم بجلبه لاحقاً
            
            # 🔥 نتائج التحقيق (للتحديث المستقبلي)
            "is_fakeout": False, 
            "realized_move_pct": 0.0,
            
            "trading_session": session,
            "day_of_week": day_of_week,
            "btc_dominance_at_event": None, 
            "coin_sector": "Unknown", 
                
            # 🔥 بصمات الشموع المكتشفة قبل الانفجار
            "patterns_1h": patterns_1h,
            "patterns_2h": patterns_2h,
            "patterns_4h": patterns_4h,
            "patterns_1d": patterns_1d,
            
            "volume_spike_ratio": float(vol_spike_ratio),
            "market_mood_at_event": None,

            # دمج بيانات الفريمات بذكاء
            **{k: v for k, v in report_1h.items() if k not in ['last_volume', 'avg_volume_20', 'last_close']},
            **{k: v for k, v in (report_2h or {}).items() if k not in ['last_volume', 'avg_volume_20', 'last_close']},
            **{k: v for k, v in (report_4h or {}).items() if k not in ['last_volume', 'avg_volume_20', 'last_close']},
            **{k: v for k, v in (report_1d or {}).items() if k not in ['last_volume', 'avg_volume_20', 'last_close']},
            
            "is_above_ema_200_1d": bool(report_1d['last_close'] > report_1d['ema_200_1d']) if report_1d and report_1d.get('ema_200_1d') else None,
            "metadata": {"version": "Conan_v3.1_Atheer"},
            "trigger_candle_timestamp_ms": int(point_zero_timestamp)
        }

        # سحر التنظيف لحماية قاعدة البيانات
        forensic_record = clean_nans(raw_record)
        
        print(f"✅ [المحقق كونان] تم تجهيز الأدلة لـ {symbol}. السيولة الصافية للحيتان: {whale_net_flow:.2f} | حجم FVG: {fvg_size_pct:.2f}%")

        # الرفع إلى سوبابيس (تأكد من أن الدالة تستخدم Upsert لمنع التكرار)
        success = await async_manual_upsert1("forensic_reports", [forensic_record])
        
        if success:
            print(f"🎉 [المحقق كونان] تم حفظ التقرير السري لعملة {symbol} بنجاح.")
        else:
            print(f"❌ [المحقق كونان] فشل أرشفة بيانات {symbol} في سوبابيس.")

    except Exception as e:
        print(f"\n☠️ [المحقق كونان] انهيار أثناء تشريح {symbol}: {str(e)}")
        logging.error(traceback.format_exc())  
        

import time
import asyncio
import aiohttp
import logging

async def forensic_investigation_cycle(active_investigations):
    """
    🕵️‍♂️ دورة المحقق الجنائي: تبحث عن الجرائم الجديدة (الانفجارات) وتحدث الملفات المفتوحة.
    """
    logging.info("🕵️‍♂️ [المحقق كونان] بدء جولة التفتيش الجنائي...")
    current_time = time.time()
    
    # 1. تنظيف القائمة: إغلاق الملفات التي مر عليها 24 ساعة (86400 ثانية)
    keys_to_remove = [sym for sym, timestamp in active_investigations.items() if current_time - timestamp > 86400]
    for k in keys_to_remove:
        del active_investigations[k]
        logging.info(f"📁 [إغلاق ملف] تم إنهاء تتبع {k} لمرور 24 ساعة.")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10) as res:
                if res.status == 200:
                    tickers = await res.json()
                    tasks = []
                    update_tasks = []
                    
                    # 2. البحث والتحديث
                    for coin in tickers:
                        symbol = coin.get('symbol', '')
                        if not symbol.endswith('USDT') or symbol in ['USDCUSDT', 'FDUSDUSDT']: 
                            continue
                            
                        change = float(coin.get('priceChangePercent', 0))
                        vol = float(coin.get('quoteVolume', 0))
                        current_price = float(coin.get('lastPrice', 0))
                        
                        # أ. هل هي جريمة جديدة؟ (تجاوزت +40% أو -30% بسيولة جيدة)
                        if vol > 50000 and (change >= 30 or change <= -20):
                            if symbol not in active_investigations:
                                active_investigations[symbol] = current_time
                                logging.info(f"🚨 [المحقق] رصد انفجار جديد {symbol} بنسبة {change}%")
                                tasks.append(run_forensic_autopsy(symbol, change))
                        
                        # ب. هل هي جريمة تحت المراقبة؟ (تحديث السعر المباشر للعملات المخزنة)
                        if symbol in active_investigations:
                            update_tasks.append(update_live_status(symbol, current_price, change))
                    
                    # 3. التنفيذ المتوازي للمهام
                    if tasks:
                        await asyncio.gather(*tasks)
                    if update_tasks:
                        await asyncio.gather(*update_tasks)
                        
    except Exception as e:
        logging.error(f"⚠️ خطأ في دورة المحقق الجنائي: {e}")
        
    print(f"🏁 [المحقق] أنهى جولته. يتتبع حالياً {len(active_investigations)} ملف نشط.")
    
    
async def unified_trading_system():
    """
    المايسترو الأكبر للنظام: 
    1. المصنع -> 2. الرادار -> 3. المحقق
    """
    print("✅ بدء تشغيل النظام الموحد (المايسترو)...")
    
    
    while True:
        try:

            print("\n" + "="*50)
            print(f"🔄 جولة مايسترو جديدة تبدأ الآن: {datetime.now().strftime('%H:%M:%S')}")
            print("="*50)

            # ⚙️ [الخطوة الأولى]: المصنع (تحديث المؤشرات والأموال الذكية)
            print("⚙️ [1] المصنع يشتغل ويحدث كل الفريمات...")
            await update_crypto_market_data()
            print("✅ المصنع أكمل الحقن بنجاح. استراحة 60 ثانية...")
            await asyncio.sleep(60)

            # 📡 [الخطوة الثانية]: الرادار (مسح الفرص الذهبية)
            print("\n📡 [2] نداء للرادار: البيانات جاهزة، ابدأ المسح وإطلاق الإشارات...")
            await intelligence_scanner()
            print("✅ الرادار أكمل مهمته. استراحة 60 ثانية...")
            await asyncio.sleep(60)
            
            # 🕵️‍♂️ [الخطوة الثالثة]: المحقق (تسجيل الانفجارات ومتابعة الأسعار)
            print("\n🕵️‍♂️ [3] نداء للمحقق: راجع السوق، افتح ملفات جديدة، وحَدِّث الأسعار...")
            await forensic_investigation_cycle(active_investigations)
            
            # ⏳ نهاية الجولة
            print("\n⏳ جولة (سلم واستلم) اكتملت بامتياز. استراحة 60 ثانية قبل الدورة القادمة...")
            await asyncio.sleep(60)
            
        except Exception as e:
            logging.error(f"⚠️ خطأ قاتل في النظام الموحد المايسترو: {e}")
            await asyncio.sleep(30) # انتظار قصير للتعافي من الصدمة
                                                                                                                       

# 1. 🟢 ضع هذا الكلاس قبل "نظام الإنعاش الأبدي" (في منطقة عامة خارج الدوال)
class TelegramLoggerHandler(logging.Handler):
    def __init__(self, bot, chat_id):
        super().__init__()
        self.bot = bot
        self.chat_id = chat_id

    def emit(self, record):
        log_entry = self.format(record)
        if record.levelno >= logging.ERROR:
            try:
                loop = asyncio.get_event_loop()
                loop.create_task(self.send_log(log_entry))
            except RuntimeError:
                pass

    async def send_log(self, message):
        try:
            msg = f"⚠️ <b>تنبيـه خطأ في النظام:</b>\n<code>{message[:3500]}</code>"
            await self.bot.send_message(self.chat_id, msg, parse_mode="HTML")
        except Exception:
            pass

# ==========================================
# 5. نهاية الملف: نظام الإنعاش الأبدي 24/7 (النبض الذاتي) ⚡
# ==========================================
import os
import asyncio
import logging
import random
import aiohttp
from aiohttp import web

# ==========================================
# 5. نظام الإنعاش الأبدي: "لا تأخذه سنة ولا نوم" ⚡
# ==========================================

async def handle_ping(request):
    """استجابة سريعة لإخبار السيرفر أن النظام مستيقظ"""
    return web.Response(
        text="Alive & Vigilant ⚡", 
        headers={"Connection": "keep-alive"}
    )


async def handle_telegram_login(request):
    return web.Response(text="✅ Data Received")


async def self_resuscitation():
    """النبض الذاتي: البوت يوقظ نفسه لمنع النوم (Anti-Idle)"""
    render_url = os.getenv("RENDER_EXTERNAL_URL") 
    if not render_url: return

    while True:
        try:
            # كسر التخزين المؤقت لضمان وصول الطلب للمعالج مباشرة
            rand_ping = f"{render_url}?v={random.randint(1, 99999)}"
            async with aiohttp.ClientSession() as session:
                async with session.get(rand_ping, timeout=10) as response:
                    logging.info(f"💉 [نبضة حية]: {response.status}")
        except Exception as e:
            logging.error(f"⚠️ [فشل النبض]: {e}")
        
        await asyncio.sleep(240) # كل 4 دقائق


async def watch_dog(task_func, *args):
    """
    بروتوكول اليقظة: مراقب دائم للمحركات.
    إذا توقف أي محرك (سنة) أو انهار (نوم)، يعيده للحياة فوراً.
    """
    while True:
        try:
            logging.info(f"🛡️ تشغيل محرك: {task_func.__name__}")
            await task_func(*args)
        except Exception as e:
            logging.error(f"🚨 انهيار في {task_func.__name__}: {e}")
            logging.info("♻️ إعادة التشغيل التلقائي الآن...")
            await asyncio.sleep(10) # انتظار بسيط لتجنب التكرار السريع عند الخطأ


async def main_startup():
    # 2. 🟢 ضع هذا الإعداد هنا في أول سطر داخل دالة main_startup
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(), # للطباعة في شاشة راندر كالعادة
            TelegramLoggerHandler(bot, GROUP_ID) # ليرسل الأخطاء للقروب فوراً
        ]
    )

    # أ) إعداد سيرفر الويب للبقاء Online (مهم للمنصات مثل Render/Heroku)
    app = web.Application()
    app.router.add_get('/', handle_ping)
    app.router.add_get('/login', handle_telegram_login)
    
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logging.info(f"🌐 Server Active on port {port}")

    # ب) تشغيل المحركات تحت حماية الـ WatchDog
    asyncio.create_task(watch_dog(unified_trading_system))
    asyncio.create_task(watch_dog(self_resuscitation))
    #asyncio.create_task(watch_dog(trade_reaper)) 
    
        
    # ج) تشغيل البوت الرئيسي (Aiogram) مع نظام إعادة المحاولة الصامد
    while True:
        try:
            logging.info("🚀 إقلاع محرك التليجرام... النظام تحت الحماية القصوى.")
            await bot.delete_webhook(drop_pending_updates=True)
            await dp.start_polling(bot)
        except Exception as e:
            logging.error(f"❌ خطأ في البوت: {e}")
            logging.info("🔄 محاولة إعادة التشغيل تلقائياً خلال 10 ثوانٍ...")
            await asyncio.sleep(10)
    
if __name__ == '__main__':
    try:
        # تشغيل المحرك الرئيسي
        asyncio.run(main_startup())
    except KeyboardInterrupt:
        print("🛑 تم إيقاف النظام يدوياً من قبل أثير.")
    except Exception as e:
        # 🟢 طباعة إجبارية باللون الأحمر في راندر لكشف الخطأ القاتل
        print("\n" + "❌"*20)
        print(f"💥 انهيار قاتل منع البوت من الإقلاع:")
        print(f"{type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        print("❌"*20 + "\n")
        
        logging.critical(f"💥 انهيار غير متوقع في النظام: {e}")
