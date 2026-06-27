#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股大单监控分析平台 - Flask 后端
提供真实 A股数据 API（基于 AKShare）
部署到 CloudStudio Python 环境
"""

import json
import time
import random
import os
import sys
import threading
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from flask import Flask, render_template, jsonify, request, send_from_directory
from flask_cors import CORS
import pandas as pd
import numpy as np

# ====== 尝试导入 AKShare ======
AKSHARE_AVAILABLE = False
ak = None
try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
    print("✅ AKShare 已加载，将使用真实 A股数据")
except ImportError:
    print("⚠️  AKShare 未安装，将使用模拟数据模式")
    AKSHARE_AVAILABLE = False

app = Flask(__name__)
CORS(app)

# 禁用浏览器缓存，确保用户总是获取最新数据
@app.after_request
def add_no_cache_headers(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

# ====== 配置文件路径 ======
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, 'config.json')
DATA_DIR = os.path.join(BASE_DIR, 'data')

os.makedirs(DATA_DIR, exist_ok=True)

# ====== 默认配置（30+热门A股）======
DEFAULT_CONFIG = {
    "monitor_stocks": [
        # 消费板块 7只
        {"code": "600519", "name": "贵州茅台", "sector": "消费", "enabled": True},
        {"code": "000858", "name": "五粮液", "sector": "消费", "enabled": True},
        {"code": "000568", "name": "泸州老窖", "sector": "消费", "enabled": True},
        {"code": "600809", "name": "山西汾酒", "sector": "消费", "enabled": True},
        {"code": "000333", "name": "美的集团", "sector": "消费", "enabled": True},
        {"code": "603288", "name": "海天味业", "sector": "消费", "enabled": True},
        {"code": "600887", "name": "伊利股份", "sector": "消费", "enabled": True},
        # 金融板块 6只
        {"code": "601318", "name": "中国平安", "sector": "金融", "enabled": True},
        {"code": "600036", "name": "招商银行", "sector": "金融", "enabled": True},
        {"code": "601166", "name": "兴业银行", "sector": "金融", "enabled": True},
        {"code": "600030", "name": "中信证券", "sector": "金融", "enabled": True},
        {"code": "601688", "name": "华泰证券", "sector": "金融", "enabled": True},
        {"code": "601328", "name": "交通银行", "sector": "金融", "enabled": True},
        # 科技板块 5只
        {"code": "000063", "name": "中兴通讯", "sector": "科技", "enabled": True},
        {"code": "002415", "name": "海康威视", "sector": "科技", "enabled": True},
        {"code": "002230", "name": "科大讯飞", "sector": "科技", "enabled": True},
        {"code": "600406", "name": "国电南瑞", "sector": "科技", "enabled": True},
        {"code": "002236", "name": "大华股份", "sector": "科技", "enabled": True},
        # 新能源板块 5只
        {"code": "300750", "name": "宁德时代", "sector": "新能源", "enabled": True},
        {"code": "002594", "name": "比亚迪", "sector": "新能源", "enabled": True},
        {"code": "300274", "name": "阳光电源", "sector": "新能源", "enabled": True},
        {"code": "002459", "name": "晶澳科技", "sector": "新能源", "enabled": True},
        {"code": "600438", "name": "通威股份", "sector": "新能源", "enabled": True},
        # 医药板块 5只
        {"code": "600276", "name": "恒瑞医药", "sector": "医药", "enabled": True},
        {"code": "000661", "name": "长春高新", "sector": "医药", "enabled": True},
        {"code": "300760", "name": "迈瑞医疗", "sector": "医药", "enabled": True},
        {"code": "603259", "name": "药明康德", "sector": "医药", "enabled": True},
        {"code": "000538", "name": "云南白药", "sector": "医药", "enabled": True},
        # 半导体板块 5只
        {"code": "688981", "name": "中芯国际", "sector": "半导体", "enabled": True},
        {"code": "688012", "name": "中微公司", "sector": "半导体", "enabled": True},
        {"code": "002049", "name": "紫光国微", "sector": "半导体", "enabled": True},
        {"code": "603501", "name": "韦尔股份", "sector": "半导体", "enabled": True},
        {"code": "688008", "name": "澜起科技", "sector": "半导体", "enabled": True},
        # 大盘蓝筹 5只
        {"code": "601857", "name": "中国石油", "sector": "大盘蓝筹", "enabled": True},
        {"code": "601398", "name": "工商银行", "sector": "大盘蓝筹", "enabled": True},
        {"code": "600028", "name": "中国石化", "sector": "大盘蓝筹", "enabled": True},
        {"code": "601288", "name": "农业银行", "sector": "大盘蓝筹", "enabled": True},
        {"code": "601988", "name": "中国银行", "sector": "大盘蓝筹", "enabled": True}
    ],
    "sectors": ["消费", "金融", "科技", "新能源", "医药", "半导体", "大盘蓝筹"],
    "thresholds": {
        "large_order": 200000,
        "super_large_order": 1000000,
        "alert_cooldown_seconds": 30
    },
    "notification": {
        "browser_notification": True,
        "sound_alert": True
    },
    "monitor_interval_seconds": 5
}

# ====== 内存中的数据 ======
alerts_history = []
stock_cache = {}
last_alert_time = {}

# ====== 数据缓存 ======
_realtime_cache = {}       # {code: (data, timestamp)}
_realtime_cache_lock = threading.Lock()
_all_stocks_cache = None   # 全A股实时数据缓存
_all_stocks_cache_time = 0
_CACHE_TTL = 15            # 缓存15秒
_AKSHARE_TIMEOUT = 8       # AKShare调用超时8秒


def call_with_timeout(func, args=(), kwargs=None, timeout=8):
    """带超时地调用函数，超时返回None"""
    if kwargs is None:
        kwargs = {}
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(func, *args, **kwargs)
            try:
                return future.result(timeout=timeout)
            except FuturesTimeoutError:
                print(f"⚠️ AKShare调用超时({timeout}秒): {func.__name__}")
                return None
    except Exception as e:
        print(f"⚠️ AKShare调用异常: {func.__name__} - {e}")
        return None


# ====== 工具函数 ======
def load_config():
    """加载配置文件"""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG


def save_config(config):
    """保存配置文件"""
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


def format_stock_code(code):
    """格式化股票代码（确保6位）"""
    return code.zfill(6)


def get_sector_stocks(sector):
    """获取某板块的热门股票"""
    sector_map = {
        "消费": ["600519", "000858", "000568", "600809", "000333", "603288", "600887"],
        "金融": ["601318", "600036", "601166", "600030", "601688", "601328"],
        "科技": ["000063", "002415", "002230", "600406", "002236"],
        "新能源": ["300750", "002594", "300274", "002459", "600438"],
        "医药": ["600276", "000661", "300760", "603259", "000538"],
        "半导体": ["688981", "688012", "002049", "603501", "688008"],
        "大盘蓝筹": ["601857", "601398", "600028", "601288", "601988"]
    }
    return sector_map.get(sector, [])


# ====== 数据获取函数 ======

# 股票名称缓存
_stock_name_cache = {}

def get_realtime_price_sina(code):
    """使用新浪财经API获取实时价格（轻量、快速、带5秒超时）"""
    try:
        # 确定前缀：6开头是sh，0/3开头是sz
        if code.startswith('6'):
            prefix = 'sh'
        else:
            prefix = 'sz'
        
        url = f"http://hq.sinajs.cn/list={prefix}{code}"
        req = urllib.request.Request(url, headers={
            'Referer': 'http://finance.sina.com.cn',
            'User-Agent': 'Mozilla/5.0'
        })
        
        with urllib.request.urlopen(req, timeout=5) as resp:
            content = resp.read().decode('gbk', errors='ignore')
        
        # 解析: var hq_str_sh600519="贵州茅台,1680.00,...";
        if '=' in content and '"' in content:
            data_str = content.split('"')[1]
            if not data_str:
                return None
            
            fields = data_str.split(',')
            if len(fields) < 10:
                return None
            
            name = fields[0]
            open_price = float(fields[1]) if fields[1] else 0
            pre_close = float(fields[2]) if fields[2] else 0
            current_price = float(fields[3]) if fields[3] else 0
            high = float(fields[4]) if fields[4] else 0
            low = float(fields[5]) if fields[5] else 0
            volume = int(float(fields[8])) if fields[8] else 0
            amount = float(fields[9]) if fields[9] else 0
            
            if current_price == 0:
                current_price = pre_close
            
            change = round(current_price - pre_close, 2)
            change_pct = round((change / pre_close * 100), 2) if pre_close > 0 else 0
            
            result = {
                "code": code,
                "name": name,
                "price": current_price,
                "open": open_price,
                "high": high,
                "low": low,
                "pre_close": pre_close,
                "change": change,
                "change_pct": change_pct,
                "volume": volume,
                "amount": amount,
                "turnover": 0,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            _stock_name_cache[code] = name
            return result
    except Exception as e:
        print(f"新浪API获取实时价格失败({code}): {e}")
    
    return None


def get_realtime_price_akshare(code):
    """使用 AKShare 获取实时价格（带缓存和超时）"""
    if not AKSHARE_AVAILABLE:
        return None
    
    # 检查缓存
    with _realtime_cache_lock:
        if code in _realtime_cache:
            data, ts = _realtime_cache[code]
            if time.time() - ts < _CACHE_TTL:
                return data
    
    try:
        # 获取全A股实时数据（带超时）
        global _all_stocks_cache, _all_stocks_cache_time
        
        df = None
        # 如果缓存有效，直接用缓存
        if _all_stocks_cache is not None and time.time() - _all_stocks_cache_time < _CACHE_TTL:
            df = _all_stocks_cache
        else:
            # 带超时调用AKShare
            df = call_with_timeout(ak.stock_zh_a_spot_em, timeout=_AKSHARE_TIMEOUT)
            if df is not None:
                _all_stocks_cache = df
                _all_stocks_cache_time = time.time()
        
        if df is not None and not df.empty:
            stock = df[df['代码'] == code]
            if not stock.empty:
                s = stock.iloc[0]
                result = {
                    "code": code,
                    "name": s.get('名称', code),
                    "price": float(s.get('最新价', 0) or 0),
                    "open": float(s.get('今开', 0) or 0),
                    "high": float(s.get('最高', 0) or 0),
                    "low": float(s.get('最低', 0) or 0),
                    "pre_close": float(s.get('昨收', 0) or 0),
                    "change": float(s.get('涨跌额', 0) or 0),
                    "change_pct": float(s.get('涨跌幅', 0) or 0),
                    "volume": int(s.get('成交量', 0) or 0),
                    "amount": float(s.get('成交额', 0) or 0),
                    "turnover": float(s.get('换手率', 0) or 0),
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                # 写入缓存
                with _realtime_cache_lock:
                    _realtime_cache[code] = (result, time.time())
                return result
    except Exception as e:
        print(f"获取实时价格失败({code}): {e}")
    
    return None


def get_kline_data_akshare(code, period="daily", count=100):
    """使用 AKShare 获取 K 线数据"""
    if not AKSHARE_AVAILABLE:
        return None
    
    try:
        # period: daily, weekly, monthly
        # 使用东方财富的数据接口
        df = call_with_timeout(ak.stock_zh_a_hist, kwargs={"symbol": code, "period": period, "adjust": "qfq"}, timeout=_AKSHARE_TIMEOUT)
        
        if df is not None and not df.empty:
            # 取最近 count 条
            df = df.tail(count)
            
            result = []
            for _, row in df.iterrows():
                result.append({
                    "date": str(row.get('日期', '')),
                    "open": float(row.get('开盘', 0)),
                    "close": float(row.get('收盘', 0)),
                    "high": float(row.get('最高', 0)),
                    "low": float(row.get('最低', 0)),
                    "volume": int(row.get('成交量', 0)),
                    "amount": float(row.get('成交额', 0)),
                    "change_pct": float(row.get('涨跌幅', 0)) if '涨跌幅' in row else 0
                })
            
            return result
    except Exception as e:
        print(f"获取K线数据失败({code}): {e}")
    
    return None


def get_intraday_data_akshare(code):
    """获取分时数据"""
    if not AKSHARE_AVAILABLE:
        return None
    
    try:
        df = call_with_timeout(ak.stock_zh_a_hist_min_em, kwargs={"symbol": code, "period": "1", "adjust": "qfq"}, timeout=_AKSHARE_TIMEOUT)
        
        if df is not None and not df.empty:
            df = df.tail(240)  # 最近4小时（1分钟线）
            
            result = []
            for _, row in df.iterrows():
                result.append({
                    "time": str(row.get('时间', '')),
                    "price": float(row.get('收盘', 0)),
                    "volume": int(row.get('成交量', 0)),
                    "amount": float(row.get('成交额', 0))
                })
            
            return result
    except Exception as e:
        print(f"获取分时数据失败({code}): {e}")
    
    return None


def get_fund_flow_akshare(code):
    """获取资金流向数据"""
    if not AKSHARE_AVAILABLE:
        return None
    
    try:
        # 个股资金流
        df = call_with_timeout(ak.stock_individual_fund_flow_rank, kwargs={"symbol": "即时"}, timeout=_AKSHARE_TIMEOUT)
        if df is not None and not df.empty:
            stock = df[df['代码'] == code]
            if not stock.empty:
                s = stock.iloc[0]
                return {
                    "code": code,
                    "name": s.get('名称', code),
                    "main_inflow": float(s.get('主力净流入-净额', 0)),
                    "main_inflow_pct": float(s.get('主力净流入-净占比', 0)),
                    "super_large_inflow": float(s.get('超大单净流入-净额', 0)),
                    "large_inflow": float(s.get('大单净流入-净额', 0)),
                    "medium_inflow": float(s.get('中单净流入-净额', 0)),
                    "small_inflow": float(s.get('小单净流入-净额', 0))
                }
    except Exception as e:
        print(f"获取资金流向失败({code}): {e}")
    
    return None


def get_dragon_tiger_list():
    """获取龙虎榜数据"""
    if not AKSHARE_AVAILABLE:
        return None
    
    try:
        today = datetime.now().strftime("%Y%m%d")
        df = call_with_timeout(ak.stock_lhb_detail_em, kwargs={"date": today}, timeout=_AKSHARE_TIMEOUT)
        
        if df is not None and not df.empty:
            result = []
            for _, row in df.iterrows():
                result.append({
                    "code": str(row.get('代码', '')),
                    "name": str(row.get('名称', '')),
                    "price": float(row.get('收盘价', 0)),
                    "change_pct": float(row.get('涨跌幅', 0)),
                    "reason": str(row.get('上榜原因', '')),
                    "net_buy": float(row.get('净买额', 0)),
                    "buy_amount": float(row.get('买入额', 0)),
                    "sell_amount": float(row.get('卖出额', 0))
                })
            
            return result[:20]  # 取前20条
    except Exception as e:
        print(f"获取龙虎榜失败: {e}")
    
    return None


def get_sector_list_akshare():
    """获取板块数据"""
    if not AKSHARE_AVAILABLE:
        return None
    
    try:
        # 获取行业板块
        df = call_with_timeout(ak.stock_board_industry_name_em, timeout=_AKSHARE_TIMEOUT)
        
        if df is not None and not df.empty:
            result = []
            for _, row in df.iterrows():
                result.append({
                    "name": str(row.get('板块名称', '')),
                    "code": str(row.get('板块代码', '')),
                    "change_pct": float(row.get('涨跌幅', 0)),
                    "lead_stock": str(row.get('领涨股票', '')),
                    "net_inflow": float(row.get('主力净流入', 0)) if '主力净流入' in row else 0
                })
            
            return result
    except Exception as e:
        print(f"获取板块数据失败: {e}")
    
    return None


# ====== 技术指标计算 ======
def calculate_ma(data, periods=[5, 10, 20]):
    """计算移动平均线"""
    result = {}
    closes = [d['close'] for d in data]
    
    for period in periods:
        ma_values = []
        for i in range(len(closes)):
            if i < period - 1:
                ma_values.append(None)
            else:
                ma_values.append(round(sum(closes[i-period+1:i+1]) / period, 2))
        result[f'ma{period}'] = ma_values
    
    return result


def calculate_macd(data, fast=12, slow=26, signal=9):
    """计算 MACD 指标"""
    closes = [d['close'] for d in data]
    
    # 计算 EMA
    def ema(prices, period):
        k = 2 / (period + 1)
        ema_values = [prices[0]]
        for i in range(1, len(prices)):
            ema_values.append(prices[i] * k + ema_values[-1] * (1 - k))
        return ema_values
    
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    
    dif = [ema_fast[i] - ema_slow[i] for i in range(len(closes))]
    
    # 计算 DEA（Signal）
    dea = ema(dif, signal)
    
    macd = [(dif[i] - dea[i]) * 2 for i in range(len(closes))]
    
    return {
        'dif': [round(v, 4) for v in dif],
        'dea': [round(v, 4) for v in dea],
        'macd': [round(v, 4) for v in macd]
    }


def calculate_rsi(data, periods=[6, 12]):
    """计算 RSI 指标"""
    closes = [d['close'] for d in data]
    
    def rsi(prices, period):
        rsi_values = [None] * period
        for i in range(period, len(prices)):
            deltas = [prices[j] - prices[j-1] for j in range(i-period+1, i+1)]
            gains = [d if d > 0 else 0 for d in deltas]
            losses = [-d if d < 0 else 0 for d in deltas]
            
            avg_gain = sum(gains) / period
            avg_loss = sum(losses) / period
            
            if avg_loss == 0:
                rsi_values.append(100)
            else:
                rs = avg_gain / avg_loss
                rsi_values.append(100 - 100 / (1 + rs))
        
        return [round(v, 2) if v is not None else None for v in rsi_values]
    
    result = {}
    for period in periods:
        result[f'rsi{period}'] = rsi(closes, period)
    
    return result


def calculate_bollinger_bands(data, period=20, std_dev=2):
    """计算布林带"""
    closes = [d['close'] for d in data]
    
    upper = []
    middle = []
    lower = []
    
    for i in range(len(closes)):
        if i < period - 1:
            upper.append(None)
            middle.append(None)
            lower.append(None)
        else:
            window = closes[i-period+1:i+1]
            ma = sum(window) / period
            std = (sum((x - ma) ** 2 for x in window) / period) ** 0.5
            
            middle.append(round(ma, 2))
            upper.append(round(ma + std_dev * std, 2))
            lower.append(round(ma - std_dev * std, 2))
    
    return {
        'boll_upper': upper,
        'boll_middle': middle,
        'boll_lower': lower
    }


# ====== 模拟数据生成 ======
def generate_demo_realtime(code):
    """生成模拟实时数据"""
    base_prices = {
        "600519": 1680.0, "000858": 145.0, "601318": 48.5, "600036": 32.8,
        "000333": 62.0, "300750": 195.0, "002594": 268.0, "600276": 42.0,
        "688981": 28.5, "000063": 28.0, "000568": 168.0, "600809": 245.0,
        "603288": 38.5, "600887": 26.8, "601166": 18.2, "600030": 22.5,
        "601688": 18.8, "601328": 7.2, "300274": 82.0, "002459": 68.0,
        "600438": 32.5, "000661": 425.0, "300760": 285.0, "603259": 48.0,
        "000538": 52.0, "688012": 158.0, "002049": 95.0, "603501": 68.0,
        "688008": 58.0, "601857": 9.8, "601398": 6.2, "600028": 6.5,
        "601288": 4.8, "601988": 5.2
    }
    
    base_price = base_prices.get(code, 50.0)
    change_pct = random.uniform(-5, 5)
    price = base_price * (1 + change_pct / 100)
    
    return {
        "code": code,
        "name": next((s['name'] for s in load_config()['monitor_stocks'] if s['code'] == code), code),
        "price": round(price, 2),
        "open": round(base_price * (1 + random.uniform(-1, 1) / 100), 2),
        "high": round(price * (1 + random.uniform(0, 1) / 100), 2),
        "low": round(price * (1 - random.uniform(0, 1) / 100), 2),
        "pre_close": round(base_price, 2),
        "change": round(price - base_price, 2),
        "change_pct": round(change_pct, 2),
        "volume": random.randint(100000, 10000000),
        "amount": random.randint(10000000, 1000000000),
        "turnover": round(random.uniform(0.5, 5), 2),
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


def generate_demo_kline(code, count=100):
    """生成模拟 K 线数据"""
    base_prices = {
        "600519": 1680.0, "000858": 145.0, "601318": 48.5, "600036": 32.8,
        "000333": 62.0, "300750": 195.0, "002594": 268.0, "600276": 42.0,
        "688981": 28.5, "000063": 28.0
    }
    
    base_price = base_prices.get(code, 50.0)
    result = []
    
    current_price = base_price * 0.8  # 从80%开始
    for i in range(count):
        date = (datetime.now() - timedelta(days=count-i)).strftime("%Y-%m-%d")
        
        change = random.uniform(-3, 3) / 100
        open_price = current_price
        close_price = current_price * (1 + change)
        high_price = max(open_price, close_price) * (1 + random.uniform(0, 1) / 100)
        low_price = min(open_price, close_price) * (1 - random.uniform(0, 1) / 100)
        
        result.append({
            "date": date,
            "open": round(open_price, 2),
            "close": round(close_price, 2),
            "high": round(high_price, 2),
            "low": round(low_price, 2),
            "volume": random.randint(100000, 10000000),
            "amount": random.randint(10000000, 1000000000),
            "change_pct": round(change * 100, 2)
        })
        
        current_price = close_price
    
    return result


def generate_demo_intraday(code):
    """生成模拟分时数据"""
    base_prices = {
        "600519": 1680.0, "000858": 145.0, "601318": 48.5, "600036": 32.8,
        "000333": 62.0, "300750": 195.0, "002594": 268.0, "600276": 42.0,
        "688981": 28.5, "000063": 28.0
    }
    
    base_price = base_prices.get(code, 50.0)
    result = []
    
    # 生成当天分时数据（9:30-15:00，每分钟一个点）
    current_price = base_price
    start_time = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
    
    for i in range(240):  # 4小时 = 240分钟
        time_str = (start_time + timedelta(minutes=i)).strftime("%H:%M")
        
        if i % 10 == 0:  # 每10分钟更新一次价格
            current_price = current_price * (1 + random.uniform(-0.5, 0.5) / 100)
        
        result.append({
            "time": time_str,
            "price": round(current_price, 2),
            "volume": random.randint(1000, 100000),
            "amount": random.randint(100000, 10000000)
        })
    
    return result


def generate_demo_fund_flow(code):
    """生成模拟资金流向（内部一致）"""
    # 先确定总主力净流入
    main_inflow = random.randint(-100000000, 100000000)
    
    # 按大致比例拆分为超大单、大单、中单、小单
    # 超大单: 40%, 大单: 30%, 中单: 20%, 小单: 10%
    super_large = int(main_inflow * random.uniform(0.35, 0.45))
    large = int(main_inflow * random.uniform(0.25, 0.35))
    medium = int(main_inflow * random.uniform(0.15, 0.25))
    small = main_inflow - super_large - large - medium  # 确保合计精确相等
    
    return {
        "code": code,
        "name": next((s['name'] for s in load_config()['monitor_stocks'] if s['code'] == code), code),
        "main_inflow": main_inflow,
        "main_inflow_pct": round(main_inflow / 1e8 * random.uniform(0.5, 1.5), 2),
        "super_large_inflow": super_large,
        "large_inflow": large,
        "medium_inflow": medium,
        "small_inflow": small
    }


def generate_demo_dragon_tiger():
    """生成模拟龙虎榜"""
    stocks = [
        ("600519", "贵州茅台"), ("000858", "五粮液"), ("300750", "宁德时代"),
        ("002594", "比亚迪"), ("600276", "恒瑞医药"), ("000063", "中兴通讯"),
        ("600809", "山西汾酒"), ("000568", "泸州老窖"), ("603259", "药明康德"),
        ("002415", "海康威视")
    ]
    
    result = []
    for code, name in random.sample(stocks, min(8, len(stocks))):
        result.append({
            "code": code,
            "name": name,
            "price": round(random.uniform(20, 300), 2),
            "change_pct": round(random.uniform(-10, 10), 2),
            "reason": random.choice(["日涨幅偏离值达7%", "日换手率达20%", "连续3个交易日内涨幅偏离值累计达20%"]),
            "net_buy": random.randint(-100000000, 100000000),
            "buy_amount": random.randint(100000000, 1000000000),
            "sell_amount": random.randint(100000000, 1000000000)
        })
    
    return result


def get_custom_sector_summary():
    """根据配置的板块和实时数据计算板块汇总"""
    config = load_config()

    # 按板块分组
    sector_stocks = {}
    for s in config.get('monitor_stocks', []):
        sec = s.get('sector', '其他')
        if sec not in sector_stocks:
            sector_stocks[sec] = []
        sector_stocks[sec].append(s)

    # 关键修复：如果实时缓存为空，主动批量拉取一次，确保板块数据可用
    with _realtime_cache_lock:
        cache_size = len(_realtime_cache)
    if cache_size == 0:
        all_codes = [s.get('code', '') for s in config.get('monitor_stocks', []) if s.get('enabled', True)]
        try:
            get_batch_realtime_sina(all_codes)
        except Exception as e:
            print(f"板块汇总预拉取失败: {e}")

    result = []
    for sector_name, stocks in sector_stocks.items():
        # 获取该板块各股票的实时数据
        changes = []
        lead_stock_name = stocks[0].get('name', '-') if stocks else '-'
        max_change = -9999

        for stock in stocks:
            code = stock.get('code', '')

            # 检查缓存中的实时数据
            with _realtime_cache_lock:
                cached = _realtime_cache.get(code)
                if cached:
                    data, _ = cached
                    change_pct = data.get('change_pct', 0)
                    name = data.get('name', stock.get('name', ''))
                    changes.append(change_pct)

                    # 找领涨股（涨幅最大）
                    if change_pct > max_change:
                        max_change = change_pct
                        lead_stock_name = name

        # 计算板块平均涨跌幅
        if changes:
            avg_change = round(sum(changes) / len(changes), 2)
        else:
            # 缓存都没有时，不再用随机数，而是标记为 0 并提示无数据
            avg_change = 0.0

        # 净流入基于涨跌幅估算（涨幅为正则净流入为正）
        net_inflow = avg_change * 1e8 if avg_change != 0 else 0

        result.append({
            "name": sector_name,
            "code": "",
            "change_pct": avg_change,
            "lead_stock": lead_stock_name,
            "net_inflow": net_inflow,
            "stock_count": len(stocks)
        })

    # 按涨跌幅排序
    result.sort(key=lambda x: x['change_pct'], reverse=True)
    return result


def generate_demo_sector_list():
    """生成模拟板块数据（已废弃，保留兼容）"""
    return get_custom_sector_summary()


# ====== Flask 路由 ======

@app.route('/')
def index():
    """主页面"""
    return render_template('index.html')


@app.route('/api/status')
def api_status():
    """API 状态"""
    return jsonify({
        "status": "ok",
        "akshare_available": AKSHARE_AVAILABLE,
        "sina_api": True,
        "data_mode": "real",
        "data_source": "sina + akshare",
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })


def get_batch_realtime_sina(codes):
    """批量获取多只股票的实时数据（新浪API，一次请求）"""
    try:
        # 构造新浪代码列表
        sina_codes = []
        for code in codes:
            code = format_stock_code(code)
            prefix = 'sh' if code.startswith('6') else 'sz'
            sina_codes.append(f"{prefix}{code}")
        
        url = f"http://hq.sinajs.cn/list={','.join(sina_codes)}"
        req = urllib.request.Request(url, headers={
            'Referer': 'http://finance.sina.com.cn',
            'User-Agent': 'Mozilla/5.0'
        })
        
        with urllib.request.urlopen(req, timeout=8) as resp:
            content = resp.read().decode('gbk', errors='ignore')
        
        results = []
        lines = content.strip().split('\n')
        for i, line in enumerate(lines):
            if i >= len(codes):
                break
            
            code = format_stock_code(codes[i])
            if '=' in line and '"' in line:
                data_str = line.split('"')[1]
                if not data_str:
                    continue
                
                fields = data_str.split(',')
                if len(fields) < 10:
                    continue
                
                name = fields[0]
                open_price = float(fields[1]) if fields[1] else 0
                pre_close = float(fields[2]) if fields[2] else 0
                current_price = float(fields[3]) if fields[3] else 0
                high = float(fields[4]) if fields[4] else 0
                low = float(fields[5]) if fields[5] else 0
                volume = int(float(fields[8])) if fields[8] else 0
                amount = float(fields[9]) if fields[9] else 0
                
                if current_price == 0:
                    current_price = pre_close
                
                change = round(current_price - pre_close, 2)
                change_pct = round((change / pre_close * 100), 2) if pre_close > 0 else 0
                
                results.append({
                    "code": code,
                    "name": name,
                    "price": current_price,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "pre_close": pre_close,
                    "change": change,
                    "change_pct": change_pct,
                    "volume": volume,
                    "amount": amount,
                    "turnover": 0,
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                _stock_name_cache[code] = name

                # 关键修复：同步写入实时缓存，供板块汇总等接口读取
                with _realtime_cache_lock:
                    _realtime_cache[code] = ({
                        "code": code,
                        "name": name,
                        "price": current_price,
                        "change_pct": change_pct,
                        "change": change,
                        "amount": amount,
                        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }, time.time())
        
        return results
    except Exception as e:
        print(f"新浪批量API失败: {e}")
        return []


@app.route('/api/stocks/realtime')
def api_all_realtime():
    """批量获取所有监控股票的实时数据"""
    config = load_config()
    codes = [s['code'] for s in config['monitor_stocks'] if s.get('enabled', True)]
    
    # 优先使用新浪批量API
    results = get_batch_realtime_sina(codes)
    if results:
        return jsonify({"data": results, "source": "sina"})
    
    # 备用：逐个获取
    results = []
    for code in codes:
        data = get_realtime_price_sina(code)
        if data:
            results.append(data)
    
    if results:
        return jsonify({"data": results, "source": "sina"})
    
    # 最终备用：模拟数据
    results = [generate_demo_realtime(code) for code in codes]
    return jsonify({"data": results, "source": "demo"})


@app.route('/api/config')
def api_get_config():
    """获取配置"""
    return jsonify(load_config())


@app.route('/api/config', methods=['POST'])
def api_update_config():
    """更新配置"""
    config = request.json
    save_config(config)
    return jsonify({"status": "success", "message": "配置已更新"})


@app.route('/api/stock/realtime/<code>')
def api_realtime(code):
    """获取实时行情"""
    code = format_stock_code(code)
    
    # 优先使用新浪API（快速、轻量）
    data = get_realtime_price_sina(code)
    if data:
        return jsonify(data)
    
    # 备用：AKShare（带超时）
    if AKSHARE_AVAILABLE:
        data = get_realtime_price_akshare(code)
        if data:
            return jsonify(data)
    
    # 最终备用：模拟数据
    return jsonify(generate_demo_realtime(code))


@app.route('/api/stock/kline/<code>')
def api_kline(code):
    """获取 K 线数据"""
    code = format_stock_code(code)
    period = request.args.get('period', 'daily')
    count = int(request.args.get('count', 100))
    
    source = "demo"
    data = None
    if AKSHARE_AVAILABLE:
        data = get_kline_data_akshare(code, period, count)
        if data:
            source = "akshare"
    
    if not data:
        data = generate_demo_kline(code, count)
    
    # 从配置中查找股票名称
    config = load_config()
    name = next((s['name'] for s in config['monitor_stocks'] if s['code'] == code), code)
    
    # 计算技术指标
    indicators = {}
    indicators.update(calculate_ma(data))
    indicators.update(calculate_macd(data))
    indicators.update(calculate_rsi(data))
    indicators.update(calculate_bollinger_bands(data))
    
    return jsonify({
        "code": code,
        "name": name,
        "source": source,
        "period": period,
        "data": data,
        "indicators": indicators
    })


@app.route('/api/stock/intraday/<code>')
def api_intraday(code):
    """获取分时数据"""
    code = format_stock_code(code)
    
    source = "demo"
    data = None
    if AKSHARE_AVAILABLE:
        data = get_intraday_data_akshare(code)
        if data:
            source = "akshare"
    
    if not data:
        data = generate_demo_intraday(code)
    
    config = load_config()
    name = next((s['name'] for s in config['monitor_stocks'] if s['code'] == code), code)
    
    return jsonify({
        "code": code,
        "name": name,
        "source": source,
        "data": data
    })


@app.route('/api/stock/fundflow/<code>')
def api_fundflow(code):
    """获取资金流向"""
    code = format_stock_code(code)
    
    source = "demo"
    data = None
    if AKSHARE_AVAILABLE:
        data = get_fund_flow_akshare(code)
        if data:
            source = "akshare"
    
    if not data:
        data = generate_demo_fund_flow(code)
    
    # 生成历史数据（最近10天，内部一致）
    history = []
    for i in range(10):
        date = (datetime.now() - timedelta(days=10-i)).strftime("%Y-%m-%d")
        mi = random.randint(-100000000, 100000000)
        sl = int(mi * random.uniform(0.35, 0.45))
        lg = int(mi * random.uniform(0.25, 0.35))
        md = int(mi * random.uniform(0.15, 0.25))
        sm = mi - sl - lg - md
        history.append({
            "date": date,
            "main_inflow": mi,
            "super_large": sl,
            "large": lg,
            "medium": md,
            "small": sm
        })
    
    data['history'] = history
    data['source'] = source
    
    return jsonify(data)


@app.route('/api/dragontiger')
def api_dragon_tiger():
    """获取龙虎榜"""
    source = "demo"
    data = None
    if AKSHARE_AVAILABLE:
        data = get_dragon_tiger_list()
        if data:
            source = "akshare"
    
    if not data:
        data = generate_demo_dragon_tiger()
    
    # 给每条记录添加数据源标记
    for item in data:
        item['_source'] = source
    
    return jsonify(data)


@app.route('/api/sectors')
def api_sectors():
    """获取板块数据 - 始终使用自定义板块"""
    # 强制只使用自定义板块汇总
    data = get_custom_sector_summary()

    # 如果自定义板块为空（不应该发生），返回空列表而非AKShare数据
    if not data:
        data = []
    
    # 标记数据源
    for item in data:
        item['_source'] = 'custom'

    return jsonify(data)


@app.route('/api/stocks/add', methods=['POST'])
def api_add_stock():
    """添加股票"""
    data = request.json
    code = format_stock_code(data.get('code', ''))
    name = data.get('name', '')
    sector = data.get('sector', '其他')
    
    if not code:
        return jsonify({"status": "error", "message": "股票代码不能为空"})
    
    config = load_config()
    
    # 检查是否已存在
    if any(s['code'] == code for s in config['monitor_stocks']):
        return jsonify({"status": "error", "message": f"股票 {code} 已在列表中"})
    
    # 获取股票名称
    if not name and AKSHARE_AVAILABLE:
        try:
            info = ak.stock_individual_info_em(symbol=code)
            if info is not None:
                for _, row in info.iterrows():
                    if row['item'] == '股票简称':
                        name = row['value']
                        break
        except:
            pass
    
    if not name:
        name = code
    
    config['monitor_stocks'].append({
        "code": code,
        "name": name,
        "sector": sector,
        "enabled": True
    })
    
    save_config(config)
    
    return jsonify({"status": "success", "message": f"已添加 {name}({code})"})


@app.route('/api/stocks/remove', methods=['POST'])
def api_remove_stock():
    """移除股票"""
    data = request.json
    code = format_stock_code(data.get('code', ''))
    
    config = load_config()
    config['monitor_stocks'] = [s for s in config['monitor_stocks'] if s['code'] != code]
    save_config(config)
    
    return jsonify({"status": "success", "message": f"已移除 {code}"})


@app.route('/api/stocks/toggle', methods=['POST'])
def api_toggle_stock():
    """启用/禁用股票"""
    data = request.json
    code = format_stock_code(data.get('code', ''))
    enabled = data.get('enabled', True)
    
    config = load_config()
    for stock in config['monitor_stocks']:
        if stock['code'] == code:
            stock['enabled'] = enabled
            break
    
    save_config(config)
    
    return jsonify({"status": "success"})


@app.route('/api/alerts')
def api_alerts():
    """获取告警历史"""
    return jsonify(alerts_history[-50:])


@app.route('/api/alerts/add', methods=['POST'])
def api_add_alert():
    """添加告警（模拟大单检测）"""
    global alerts_history, last_alert_time
    
    data = request.json
    code = format_stock_code(data.get('code', ''))
    
    # 检查冷却时间
    now = time.time()
    if code in last_alert_time:
        if now - last_alert_time[code] < 30:  # 30秒冷却
            return jsonify({"status": "cooldown"})
    
    last_alert_time[code] = now
    
    # 生成告警
    config = load_config()
    stock_info = next((s for s in config['monitor_stocks'] if s['code'] == code), None)
    stock_name = stock_info['name'] if stock_info else code
    
    alert = {
        "id": len(alerts_history) + 1,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "code": code,
        "name": stock_name,
        "type": random.choice(["大单买入", "超大单买入", "主力资金流入"]),
        "amount": random.randint(200000, 5000000),
        "price": data.get('price', 0),
        "change_pct": data.get('change_pct', 0)
    }
    
    alerts_history.append(alert)
    if len(alerts_history) > 100:
        alerts_history.pop(0)
    
    return jsonify({"status": "success", "alert": alert})


@app.route('/api/backtest')
def api_backtest():
    """策略回测（模拟数据）"""
    # 模拟回测结果
    dates = []
    strategy_returns = []
    benchmark_returns = []
    
    base_strategy = 1.0
    base_benchmark = 1.0
    
    for i in range(100):
        date = (datetime.now() - timedelta(days=100-i)).strftime("%Y-%m-%d")
        base_strategy *= (1 + random.uniform(-0.02, 0.03))
        base_benchmark *= (1 + random.uniform(-0.015, 0.02))
        
        dates.append(date)
        strategy_returns.append(round((base_strategy - 1) * 100, 2))
        benchmark_returns.append(round((base_benchmark - 1) * 100, 2))
    
    return jsonify({
        "_source": "demo",
        "dates": dates,
        "strategy_returns": strategy_returns,
        "benchmark_returns": benchmark_returns,
        "metrics": {
            "total_return": round((base_strategy - 1) * 100, 2),
            "annual_return": round((base_strategy ** (252/100) - 1) * 100, 2),
            "max_drawdown": round(random.uniform(-15, -5), 2),
            "win_rate": round(random.uniform(50, 65), 2),
            "trade_count": random.randint(30, 60)
        }
    })


# ====== 主函数 ======
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8888))
    
    print("=" * 60)
    print("  A股大单监控分析平台")
    print(f"  数据模式: {'真实数据(AKShare)' if AKSHARE_AVAILABLE else '模拟数据'}")
    print(f"  访问地址: http://0.0.0.0:{port}")
    print("=" * 60)
    
    # 确保配置存在
    load_config()
    
    app.run(host='0.0.0.0', port=port, debug=False)
