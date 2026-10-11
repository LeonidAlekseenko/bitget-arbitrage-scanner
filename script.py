import asyncio
import json
import sys
import websockets
import os
import csv
from datetime import datetime


BITGET_WS_URL = "wss://ws.bitget.com/v2/ws/public"


# Настройки комиссий Bitget (Taker)
SPOT_TAKER_FEE = 0.0010      # 0.10%
FUTURES_TAKER_FEE = 0.0006   # 0.06%
# Итоговая комиссия за полный цикл (открытие + закрытие на обоих рынках)
TOTAL_CYCLE_FEE_PCT = (SPOT_TAKER_FEE * 2 + FUTURES_TAKER_FEE * 2) * 100 # 0.32%

CSV_FILE = "arbitrage_opportunities.csv"

# Имена ключей в ВЕРХНЕМ регистре (требование Bitget API v2)
prices = {
    "SPOT": {"bid": 0.0, "ask": 0.0},
    "USDT-FUTURES": {"bid": 0.0, "ask": 0.0}
}

# Инициализация CSV файла при старте скрипта
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Timestamp", "Type", "Spot_Bid", "Spot_Ask", "Fut_Bid", "Fut_Ask", "Dirty_Spread_%", "Net_Profit_%"])

async def send_ping(ws):
    """Каждые 30 секунд отправляет ping для удержания соединения"""
    while True:
        await asyncio.sleep(30)
        try:
            await ws.send("ping")
        except websockets.exceptions.ConnectionClosed:
            break

def log_to_csv(strategy_type, spot_bid, spot_ask, fut_bid, fut_ask, dirty_pct, net_pct):
    """Запись прибыльного арбитражного окна в файл"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    with open(CSV_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, strategy_type, spot_bid, spot_ask, fut_bid, fut_ask, f"{dirty_pct:.4f}%", f"{net_pct:.4f}%"])

def calculate_spread():
    """Математический расчет арбитражного окна с вычетом комиссий"""
    spot_ask = prices["SPOT"]["ask"]
    spot_bid = prices["SPOT"]["bid"]
    fut_ask = prices["USDT-FUTURES"]["ask"]
    fut_bid = prices["USDT-FUTURES"]["bid"]

    if 0.0 in [spot_ask, spot_bid, fut_ask, fut_bid]:
        return

    # 1. Контанго (Фьючерс дороже Спота): Купить Спот (Ask) -> Шорт Фьючерс (Bid)
    dirty_contango = ((fut_bid - spot_ask) / spot_ask) * 100
    net_contango = dirty_contango - TOTAL_CYCLE_FEE_PCT

    # 2. Бэквордация (Спот дороже Фьючерса): Купить Фьючерс (Ask) -> Селл Спот (Bid)
    dirty_backwardation = ((spot_bid - fut_ask) / fut_ask) * 100
    net_backwardation = dirty_backwardation - TOTAL_CYCLE_FEE_PCT

    # Базовая строка лога цен для вывода в консоль
    log_line = f"SPOT: {spot_bid:.1f}/{spot_ask:.1f} | FUT: {fut_bid:.1f}/{fut_ask:.1f} | "

    # Проверяем реальную прибыльность с учетом комиссий
    if net_contango > 0:
        log_line += f"📈 ЧИСТЫЙ АРБИТРАЖ (Контанго): +{net_contango:.4f}% (Грязный: {dirty_contango:.4f}%)"
        print(log_line)
        log_to_csv("Contango", spot_bid, spot_ask, fut_bid, fut_ask, dirty_contango, net_contango)
        
    elif net_backwardation > 0:
        log_line += f"📉 ЧИСТЫЙ АРБИТРАЖ (Бэквордация): +{net_backwardation:.4f}% (Грязный: {dirty_backwardation:.4f}%)"
        print(log_line)
        log_to_csv("Backwardation", spot_bid, spot_ask, fut_bid, fut_ask, dirty_backwardation, net_backwardation)
        
    else:
        # Если чистой прибыли нет, выводим текущее состояние в консоль (но в файл не пишем)
        max_spread = max(dirty_contango, dirty_backwardation)
        log_line += f"⏸ Окон нет. Лучший грязный спред: {max_spread:.4f}% | Чистый: {max_spread - TOTAL_CYCLE_FEE_PCT:.4f}% (Порог: {TOTAL_CYCLE_FEE_PCT}%)"
        print(log_line)

    sys.stdout.flush()

async def get_arbitrage_spread():
    try:
        async with websockets.connect(BITGET_WS_URL, open_timeout=10) as ws:
            print("✅ 1. Соединение с Bitget WS установлено.")
            asyncio.create_task(send_ping(ws))

            subscription_message = {
                "op": "subscribe",
                "args": [
                    {"instType": "SPOT", "channel": "ticker", "instId": "BTCUSDT"},
                    {"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"}
                ]
            }

            await ws.send(json.dumps(subscription_message))
            print("✅ 2. Запрос на подписку отправлен. Порог чистой прибыли: > 0.32%\n" + "-"*80)

            while True:
                response = await ws.recv()
                if response == "pong":
                    continue

                data = json.loads(response)
                
                if data.get("event") == "subscribe":
                    print(f"🔥 Успешная подписка: {data['arg'].get('instType')}:{data['arg'].get('channel')}")
                    continue
                
                if "data" in data and "arg" in data:
                    inst_type = data["arg"].get("instType")
                    
                    for ticker_info in data["data"]:
                        bid_price = float(ticker_info.get("bidPr", 0))
                        ask_price = float(ticker_info.get("askPr", 0))
                        
                        if bid_price > 0 and ask_price > 0:
                            prices[inst_type]["bid"] = bid_price
                            prices[inst_type]["ask"] = ask_price
                            calculate_spread()

    except Exception as e:
        print(f"❌ Критическая ошибка в работе бота: {e}")

if __name__ == "__main__":
    if sys.platform == "darwin":
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
        
    asyncio.run(get_arbitrage_spread())
