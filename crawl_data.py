import threading
import time
import ccxt
import os

from dotenv import load_dotenv

from mongodb import MongoDB
load_dotenv()
# Load from env file
exchanges = os.environ.get("EXCHANGES").split(',')
symbols = os.environ.get("SYMBOLS").split(',')
time_period = int(os.environ.get("TIMEPERIOD"))
# Create ccxt of exchange
exchange_ccxt = {}
for item in exchanges:
  exchange_ccxt[item] = getattr(ccxt, item)()
mongoDb = MongoDB()

def standardize_trades(trades):
    results = []
    for _trade in trades:
        _timestamp = _trade["timestamp"]
        _price = _trade["price"]
        _amount = _trade["amount"]
        _side = _trade["side"]
        _id = str(_timestamp) + "-" + str(_side) + "-" + str(_amount) + "-" + str(_price)
        result = {
            "_id": _id,
            "timestamp": _timestamp,
            "amount": _amount,
            "side": _side,
            "price": _price
        }
        results.append(result)
    return results

def get_data(exchange_name, exchange, symbols, timestamp):
  for symbol in symbols:
    database_name = f"{exchange_name}_{symbol}".replace('/','')
    order_book = exchange.fetch_order_book(symbol=symbol)
    import_history = {
      "_id": str(timestamp),
      "price": (order_book["bids"][0][0] + order_book["asks"][0][0])/2,
      "bids": order_book["bids"],
      "asks": order_book["asks"]
    }
    mongoDb.update_data_list(database=database_name,
                             collection="history",
                             data=[import_history])
    
    trades = exchange.fetch_trades(symbol=symbol)
    import_trades = standardize_trades(trades)
    mongoDb.update_data_list(database=database_name,
                             collection="trades",
                             data=import_trades)
    

def run_threads(exchanges :dict, symbols, timestamp):
    threads = []
    for exchange_name, exchange in exchanges.items():
      thread = threading.Thread(target=get_data, args=(exchange_name,exchange,symbols, timestamp))
      threads.append(thread)
      thread.start()

    for thread in threads:
        thread.join()

while 1:
  time_sleep = time_period
  time_start = int(time.time())
  time_round = int(time_start/time_period) * time_period
  try:
    run_threads(exchanges=exchange_ccxt, symbols=symbols, timestamp=time_round)
  except Exception as e:
    print(f"error {e}")
  time_now = int(time.time())
  time_next_run = (int(time_now/time_period) + 1) * time_period
  time_sleep = time_next_run - time_now
  if time_sleep > 0:
    print(f"Sleep {time_sleep} seconds until next run")
    time.sleep(time_sleep)
