import ccxt

mexc = getattr(ccxt, 'mexc')()
print(mexc.fetch_ticker(symbol = "TRAVA/USDT"))