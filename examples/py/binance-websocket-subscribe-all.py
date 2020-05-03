import os
import sys
import pprint
import traceback

pp = pprint.PrettyPrinter(depth=6)

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')

import ccxt.async_support as async_ccxt  # noqa: E402
import asyncio  # noqa: E402

loop = asyncio.get_event_loop()


async def main(exchange, symbols, eventSymbols):
    @exchange.on('err')
    def websocket_error(err, conxid):  # pylint: disable=W0612
        print(type(err).__name__ + ":" + str(err))
        traceback.print_tb(err.__traceback__)
        traceback.print_stack()
        loop.stop()

    @exchange.on('ob')
    def websocket_ob(symbol, ob):  # pylint: disable=W0612
        print("ob received from: " + symbol)
        sys.stdout.flush()

    sys.stdout.flush()

    print("subscribe: " + ','.join(symbols))
    sys.stdout.flush()
    await exchange.websocket_subscribe_all(eventSymbols)
    print("subscribed: " + ','.join(symbols))
    sys.stdout.flush()
    await asyncio.sleep(10)
    print("unsubscribe: " + ','.join(symbols[len(eventSymbols) // 2 - 1: - 1]))
    sys.stdout.flush()
    await exchange.websocket_unsubscribe_all(eventSymbols[len(eventSymbols) // 2 - 1: - 1])
    print("unsubscribed: " + ','.join(symbols[len(eventSymbols) // 2 - 1: - 1]))
    await asyncio.sleep(10)
    print("unsubscribe: " + ','.join(symbols))
    sys.stdout.flush()
    await exchange.websocket_unsubscribe_all(eventSymbols)
    print("unsubscribed: " + ','.join(symbols))
    await asyncio.sleep(2)
    await exchange.close()


name = "binance"
ex_config = {
    'apiKey': 'key',
    'secret': 'secret',
    'password': 'password',
    'uid': '',
    'verbose': 2,
}
exch = getattr(async_ccxt, name.lower())(ex_config)
symbols = ["BTC/USDT"]
eventSymbols = [{
    "event": "ob",
    "symbol": symbols[0],
    "params": {
        'limit': 20
    }
}]
loop.run_until_complete(main(exch, symbols, eventSymbols))
