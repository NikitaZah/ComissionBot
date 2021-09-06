import queue
import threading

from binance import Client, ThreadedWebsocketManager
from binance.exceptions import BinanceAPIException
from configparser import ConfigParser
import data
from multiprocessing import Process, ProcessError,  Queue
from threading import Thread
from tqdm import tqdm
import pandas as pd

config = ConfigParser()
config.read_file(open('config.cfg'))
api_key = config.get('BINANCE', 'API_KEY')
api_secret = config.get('BINANCE', 'API_SECRET')

client = Client(api_key, api_secret)

curr_percent = 0.125


def get_volume(symbol: str, minutes: int):
    try:
        candles = client.futures_klines(symbol=symbol, interval='1m', limit=minutes)
    except BinanceAPIException as err:
        print(f'error during getting candles for {symbol}:\n{err.message}')
        return -1
    volume = 0
    for candle in candles:
        volume += float(candle[5])  # candle volume
    return volume


def get_average_length(symbol: str, interval: str, amount: int):
    if amount < 2:
        return -1
    try:
        candles = client.futures_klines(symbol=symbol, interval=interval, limit=amount)
    except BinanceAPIException as err:
        print(f'error during getting candles for {symbol}:\n{err.message}')
        return -2
    length = 0
    for candle in candles:
        length += float(candle[2]) - float(candle[3])   # high - low
    length -= (float(candles[-1][2]) - float(candles[-1][3]))
    average_length = length / (len(candles) - 1)
    return average_length


def get_large_limit(symbol: str, volume: float, percentage: float):
    try:
        order_book = client.futures_order_book(symbol=symbol, limit=50)
    except BinanceAPIException as err:
        print(f'error during getting order book for {symbol}:\n{err.message}')
        return (-1, -1), (-1, -1)

    ask, bid = (0, 0), (0, 0)
    for order in order_book['bids']:
        if float(order[1]) > volume * percentage:   # QTY > V * %
            bid = float(order[0]), float(order[1])
            break
    for order in order_book['asks']:
        if float(order[1]) > volume * percentage:
            ask = float(order[0]), float(order[1])
            break
    return bid, ask


def extract_pairs(q: Queue, wasted_pairs: Queue):
    pairs = data.pairs
    pairs_in_work = []
    for pair in pairs:
        leverage = client.futures_change_leverage(symbol=pair, leverage=10)
    while True:
        for pair in tqdm(pairs, desc=f'checking'):
            if pair in pairs_in_work:
                continue
            volume = get_volume(pair, minutes=10)
            if volume <= 0:
                continue

            limits = get_large_limit(pair, volume, percentage=curr_percent)  # (bid, ask)

            if limits[0][0] <= 0 and limits[1][0] <= 0:
                continue

            last_trade = (client.futures_recent_trades(symbol=pair, limit=1))[0]
            actual_price = float(last_trade['price'])

            if abs(limits[0][0] - actual_price) > abs(limits[1][0] - actual_price):   # выбираем ближайшую лимитку
                offer_kind = 'ask'
                offer_price, offer_volume = limits[1]
            else:
                offer_kind = 'bid'
                offer_price, offer_volume = limits[0]

            average_length = get_average_length(pair, '5m', 3)
            if average_length < 0:
                continue

            if abs(actual_price - offer_price) > 0.5 * average_length:  # оффер дальше чем половина средней длины свечи
                continue

            print(f'\n{pair} is interesting:\n{offer_kind}, price: {offer_price}\nactual price: {actual_price}\n'
                  f'volume = {volume}\noffer volume = {offer_volume}\naverage len = {average_length}')
            q.put([pair, offer_kind, offer_price])
            pairs_in_work.append(pair)
        while True:
            if wasted_pairs.empty():
                break
            else:
                wasted_pair = wasted_pairs.get()
                pairs_in_work.remove(wasted_pair)


def abuse_pairs(q: Queue, wasted_pairs: Queue):
    while True:
        try:
            symbol, offer_kind, offer_price = q.get()
            print(f'abuse pairs got {symbol}\n')

            t = Thread(target=abuse_pair, args=(symbol, offer_kind, offer_price, wasted_pairs), daemon=True)
            t.start()
            print(f'currently active threads: {threading.active_count()}\n')
        except:
            raise


def abuse_pair(pair: str, offer_kind: str, offer_price: float, wasted_pairs: Queue):
    volume = get_volume(pair, 10)
    if volume <= 0:
        wasted_pairs.put(pair)
        return 0
    refresh_time = client.get_server_time()['serverTime']

    twm = ThreadedWebsocketManager(api_key, api_secret)
    twm.start()
    order_book_socket = queue.LifoQueue()
    trades_socket = queue.Queue()

    def socket_handler(msg):
        msg = msg['data']
        if msg['e'] == 'depthUpdate':
            order_book = {'bids': msg['b'], 'asks': msg['a']}
            order_book_socket.put(order_book)

        elif msg['e'] == 'aggTrade':
            trades_socket.put(item=[float(msg['p']), msg['E'], float(msg['q'])])
        else:
            print(f'unknown message type: {msg["e"]}')

    streams = [pair.lower()+'@aggTrade', pair.lower()+'@depth20@100ms']
    stream_name = twm.start_futures_multiplex_socket(callback=socket_handler, streams=streams)
    print(f'\nabuse pair started socket for {pair}\ntrade stream = {stream_name}\n')
    waited = waiting_for_touch(pair, offer_kind, offer_price, volume, refresh_time, order_book_socket, trades_socket)
    if not waited:
        print(f'\n{pair}: failed to wait the touch\n')
        twm.stop()
        wasted_pairs.put(pair)
        return -1
    print(f'\n{pair}: we got touch')
    waited = waiting_limit_destruction(offer_kind, offer_price, order_book_socket, trades_socket)
    if not waited:
        print(f'\n{pair}: failed to wait limit destruction\n')
        twm.stop()
        wasted_pairs.put(pair)
        return -2
    print(f'{pair}: destruction waited!\ntrades queue size before starting track: {trades_socket.qsize()}')
    track(pair, offer_kind, offer_price, trades_socket.qsize(), trades_socket)
    twm.stop()
    wasted_pairs.put(pair)
    print(f'abuse pair completed tracking for {pair} successfully')
    return 1


def waiting_for_touch(symbol: str, offer_kind: str, offer_price: float, volume: float, refresh_time: int,
                      order_book_socket: queue.LifoQueue, trades_socket: queue.Queue) -> bool:
    while True:
        conditions = False
        try:
            order_book = order_book_socket.get()
            if offer_kind == 'bid':
                for bid in order_book['bids']:
                    if offer_price == float(bid[0]):
                        if float(bid[1]) < curr_percent * volume:
                            return conditions
                        conditions = True
                        break
            else:
                for ask in order_book['asks']:
                    if offer_price == float(ask[0]):
                        if float(ask[1]) < curr_percent * volume:
                            return conditions
                        conditions = True
                        break
        except queue.Empty:
            conditions = True
            print('order book queue is empty!')
        if not conditions:
            return conditions
        try:
            trade = trades_socket.get()
            if trade[0] == offer_price:
                return True
            time_end = trade[1]
            if time_end-refresh_time > 30000000:
                volume = get_volume(symbol, 10)
                refresh_time = time_end
        except queue.Empty:
            print('trades queue is empty!')


def waiting_limit_destruction(offer_kind: str, offer_price: float, order_book_socket: queue.LifoQueue,
                              trades_socket: queue.Queue) -> bool:
    truly_consumed_volume = 0
    offer_volume = -1
    while True:
        limit_disappeared = True
        try:
            order_book = order_book_socket.get()
            if offer_kind == 'bid':
                for bid in order_book['bids']:
                    if offer_price == float(bid[0]):
                        if offer_volume == -1:
                            offer_volume = float(bid[1])
                        if float(bid[1]) < 0.2 * offer_volume:
                            if truly_consumed_volume > 0.7 * offer_volume:
                                return True
                            else:
                                print(f'ATTENTION PLEASE!!!\ntruly consumed volume = {truly_consumed_volume}\n'
                                      f'offer volume = {offer_volume}\norder book:\n {order_book}\n '
                                      f'offer price = {offer_price}')
                                break
                        limit_disappeared = False
                        break
            else:
                for ask in order_book['asks']:
                    if offer_price == float(ask[0]):
                        if offer_volume == -1:
                            offer_volume = float(ask[1])
                        if float(ask[1]) < 0.2 * offer_volume:
                            if truly_consumed_volume > 0.6 * offer_volume:
                                return True
                            else:
                                print(f'ATTENTION PLEASE!!!\ntruly consumed volume = {truly_consumed_volume}\n'
                                      f'offer volume = {offer_volume}\norder book:\n {order_book}\n '
                                      f'offer price = {offer_price}')
                                break
                        limit_disappeared = False
                        break
        except queue.Empty:
            print('order_book queue is empty during limit destruction')
            limit_disappeared = False
        if limit_disappeared:
            print('limit disappeared!')
            return False
        try:
            trade = trades_socket.get()
            if trade[0] == offer_price:
                truly_consumed_volume += trade[2]
        except queue.Empty:
            print('trades queue is empty during limit destruction')


def track(symbol: str, offer_kind: str, offer_price: float, trades_lost_num: int, trades_socket: queue.Queue):
    prices = []

    if offer_kind == 'ask':
        TP = offer_price + 0.005 * offer_price
        SL = offer_price - 0.001 * offer_price

        # try:
        #
        #     order = client.futures_create_order(symbol=symbol, side=Client.SIDE_BUY, type=Client.FUTURE_ORDER_TYPE_MARKET,
        #                                         quoteOrderQty=30)
        #     order_TP = client.futures_create_order(symbol=symbol, side=Client.SIDE_SELL, price=TP, stopPrice=SL,
        #                                            type=Client.FUTURE_ORDER_TYPE_TAKE_PROFIT, closePosition=True)
        #     order_SL = client.futures_create_order(symbol=symbol, side=Client.SIDE_SELL, price=SL, stopPrice=TP,
        #                                            type=Client.FUTURE_ORDER_TYPE_STOP, closePosition=True)
        # except Exception as err:
        #     print(f'cannot place orders. Error: {err}\n')

        def take_profit(trade_price: float):
            return trade_price > TP

        def stop_loss(trade_price: float):
            return trade_price < SL
    else:
        TP = offer_price - 0.005 * offer_price
        SL = offer_price + 0.001 * offer_price

        # try:
        #     order = client.futures_create_order(symbol=symbol, side=Client.SIDE_SELL, type=Client.FUTURE_ORDER_TYPE_MARKET,
        #                                         quoteOrderQty=30)
        #     order_TP = client.futures_create_order(symbol=symbol, side=Client.SIDE_BUY, price=TP, stopPrice=SL,
        #                                            type=Client.FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET, closePosition=True)
        #     order_SL = client.futures_create_order(symbol=symbol, side=Client.SIDE_BUY, price=SL, stopPrice=TP,
        #                                            type=Client.FUTURE_ORDER_TYPE_STOP_MARKET, closePosition=True)
        # except Exception as err:
        #     print(f'cannot place orders. Error: {err}\n')

        def take_profit(trade_price: float):
            return trade_price < TP

        def stop_loss(trade_price: float):
            return trade_price > SL
    while True:
        try:
            trade = trades_socket.get()
            prices.append(trade[0])
            if take_profit(trade[0]):
                result = 'TP'
                break
            if stop_loss(trade[0]):
                result = 'SL'
                break
        except queue.Empty:
            print('trades queue is empty during tracking')
            continue
    filename = 'data/' + symbol + '.csv'
    notation = [offer_kind, offer_price, trades_lost_num, result]
    notation.extend(prices)
    print(f'NOTATION: {notation[:10]}')
    note = pd.Series(notation)
    with open(filename, 'a') as file:
        note.to_csv(file, header=True, index=False)
        print(f'record created in {filename}')
        file.close()


def main():
    q = Queue()
    w = Queue()

    while True:
        p1 = Process(target=extract_pairs, args=(q, w), daemon=True)
        p2 = Process(target=abuse_pairs, args=(q, w), daemon=True)
        try:
            p1.start()
            p2.start()

            p1.join()
            p2.join()
        except Exception as err:
            print(f'\nPROCESS ERROR OCCURRED: {err}\n')
            if p1.is_alive():
                p1.terminate()
            if p2.is_alive():
                p2.terminate()
            continue


if __name__ == "__main__":
    main()
