import asyncio
import json
import logging
from decimal import Decimal
from typing import List, Union

from pandas import to_datetime
from pandas_ta import DataFrame, rsi, vwap
from websocket import WebSocketApp


class Base:
    """Base class"""
    BINANCE_STREAM: str = "wss://stream.binance.com:9443/ws/btcusdt@kline_5m"
    BITFINEX_STREAM: str = "wss://api-pub.bitfinex.com/ws/2"
    RSI_LENGTH: int = 14

    def __init__(self) -> None:
        self._binance_close_prices: List[Union[Decimal, float]] = []
        self._bitfinex_close_prices: List[Union[Decimal, float]] = []
        self._df_bitfinex: DataFrame = DataFrame()

    @staticmethod
    def on_error(ws: WebSocketApp, error: str) -> None:
        """Handler error"""
        logging.error(f"Error: {error}")

    @staticmethod
    def on_close(
        ws: WebSocketApp, close_status_code: int, close_msg: str
    ) -> None:
        """Handler closed"""
        logging.info("Connection closed")

    @staticmethod
    def on_open(ws: WebSocketApp) -> None:
        """Handler opened"""
        logging.info("Opened connection")


class Binance(Base):
    """Class work with binance"""
    def __init__(self):
        super().__init__()
        self.last_binance_candle_time = None

    @property
    def binance_close_prices(self) -> List[Union[Decimal, float]]:
        return self._binance_close_prices

    @binance_close_prices.setter
    def binance_close_prices(self, obj: Union[Decimal, float]) -> None:
        self._binance_close_prices.append(obj)

    def on_message(self, ws: WebSocketApp, message: str) -> None:
        """Handler message"""
        try:
            data = json.loads(message)
            kline = data['k']
            close_price = Decimal(kline['c'])
            self.binance_close_prices = close_price
            candle_time = int(kline['t']) // 1000
            if self.last_binance_candle_time is None:
                self.last_binance_candle_time = candle_time
                pass
            if candle_time - self.last_binance_candle_time >= 300:
                self.last_binance_candle_time = candle_time
                self.binance_close_prices = close_price
                if len(self.binance_close_prices) > self.RSI_LENGTH:
                    df = DataFrame({'close': self.binance_close_prices})
                    rsi_values = rsi(df['close'], length=self.RSI_LENGTH)
                    logging.info(
                        f"Binance - Close: {close_price}; "
                        f"RSI: {rsi_values.tolist()[-1]}"
                    )
        except Exception as ex:
            logging.error(f"Error processing message: {ex}")


class Bitfinex(Base):
    """Class work with Bitfinex"""
    def __init__(self) -> None:
        super().__init__()
        self.last_bitfinex_candle_time = None

    @property
    def bitfinex_close_prices(self) -> List[Union[Decimal, float]]:
        return self._bitfinex_close_prices

    @bitfinex_close_prices.setter
    def bitfinex_close_prices(self, obj: Decimal) -> None:
        self._bitfinex_close_prices.append(obj)

    @staticmethod
    def on_open(ws: WebSocketApp) -> None:
        request = {
            "event": "subscribe",
            "channel": "candles",
            "key": "trade:1m:tBTCUSD"
        }
        ws.send(json.dumps(request))

    def on_message(self, ws: WebSocketApp, message: str) -> None:
        try:
            data = json.loads(message)
            if isinstance(data, list) and isinstance(data[1][0], int):
                candle_data = data[1]
                if isinstance(candle_data, list) and len(candle_data) > 5:
                    candle_time = candle_data[0] // 1000
                    close_price = candle_data[2]
                    high_price = candle_data[3]
                    low_price = candle_data[4]
                    volume_price = candle_data[5]
                    if self.last_bitfinex_candle_time is None:
                        self.last_bitfinex_candle_time = candle_time
                        pass
                    if candle_time - self.last_bitfinex_candle_time >= 60:
                        self.last_bitfinex_candle_time = candle_time
                        self.bitfinex_close_prices = close_price
                        df = DataFrame({
                            "high": [high_price],
                            "low": [low_price],
                            "close": [close_price],
                            "volume": [volume_price]
                        })
                        df.set_index(to_datetime(
                            [candle_data[0]], unit="ms"
                        ), inplace=True)
                        df["vwap"] = vwap(
                            close=df["close"],
                            volume=df["volume"],
                            high=df["high"],
                            low=df["low"]
                        )
                        logging.info(f"Bitfinex - Close: {close_price}; "
                                     f"VWAP: {df['vwap'].tolist()[-1]}")
        except Exception as ex:
            logging.error(f"Error processing message: {ex}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    binance = Binance()
    ws_binance = WebSocketApp(
        Base.BINANCE_STREAM,
        on_message=binance.on_message,
        on_error=Base.on_error,
        on_close=Base.on_close,
        on_open=Base.on_open,
    )
    bitfinex = Bitfinex()
    ws_bitfinex = WebSocketApp(
        Base.BITFINEX_STREAM,
        on_message=bitfinex.on_message,
        on_error=Base.on_error,
        on_close=Base.on_close,
        on_open=Bitfinex.on_open
    )

    async def run_clients():
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(None, ws_binance.run_forever),
            loop.run_in_executor(None, ws_bitfinex.run_forever)
        ]

        await asyncio.gather(*tasks)

    asyncio.run(run_clients())
