import json
import logging
from decimal import Decimal
from typing import List

import websocket
from pandas_ta import DataFrame, rsi
from websocket._app import WebSocketApp


class Base:
    """Base class"""
    BINANCE_STREAM: str = "wss://stream.binance.com:9443/ws/btcusdt@kline_5m"
    RSI_LENGTH: int = 14

    def __init__(self) -> None:
        self._binance_close_prices: List = []
        self._bitfinex_close_prices: List = []

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

    @property
    def binance_close_prices(self) -> List[Decimal]:
        return self._binance_close_prices

    @binance_close_prices.setter
    def binance_close_prices(self, obj: Decimal) -> None:
        self._binance_close_prices.append(obj)

    def on_message(self, ws: WebSocketApp, message: str) -> str:
        """Handler message"""
        try:
            data = json.loads(message)
            kline = data['k']
            close_price = Decimal(kline['c'])
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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    binance = Binance()
    ws_binance = websocket.WebSocketApp(
        binance.BINANCE_STREAM,
        on_message=binance.on_message,
        on_error=Binance.on_error,
        on_close=Binance.on_close,
        on_open=Binance.on_open,
    )
    ws_binance.run_forever()
