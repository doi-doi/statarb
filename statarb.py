from decimal import Decimal
from typing import Dict

import pandas as pd
from hummingbot.connector.connector_base import ConnectorBase

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.core.data_type.common import OrderType, PositionAction
from hummingbot.core.event.events import BuyOrderCreatedEvent, SellOrderCreatedEvent
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.data_feed.candles_feed.candles_factory import CandlesFactory
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class StatArb(ScriptStrategyBase):
    exchange: str = "binance_perpetual_testnet"  # "binance_perpetual"
    trading_pair_1: str = "ETH-USDT"  # "JUP-USDT"
    trading_pair_2: str = "BTC-USDT"  # "SOL-USDT"

    candles_1 = CandlesFactory.get_candle(
        CandlesConfig(connector="binance_perpetual", trading_pair=trading_pair_1, interval="1m", max_records=300))
    candles_2 = CandlesFactory.get_candle(
        CandlesConfig(connector="binance_perpetual", trading_pair=trading_pair_2, interval="1m", max_records=300))

    markets = {exchange: {trading_pair_1, trading_pair_2}}

    hedge_ratio = float(0.3)  # 0.07
    zscore_length = 200
    spread_length = zscore_length + 50
    entry_threshold = 1
    exit_threshold = 0
    sl_threshold = 3

    order_amount_usd = Decimal(200)
    position_open = False  # Tracks whether a position is open or not
    long_order_id = None  # Store long order ID
    short_order_id = None  # Store short order ID

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        self.candles_1.start()
        self.candles_2.start()

    @property
    def all_candles_ready(self):
        return all([self.candles_2.ready, self.candles_1.ready])

    def on_tick(self):
        if self.all_candles_ready:
            df = self.calculate_spread_and_zscore()
            if df is not None:
                zscore = df["z_score"].iloc[-1]

                conversion_rate_1 = RateOracle.get_instance().get_pair_rate(self.trading_pair_1)
                conversion_rate_2 = RateOracle.get_instance().get_pair_rate(self.trading_pair_2)

                # Check if the conversion rates are valid
                if conversion_rate_1 is None or conversion_rate_2 is None:
                    self.logger().warning("Conversion rate is None for one or both trading pairs.")
                    return

                amount_1 = self.order_amount_usd / conversion_rate_1
                amount_2 = self.order_amount_usd / conversion_rate_2
                price_1 = self.connectors[self.exchange].get_mid_price(self.trading_pair_1) * Decimal(0.99)
                price_2 = self.connectors[self.exchange].get_mid_price(self.trading_pair_2) * Decimal(0.99)

                if zscore > self.entry_threshold and zscore < self.sl_threshold and not self.position_open:
                    # Open long ETH-USDT and short BTC-USDT positions
                    self.long_order_id = self.buy(
                        connector_name=self.exchange,
                        trading_pair=self.trading_pair_1,
                        amount=amount_1,
                        order_type=OrderType.MARKET,
                        price=price_1,
                        position_action=PositionAction.OPEN,
                    )
                    self.short_order_id = self.sell(
                        connector_name=self.exchange,
                        trading_pair=self.trading_pair_2,
                        amount=amount_2,
                        order_type=OrderType.MARKET,
                        price=price_2,
                        position_action=PositionAction.OPEN,
                    )
                    self.position_open = True  # Mark that a position is now open

                elif (zscore < self.exit_threshold or zscore > self.entry_threshold) and self.position_open:
                    # Close long ETH-USDT and short BTC-USDT positions using stored order IDs
                    if self.long_order_id:
                        self.buy(
                            connector_name=self.exchange,
                            trading_pair=self.trading_pair_1,
                            amount=amount_1,
                            order_type=OrderType.MARKET,
                            price=price_1,
                            position_action=PositionAction.CLOSE,
                        )
                    if self.short_order_id:
                        self.sell(
                            connector_name=self.exchange,
                            trading_pair=self.trading_pair_2,
                            amount=amount_2,
                            order_type=OrderType.MARKET,
                            price=price_2,
                            position_action=PositionAction.CLOSE,
                        )
                    self.position_open = False  # Mark that the position is now closed

    async def on_stop(self):
        self.candles_1.stop()
        self.candles_2.stop()

    def format_status(self) -> str:
        if not self.ready_to_trade:
            return "Market connectors are not ready."

        lines = []

        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        if self.all_candles_ready:
            lines.extend([
                "\n######################################### Market Data ##########################################\n"
            ])

            # Calculate spread and z-score
            df = self.calculate_spread_and_zscore()
            if df is not None:
                # Add headers as the first row
                lines.append(f"TIMESTAMP           | {self.trading_pair_1} | {self.trading_pair_2} | SPREAD  | Z-SCORE")

                # Loop through the data rows and add them
                for i, row in df.tail(10).iterrows():
                    lines.append(
                        f"{row['timestamp']} |  {row['close_1']:.4f}  | {row['close_2']:.4f} | {row['spread']:.4f} | {row['z_score']:.4f}"
                    )

            lines.append(
                "\n------------------------------------------------------------------------------------------------\n")

        else:
            lines.extend(["", "  No data collected."])

        return "\n".join(lines)

    def calculate_spread_and_zscore(self):
        """
        Calculates the spread and Z-score between the two close prices and adds them to the merged DataFrame.
        :return: DataFrame with timestamp, close prices, spread, and Z-score.
        """
        df1 = self.candles_1.candles_df[['timestamp', 'close']].copy()
        df2 = self.candles_2.candles_df[['timestamp', 'close']].copy()

        # Merge on timestamp
        df = pd.merge(df1, df2, on="timestamp", suffixes=('_1', '_2'))

        if len(df) >= self.spread_length:
            df["spread"] = df["close_1"] - (df["close_2"] * self.hedge_ratio)

            # Calculate mean and std over the zscore_length window for Z-score calculation
            rolling_mean = df["spread"].rolling(window=self.zscore_length).mean()
            rolling_std = df["spread"].rolling(window=self.zscore_length).std()

            # Calculate Z-score and store it in the DataFrame
            df["z_score"] = (df["spread"] - rolling_mean) / rolling_std

            # Ensure timestamp is in human-readable format
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s', errors='coerce')

            return df
        else:
            self.logger().error("Not enough data to calculate the spread and z-score.")
            return None
