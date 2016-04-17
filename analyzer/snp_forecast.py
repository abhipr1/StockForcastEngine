#!/usr/bin/python
# -*- coding: utf-8 -*-
# analyzer.snp_forecast.py

'''
@summary: .
'''


import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis as QDA


from strategy.strategy import Strategy
from events.events_impl import SignalEvent
from backtest.backtest import Backtest

from datahandler.csv_data_handler import HistoricCSVDataHandler
from execution.simulated_execution import SimulatedExecutionHandler
from portfolio.portfolio import Portfolio
from utils.create_lagged_series import create_lagged_series
import logging


class SPYDailyForecastStrategy(Strategy):

    """
    S&P500 forecast strategy. It uses a Quadratic Discriminant
    Analyzer to predict the returns for a subsequent time
    period and then generated long/exit signals based on the
    prediction.
    """

    def __init__(self, bars, events):
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.events = events

        self.strategy_id = '000001'
        self.datetime = datetime.datetime.utcnow()

        self.model_start_date = datetime.datetime(2001, 1, 10)
        self.model_end_date = datetime.datetime(2006, 1, 3)
        
        self.long_market = False
        self.short_market = False
        self.bar_index = 0
        self.up_count = 0
        self.down_count = 0
        
        self.model = self.create_symbol_forecast_model()

    def create_symbol_forecast_model(self):
        # Create a lagged series of the S&P500 US stock market index
        snpret = create_lagged_series(
            self.symbol_list[0], self.model_start_date,
            self.model_end_date, lags=5
        )

        # Use the prior two days of returns as predictor
        # values, with direction as the response
        X = snpret[["Lag1", "Lag2"]]
        y = snpret["Direction"]
        
        # Skip days with NaN
        skip_till_date = self.model_start_date + relativedelta(days=3)
        X = X[X.index > skip_till_date]
        y = y[y.index > skip_till_date]
        logging.debug(snpret[snpret.index > skip_till_date])

        model = QDA()
        model.fit(X, y)
        return model

    def calculate_signals(self, event):
        """
        Calculate the SignalEvents based on market data.
        """
        sid = self.strategy_id
        sym = self.symbol_list[0]
        dt = self.datetime

        if event.type == 'MARKET':
            self.bar_index += 1
            if self.bar_index > 5:
                lags = self.bars.get_latest_bars_values(
                    self.symbol_list[0], "adj_close", bars=3
                )
                pred_series = pd.Series(
                    {
                        'Lag0': lags[0],
                        'Lag1': lags[1],
                        'Lag2': lags[2]
                    }
                ).pct_change()*100.0
                pred_series = pred_series.drop('Lag0')
                pred = self.model.predict(pred_series)

                if pred > 0:
                    self.up_count += 1
                    logging.debug("Prediction Upward.")
                else:
                    self.down_count += 1
                    logging.debug("Prediction Downward")

                if pred > 0 and not self.long_market:
                    self.long_market = True
                    signal = SignalEvent(sid, sym, dt, 'LONG', 1.0)
                    self.events.put(signal)

                if pred < 0 and self.long_market:
                    self.long_market = False
                    signal = SignalEvent(sid, sym, dt, 'EXIT', 1.0)
                    self.events.put(signal)
                    
    def dump_updown_count(self):
        logging.info("Up [%d]" % self.up_count)
        logging.info("Down [%d]" % self.down_count)
        

if __name__ == "__main__":
    import sys
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)
    
    fileHandler = logging.FileHandler("dump.log")
    fileHandler.setFormatter(formatter)
    root.addHandler(fileHandler)
    
    csv_dir = '/home/divya/PycharmProjects/StockAnalyzer/csv_data/'
    symbol_list = ['SPY']
    initial_capital = 100000.0
    
    start_date = datetime.datetime(2006, 1, 3)
    heartbeat = 0.0
    backtest = Backtest(csv_dir,
                        symbol_list,
                        initial_capital,
                        heartbeat,
                        start_date,
                        HistoricCSVDataHandler,
                        SimulatedExecutionHandler,
                        Portfolio,
                        SPYDailyForecastStrategy)

    backtest.simulate_trading()