#!/usr/bin/python
# -*- coding: utf-8 -*-
# datahandler.csv_data_handler.py

'''
@summary: DataHandler that reads CSV files from disk and provide an
          interface to obtain data in a manner identical to a market feed.
'''

# General imports
import os

from datahandler import DataHandler
from events.events_impl import MarketEvent
import numpy as np
import pandas as pd
import logging

class HistoricCSVDataHandler(DataHandler):

    """
    HistoricCSVDataHandler is designed to read CSV files for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface.

    The CSV files are expected to have the following structure:
    [date-time, open, high, low, close, volume, adj_close]

    """

    def __init__(self, events, csv_dir, symbol_list, start_date):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and a list of symbols.

        It will be assumed that all files are of the form
       'symbol.csv', where symbol is a string in the list.

        :param events: The Event Queue
        :param csv_dir: absolute directory path to the CSV files.
        :param symbol_list: A list of symbol strings.
        :param start_date: (date) the start datetime of the strategy.
        """
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True
        self.bar_index = 0
        self.start_date = start_date
        self.all_data_dic = {}  # access data in list form for testing
        self._open_convert_csv_files()

    def _open_convert_csv_files(self):
        """
        Opens the CSV files from the data directory, converting
        them into pandas DataFrames within a symbol dictionary.

        For this handler it will be assumed that the data is
        taken from Yahoo. Thus its format will be respected.
        """
        comb_index = None
        headers = [
            'date', 'open', 'high', 'low', 'close', 'volume', 'adj_close']

        for symbol in self.symbol_list:
            # Load the CSV file with no header information, indexed on date
            path = os.path.join(self.csv_dir, '%s.csv' % symbol)
            
            # Instance of 'tuple' has no 'sort' member
            # but is not a tuple is a pandas dataframe pylint is confused
            self.symbol_data[symbol] = pd.read_csv(path,
                                                   header=0,
                                                   index_col=0,
                                                   parse_dates=True,
                                                   names=headers).sort_index()
            
            self.symbol_data[symbol] = self.symbol_data[symbol]\
                [self.symbol_data[symbol].index >= self.start_date]
            
            # Combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[symbol].index 
            else:
                comb_index.union(self.symbol_data[symbol].index)
            
            # Set the latest symbol_data to None
            self.latest_symbol_data[symbol] = []

        # Reindex the dataframes
        for symbol in self.symbol_list:
            self.all_data_dic[symbol] = self.symbol_data[symbol].\
                reindex(index=comb_index, method=None)
                
            self.symbol_data[symbol] = self.symbol_data[symbol].\
                reindex(index=comb_index, method=None).iterrows()
                
    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed.
        """
        for symbol_gen in self.symbol_data[symbol]:
            yield symbol_gen

    def get_latest_bar(self, symbol):
        """
        Returns the last bar from the latest_symbol list.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            raise KeyError("Symbol is not available in the data set.")
        else:
            if not bars_list:
                raise KeyError('latest_symbol_data has not been initialized.')
            else:
                return bars_list[-1]

    def get_latest_bars(self, symbol, bars=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            raise KeyError("Symbol is not available in the data set.")
        else:
            if not bars_list:
                raise KeyError('latest_symbol_data has not been initialized.')
            else:
                return bars_list[-2*bars:]

    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            raise KeyError("Symbol is not available in the data set.")
        else:
            if not bars_list:
                raise KeyError('latest_symbol_data has not been initialized.')
            else:
                return bars_list[-1][0]

    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI
        values from the pandas Bar series object.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            raise KeyError("Symbol is not available in the data set.")
        else:
            if not bars_list:
                raise KeyError('latest_symbol_data has not been initialized.')
            else:
                return getattr(bars_list[-1][1], val_type)

    def get_latest_bars_values(self, symbol, val_type, bars=1):
        """
        Returns the last N bar values from the
        latest_symbol list, or N-k if less available.
        """
        try:
            bars_list = self.get_latest_bars(symbol, bars)
        except KeyError:
            raise KeyError("Symbol is not available in the data set.")
        else:
            if not bars_list:
                raise KeyError('latest_symbol_data has not been initialized.')
            else:
                logging.debug("Bar List")
                logging.debug(bars_list)
                return np.array([getattr(b[1], val_type) for b in bars_list])

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for symbol in self.symbol_list:
            try:
                bars = next(self._get_new_bar(symbol))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bars is not None:
                    self.latest_symbol_data[symbol].append(bars)
        self.events.put(MarketEvent())