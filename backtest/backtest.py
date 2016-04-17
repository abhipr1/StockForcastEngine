#!/usr/bin/python
# -*- coding: utf-8 -*-
# backtest.backtest.py

# general imports
try:
    import Queue as queue
except ImportError:
    import queue
import time
import logging
import matplotlib.pyplot as plt

class Backtest(object):

    """
    Encapsulates the settings and components for carrying out
    an event-driven backtest.
    """

    def __init__(self, source_dir, symbol_list, initial_capital,
                 heartbeat, start_date, data_handler,
                 execution_handler, portfolio, strategy):
        """
        Initializes the backtest.

        :param source_dir: (str) database location or CSV directory.
        :param symbol_list: (list/str) symbols in database or filename in CSV.
        :param initial_capital: (dbl) the starting capital for the portfolio.
        :param heartbeat: (dbl) simulate heartbeat in seconds.
        :param start_date: (date) the start datetime of the strategy.
        :param data_handler: (obj)  Handles the market data feed.
        :param execution_handler: (obj)  Handles the orders/fills for trades.
        :param portfolio: (obj) Keeps track of portfolio positions.
        :param strategy: (obj) generates signals based on market data.
        """

        self.source_dir = source_dir
        self.symbol_list = symbol_list
        self.initial_capital = initial_capital
        self.heartbeat = heartbeat
        self.start_date = start_date

        self.data_handler_cls = data_handler
        self.execution_handler_cls = execution_handler
        self.portfolio_cls = portfolio
        self.strategy_cls = strategy

        self.events = queue.Queue()

        self.signals = 0
        self.orders = 0
        self.fills = 0
        self.num_strats = 1

        self._generate_trading_instances()

    def _generate_trading_instances(self):
        """
        Generates the trading instance objects from their class types.
        """
        try:
            logging.info("Creating DataHandler...")
            self.data_handler = self.data_handler_cls(self.events,
                                                      self.source_dir,
                                                      self.symbol_list,
                                                      self.start_date)
            logging.info("Creating Strategy...")
            self.strategy = self.strategy_cls(self.data_handler,
                                              self.events)
            logging.info("Creating Portfolio...")
            self.portfolio = self.portfolio_cls(self.data_handler,
                                                self.events,
                                                self.start_date,
                                                self.initial_capital)
            logging.info("Creating ExecutionHandler...")
            self.execution_handler = self.execution_handler_cls(self.events)
        except:
            import sys
            print("Problem creating trading instances. Exception occurred [%s]" % sys.exc_info()[0])
            raise

    def _run_backtest(self):
        """
        Executes the backtest.
        """
        i = 0
        while True:
            i += 1
            logging.debug("Iteration [%d]" %i)
            # Update the market bars
            if self.data_handler.continue_backtest == True:
                self.data_handler.update_bars()
            else:
                break

            # Handle the events
            while True:
                try:
                    event = self.events.get(False)
                except queue.Empty:
                    break
                else:
                    if event is not None:
                        if event.type == 'MARKET':
                            self.strategy.calculate_signals(event)
                            self.portfolio.update_timeindex()

                        elif event.type == 'SIGNAL':
                            self.signals += 1
                            self.portfolio.update_signal(event)

                        elif event.type == 'ORDER':
                            self.orders += 1
                            self.execution_handler.execute_order(event)

                        elif event.type == 'FILL':
                            self.fills += 1
                            self.portfolio.update_fill(event)

            time.sleep(self.heartbeat)

    def _output_performance(self, graph=False):
        """
        Outputs the strategy performance from the backtest.
        """
        self.portfolio.create_equity_curve_dataframe()

        logging.info('*********************************')

        logging.info("Creating summary stats...")
        stats = self.portfolio.output_summary_stats()
        logging.info("Creating equity curve...")

        logging.info('**********   STATS   ************')

        logging.info('Total Return: {:.4%}'.format(stats.get('Total Return')))
        logging.info('Sharpe Ratio: {:.4}'.format(stats.get('Sharpe Ratio')))
        logging.info('Max Drawdown: {:.4%}'.format(stats.get('Max Drawdown')))
        logging.info('Drawdown Duration: {:}'.format(stats.get('Drawdown Duration')))
        logging.info('Length of Series: {:}'.format(stats.get('Length of Series')))

        logging.info('*********************************')

        logging.info("Signals: {}".format(self.signals))
        logging.info("Orders: {}".format(self.orders))
        logging.info("Fills: {}".format(self.fills))

        # plot the results
        if graph == True:
            self._graph_equity_curve(self.portfolio.equity_curve)

    def _graph_equity_curve(self, equity_curve_dataframe):
        """
         Charts the results
        :param equity_curve_dataframe:
        """
        date_axis = equity_curve_dataframe.index
        equity_curve = equity_curve_dataframe['equity_curve']
        returns = equity_curve_dataframe['returns']
        drawdown = equity_curve_dataframe['drawdown']
      
        plt.figure(1)
        
        fig = plt.subplot(311)
        plt.title('S&P500 Forcasting using QDA with lag 2')
        plt.ylabel('Portfolio value %')
        plt.plot_date(date_axis, equity_curve, '-')
        plt.grid(True)
        
        plt.subplot(312)
        plt.ylabel('Period returns %')
        plt.bar(date_axis, returns)
        plt.grid(True)
        
        plt.subplot(313)
        plt.ylabel('Drawdown %')
        plt.plot_date(date_axis, drawdown, '-')
        plt.grid(True)
        
        plt.show()

        
    def simulate_trading(self, graph_results=True):
        """
        Simulates the backtest and outputs portfolio performance.
        """
        self._run_backtest()
        self.strategy.dump_updown_count()
        self._output_performance(graph=graph_results)