#!/usr/bin/python
# -*- coding: utf-8 -*-
# performance.performance.py

'''
@summary: Group of functions that perform different performance measures.
'''

# General imports
import numpy as np
import pandas as pd
import logging

def create_sharpe_ratio(returns, periods=252):
    """
    Create the Sharpe ratio for the strategy, based on a
    benchmark of zero (i.e. no risk-free rate information).

    :param returns: A pandas Series representing period percentage returns.
    :param periods: Daily (252), Hourly (252*6.5), Minutely(252*6.5*60) etc.

    :return (dbl) sharpe ratio
    """
    return np.sqrt(periods) * (np.mean(returns)) / np.std(returns)


def create_drawdowns(pnl):
    """
    Calculate the largest peak-to-trough drawdown of the PnL curve
    as well as the duration of the drawdown. Requires that the
    pnl_returns is a pandas Series.

    :param pnl: A pandas Series representing period percentage returns.
    :return: drawdown, duration - Highest peak-to-trough drawdown and duration.
    """
    # Calculate the cumulative returns curve
    # and set up the High Water Mark
    hwm = [0]
    # Create the drawdown and duration series
    idx = pnl.index
    
    # original coding: index are dates, seems to take O(n). because the last
    # row is duplicated.
    drawdown = pd.Series(index=idx)
    duration = pd.Series(index=idx)
    
    
    # alternative 1: index are consecutive ordered numbers, seems to take O(1)
    #drawdown = pd.Series(index=np.arange(len(idx))).sort_index()
    #duration = pd.Series(index=np.arange(len(idx))).sort_index()

    # Loop over the index range
    for each_row in range(1, len(idx)):
        hwm.append(max(hwm[each_row - 1], pnl[each_row]))
        drawdown[each_row] = (hwm[each_row] - pnl[each_row])
        duration[each_row] = (
            0 if drawdown[each_row] == 0 else duration[each_row - 1] + 1)

    return drawdown, drawdown.max(), duration.max()