#!/usr/bin/python
# -*- coding: utf-8 -*-
# execution.simulated_execution.py

'''
@summary: ExecutionHandler class handles the interaction between a
          set of order objects generated by a Portfolio and the ultimate
          set of simulated Fill objects.
'''

from events import events_impl
from execution import ExecutionHandler


# General imports
import datetime


class SimulatedExecutionHandler(ExecutionHandler):

    """
    Idealized Execution.

    The simulated execution handler simply converts all order
    objects into their equivalent fill objects automatically
    without latency, slippage or fill-ratio issues.

    This allows a straightforward "first go" test of any strategy,
    before implementation with a more sophisticated execution
    handler.
    """

    def __init__(self, events_queue):
        """
        Initializes the handler, setting the event queues
        up internally.

        :param events: The queue of event objects.
        """
        self.events = events_queue

    def execute_order(self, event):
        """
        Simply converts Order objects into Fill objects naively,
        i.e. without any latency, slippage or fill ratio problems.

        :param event: Contains an Event object with order information.
        """
        if event.type == 'ORDER':
            fill_event = events_impl.FillEvent(timeindex=datetime.datetime.utcnow(),
                                               symbol=event.symbol,
                                               exchange='NSE',
                                               quantity=event.quantity,
                                               direction=event.direction,
                                               commission=None,
                                               fill_cost=0)
            self.events.put(fill_event)