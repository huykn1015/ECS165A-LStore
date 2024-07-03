import threading

from lstore.record import Record
from lstore.table import Table
from lstore.index import Index


class TransactionWorker:
    def __init__(self, transactions=[]):
        """Creates a transaction worker object."""
        self.stats = []
        self.transactions = transactions
        self.result = 0
        pass

    def add_transaction(self, t):
        """Appends t to transactions."""
        self.transactions.append(t)

    def join(self):
        """Waits for the worker to finish."""
        pass

    def run(self):
        """Runs all transaction as a thread."""

        # here you need to create a thread and call __run
        thread = threading.Thread(target=self.__run)
        thread.start()

    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
