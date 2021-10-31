from utils import Transaction, InstType
from utils import Parser
from data_manager import DataManager
import logging


class TransactionManager():
    def __init__(self) -> None:
        self.transactions = {}
        self.wait_for_site = {}     # site: transactions, value is list
        self.wait_for_variable = {}  # variable: transactions, value is list
        self.locked_by = {}         # variable: transaction / None
        self.locks = {}  # transaction: [variables]
        DataManager()
        for site, site_vars in DataManager.variable_dict.items():
            self.wait_for_site[site] = []
            for _, var in site_vars.items():
                self.locked_by[var] = None
                self.wait_for_variable[var] = []

    def detect_deadlock(self, transaction):
        logging.info("Start to do cyclic check")
        start = transaction
        youngest = transaction.time_stamp
        victim = transaction
        while transaction.blocked_by != start:
            logging.info("Transaction {0} is locked by {1}".
                         format(transaction.id, transaction.blocked_by.id))
            if transaction.blocked_by is None:
                logging.info("No deadlock found")
                return False
            transaction = transaction.blocked_by
            if transaction.time_stamp > youngest:
                youngest = transaction.time_stamp
                victim = transaction
        logging.info("Deadlock detected! Kill Transaction {}".
                     format(victim.id))
        return victim

    def add_transaction(self, transaction):
        logging.info("Transaction {} added".format(transaction.id))
        self.transactions[transaction.id] = transaction
        self.locks[transaction] = []

    def abort(self, transaction):
        logging.info("Abort Transaction {}".format(transaction.id))
        self.release_lock()
        self.transactions.pop(transaction.id)

        if transaction.wait_for_variable is not None:
            self.wait_for_variable[transaction.wait_for_variable].\
                remove(transaction)

        if transaction.wait_for_site is not None:
            self.wait_for_site[transaction.wait_for_site].remove(transaction)

        self.locks.pop(transaction)

    def commit(self, transaction):
        DataManager.commit(transaction)
        self.release_lock(transaction)

    def release_lock(self, transaction):
        logging.info("Release all locks from Transaction {}".
                     format(transaction.id))
        locked_variables = self.locks[transaction]
        logging.info("Released: {}".format(", ".join(locked_variables)))
        if locked_variables == []:
            return
        for locked_variable in locked_variables:
            self.locked_by[locked_variable] = None
            # TODO let other T work
        self.locks[transaction] = []

    def handle(self, inst):
        instruction = Parser.parse_instruction(inst)
        logging.info("Parsed instruction:\n{}".format(instruction))
        if instruction.inst_type == InstType.BEGIN:
            self.add_transaction(Transaction(instruction.tid, False))
            return
        elif instruction.inst_type == InstType.BEGINRO:
            self.add_transaction(Transaction(instruction.tid, True))
            return
        elif instruction.inst_type == InstType.END:
            if instruction.tid not in self.transactions:
                logging.info("the instruction belongs to aborted\
                             Transaction {}".format(instruction.tid))
                return
            self.commit(self.transactions[instruction.tid])
            self.locks.pop(self.transactions[instruction.tid])
            self.transactions.pop(instruction.tid)
            return
        elif instruction.inst_type == InstType.FAIL:
            DataManager.fail(instruction.target)
            return
        elif instruction.inst_type == InstType.RECOVER:
            DataManager.recover(instruction.target)
            # TODO wait for site
            return

        if instruction.tid not in self.transactions:
            logging.info("the instruction belongs to aborted\
                             Transaction {}".format(instruction.tid))
            return
        transaction = self.transactions[instruction.tid]
        transaction.current_inst = instruction
        if instruction.inst_type == InstType.W:
            # TODO

            pass
        elif transaction.read_only:
            transaction.snap_shot = DataManager.variable_dict
            # TODO
            pass
        else:
            # TODO
            pass
        return
