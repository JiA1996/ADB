from utils import Instruction, Transaction, InstType
from data_manager import DataManager


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
        start = transaction
        youngest = transaction.time_stamp
        victim = transaction
        while transaction.blocked_by != start:
            if transaction.blocked_by is None:
                return False
            transaction = transaction.blocked_by
            if transaction.time_stamp > youngest:
                youngest = transaction.time_stamp
                victim = transaction
        return victim

    def add_transaction(self, transaction):
        self.transactions[transaction.id] = transaction
        self.locks[transaction] = []

    def abort(self, transaction):
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
        locked_variables = self.locks[transaction]
        if locked_variables == []:
            return
        for locked_variable in locked_variables:
            self.locked_by[locked_variable] = None
            # TODO let other T work
        self.locks[transaction] = []

    @staticmethod
    def parse(inst):
        def is_number(char):
            return char.isnumeric() or char == "."

        def to_numeric(num):
            f = float(num)
            i = int(f)
            return i if i == f else f

        split_at = inst.index('(')
        inst_type = InstType[(inst[0:split_at]).upper()]
        remaining = inst[split_at + 1: -1].split(',')
        remaining = [list(filter(is_number, x)) for x in remaining]
        remaining = [to_numeric(''.join(x)) for x in remaining]
        if inst_type in [InstType.FAIL, InstType.RECOVER]:
            return Instruction(inst_type, target=remaining[0])
        elif inst_type in [InstType.BEGIN, InstType.END, InstType.BEGINRO]:
            return Instruction(inst_type, tid=remaining[0])
        elif inst_type == InstType.W:
            return Instruction(inst_type, remaining[0],
                            remaining[1], remaining[2])
        else:
            return Instruction(inst_type, remaining[0], remaining[1])

    def handle(self, inst):
        instruction = TransactionManager.parse(inst)
        if instruction.inst_type == InstType.BEGIN:
            self.add_transaction(Transaction(instruction.tid, False))
            return
        elif instruction.inst_type == InstType.BEGINRO:
            self.add_transaction(Transaction(instruction.tid, True))
            return
        elif instruction.inst_type == InstType.END:
            self.commit(self.transactions[instruction.tid])
            self.locks.pop(self.transactions[instruction.tid])
            self.transactions.pop(instruction.tid)
            return
        elif instruction.inst_type == InstType.FAIL:
            DataManager.fail(instruction.target)
        elif instruction.inst_type == InstType.RECOVER:
            DataManager.recover(instruction.target)
            # TODO wait for site

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
