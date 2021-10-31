from utils import Transaction, InstType
from data_manager import DataManager
import logging


class TransactionManager():
    def __init__(self) -> None:
        self.transactions = {}
        self.site_waited_by = {}     # site: transactions, value is list
        self.variable_waited_by = {}  # variable: transactions, value is list
        self.rlocked_by = {}         # variable: transaction / None
        self.wlocked_by = {}         # variable: transaction / None
        self.locks = {}  # transaction: [variables]
        DataManager()
        for site_id, site in DataManager.sites.items():
            site_vars = site.variables
            self.site_waited_by[site] = []
            for _, var in site_vars.items():
                self.rlocked_by[var] = set()
                self.wlocked_by[var] = None
                self.variable_waited_by[var] = []

    def detect_deadlock(self, head, current, ts, potential_victim, victims=[]):
        logging.info("Start to do cyclic check")
        for blocker in current.blocked_by:
            logging.info("Transaction {0} is locked by {1}".
                         format(current.id, current.blocked_by.id))
            current = blocker
            if current.time_stamp > ts:
                ts = current.time_stamp
                potential_victim = current
            if current == head:
                victims.append(potential_victim)
                logging.info("Deadlock detected! Kill Transaction {}".
                             format(potential_victim.id))
            else:
                self.detect_deadlock(self, head, current, ts,
                                     potential_victim, victims)

    def add_transaction(self, transaction):
        logging.info("Transaction {} added".format(transaction.id))
        self.transactions[transaction.id] = transaction
        self.locks[transaction] = []

    def abort(self, transaction):
        logging.info("Abort Transaction {}".format(transaction.id))
        self.wipe_history(transaction)

        for inst in transaction.blocked_inst:
            self.variable_waited_by[inst.target] =\
                list(filter(lambda x: x != transaction,
                     self.variable_waited_by[inst.target]))

        # issue
        if transaction.wait_for_site is not None:
            self.site_waited_by[transaction.wait_for_site] =\
                list(filter(lambda x: x != transaction,
                     self.site_waited_by[transaction.wait_for_site]))

        self.locks.pop(transaction)

    def commit(self, transaction):
        DataManager.commit(transaction)
        self.wipe_history(transaction)

    def wipe_history(self, transaction):
        self.release_lock(transaction)
        self.transactions.pop(transaction.id)
        for site in transaction.accessed:
            DataManager.accessed_by[site].remove(transaction)

    def release_lock(self, transaction):
        logging.info("Release all locks from Transaction {}".
                     format(transaction.id))
        locked_variables = self.locks[transaction]
        if locked_variables == []:
            return
        for locked_variable in locked_variables:
            self.rlocked_by[locked_variable].remove(transaction)
            self.wlocked_by[locked_variable] = None
        self.locks.pop(transaction)
        logging.info("Released: {}".format(", ".join(locked_variables)))

        while True:
            instrucion = self.unblock_instruction()
            if instrucion is not None:
                self.handle(instrucion)
            else:
                break

    def unblock_instruction(self, unlocked_vars):
        # want a holistic check
        # issue with transaction.wait_for_variable
        # because it only records the blocked varible required
        # by the front instruction
        transactions = []

        for var in range(1, DataManager.var_num + 1):
            if len(self.variable_waited_by[var]) > 0:
                t = self.variable_waited_by[var][0]
                transactions.append(t)

        potential_instructions = []
        for t in set(transactions):
            inst = t.blocked_inst[0]
            potential_instructions.append(inst)
            # check if site is up
            if inst.inst_type == InstType.R:
                if self.wlocked_by[inst.target] is not None:
                    potential_instructions.pop(-1)
            elif inst.inst_type == InstType.W:
                '''
                if write is not blocked by other locks,
                it either fails or succeeds
                becuase if all sites fail, write fails,
                if not all sites fail, it succeeds.
                '''
                if len(self.rlocked_by[inst.target]) > 0 or\
                   self.wlocked_by[inst.target] is not None:
                    potential_instructions.pop(-1)
            # check site is up
        if len(potential_instructions) != 0:
            instruction = min(potential_instructions,
                              lambda x: x.order)
            # clear wait
            logging.info("Take below instruction out of queue")
            logging.info(str(instruction))
            return instruction
        else:
            return None

    def show_data(self):
        for site, data in DataManager.snap_shot().items():
            print(site, ": ", data)

    def handle_begin(self, instruction):
        self.add_transaction(Transaction(instruction.tid, False))

    def handle_beginRO(self, instruction):
        transaction = Transaction(instruction.tid, True)
        self.add_transaction(transaction)
        transaction.snap_shot = DataManager.snap_shot()
        # print(transaction.snap_shot)

    def handle_end(self, instruction):
        tid = instruction.tid
        if tid not in self.transactions.keys():
            print("Transaction {} does not exist".format(tid))
            return

        transaction = self.transactions[tid]
        if transaction.should_abort:
            self.abort(transaction)
            print("Transaction {} is aborted due to site failure".format(tid))
            return

        if transaction.blocked_list != []:
            self.abort(transaction)
            print("Transaction {} is aborted due to\
                  queued instructions".format(tid))
            return

        self.commit(transaction)

    def handle_fail(self, instruction):
        DataManager.fail(instruction.target)  # add fail history and set should abort
        return

    def handle_recover(self, instruction):
        DataManager.recover(instruction.target)  # notify queue for site waiters
        return

    def handle_general_read(self, instruction):
        tid = instruction.tid
        if tid not in self.transactions.keys():
            print("Transaction {} does not exist".format(tid))
            return

        return

    def handle_RO(self, instruction):

        return

    def handle_normal_read(self, instruction):
        return

    def handle_write(self, instruction):
        tid = instruction.tid
        if tid not in self.transactions.keys():
            print("Transaction {} does not exist".format(tid))
            return

        return

    def handle(self, instruction):
        logging.info("Parsed instruction:\n{}".format(instruction))
        if instruction.inst_type == InstType.BEGIN:
            '''handle Begin'''
            self.handle_begin(instruction)
            return

        if instruction.inst_type == InstType.BEGINRO:
            '''handle BeginRO'''
            self.handle_beginRO(instruction)
            return

        if instruction.inst_type == InstType.END:
            '''handle End'''
            self.handle_end(instruction)
            return

        if instruction.inst_type == InstType.FAIL:
            self.handle_fail(instruction)
            return

        if instruction.inst_type == InstType.RECOVER:
            '''handle Recover'''
            self.handle_recover(instruction)
            return

        if instruction.inst_type == InstType.W:
            '''handle Write'''
            self.handle_write(instruction)
            return

        if instruction.inst_type == InstType.R:
            '''hanle Read'''
            self.handle_general_read(instruction)
            return

        if instruction.inst_type == InstType.DUMP:
            self.show_data()
            return

        '''
        once release lock
        cyclicly check if queue can move continuously
        once queue moves, subsequently check all waited_by
        to see if any transaction can move.
        '''
