from datetime import datetime
from enum import Enum
import logging
import json


class Transaction:
    def __init__(self, tid, read_only) -> None:
        self.id = tid
        self.time_stamp = datetime.now()
        self.blocked_inst = []
        self.blocked_by = set()  # other transactions
        self.updated_list = []
        self.read_only = read_only
        self.wait_for_site = []
        self.wait_for_variable = None
        self.snap_shot = None
        self.accessed_sites = set()
        self.should_abort = False
        self.locked_items = set()  # (site_id, var_id)
        self.locked_vars = set()


class InstType(str, Enum):
    BEGIN = "begin"
    BEGINRO = "beginRO"
    END = "end"
    W = "W"
    R = "R"
    FAIL = "fail"
    RECOVER = "recover"
    DUMP = "dump"


class Instruction:
    order = 1

    def __init__(self, inst_type, tid=None,
                 target=None, updated_val=None) -> None:
        self.inst_type = inst_type
        self.target = target
        self.updated_val = updated_val
        self.tid = tid
        self.order = Instruction.order
        Instruction.order += 1

    def __str__(self):
        return json.dumps({"type": self.inst_type, "tid": self.tid,
                           "target": self.target, "update": self.updated_val,
                           "order": self.order})


class Variable:
    def __init__(self, value, rep) -> None:
        self.value = value
        self.time_stamp = datetime.now()
        self.committed = True
        self.replicated = rep

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return str(self.value)


class Lock:
    def __init__(self, type, transaction) -> None:
        self.type = type  # 'W', 'R'
        self.transaction = transaction


class LockTable:
    def __init__(self, var_ids) -> None:
        self.locked_items = {}
        for i in var_ids:
            self.locked_items[i] = []

    def is_wlocked_by_others(self, var_id, transaction):
        if len(self.locked_items[var_id]) == 1 and\
           self.locked_items[var_id][0].type == 'W' and\
           self.locked_items[var_id][0].transaction != transaction:
            return True
        return False

    def is_rlocked_by_others(self, var_id, transaction):
        if len(self.locked_items[var_id]) > 0:
            for lock in self.locked_items[var_id]:
                if lock.type == 'R' and lock.transaction != transaction:
                    return True
        return False

    def is_wlocked_by_self(self, var_id, transaction):
        if len(self.locked_items[var_id]) == 1 and\
           self.locked_items[var_id][0].type == 'W' and\
           self.locked_items[var_id][0].transaction == transaction:
            return True
        return False

    def is_rlocked_by_self(self, var_id, transaction):
        if len(self.locked_items[var_id]) > 0:
            for lock in self.locked_items[var_id]:
                if lock.type == 'R' and lock.transaction == transaction:
                    return True
        return False

    def is_locked_by_others(self, var_id, transaction):
        return self.is_rlocked_by_others(var_id, transaction) or\
               self.is_wlocked_by_others(var_id, transaction)

    def check_resource_availbility(self, var_id,
                                   transaction, potential_lock_type):
        if potential_lock_type == 'R':
            if not self.is_wlocked_by_others(var_id, transaction):
                return True
        elif not self.is_locked_by_others(var_id, transaction):
            return True, None
        return False, self.get_blocking_transaction(var_id, transaction)

    def get_blocking_transaction(self, var_id, exception=None):
        t = set()
        for lock in self.locked_items[var_id]:
            if lock.transaction != exception:
                t.add(lock.transaction)
        return t
    # def add_rlock(self, var_id, transaction):
    #     if not self.is_wlocked_by_others(var_id, transaction):
    #         rlocked_by_self = self.is_rlocked_by_self(var_id, transaction)
    #         wlocked_by_self = self.is_wlocked_by_self(var_id, transaction)
    #         if not (rlocked_by_self or wlocked_by_self):
    #             self.locked_items[var_id].append(Lock('R', transaction))
    #         return True
    #     return False

    # def add_wlock(self, var_id, transaction):
    #     if not self.is_locked_by_others(var_id, transaction):
    #         if self.is_rlocked_by_self(var_id, transaction):
    #             return self.upgrade_lock(var_id, transaction)
    #         if not self.is_wlocked_by_self(var_id, transaction):
    #             self.locked_items[var_id].append(Lock('W', transaction))
    #         return True
    #     return False

    # def upgrade_lock(self, var_id, transaction):
    #     if len(self.locked_items[var_id]) == 1 and\
    #        self.locked_items[var_id][0].transaction == transaction:
    #         self.locked_items[var_id][0].type == 'W'
    #         return True
    #     return False

    def add_rlock_no_check(self, var_id, transaction):
        self.locked_items[var_id].append(Lock('R', transaction))

    def add_wlock_no_check(self, var_id, transaction):
        if self.is_rlocked_by_self(var_id, transaction):
            self.upgrade_lock_no_check(var_id, transaction)
        if not self.is_wlocked_by_self(var_id, transaction):
            self.locked_items[var_id].append(Lock('W', transaction))

    def upgrade_lock_no_check(self, var_id, transaction):
        self.locked_items[var_id][0].type = 'W'

    def release_lock(self, var_id, transaction):
        self.locked_items[var_id] =\
            [x for x in self.locked_items[var_id]
             if x.transaction != transaction]


class Site:
    def __init__(self, id, vars) -> None:
        self.site_id = id
        self.variables = vars
        self.up = True
        var_ids = []
        for k, _ in self.variables.items():
            var_ids.append(k)
        self.lock_table = LockTable(var_ids)

    def dump(self):
        print(self.variables)

    def clear_lock_table(self):
        var_ids = []
        for k, _ in self.variables.items():
            var_ids.append(k)
        self.lock_table = LockTable(var_ids)

    def is_wlocked(self, var_id):
        return self.lock_table.is_wlocked(var_id)

    def is_rlocked(self, var_id):
        return self.lock_table.is_rlocked(var_id)

    def is_locked(self, var_id):
        return self.lock_table.is_locked(var_id)

    def check_resource_availbility(self, var_id):
        self.lock_table.check_resource_availbility
        return True

    def add_wlock_no_check(self, var_id, transaction):
        return self.lock_table.add_wlock_no_check(var_id, transaction)

    def add_rlock_no_check(self, var_id, transaction):
        return self.lock_table.add_rlock_no_check(var_id, transaction)

    # def upgrade_lock_no_check(self, var_id, transaction):
    #     return self.lock_table.upgrade_lock_no_check(var_id, transaction)

    def release_lock(self, var_id, transaction):
        return self.lock_table.release_lock(var_id, transaction)


class Parser:
    @staticmethod
    def parse_instruction(inst) -> Instruction:
        logging.info("New Instruction! {}".format(inst))

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
