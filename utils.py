from datetime import datetime
from enum import Enum


class Transaction:
    def __init__(self, tid, read_only) -> None:
        self.id = tid
        self.time_stamp = datetime.now()
        self.current_inst = None
        self.blocked_by = None  # other transaction
        self.updated_list = []
        self.read_only = read_only
        self.wait_for_site = None
        self.wait_for_variable = None
        self.snap_shot = None


class InstType(Enum):
    BEGIN = 1
    BEGINRO = 2
    END = 3
    W = 4
    R = 5
    FAIL = 6
    RECOVER = 7


class Instruction:
    def __init__(self, inst_type, tid=None,
                 target=None, updated_val=None) -> None:
        self.inst_type = inst_type
        self.target
        self.updated_val = updated_val
        self.tid = tid


class Variable:
    def __init__(self, value) -> None:
        self.value = value
        self.time_stamp = datetime.now()
        self.committed = True
