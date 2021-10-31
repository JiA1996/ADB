from datetime import datetime
from enum import Enum
import logging
import json


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


class InstType(str, Enum):
    BEGIN = "begin"
    BEGINRO = "beginRO"
    END = "end"
    W = "W"
    R = "R"
    FAIL = "fail"
    RECOVER = "recover"


class Instruction:
    def __init__(self, inst_type, tid=None,
                 target=None, updated_val=None) -> None:
        self.inst_type = inst_type
        self.target = target
        self.updated_val = updated_val
        self.tid = tid

    def __str__(self):
        return json.dumps({"type": self.inst_type, "tid": self.tid,
                           "target": self.target, "update": self.updated_val})


class Variable:
    def __init__(self, value) -> None:
        self.value = value
        self.time_stamp = datetime.now()
        self.committed = True


class Parser:
    @staticmethod
    def parse_instruction(inst):
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
