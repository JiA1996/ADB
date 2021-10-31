from transaction_manager import TransactionManager
from data_manager import DataManager
from utils import Parser
import logging
import sys
import os

logging.basicConfig(filename='history.log', level=logging.DEBUG)
with open("history.log", "w"):
    pass

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        dir_path = sys.argv[1]
    else:
        raise AttributeError("Missing test case path")

    for test in os.listdir(dir_path):
        if not test.startswith("testcase"):
            continue
        test_path = dir_path + '/' + test
        f = open(test_path, "r")
        print(55*"#")
        print(20*"#", test, 20*"#")
        print(55*"#")
        tm = TransactionManager()
        # DataManager.dump()
        for line in f.readlines():
            line = line.strip()
            if line and not line.startswith(("#", "//")):
                instruction = Parser.parse_instruction(line)
                tm.handle(instruction)
        f.close()
