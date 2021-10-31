from datetime import datetime
from utils import Variable
import logging


class DataManager():
    is_up = {}
    variable_dict = {}
    all_up = True
    '''
    is_up[id] = True/False
    Variable_dict[site_id] = [1,2,3,4,5,...20]

    write(site_id, xi, value)
    Varibale_dict[site_id][xi] = value
    '''

    def __init__(self) -> None:
        for i in range(1, 11):
            DataManager.is_up[i] = True
            DataManager.variable_dict[i] = {}
            DataManager.all_up = True

        for i in range(1, 21):
            if i % 2 == 0:
                for j in range(1, 11):
                    DataManager.variable_dict[j][i] = Variable(10*i)
            else:
                DataManager.variable_dict[1 + i % 10][i] = Variable(10*i)

    @staticmethod
    def write(site, label, value):
        target = DataManager.variable_dict[site][label]
        target.value = value
        target.time_stamp = datetime.now()
        target.committed = True

    @staticmethod
    def commit(transaction):
        for site, label, value in transaction.updated_list:
            logging.info("Update X{0} at site {1} to {2}".
                         format(label, site, value))
            DataManager.write(site, label, value)
        logging.info("Transaction {} commits".format(transaction.id))

    @staticmethod
    def fail(site):
        DataManager.all_up = False
        DataManager.is_up[site] = False
        logging.info("Site {} fails".format(site))

    def recover(site):
        assert not DataManager.is_up[site]
        DataManager.is_up[site] = True
        for _, v in DataManager.variable_dict[site].items():
            v.committed = False

        logging.info("Site {} recovers".format(site))
        DataManager.all_up = True
        for _, status in DataManager.is_up.items():
            if not status:
                DataManager.all_up = False
                logging.info("Currently NOT all sites are up")
                return
        logging.info("Currently all sites are up")
    '''
    @staticmethod
    def get_all_sites(label):
        get_all = True
        if label % 2 == 0:

            return list(range(1, 11))
        else:
            return [1 + label % 10]
    '''

    @staticmethod
    def get_one_site(label):
        # No down site is returned
        if label % 2 == 0:
            for i in range(1, 11):
                if DataManager.is_up[i]:
                    return i
            return None
        else:
            site = 1 + label % 10
            if DataManager.is_up[site]:
                return site
            return None

    @staticmethod
    def read(site, label):
        var = DataManager.variable_dict[site][label]
        if var.committed:
            return var.value
        else:
            return None
