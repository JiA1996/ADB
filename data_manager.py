from datetime import datetime
from utils import Variable, Site
import logging
import copy


class DataManager():
    # is_up = {}
    # variable_dict = {}
    # working_sites = []
    # accessed_by = {}
    # var_num = 0
    # site_num = 0
    sites = {}
    site_num = 10
    var_num = 20
    '''
    is_up[id] = True/False
    Variable_dict[site_id] = [1,2,3,4,5,...20]

    write(site_id, xi, value)
    Varibale_dict[site_id][xi] = value
    '''

    def __init__(self) -> None:
        for i in range(1, DataManager.site_num + 1):
            vars = {}
            for j in range(1, DataManager.var_num + 1):
                if j % 2 == 0:
                    vars[j] = Variable(10*j, True)
                else:
                    if i == 1 + j % 10:
                        vars[j] = Variable(10*j, False)
            DataManager.sites[i] = Site(i, vars)
        # DataManager.var_num = 20
        # DataManager.site_num = 10
        # for i in range(1, DataManager.site_num + 1):
        #     DataManager.is_up[i] = True
        #     DataManager.variable_dict[i] = {}
        #     DataManager.working_sites.append(i)
        #     DataManager.accessed_by[i] = set()

        # for i in range(1, DataManager.var_num + 1):
        #     if i % 2 == 0:
        #         for j in range(1, 11):
        #             DataManager.variable_dict[j][i] = Variable(10*i)
        #     else:
        #         DataManager.variable_dict[1 + i % 10][i] = Variable(10*i)

    @staticmethod
    def write(site_id, var_id, value):
        var = DataManager.sites[site_id].variables[var_id]
        var.value = value
        var.time_stamp = datetime.now()
        var.committed = True

    @staticmethod
    def commit(transaction):
        for site, var, value in transaction.updated_list:
            logging.info("Update X{0} at site {1} to {2}".
                         format(var, site, value))
            DataManager.write(site, var, value)
        logging.info("Transaction {} commits".format(transaction.id))

    @staticmethod
    def fail(site):
        DataManager.sites[site].up = False
        DataManager.sites[site].clear_lock_table()
        logging.info("Site {} fails".format(site))

    def recover(site):
        assert not DataManager.sites[site].up
        DataManager.sites[site].up = True
        for _, v in DataManager.sites[site].variables.items():
            v.committed = False

        logging.info("Site {} recovers".format(site))

    @staticmethod
    def read_no_check(site_id, var_id):
        site = DataManager.sites[site_id]
        var = site.variables[var_id]
        if not var.replicated:
            return var.value
        elif site.variables[var_id].committed:
            return var.value
        else:
            return None

    @staticmethod
    def dump():
        for i, site in DataManager.sites.items():
            print("Site {}:".format(i))
            site.dump()

    @staticmethod
    def snap_shot():
        data = {}
        for i in range(1, DataManager.site_num):
            if DataManager.sites[i].up:
                data[i] = {}
                local_data = DataManager.sites[i].variables
                for j, var in local_data.items():
                    if var.committed:
                        data[i][j] = var
        return copy.deepcopy(data)

    @staticmethod
    def get_up_sites(var_id):
        up = []
        if var_id % 2 == 0:
            sites = range(1, DataManager.var_num + 1)
        else:
            sites = [1 + var_id % 10]
        for site_id in sites:
            if DataManager.sites[site_id].up:
                up.append(site_id)
        return up

    @staticmethod
    def wlock_all(var_id, transaction, up_sites):
        cannot_lock_on_sites = []

        for site_id in up_sites:
            available, blocking_trans = DataManager.sites[site_id].\
                    check_resource_availability(var_id, transaction, 'W')
            if not available:
                transaction.blocked_by.union(blocking_trans)
                cannot_lock_on_sites.append(site_id)

        if cannot_lock_on_sites == []:
            for site_id in up_sites:
                DataManager.sites[site_id].\
                    add_wlock_no_check(var_id, transaction)
            return True  # , cannot_lock_on_sites
        return False  # , cannot_lock_on_sites

    @staticmethod
    def rlock_one(var_id, transaction, up_sites):
        assert len(up_sites) > 0
        locked_site = None
        available, blocking_trans = DataManager.sites[up_sites[0]].\
            check_resource_availability(var_id, transaction, 'R')
        if not available:
            transaction.blocked_by.union(blocking_trans)
            return False, None

        for site_id in up_sites:
            if DataManager.sites[site_id].variables[var_id].committed:
                DataManager.sites[site_id].\
                        add_rlock_no_check(var_id, transaction)
                return True, site_id
            else:
                locked_site = site_id

        if locked_site is not None and\
           not DataManager.sites[locked_site].variables[var_id].replicated:
            DataManager.sites[locked_site].\
                    add_rlock_no_check(var_id, transaction)
            return True, locked_site
        return False, None

    def release_lock(transaction):
        for site_id, var_id in transaction.locked_items:
            DataManager.sites[site_id].release_lock(var_id, transaction)
