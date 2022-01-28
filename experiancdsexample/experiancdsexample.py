
'''Project Short Description (default ini)

Project long description or extended summary goes in here (default ini)
'''

# lerouxcoral@gmail.com
# johan12fd1@gmail.com

import argparse
import configparserext
import logging
from pathlib import Path
# import sys
from termcolor import colored
from beetools import beeutils
from beetools.beearchiver import Archiver
from sqldbwrpr import MySQL
import datetime


_PROJ_DESC = __doc__.split('\n')[0]
_PROJ_PATH = Path(__file__)
_PROJ_NAME = _PROJ_PATH.stem


class ExperianCDSExample:
    '''Class description
    '''
    def __init__(
        self,
        p_ini_pth,
        p_logger = False
    ):
        '''Method description
        '''
        self.success = True
        self.ini_pth = p_ini_pth
        self.logger_name = None
        self.logger = None
        if p_logger:
            self.logger_name = "ExperianCDSExample"
            self.logger = logging.getLogger(self.logger_name)
        self.ini = configparserext.ConfigParserExt(inline_comment_prefixes = '#')
        self.verbose = False

        self.ini.read([self.ini_pth])
        self.proj_root_dir = _PROJ_PATH.parents[1]


        self.admin_user = ['root', 'En0l@Gay']
        self.db_name = 'ExperianPersonel'
        self.db_user = ['rtinstall', 'Rt1nst@ll']
        self.db_host_PROJ_NAME = 'localhost'
        self.db_user_rights = [self.db_user[0], self.db_host_PROJ_NAME, '*', '*', 'ALL']
        self.new_users = [
            ['Testing01', '1re$UtseT', 'localhost'],
            ['Testing02', '2re$UtseT', 'localhost'],
        ]
        self.new_user_rights = [
            [self.new_users[0][0], self.new_users[0][2], '*', '*', 'ALL'],
            [self.new_users[1][0], self.new_users[1][2], '*', '*', 'SELECT', 'INSERT'],
        ]
        self.test_data_folder = Path(__file__).absolute().parents[2] / _PROJ_NAME / 'data'
        self.people_path = Path(self.test_data_folder, 'Import', 'People.csv')
        self.address_path = Path(self.test_data_folder, 'Import', 'Address.csv')
        self.phone_path = Path(self.test_data_folder, 'Import', 'Phone.csv')
        self.people_phone_path = Path(self.test_data_folder, 'Import', 'PeoplePhone.csv')
        self.people_address_path = Path(self.test_data_folder, 'Import', 'PeopleAddress.csv')
        self.export_path = self.test_data_folder / 'Export'
        self.db_structure = {
            'People': {
                'SosSec': {
                    'Type': ['bigint'],
                    'Params': {
                        'PrimaryKey': ['Y', 'A'],
                        'FKey': [],
                        'Index': [],
                        'NN': 'Y',
                        'B': '',
                        'UN': '',
                        'ZF': '',
                        'AI': '',
                        'G': '',
                        'DEF': '',
                    },
                    'Possible Values': '',
                    'Comment': 'Sosial security nr filled with zeros',
                },
                'Surname': {
                    'Type': ['varchar', 45],
                    'Params': {
                        'PrimaryKey': ['', ''],
                        'FKey': [],
                        'Index': [],
                        'NN': 'Y',
                        'B': '',
                        'UN': '',
                        'ZF': '',
                        'AI': '',
                        'G': '',
                        'DEF': '',
                    },
                    'Possible Values': '',
                    'Comment': 'Surname of member',
                },
                'Name': {
                    'Type': ['varchar', 45],
                    'Params': {
                        'PrimaryKey': ['', ''],
                        'FKey': [],
                        'Index': [],
                        'NN': 'Y',
                        'B': '',
                        'UN': '',
                        'ZF': '',
                        'AI': '',
                        'G': '',
                        'DEF': '',
                    },
                    'Possible Values': '',
                    'Comment': 'Name of the member',
                },
                'DOB': {
                    'Type'           : ['date'],
                    'Params'         : {
                        'PrimaryKey': ['', ''],
                        'FKey'      : [],
                        'Index'     : [],
                        'NN'        : 'Y',
                        'B'         : '',
                        'UN'        : '',
                        'ZF'        : '',
                        'AI'        : '',
                        'G'         : '',
                        'DEF'       : '',
                    },
                    'Possible Values': '',
                    'Comment'        : 'Name of the member',
                },
            },
            'Address': {
                'AddressId': {
                    'Type': ['int'],
                    'Params': {
                        'PrimaryKey': ['Y', 'A'],
                        'FKey': [],
                        'Index': [],
                        'NN': 'Y',
                        'B': '',
                        'UN': '',
                        'ZF': '',
                        'AI': '',
                        'G': '',
                        'DEF': '',
                    },
                    'Possible Values': '',
                    'Comment': '',
                },
                'Street1': {
                    'Type': ['varchar', 45],
                    'Params': {
                        'PrimaryKey': ['', ''],
                        'FKey': [],
                        'Index': [],
                        'NN': 'Y',
                        'B': '',
                        'UN': '',
                        'ZF': '',
                        'AI': '',
                        'G': '',
                        'DEF': '',
                    },
                    'Possible Values': '',
                    'Comment': '',
                },
                'Street2': {
                    'Type': ['varchar', 45],
                    'Params': {
                        'PrimaryKey': ['', ''],
                        'FKey': [],
                        'Index': [],
                        'NN': '',
                        'B': '',
                        'UN': '',
                        'ZF': '',
                        'AI': '',
                        'G': '',
                        'DEF': '',
                    },
                    'Possible Values': '',
                    'Comment': 'Name of country',
                },
                'City': {
                    'Type'           : ['varchar', 45],
                    'Params'         : {
                        'PrimaryKey': ['', ''],
                        'FKey'      : [],
                        'Index'     : [],
                        'NN'        : '',
                        'B'         : '',
                        'UN'        : '',
                        'ZF'        : '',
                        'AI'        : '',
                        'G'         : '',
                        'DEF'       : '',
                    },
                    'Possible Values': '',
                    'Comment'        : 'Name of country',
                },
                'Code': {
                    'Type'           : ['varchar', 4],
                    'Params'         : {
                        'PrimaryKey': ['', ''],
                        'FKey'      : [],
                        'Index'     : [],
                        'NN'        : '',
                        'B'         : '',
                        'UN'        : '',
                        'ZF'        : '',
                        'AI'        : '',
                        'G'         : '',
                        'DEF'       : '',
                    },
                    'Possible Values': '',
                    'Comment'        : '',
                },
            },
            'Phone': {
                'PhoneId': {
                    'Type': ['int'],
                    'Params': {
                        'PrimaryKey': ['Y', 'A'],
                        'FKey': [],
                        'Index': [],
                        'NN': 'Y',
                        'B': '',
                        'UN': '',
                        'ZF': '',
                        'AI': '',
                        'G': '',
                        'DEF': '',
                    },
                    'Possible Values': '',
                    'Comment': '',
                },
                'Nr': {
                    'Type': ['varchar', 11],
                    'Params': {
                        'PrimaryKey': ['', ''],
                        'FKey': [],
                        'Index': [],
                        'NN': 'Y',
                        'B': '',
                        'UN': '',
                        'ZF': '',
                        'AI': '',
                        'G': '',
                        'DEF': '',
                    },
                    'Possible Values': '',
                    'Comment': '',
                },
            },
            'PeoplePhone': {
                'SosSec' : {
                    'Type'           : ['bigint'],
                    'Params'         : {
                        'PrimaryKey': ['Y', 'A'],
                        'FKey'      : [],
                        'Index'     : [],
                        'NN'        : 'Y',
                        'B'         : '',
                        'UN'        : '',
                        'ZF'        : '',
                        'AI'        : '',
                        'G'         : '',
                        'DEF'       : '',
                    },
                    'Possible Values': '',
                    'Comment'        : 'Sosial security nr filled with zeros',
                },
                'PhoneId': {
                    'Type'           : ['int'],
                    'Params'         : {
                        'PrimaryKey': ['Y', 'A'],
                        'FKey'      : [],
                        'Index'     : [],
                        'NN'        : 'Y',
                        'B'         : '',
                        'UN'        : '',
                        'ZF'        : '',
                        'AI'        : '',
                        'G'         : '',
                        'DEF'       : '',
                    },
                    'Possible Values': '',
                    'Comment'        : 'Surname of member',
                },
            },
            'PeopleAddress': {
                'SosSec'   : {
                    'Type'           : ['bigint'],
                    'Params'         : {
                        'PrimaryKey': ['Y', 'A'],
                        'FKey'      : [],
                        'Index'     : [],
                        'NN'        : 'Y',
                        'B'         : '',
                        'UN'        : '',
                        'ZF'        : '',
                        'AI'        : '',
                        'G'         : '',
                        'DEF'       : '',
                    },
                    'Possible Values': '',
                    'Comment'        : 'Sosial security nr filled with zeros',
                },
                'AddressId': {
                    'Type'           : ['int'],
                    'Params'         : {
                        'PrimaryKey': ['Y', 'A'],
                        'FKey'      : [],
                        'Index'     : [],
                        'NN'        : 'Y',
                        'B'         : '',
                        'UN'        : '',
                        'ZF'        : '',
                        'AI'        : '',
                        'G'         : '',
                        'DEF'       : '',
                    },
                    'Possible Values': '',
                    'Comment'        : 'Surname of member',
                },
            },
        }
        self.my_sql_db = None

    def create_db(
        self,
        # p_db_host_PROJ_NAME,
        # p_db_user,
        # p_db_name,
        # p_user_rigthts,
        # p_db_structure,
        # p_admin_user,
    ):
        success = True
        print('\nTest initialization, creation and population of database...')
        self.my_sql_db = MySQL(
            _PROJ_NAME,
            p_host_name = self.db_host_PROJ_NAME,
            p_user_name = self.db_user[0],
            p_password = self.db_user[1],
            p_user_rights = self.new_user_rights,
            p_recreate_db = True,
            p_db_name = self.db_name,
            p_db_structure = self.db_structure,
            p_batch_size = 1,
            p_admin_userName = self.admin_user[0],
            p_admin_user_password = self.admin_user[1],
        )
        # self.my_sql_db.close()

    # end t_init
    def import_data(self):
        ''''''
        success = True
        print('\nImport of test data from csv...')
        self.my_sql_db.import_csv('People', str(self.people_path))
        self.my_sql_db.import_csv('Address', str(self.address_path))
        self.my_sql_db.import_csv('Phone', str(self.phone_path))
        self.my_sql_db.import_csv('PeoplePhone', str(self.people_phone_path))
        self.my_sql_db.import_csv('PeopleAddress', str(self.people_address_path))
        pass
    
    def export_people(self):
        dest_pth = self.export_path / 'PeopleExp.csv'
        self.my_sql_db.export_to_csv(str(dest_pth), 'People')
        pass
    
    def people_in_30s(self):
        dest_pth = self.export_path / 'People30sExp.csv'
        sql_qry = '''SELECT People.Name, People.Surname, Phone.Nr FROM People
            JOIN PeoplePhone ON People.SosSec = PeoplePhone.SosSec
            JOIN Phone on PeoplePhone.PhoneId = Phone.PhoneId
                WHERE People.DOB > '1983-01-01' AND People.DOB < '1992-12-31';'''
        self.my_sql_db.export_to_csv(str(dest_pth), 'PeoplePhone', p_delimeter = ',', p_sql_query = ['*', sql_qry])
        pass
    
    def group_by_age(self):
        sql_str ='''SELECT DOB FROM People'''
        self.my_sql_db.cur.execute(sql_str)
        people_res = self.my_sql_db.cur.fetchall()
        age_groups = {}
        for rec in people_res:
            age = self.calc_age(rec[0])
            age_group = self.get_age_group(age)
            if age_group not in age_groups:
                age_groups[age_group] = { 'addreses' : 1, 'phonenrs' : 1}
            else:
                age_groups[age_group]['addreses'] +=1
                age_groups[age_group]['phonenrs'] +=1
            pass
        pass
    
    @staticmethod
    def calc_age(dob):
        # today = datetime.datetime.now().year
        age = datetime.datetime.now().year - dob.year
        return age
    
    @staticmethod
    def get_age_group(age):
        if age <= 5:
            return '1-5'
        elif age > 5 and age <= 10:
            return '6-10'
        elif age > 10 and age <= 25:
            return '11-15'
        elif age > 15 and age <= 20:
            return '16-20'
        elif age > 20 and age <= 25:
            return '21-25'
        elif age > 25 and age <= 30:
            return '26-30'
        elif age > 30 and age <= 35:
            return '31-35'
        elif age > 35 and age <= 40:
            return '36-40'
        elif age > 40 and age <= 45:
            return '41-45'
        elif age > 45 and age <= 50:
            return '46-50'
        elif age > 50 and age <= 55:
            return '51-55'
        elif age > 55 and age <= 60:
            return '56-60'
        elif age > 60 and age <= 65:
            return '61-65'
        elif age > 65 and age <= 70:
            return '66-70'
        else:
            return '71+'

        pass


    def run(self):
        '''Method description
        '''
        self.create_db()
        self.import_data()
        # self.export_people()
        # self.people_in_30s()
        self.group_by_age()
        self.my_sql_db.close()
        pass


def init_logger():
    logger = logging.getLogger(ExperianCDSExample)
    logger.setLevel(beeutils.DEF_LOG_LEV)
    file_handle = logging.FileHandler(beeutils.LOG_FILE_NAME, mode = 'w')
    file_handle.setLevel(beeutils.DEF_LOG_LEV_FILE)
    console_handle = logging.StreamHandler()
    console_handle.setLevel(beeutils.DEF_LOG_LEV_CON)
    file_format = logging.Formatter(beeutils.LOG_FILE_FORMAT, datefmt = beeutils.LOG_DATE_FORMAT)
    console_format = logging.Formatter(beeutils.LOG_CONSOLE_FORMAT)
    file_handle.setFormatter(file_format)
    console_handle.setFormatter(console_format)
    logger.addHandler(file_handle)
    logger.addHandler(console_handle)


def read_args():
    arg_parser = argparse.ArgumentParser(description = 'Get configuration parameters')
    # arg_parser.add_argument(
    #     'project_name',
    #     nargs = '+',
    #     help = 'Project name',
    # )
    arg_parser.add_argument(
        '-c',
        '--config-path',
        help = 'Config file name',
        default = arg_parser.prog[:arg_parser.prog.find('.') + 1] + 'ini'
    )
    # arg_parser.add_argument(
    #     '-e',
    #     '--arc-extern-dir',
    #     help = 'Path to external archive',
    #     default = None
    # )
    args = arg_parser.parse_args()
    # arc_extern_dir = args.arc_extern_dir
    ini_path = args.config_path
    # project_name = args.project_name[0]
    return ini_path

if __name__ == '__main__':
    ini_pth = read_args()
    # init_logger()
    # b_tls = Archiver(_PROJ_DESC, _PROJ_PATH, p_app_ini_file_name = ini_pth, p_arc_extern_dir = arc_extern_dir)
    # b_tls.print_header(p_cls = False)
    t_experiancdsexample = ExperianCDSExample(ini_pth)
    if t_experiancdsexample.success:
        t_experiancdsexample.run()
    # b_tls.print_footer()
# end __main__
