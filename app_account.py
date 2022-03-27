import jaydebeapi
import sqlite3
import pandas as pd
import openpyxl
import datetime



conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar' )

cursor = conn.cursor()

def  accountsSql():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_STG_accounts as
                                select
                                        ACCOUNT,
                                        VALID_TO,
                                        CLIENT,
                                        CREATE_DT,
                                        UPDATE_DT
                                from bank.accounts
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

        
def histAccountSql():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_DWH_DIM_accounts_hist(
                                ACCOUNT varchar(128),
                                VALID_TO timestamp,
                                CLIENT varchar(128),
                                effective_from date default current_date,
                                effective_to date default (to_date('2999-12-31', 'YYYY-MM-DD')),
                                deleted_flg integer default 0
                        )
                ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')
        try:
                cursor.execute('''
                        CREATE VIEW v_accounts as
                                select
                                        ACCOUNT,
                                        VALID_TO,
                                        CLIENT
                                from de2hk.s_19_DWH_DIM_accounts_hist
                                where deleted_flg = 0
                                and current_date between effective_from and effective_to
                ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такое представление уже есть!')

def createNewAccount():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_STG_new_accounts as 
                                select 
                                        t1.ACCOUNT,
                                        t1.VALID_TO,
                                        t1.CLIENT
                                from de2hk.s_19_STG_accounts t1
                                left join v_accounts t2
                                on t1.CLIENT = t2.CLIENT
                                where t2.CLIENT is null
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

def createDeletedAccount():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_STG_delete_accounts as 
                                select 
                                        t1.ACCOUNT,
                                        t1.VALID_TO,
                                        t1.CLIENT
                                from v_accounts t1
                                left join de2hk.s_19_STG_accounts t2
                                on t1.CLIENT = t2.CLIENT
                                where t2.CLIENT is null
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

def change_hist_account():
        cursor.execute(''' 
                UPDATE de2hk.s_19_DWH_DIM_accounts_hist
                set effective_to = current_date - 0.001
                where CLIENT in (select CLIENT from de2hk.s_19_STG_delete_accounts)
                and effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
        ''')

        
        cursor.execute('''
                insert into de2hk.s_19_DWH_DIM_accounts_hist (
                        ACCOUNT,
                        VALID_TO,
                        CLIENT
                        )
                select 
                        ACCOUNT,
                        VALID_TO,
                        CLIENT
                from de2hk.s_19_STG_new_accounts
        ''')

        cursor.execute('''
                insert into de2hk.s_19_DWH_DIM_accounts_hist(
                        ACCOUNT,
                        VALID_TO,
                        CLIENT,
                        deleted_flg
                        )
                select 
                        ACCOUNT,
                        VALID_TO,
                        CLIENT,
                        1
                from de2hk.s_19_STG_delete_accounts
        ''')






def showAccountTable():
        print('_-'*20)
        print('de2hk.s_19_DWH_DIM_accounts_hist')
        print('_-'*20)

        cursor.execute('select * from de2hk.s_19_DWH_DIM_accounts_hist')
        print([t[0] for t in cursor.description])
        for row in cursor.fetchall():
                print(row)
        print('\n'*2)

def deleteAccountTables():
        cursor.execute('DROP TABLE de2hk.s_19_STG_accounts')
        cursor.execute('DROP VIEW v_accounts')
        cursor.execute('DROP TABLE de2hk.s_19_STG_new_accounts')
        cursor.execute('DROP TABLE de2hk.s_19_STG_delete_accounts')
        
if __name__ == '__main__':

        accountsSql()
        histAccountSql()
        createNewAccount()
        createDeletedAccount()
        change_hist_account()
        showAccountTable()
        deleteAccountTables()
        