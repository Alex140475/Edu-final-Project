import jaydebeapi
import sqlite3
import pandas as pd
import openpyxl
import datetime
import os



conn = jaydebeapi.connect(
 	'oracle.jdbc.driver.OracleDriver',
 	'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
 	['de2hk', 'bilbobaggins'],
 	'ojdbc7.jar' )

cursor = conn.cursor()
# Создание Таблицы фактов «черный список» паспортов.

def xlsxPassSql():

        path_list = os.listdir()
        for row in path_list:
                if 'passport_blacklist' in row:
                        path = row
        
        df=pd.read_excel(path)
        df = df.astype(str)

        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_STG_passport_blacklist(
                                entry_dt varchar(128),
                                passport_num varchar(128))''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

        cursor.executemany('''
                insert into de2hk.s_19_STG_passport_blacklist
                        (
                        entry_dt,
                        passport_num)
                VALUES (?, ?)
                ''', df.values.tolist())
        os.renames(path, 'archive\\'+path+'.blackup')
def createPassTable():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_DWH_FACT_pssprt_blcklst(
                                entry_dt varchar(128),
                                passport_num varchar(128),
                                effective_from date default current_date,
                                effective_to date default (to_date('2999-12-31', 'YYYY-MM-DD')),
                                deleted_flg integer default 0,
                                unique(passport_num)
                                )
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')        

def histPassSql():
        cursor.execute('''
                insert into de2hk.s_19_DWH_FACT_pssprt_blcklst(
                        entry_dt,
                        passport_num
                        )
                select 
                        t1.entry_dt,
                        t1.passport_num
                from de2hk.s_19_STG_passport_blacklist t1
                left join de2hk.s_19_DWH_FACT_pssprt_blcklst t2
                on t1.PASSPORT_NUM = t2.passport_num
                where t2.passport_num is null
                ''')
        try:
                cursor.execute('''
                        CREATE VIEW v_pass as
                                select
                                        entry_dt,
                                        passport_num
                                from de2hk.s_19_DWH_FACT_pssprt_blcklst
                                where deleted_flg = 0
                                and current_date between effective_from and effective_to
                ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такое пердставление уже есть!')

def createDeletedPass():
        try:
                cursor.execute('''
                        CREATE TABLE de2hk.s_19_STG_delete_pass as
                                select
                                        t1.entry_dt,
                                        t1.passport_num
                                from v_pass t1
                                left join de2hk.s_19_STG_passport_blacklist t2
                                on t1.PASSPORT_NUM = t2.passport_num
                                where t2.passport_num is null
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

def changePassHist():
        cursor.execute(''' 
                UPDATE de2hk.s_19_DWH_FACT_pssprt_blcklst
                set effective_to = current_date - 0.001
                where passport_num in (select passport_num from de2hk.s_19_STG_delete_pass)
                and effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
        ''')

def showPassTable():
        print('_-'*20)
        print('de2hk.s_19_DWH_FACT_pssprt_blcklst')
        print('_-'*20)

        cursor.execute('select * from de2hk.s_19_DWH_FACT_pssprt_blcklst')
        print([t[0] for t in cursor.description])
        for row in cursor.fetchall():
                print(row)
        print('\n'*2)

def deletePassTables():
        cursor.execute('DROP TABLE de2hk.s_19_STG_passport_blacklist')
        cursor.execute('DROP TABLE de2hk.s_19_STG_delete_pass')
        cursor.execute('DROP VIEW v_pass')


if __name__ == '__main__':


        xlsxPassSql()
        createPassTable()
        histPassSql()
        createDeletedPass()
        changePassHist()
        showPassTable()
        deletePassTables()


