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

# Создание таблицы терминалов

def xlsxTermSql():
        path_list = os.listdir()
        for row in path_list:
                if 'terminals' in row:
                        path = row

        df=pd.read_excel(path)
        
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_STG_terminals(
                                terminal_id varchar(128),
                                terminal_type varchar(128),
                                terminal_city varchar(128),
                                terminal_adress varchar(128)
                        )
                ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

        cursor.executemany('''
                insert into de2hk.s_19_STG_terminals(
                        terminal_id,
                        terminal_type,
                        terminal_city,
                        terminal_adress)
                VALUES (?, ?, ?, ?)
                ''', df.values.tolist())
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_DWH_DIM_terminals_hist(
                                terminal_id varchar(128),
                                terminal_type varchar(128),
                                terminal_city varchar(128),
                                terminal_adress varchar(128),
                                effective_from date default current_date,
                                effective_to date default (to_date('2999-12-31', 'YYYY-MM-DD')),
                                deleted_flg integer default 0                                
                                )
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')
        os.renames(path, 'archive\\'+path+'.blackup')     

def histTerminTable():
        cursor.execute('''
                insert into de2hk.s_19_DWH_DIM_terminals_hist(
                        terminal_id,
                        terminal_type,
                        terminal_city,
                        terminal_adress
                        )
                select 
                        t1.terminal_id,
                        t1.terminal_type,
                        t1.terminal_city,
                        t1.terminal_adress
                from de2hk.S_19_STG_terminals t1
                left join de2hk.s_19_DWH_DIM_terminals_hist t2
                on t1.terminal_id = t2.terminal_id
                where t2.terminal_id is null
                ''')
        try:
                cursor.execute('''
                        CREATE VIEW v_termin_hist as
                                select
                                        terminal_id,
                                        terminal_type,
                                        terminal_city,
                                        terminal_adress
                                from de2hk.s_19_DWH_DIM_terminals_hist
                                where deleted_flg = 0
                                and current_date between effective_from and effective_to
                ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такое пердставление уже есть!')

def createDeletedTermin():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.S_19_STG_delete_terminals_tmp as 
                                select 
                                        t1.terminal_id,
                                        t1.terminal_type,
                                        t1.terminal_city,
                                        t1.terminal_adress
                                from v_termin_hist t1
                                left join de2hk.S_19_STG_terminals t2
                                on t1.terminal_id = t2.terminal_id
                                where t2.terminal_id is null
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')



def change_termin_hist():

        cursor.execute(''' 
                UPDATE de2hk.s_19_DWH_DIM_terminals_hist
                set effective_to = current_date - 0.001
                where terminal_id in (select terminal_id from de2hk.S_19_STG_delete_terminals_tmp)
                and effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
        ''')

        cursor.execute('''
                insert into de2hk.s_19_DWH_DIM_terminals_hist(
                        terminal_id,
                        terminal_type,
                        terminal_city,
                        terminal_adress,
                        deleted_flg
                        )
                select
                      terminal_id,
                        terminal_type,
                        terminal_city,
                        terminal_adress,
                        1
                from de2hk.S_19_STG_delete_terminals_tmp
        ''')


def showTerminTable():
        print('_-'*20)
        print('de2hk.s_19_DWH_DIM_terminals_hist')
        print('_-'*20)

        cursor.execute('select * from de2hk.s_19_DWH_DIM_terminals_hist')
        print([t[0] for t in cursor.description])
        for row in cursor.fetchall():
                print(row)
        print('\n'*2)

def deleteTerminTables():
        cursor.execute('DROP TABLE de2hk.s_19_STG_terminals')
        cursor.execute('DROP TABLE de2hk.S_19_STG_delete_terminals_tmp')
        cursor.execute('DROP VIEW v_termin_hist')

if __name__ == '__main__':



        xlsxTermSql()
        histTerminTable()
        createDeletedTermin()
        change_termin_hist()
        showTerminTable()

        deleteTerminTables()

