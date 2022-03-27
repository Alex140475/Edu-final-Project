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

def  clientsSql():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_STG_clients as
                                select
                                        CLIENT_ID,
                                        LAST_NAME,
                                        FIRST_NAME,
                                        PATRONYMIC,
                                        DATE_OF_BIRTH,
                                        PASSPORT_NUM,
                                        PASSPORT_VALID_TO,
                                        PHONE,
                                        CREATE_DT,
                                        UPDATE_DT
                                from bank.clients                        
                        ''')

        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')


def histclientsSql():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_DWH_DIM_clients_hist(
                                CLIENT_ID varchar(128),
                                LAST_NAME varchar(128),
                                FIRST_NAME varchar(128),
                                PATRONYMIC varchar(128),
                                DATE_OF_BIRTH date,
                                PASSPORT_NUM varchar(128),
                                PASSPORT_VALID_TO date,
                                PHONE varchar(128),
                                effective_from date default current_date,
                                effective_to date default (to_date('2999-12-31', 'YYYY-MM-DD')),
                                deleted_flg integer default 0
                        )
                ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')
        try:
                cursor.execute('''
                        CREATE VIEW v_clients as
                                select
                                        CLIENT_ID,
                                        LAST_NAME,
                                        FIRST_NAME,
                                        PATRONYMIC,
                                        DATE_OF_BIRTH,
                                        PASSPORT_NUM,
                                        PASSPORT_VALID_TO,
                                        PHONE
                                from de2hk.s_19_DWH_DIM_clients_hist
                                where deleted_flg = 0
                                and current_date between effective_from and effective_to
                ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такое представление уже есть!')

def createNewClients():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_STG_new_clients as 
                                select 
                                        t1.CLIENT_ID,
                                        t1.LAST_NAME,
                                        t1.FIRST_NAME,
                                        t1.PATRONYMIC,
                                        t1.DATE_OF_BIRTH,
                                        t1.PASSPORT_NUM,
                                        t1.PASSPORT_VALID_TO,
                                        t1.PHONE
                                from de2hk.s_19_STG_clients t1
                                left join v_clients t2
                                on t1.CLIENT_ID = t2.CLIENT_ID
                                where t2.CLIENT_ID is null
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

def createDeletedClients():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_STG_delete_clients as 
                                select 
                                        t1.CLIENT_ID,
                                        t1.LAST_NAME,
                                        t1.FIRST_NAME,
                                        t1.PATRONYMIC,
                                        t1.DATE_OF_BIRTH,
                                        t1.PASSPORT_NUM,
                                        t1.PASSPORT_VALID_TO,
                                        t1.PHONE
                                from v_clients t1
                                left join de2hk.s_19_STG_clients t2
                                on t1.CLIENT_ID = t2.CLIENT_ID
                                where t2.CLIENT_ID is null
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

def createChangedClients():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_STG_change_clients as 
                                select 
                                        t1.CLIENT_ID,
                                        t1.LAST_NAME,
                                        t1.FIRST_NAME,
                                        t1.PATRONYMIC,
                                        t1.DATE_OF_BIRTH,
                                        t1.PASSPORT_NUM,
                                        t1.PASSPORT_VALID_TO,
                                        t1.PHONE
                                from de2hk.s_19_STG_clients t1
                                inner join v_clients t2
                                on t1.CLIENT_ID = t2.CLIENT_ID
                                and (t1.LAST_NAME       <> t2.LAST_NAME
                                or t1.FIRST_NAME        <> t2.FIRST_NAME
                                or t1.PASSPORT_NUM      <> t2.PASSPORT_NUM
                                or t1.PASSPORT_VALID_TO <> t2.PASSPORT_VALID_TO
                                or t1.PHONE             <> t2.PHONE
                                )
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

def change_hist_clients():
        cursor.execute(''' 
                UPDATE de2hk.s_19_DWH_DIM_clients_hist
                set effective_to = current_date - 0.001
                where CLIENT_ID in (select CLIENT_ID from de2hk.s_19_STG_delete_clients)
                and effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
        ''')

        cursor.execute(''' 
                UPDATE de2hk.s_19_DWH_DIM_clients_hist
                set effective_to = current_date - 0.001
                where CLIENT_ID in (select CLIENT_ID from de2hk.s_19_STG_change_clients)
                and effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
        ''')



        cursor.execute('''
                insert into de2hk.s_19_DWH_DIM_clients_hist (
                        CLIENT_ID,
                        LAST_NAME,
                        FIRST_NAME,
                        PATRONYMIC,
                        DATE_OF_BIRTH,
                        PASSPORT_NUM,
                        PASSPORT_VALID_TO,
                        PHONE                        
                        )
                select 
                        CLIENT_ID,
                        LAST_NAME,
                        FIRST_NAME,
                        PATRONYMIC,
                        DATE_OF_BIRTH,
                        PASSPORT_NUM,
                        PASSPORT_VALID_TO,
                        PHONE
                from de2hk.s_19_STG_new_clients
        ''')

        cursor.execute('''
                insert into de2hk.s_19_DWH_DIM_clients_hist(
                        CLIENT_ID,
                        LAST_NAME,
                        FIRST_NAME,
                        PATRONYMIC,
                        DATE_OF_BIRTH,
                        PASSPORT_NUM,
                        PASSPORT_VALID_TO,
                        PHONE
                        )
                select 
                        CLIENT_ID,
                        LAST_NAME,
                        FIRST_NAME,
                        PATRONYMIC,
                        DATE_OF_BIRTH,
                        PASSPORT_NUM,
                        PASSPORT_VALID_TO,
                        PHONE 
                from de2hk.s_19_STG_change_clients
        ''')

        cursor.execute('''
                insert into de2hk.s_19_DWH_DIM_clients_hist(
                        CLIENT_ID,
                        LAST_NAME,
                        FIRST_NAME,
                        PATRONYMIC,
                        DATE_OF_BIRTH,
                        PASSPORT_NUM,
                        PASSPORT_VALID_TO,
                        PHONE,
                        deleted_flg
                        )
                select 
                        CLIENT_ID,
                        LAST_NAME,
                        FIRST_NAME,
                        PATRONYMIC,
                        DATE_OF_BIRTH,
                        PASSPORT_NUM,
                        PASSPORT_VALID_TO,
                        PHONE,
                        1
                from de2hk.s_19_STG_delete_clients
        ''')






def showClientsTable():
        print('_-'*20)
        print('de2hk.s_19_DWH_DIM_clients_hist')
        print('_-'*20)

        cursor.execute('select * from de2hk.s_19_DWH_DIM_clients_hist')
        print([t[0] for t in cursor.description])
        for row in cursor.fetchall():
                print(row)
        print('\n'*2)

def deleteClientsTables():
        cursor.execute('DROP TABLE de2hk.s_19_STG_clients')
        cursor.execute('DROP TABLE de2hk.s_19_STG_new_clients')
        cursor.execute('DROP VIEW v_clients')
        cursor.execute('DROP TABLE de2hk.s_19_STG_delete_clients')
        cursor.execute('DROP TABLE de2hk.s_19_STG_change_clients')

if __name__ == '__main__':        
        clientsSql()
        histclientsSql()
        createNewClients()
        createDeletedClients()
        createChangedClients()
        change_hist_clients()

        showClientsTable()
        deleteClientsTables()
