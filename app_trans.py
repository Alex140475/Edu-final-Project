import jaydebeapi
import sqlite3
import pandas as pd
import openpyxl
import datetime
import os

# Создание таблицы транзакций

conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar' )

cursor = conn.cursor()

def txtTransSql():
        path_list = os.listdir()
        for row in path_list:
                if 'transactions' in row:
                        path = row

        df=pd.read_csv(path, sep=';')
        df = df.astype(str)

        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_STG_transaktions(
                                transaction_id varchar(128),
                                transaction_date varchar(128),
                                amount varchar(128),
                                card_num varchar(128),
                                oper_type varchar(128),
                                oper_result varchar(128),
                                terminal varchar(128)
                        )
                ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

        cursor.executemany('''
                insert into de2hk.s_19_STG_transaktions(
                        transaction_id,
                        transaction_date,
                        amount,
                        card_num,
                        oper_type,
                        oper_result,
                        terminal
                )VALUES(?, ?, ?, ?, ?, ?, ?)
                ''', df.values.tolist())

        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_DWH_FACT_transaktions(
                                transaction_id varchar(128) primary key,
                                transaction_date varchar(128),
                                amount varchar(128),
                                card_num varchar(128),
                                oper_type varchar(128),
                                oper_result varchar(128),
                                terminal varchar(128)                                
                                )
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')  
        os.renames(path, 'archive\\'+path+'.blackup')      

def histTransSql():
        try:
                cursor.execute('''
                        insert into de2hk.s_19_DWH_FACT_transaktions(
                                transaction_id,
                                transaction_date,
                                amount,
                                card_num,
                                oper_type,
                                oper_result,
                                terminal
                                )
                        select 
                                transaction_id,
                                transaction_date,
                                amount,
                                card_num,
                                oper_type,
                                oper_result,
                                terminal
                        from de2hk.S_19_STG_transaktions
                ''')
        except jaydebeapi.DatabaseError:
                print('Повторная загрузка файла')

def showTransTable():
        print('_-'*20)
        print('de2hk.s_19_DWH_FACT_transaktions')
        print('_-'*20)

        cursor.execute('select * from de2hk.s_19_DWH_FACT_transaktions')
        print([t[0] for t in cursor.description])
        for row in cursor.fetchall():
                print(row)
        print('\n'*2)

def dropTransTables():
        cursor.execute('DROP TABLE de2hk.s_19_STG_transaktions')

def deleteTransTables():
        try:
                cursor.execute('TRUNCATE TABLE de2hk.s_19_DWH_FACT_transaktions')
        except jaydebeapi.DatabaseError:
                print('Таблица ещё не создана')
        

if __name__ == '__main__':

        deleteTransTables()
        txtTransSql()
        histTransSql()
        showTransTable()
        dropTransTables()
