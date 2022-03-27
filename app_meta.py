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


def cangeTable():
        
        cursor.execute('''
                UPDATE de2hk.s_19_DWH_FACT_transaktions
                SET card_num = replace(card_num, ' ')
                ''')
        
        cursor.execute('''
                UPDATE de2hk.s_19_DWH_DIM_cards_hist
                SET card_num = replace(card_num, ' ')
                ''')

def evenSTGTable():
        try:
                cursor.execute('''
                        CREATE TABLE de2hk.s_19_STG_REP_FRAUD_TRANS as
                                select
                                        t1.transaction_id,
                                        t1.transaction_date,
                                        t1.card_num,
                                        t1.oper_type,
                                        t1.amount,
                                        t1.oper_result,
                                        t1.terminal,
                                        t2.terminal_city,
                                        t3.account
                                from de2hk.s_19_DWH_FACT_transaktions t1
                                inner join de2hk.s_19_DWH_DIM_terminals_hist t2
                                on t1.terminal = t2.terminal_id
                                inner join de2hk.s_19_DWH_DIM_cards_hist t3
                                on t1.card_num = t3.card_num
                        ''')        
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')


def metaTable():
        try:
                cursor.execute('''
                        CREATE TABLE de2hk.s_19_META_INFO as
                                select
                                        t1.transaction_id,
                                        t1.transaction_date,
                                        t1.card_num,
                                        t1.oper_type,
                                        t1.amount,
                                        t1.oper_result,
                                        t1.terminal,
                                        t1.terminal_city,
                                        t1.account,
                                        t2.VALID_TO,
                                        t2.CLIENT,
                                        t3.CLIENT_ID,
                                        t3.LAST_NAME,
                                        t3.FIRST_NAME,
                                        t3.PATRONYMIC,
                                        t3.PASSPORT_NUM,
                                        t3.PHONE
                                from de2hk.s_19_STG_REP_FRAUD_TRANS t1
                                inner join de2hk.s_19_DWH_DIM_accounts_hist t2
                                        on t1.account = t2.ACCOUNT
                                inner join de2hk.s_19_DWH_DIM_clients_hist t3
                                        on t2.CLIENT = t3.CLIENT_ID
                                order by transaction_date
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

def showMetaTable():
        print('_-'*20)
        print('de2hk.s_19_META_INFO')
        print('_-'*20)

        cursor.execute('select * from de2hk.s_19_META_INFO')
        print([t[0] for t in cursor.description])
        for row in cursor.fetchall():
                print(row)
        print('\n'*2)

def deleteMetaTables():
        cursor.execute('DROP TABLE de2hk.s_19_STG_REP_FRAUD_TRANS')
        
if __name__ == '__main__':



        cangeTable()
        evenSTGTable()
        metaTable()

        showMetaTable()

        deleteMetaTables()
