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

def  cardsSql():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_STG_cards(
                                card_num varchar(128),
                                account varchar(128),
                                create_dt date,
                                update_dt date
                                )
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

def insCardsSql():
        cursor.execute('''
                insert into de2hk.s_19_STG_cards(
                        card_num,
                        account,
                        create_dt,
                        update_dt
                        )
                select 
                        card_num,
                        account,
                        create_dt,
                        update_dt
                from bank.cards
        ''')
def histCardsSql():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_DWH_DIM_cards_hist(
                                card_num varchar(128),
                                account varchar(128),
                                effective_from date default current_date,
                                effective_to date default (to_date('2999-12-31', 'YYYY-MM-DD')),
                                deleted_flg integer default 0
                        )
                ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')
        try:
                cursor.execute('''
                        CREATE VIEW v_cards as
                                select
                                        card_num,
                                        account
                                from de2hk.s_19_DWH_DIM_cards_hist
                                where deleted_flg = 0
                                and current_date between effective_from and effective_to
                ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такое представление уже есть!')

def createNewCards():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_STG_new_cards as 
                                select 
                                        t1.card_num,
                                        t1.account
                                from de2hk.s_19_STG_cards t1
                                left join v_cards t2
                                on t1.card_num = t2.card_num
                                where t2.card_num is null
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

def createDeletedCards():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_STG_delete_cards as 
                                select 
                                        t1.card_num,
                                        t1.account
                                from v_cards t1
                                left join de2hk.s_19_STG_cards t2
                                on t1.card_num = t2.card_num
                                where t2.card_num is null
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

def createChangedCards():
        try:
                cursor.execute(''' 
                        CREATE TABLE de2hk.s_19_STG_change_cards as 
                                select 
                                        t1.card_num,
                                        t1.account
                                from de2hk.s_19_STG_cards t1
                                inner join v_cards t2
                                on t1.card_num = t2.card_num
                                and t1.account <> t2.account
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

def change_hist_cards():
        cursor.execute(''' 
                UPDATE de2hk.s_19_DWH_DIM_cards_hist
                set effective_to = current_date - 0.001
                where account in (select account from de2hk.s_19_STG_delete_cards)
                and effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
        ''')

        cursor.execute(''' 
                UPDATE de2hk.s_19_DWH_DIM_cards_hist
                set effective_to = current_date - 0.001
                where account in (select account from de2hk.s_19_STG_change_cards)
                and effective_to = to_date('2999-12-31', 'YYYY-MM-DD')
        ''')



        cursor.execute('''
                insert into de2hk.s_19_DWH_DIM_cards_hist (
                        card_num,
                        account
                        )
                select 
                        card_num,
                        account
                from de2hk.s_19_STG_new_cards
        ''')

        cursor.execute('''
                insert into de2hk.s_19_DWH_DIM_cards_hist(
                        card_num,
                        account
                        )
                select 
                        card_num,
                        account
                from de2hk.s_19_STG_change_cards
        ''')

        cursor.execute('''
                insert into de2hk.s_19_DWH_DIM_cards_hist(
                        card_num,
                        account,
                        deleted_flg
                        )
                select 
                        card_num,
                        account,
                        1
                from de2hk.s_19_STG_delete_cards
        ''')
def showCardsTable():
        print('_-'*20)
        print('de2hk.s_19_DWH_DIM_cards_hist')
        print('_-'*20)

        cursor.execute('select * from de2hk.s_19_DWH_DIM_cards_hist')
        print([t[0] for t in cursor.description])
        for row in cursor.fetchall():
                print(row)
        print('\n'*2)

def deleteCardTables():
        cursor.execute('DROP TABLE de2hk.s_19_STG_cards')
        cursor.execute('DROP TABLE de2hk.s_19_STG_new_cards')
        cursor.execute('DROP VIEW v_cards')
        cursor.execute('DROP TABLE de2hk.s_19_STG_delete_cards')
        cursor.execute('DROP TABLE de2hk.s_19_STG_change_cards')
        

if __name__ == '__main__':



        cardsSql()
        insCardsSql()
        histCardsSql()
        createNewCards()
        createDeletedCards()
        createChangedCards()
        change_hist_cards()
        showCardsTable()
        
        deleteCardTables()
        