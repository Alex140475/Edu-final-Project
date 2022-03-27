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


def fraudTable():
	try:
		cursor.execute('''
			CREATE TABLE de2hk.s_19_REP_FRAUD(
				id integer primary key,
				event_dt varchar(128),
				fio varchar(128),
				phone varchar(128),
				passport_num varchar(128),
				event_type varchar(128),
				report_dt date default current_date
                        )
            ''')
	except jaydebeapi.DatabaseError:
		print('[+] Такая таблица уже есть!')
	try:
		cursor.execute(''' CREATE SEQUENCE seq_s_19 START WITH 1''')
	except jaydebeapi.DatabaseError:
		print('[+] Такой SEQUENCE уже есть!')


def fraudPass():
	try:
		cursor.execute('''
			CREATE TABLE de2hk.s_19_STG_pass_blac as 
				select distinct
					t1.transaction_date as event_dt,
					t1.LAST_NAME ||' '|| t1.FIRST_NAME ||' '|| t1.PATRONYMIC as fio,
					t1.PHONE,
					t1.PASSPORT_NUM
				from de2hk.s_19_META_INFO t1
				inner join de2hk.s_19_DWH_FACT_pssprt_blcklst t2
				on t1.PASSPORT_NUM = t2.PASSPORT_NUM
				where t2.PASSPORT_NUM is not null
				and oper_result = 'SUCCESS'
			''')
	except jaydebeapi.DatabaseError:
		print('[+] Такая таблица уже есть!')

def fraudAccount():
	try:
		cursor.execute('''
			CREATE VIEW de2hk.s_19_STG_account_stop as
				select distinct
					transaction_date as event_dt,
					LAST_NAME ||' '|| FIRST_NAME ||' '|| PATRONYMIC as fio,
					PHONE,
					PASSPORT_NUM
				from de2hk.s_19_META_INFO
				where VALID_TO < to_date('2021-03-03', 'YYYY-MM-DD')
				and oper_result = 'SUCCESS'
			''')

	except jaydebeapi.DatabaseError:
		print('[+] Такое представление уже есть!')

def fraudSity():
        try:
                cursor.execute('''
                        CREATE VIEW de2hk.s_19_STG_sity as
                                select distinct
                                        t1.card_num,
                                        t1.transaction_date as event_dt,
                                        t1.LAST_NAME ||' '|| t1.FIRST_NAME ||' '|| t1.PATRONYMIC as fio,
                                        t1.PHONE,
                                        t1.PASSPORT_NUM,
                                        t3.terminal_city
                                from de2hk.s_19_META_INFO t1, de2hk.s_19_DWH_FACT_transaktions t2, de2hk.s_19_DWH_DIM_terminals_hist t3
                                where t1.card_num = t2.card_num
                                and t2.terminal = t3.terminal_id
                                and t1.terminal_city <> t3.terminal_city
                                and (to_date(to_char(t1.transaction_date), 'YYYY-MM-DD HH24:MI:SS') - to_date(to_char(t2.transaction_date), 'YYYY-MM-DD HH24:MI:SS'))*24*60 <= 60
                                
                                
                        ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такое представление уже есть!')

def fraudAmount():
        try:
        
                cursor.execute('''
                UPDATE de2hk.s_19_META_INFO
                SET amount = replace(amount, ',', '.')
                ''')

                cursor.execute('''
                UPDATE de2hk.s_19_DWH_FACT_transaktions
                SET amount = replace(amount, ',', '.')
                ''')

                cursor.execute('''
                
                        CREATE table de2hk.s_19_STG_amount as
                                select distinct
                                        t1.transaction_id,
                                        t1.card_num,
                                        t1.amount,
                                        t1.oper_result,
                                        t1.transaction_date,
                                        t1.LAST_NAME,
                                        t1.FIRST_NAME,
                                        t1.PATRONYMIC,
                                        t1.PHONE,
                                        t1.PASSPORT_NUM
                                from de2hk.s_19_META_INFO t1, de2hk.s_19_META_INFO t2
                                where t1.card_num = t2.card_num
                                and t2.transaction_id in (select transaction_id from de2hk.s_19_META_INFO where oper_result = 'REJECT')
                                and to_timestamp(to_char(t1.transaction_date), 'YYYY-MM-DD HH24:MI:SS') - to_timestamp(to_char(t2.transaction_date), 'YYYY-MM-DD HH24:MI:SS') <= INTERVAL '0 0:20:00' DAY TO SECOND
                                order by t1.card_num, t1.transaction_id
                
                                
                                ''')
        except jaydebeapi.DatabaseError:
                print('[+] Такая таблица уже есть!')

        try:

                cursor.execute('''
                        CREATE VIEW de2hk.s_19_STG_sum as
                                select 
                                        transaction_id,
                                        card_num,
                                        amount,
                                        oper_result,
                                        transaction_date as event_dt,
                                        LAST_NAME ||' '|| FIRST_NAME ||' '|| PATRONYMIC as fio,
                                        PHONE,
                                        PASSPORT_NUM,
                                        lag(amount, 1) over(partition by card_num order by transaction_id) as lag_1,
                                        lag(amount, 2) over(partition by card_num order by transaction_id) as lag_2,
                                        lag(amount, 3) over(partition by card_num order by transaction_id) as lag_3,
                                        lag(oper_result, 1) over(partition by card_num order by transaction_id) as oper_1,
                                        lag(oper_result, 2) over(partition by card_num order by transaction_id) as oper_2,
                                        lag(oper_result, 3) over(partition by card_num order by transaction_id) as oper_3
                                from de2hk.s_19_STG_amount
                        ''')

        except jaydebeapi.DatabaseError:
                print('[+] Такое представление уже есть!')
                
        
def fraudInsert():
	cursor.execute('''
		insert into de2hk.s_19_REP_FRAUD(
			id,
			event_dt,
			fio,
			phone,
			passport_num,
			event_type
			)
		select
			seq_s_19.NEXTVAL,
			event_dt,
			fio,
			PHONE,
			PASSPORT_NUM,
			'Pass in blacklist'
		from de2hk.s_19_STG_pass_blac
		
		''')

	cursor.execute('''
		insert into de2hk.s_19_REP_FRAUD(
			id,
			event_dt,
			fio,
			phone,
			passport_num,
			event_type
			)
		select
			seq_s_19.NEXTVAL, 
			event_dt,
			fio,
			PHONE,
			PASSPORT_NUM,
			'Операции при недействующем договоре'
		from de2hk.s_19_STG_account_stop

		''')

	cursor.execute('''
		insert into de2hk.s_19_REP_FRAUD(
			id,
			event_dt,
			fio,
			phone,
			passport_num,
			event_type
			)
		select
			seq_s_19.NEXTVAL, 
			event_dt,
			fio,
			PHONE,
			PASSPORT_NUM,
			'Операции в разных городах'
		from de2hk.s_19_STG_sity

		''')

	cursor.execute('''
		insert into de2hk.s_19_REP_FRAUD(
			id,
			event_dt,
			fio,
			phone,
			passport_num,
			event_type
			)
                select
                	seq_s_19.NEXTVAL,
                        event_dt,
                        fio,
                        PHONE,
                        PASSPORT_NUM,
                        'Попытка подбора суммы'
                from de2hk.s_19_STG_sum
                where amount < lag_1
                and lag_1 < lag_2
                and lag_2 < lag_3
                and oper_result = 'SUCCESS'
                and oper_1 = 'REJECT'
                and oper_2 = 'REJECT'
                and oper_3 = 'REJECT'
                ''')
        
def showFraudTable():
	try:
		print('_-'*20)
		print('de2hk.s_19_REP_FRAUD')
		print('_-'*20)

		cursor.execute('select * from de2hk.s_19_REP_FRAUD')
		print([t[0] for t in cursor.description])
		for row in cursor.fetchall():
			print(row)
	except jaydebeapi.DatabaseError:
		print('\n'*2)

def deleteTmpTables():
	cursor.execute('DROP TABLE de2hk.s_19_STG_pass_blac')
	cursor.execute('DROP VIEW de2hk.s_19_STG_account_stop')
	cursor.execute('DROP VIEW de2hk.s_19_STG_sity')
	cursor.execute('DROP table de2hk.s_19_STG_amount')
	cursor.execute('DROP VIEW de2hk.s_19_STG_sum')
	cursor.execute('DROP TABLE de2hk.s_19_META_INFO')
    	

if __name__ == '__main__':

	fraudTable()
	fraudPass()
	fraudAccount()
	fraudSity()
	fraudAmount()
	fraudInsert()
	showFraudTable()

	deleteTmpTables()

