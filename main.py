import jaydebeapi
import sqlite3
import pandas as pd
import openpyxl
import datetime
import app_trans
import app_termin
import app_meta
import app_event
import app_clients
import app_cards
import app_account
import app_pass
import os


conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:de2hk/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
        ['de2hk', 'bilbobaggins'],
        'ojdbc7.jar' )

cursor = conn.cursor()

if not os.access('archive', os.F_OK):
        os.mkdir('archive')


app_pass.xlsxPassSql()
app_pass.createPassTable()
app_pass.histPassSql()
app_pass.createDeletedPass()
app_pass.changePassHist()
app_pass.showPassTable()
app_pass.deletePassTables()

app_trans.deleteTransTables()
app_trans.txtTransSql()
app_trans.histTransSql()
app_trans.showTransTable()
app_trans.dropTransTables()

app_termin.xlsxTermSql()
app_termin.histTerminTable()
app_termin.createDeletedTermin()
app_termin.change_termin_hist()
app_termin.showTerminTable()
app_termin.deleteTerminTables()

app_clients.clientsSql()
app_clients.histclientsSql()
app_clients.createNewClients()
app_clients.createDeletedClients()
app_clients.createChangedClients()
app_clients.change_hist_clients()
app_clients.showClientsTable()
app_clients.deleteClientsTables()


app_cards.cardsSql()
app_cards.insCardsSql()
app_cards.histCardsSql()
app_cards.createNewCards()
app_cards.createDeletedCards()
app_cards.createChangedCards()
app_cards.change_hist_cards()
app_cards.showCardsTable()
app_cards.deleteCardTables()

app_account.accountsSql()
app_account.histAccountSql()
app_account.createNewAccount()
app_account.createDeletedAccount()
app_account.change_hist_account()
app_account.showAccountTable()
app_account.deleteAccountTables()

app_meta.cangeTable()
app_meta.evenSTGTable()
app_meta.metaTable()
app_meta.showMetaTable()
app_meta.deleteMetaTables()

app_event.fraudTable()
app_event.fraudPass()
app_event.fraudAccount()
app_event.fraudSity()
app_event.fraudAmount()
app_event.fraudInsert()
app_event.showFraudTable()
app_event.deleteTmpTables()

#cursor.execute('DROP TABLE de2hk.s_19_DWH_FACT_pssprt_blcklst')
#cursor.execute('DROP TABLE de2hk.s_19_DWH_FACT_transaktions')
#cursor.execute('DROP TABLE de2hk.s_19_DWH_DIM_terminals_hist')
#cursor.execute('DROP TABLE de2hk.s_19_DWH_DIM_clients_hist')
#cursor.execute('DROP TABLE de2hk.s_19_DWH_DIM_cards_hist')
#cursor.execute('DROP TABLE de2hk.s_19_DWH_DIM_accounts_hist')
#cursor.execute('DROP TABLE de2hk.s_19_META_INFO')