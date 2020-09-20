from nsetools import Nse
import json
import cx_Oracle
import sys
nse = Nse()
q = nse.get_quote('M&MFIN') 
con = cx_Oracle.connect('EQUITY/EQUITY@127.0.0.1/XE')
cur = con.cursor()
VAR_LAST_ONE_QTY = cur.var(cx_Oracle.NUMBER)
VAR_CURR_TRD_QTY = cur.var(cx_Oracle.NUMBER)
VAR_LAST_FIVE_QTY = cur.var(cx_Oracle.NUMBER)
VAR_CURRENT_PRC = cur.var(cx_Oracle.NUMBER)
VAR_ONE_DAY_RATIO = cur.var(cx_Oracle.NUMBER)
VAR_FIVE_DAY_RATIO = cur.var(cx_Oracle.NUMBER)
cur.callproc('NSE_MOVEMENT_TRANS_PRC', (json.dumps(q),'M&MFIN',VAR_LAST_ONE_QTY,VAR_CURR_TRD_QTY,VAR_ONE_DAY_RATIO,VAR_LAST_FIVE_QTY,VAR_FIVE_DAY_RATIO,VAR_CURRENT_PRC))
cur.close()