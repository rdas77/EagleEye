from nsetools import Nse
import json
import cx_Oracle
import sys
nse = Nse()
q = nse.get_quote(sys.argv[1]) 
con = cx_Oracle.connect('EQUITY/EQUITY@127.0.0.1/XE')
cur = con.cursor()
VAR_LAST_ONE_QTY = cur.var(cx_Oracle.NUMBER)
VAR_CURR_TRD_QTY = cur.var(cx_Oracle.NUMBER)
VAR_LAST_FIVE_QTY = cur.var(cx_Oracle.NUMBER)
VAR_CURRENT_PRC = cur.var(cx_Oracle.NUMBER)
VAR_ONE_DAY_RATIO = cur.var(cx_Oracle.NUMBER)
VAR_FIVE_DAY_RATIO = cur.var(cx_Oracle.NUMBER)
cur.callproc('myproc', (json.dumps(q),sys.argv[1],VAR_LAST_ONE_QTY,VAR_CURR_TRD_QTY,VAR_ONE_DAY_RATIO,VAR_LAST_FIVE_QTY,VAR_FIVE_DAY_RATIO,VAR_CURRENT_PRC))
print ("Last one day traded qty: " ,VAR_LAST_ONE_QTY.getvalue())
print ("The current traded qty: " ,VAR_CURR_TRD_QTY.getvalue())
print ("Last one day ratio: " ,VAR_ONE_DAY_RATIO.getvalue())
print ("The last five days traded qty: " ,VAR_LAST_FIVE_QTY.getvalue())
print ("The last five days ratio: " ,VAR_FIVE_DAY_RATIO.getvalue())
print ("The current price: " ,VAR_CURRENT_PRC.getvalue())
cur.close()
