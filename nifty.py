import sys
from nsetools import Nse
import json
import cx_Oracle
import sys
nse = Nse()
##NIFTY = ['TCS','WIPRO','HCLTECH','TECHM','COALINDIA','HINDPETO','INFY','INFRATEL','INDUSINDBK','ONGC','ADANIPORTS','LUPIN','CIPLA','TATASTEEL','BOSCHLTD','HINDUNILVR','RELIANCE','ZEEL','MARUTI','AUROPHARMA','HEROMOTOCO','HDFCBANK','SUNPHARMA','M&M','AXISBANK','ICICIBANK','AMBUJACEM','ITC','POWERGRID','BPCL','ULTRACEMCO','GAIL','BHARTIARTL','LT','IOC','YESBANK','KOTAKBANK','HDFC','IBULHSGFIN','VEDL','HINDALCO','BAJFINANCE','SBIN','TATAMOTORS','BAJAJ-AUTO','ASIANPAINT','DRREDDY','NTPC','EICHERMOT','UPL']
 
 q = nse.get_quote('TCS') 
con = cx_Oracle.connect('EQUITY/EQUITY@127.0.0.1/XE')
cur = con.cursor()
VAR_LAST_ONE_QTY = cur.var(cx_Oracle.NUMBER)
VAR_CURR_TRD_QTY = cur.var(cx_Oracle.NUMBER)
VAR_LAST_FIVE_QTY = cur.var(cx_Oracle.NUMBER)
VAR_CURRENT_PRC = cur.var(cx_Oracle.NUMBER)
VAR_ONE_DAY_RATIO = cur.var(cx_Oracle.NUMBER)
VAR_FIVE_DAY_RATIO = cur.var(cx_Oracle.NUMBER)
cur.callproc('myproc', (json.dumps(q),'TCS',VAR_LAST_ONE_QTY,VAR_CURR_TRD_QTY,VAR_ONE_DAY_RATIO,VAR_LAST_FIVE_QTY,VAR_FIVE_DAY_RATIO,VAR_CURRENT_PRC))
q = nse.get_quote('WIPRO') 
cur.callproc('myproc', (json.dumps(q),'WIPRO',VAR_LAST_ONE_QTY,VAR_CURR_TRD_QTY,VAR_ONE_DAY_RATIO,VAR_LAST_FIVE_QTY,VAR_FIVE_DAY_RATIO,VAR_CURRENT_PRC))
cur.close()