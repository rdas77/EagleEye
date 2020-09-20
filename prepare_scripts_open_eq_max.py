import cx_Oracle
import sys
con = cx_Oracle.connect('EQUITY/EQUITY@127.0.0.1/XE')
cur = con.cursor()
cur.callproc('PREPARE_SCRIPT_OPEN_MAX_PRC')
cur.close()