import os
import datetime
import cx_Oracle
con = cx_Oracle.connect('EQUITY/EQUITY@127.0.0.1/XE')
cur = con.cursor()
#Current_Date = datetime.datetime.today().strftime ('%d-%b-%Y')
#os.rename(r'C:\Users\das\Downloads\sec_bhavdata_full.csv',r'C:\Migration\Bhavcopy\sec_bhavdata_full_' + str(Current_Date) + '.csv')
os.rename(r'C:\Users\das\Downloads\sec_bhavdata_full.csv',r'C:\Migration\Bhavcopy\sec_bhavdata_full_' + cur.callfunc("lastload_dt_bhavcopy_fnc", cx_Oracle.STRING) + '.csv')
cur.callproc('eq_analysis_vol_up')
cur.callproc('eq_analysis_vol_up')
cur.close()