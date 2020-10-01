import os
import sys
import csv
import cx_Oracle


db = cx_Oracle.connect('EQUITY/EQUITY@localhost:1521/XE')

cursor = db.cursor()

reader = csv.reader(open(r"C:\Users\das\Downloads\cm01OCT2020bhav.csv", "r"))
next(reader)
#lines = []
for line in reader:
     #lines.append(line)

     cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'dd-mon-yyyy' ")
     #cursor.executemany("INSERT INTO daily_nse_all_test VALUES( :1,:2,:3, :4,:5,:6,:7,:8,:9,:10,:11 ,:12,:13)" , lines)
     cursor.execute("INSERT INTO daily_nse_all_test VALUES( :1,:2,:3, :4,:5,:6,:7,:8,:9,:10,:11 ,:12,:13)" , line)
     #cursor.executemany("INSERT INTO daily_nse_all_test (SYMBOL) values( :1)" , lines)
     db.commit()

#import datetime
#import cx_Oracle
#con = cx_Oracle.connect('EQUITY/EQUITY@127.0.0.1/XE')
#cur = con.cursor()
#Current_Date = datetime.datetime.today().strftime ('%d-%b-%Y')
#filedir = "C:\\Users\\das\\Downloads\\"
#source = os.listdir("C:\Users\das\Downloads\")
#print(source)
#print(filedir)
#fileIn  = filedir + sys.argv[1]
#print(fileIn)
#new_file_name = "//home//career_karma//old_data.csv"
#print(sys.argv[1])
#os.rename(sys.argv[1] , 'C:\Users\das\Downloads\sec_bhavdata_full.csv')
#os.rename(r'C:\Users\das\Downloads\sec_bhavdata_full.csv',r'C:\Migration\Bhavcopy\sec_bhavdata_full_' + cur.callfunc("lastload_dt_bhavcopy_fnc", cx_Oracle.STRING) + '.csv')
#cur.callproc('eq_analysis_vol_up')
#cur.callproc('eq_analysis_vol_up')
#cur.close()
#subprocess.call('sqlldr userid=EQUITY/EQUITY@XE control=C:\Migration\python\Scripts\bhavcopynew.ctl', shell=True)

#EXECUTION python c:\Migration\python\Scripts\loadbhavcopytest.py C:\Users\das\Downloads\cm30SEP2020bhav.csv     
