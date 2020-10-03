import cx_Oracle
import csv
import sys
import os

con = cx_Oracle.connect('EQUITY/EQUITY@localhost/XE')
cur = con.cursor()
Infile="C:\\Users\\das\\Downloads\\"
Infile=Infile+sys.argv[1]
print(Infile)

#os.path.join('C:\\Users\\das\\Downloads', filename)


with open(os.path.join('C:\\Users\\das\\Downloads', sys.argv[1]), "r") as csv_file:
     
 #with open(Infile, "r") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    next(csv_reader)
    for lines in csv_reader:

       if lines[1] == 'EQ':

        #print(lines[1])

        cur.execute("insert into DAILY_NSE_ALL_TEST (SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN) values (:1, :2, :3, :4, :5, :6,:7,:8,:9,:10,:11,:12,:13)",(lines[0], lines[1], lines[2], lines[3], lines[4], lines[5],lines[6], lines[7], lines[8], lines[9], lines[10], lines[11],lines[12]))

cur.close()
con.commit()
con.close()




     

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
