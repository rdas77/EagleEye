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
         #print("Inside loop") 
         if sys.argv[1] == 'sec_bhavdata_full.csv' and lines[1].strip() == 'EQ': 
            #print(lines[1])
            #if lines[1] == 'EQ':
            #print("before insert to daily_nse_all_test") 
             cur.execute("INSERT INTO daily_nse_all_test ( symbol, series, date1, prev_close, open_price, high_price, low_price, last_price, close_price,avg_price, ttl_trd_qnty, turnover_lacs, no_of_trades, deliv_qty, deliv_per) values (:1, :2, :3, :4, :5, :6,:7,:8,:9,:10,:11,:12,:13,:14,:15)",(lines[0].strip(), lines[1].strip(), lines[2].strip(), lines[3].strip(), lines[4].strip(), lines[5].strip(),lines[6].strip(), lines[7].strip(), lines[8].strip(), lines[9].strip(), lines[10].strip(), lines[11].strip(),lines[12].strip(),lines[13].strip(),lines[14].strip()))

            #print(lines[1])
          
         elif sys.argv[1] != 'sec_bhavdata_full.csv' and lines[1].strip() == 'EQ':
                #print(lines[1])
                #if lines[1] == 'EQ':    
                   #print(lines[1])
              cur.execute("INSERT INTO daily_nse_all_test (symbol,series,open_price,high_price,low_price,close_price,last_price,prev_close,ttl_trd_qnty,turnover_lacs,date1,no_of_trades) values (:1, :2, :3, :4, :5, :6,:7,:8,:9,:10,:11,:12)",(lines[0].strip(), lines[1].strip(), lines[2].strip(), lines[3].strip(), lines[4].strip(), lines[5].strip(),lines[6].strip(), lines[7].strip(), lines[8].strip(), lines[9].strip(), lines[10].strip(), lines[11].strip()))

cur.close()
con.commit()
con.close()

 

#EXECUTION python c:\Migration\python\Scripts\loadbhavcopytest.py C:\Users\das\Downloads\cm30SEP2020bhav.csv     
