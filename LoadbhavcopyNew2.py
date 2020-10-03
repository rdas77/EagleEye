# This is beta version for loading file in EagleEye Database
# ....................................................
#Release date 03-oct-2020



import cx_Oracle
import csv
import sys
import os


def LoadBhavcopy( ipfilename ):
    con = cx_Oracle.connect('EQUITY/EQUITY@localhost/XE')
    cur = con.cursor()
    Infile="C:\\Users\\das\\Downloads\\"
    Infile=Infile+ipfilename
    print(Infile)
    print("Inside LoadBhavcopy function ")
#os.path.join('C:\\Users\\das\\Downloads', filename)


    with open(os.path.join('C:\\Users\\das\\Downloads', ipfilename), "r") as csv_file:
     
      #with open(Infile, "r") as csv_file:
         csv_reader = csv.reader(csv_file, delimiter=',')
         next(csv_reader)
         for lines in csv_reader:
         #print("Inside loop") 
             #if sys.argv[2].strip() == 'sec_bhavdata_full.csv' and lines[1].strip() == 'EQ':
              if ipfilename == 'sec_bhavdata_full.csv' and lines[1].strip() == 'EQ':
            #print(lines[1])
            #if lines[1] == 'EQ':
                #print("before insert to daily_nse_all_test --->1") 
                cur.execute("INSERT INTO daily_nse_all_test ( symbol, series, date1, prev_close, open_price, high_price, low_price, last_price, close_price,avg_price, ttl_trd_qnty, turnover_lacs, no_of_trades, deliv_qty, deliv_per) values (:1, :2, :3, :4, :5, :6,:7,:8,:9,:10,:11,:12,:13,:14,:15)",(lines[0].strip(), lines[1].strip(), lines[2].strip(), lines[3].strip(), lines[4].strip(), lines[5].strip(),lines[6].strip(), lines[7].strip(), lines[8].strip(), lines[9].strip(), lines[10].strip(), lines[11].strip(),lines[12].strip(),lines[13].strip(),lines[14].strip()))

            #print(lines[1])
          
             #elif sys.argv[2].strip() != 'sec_bhavdata_full.csv' and lines[1].strip() == 'EQ':
              elif ipfilename != 'sec_bhavdata_full.csv' and lines[1].strip() == 'EQ':  
                #print(lines[1])
                #if lines[1] == 'EQ':    
                   #print(lines[1])
                  cur.execute("INSERT INTO daily_nse_all_test (symbol,series,open_price,high_price,low_price,close_price,last_price,prev_close,ttl_trd_qnty,turnover_lacs,date1,no_of_trades) values (:1, :2, :3, :4, :5, :6,:7,:8,:9,:10,:11,:12)",(lines[0].strip(), lines[1].strip(), lines[2].strip(), lines[3].strip(), lines[4].strip(), lines[5].strip(),lines[6].strip(), lines[7].strip(), lines[8].strip(), lines[9].strip(), lines[10].strip(), lines[11].strip()))

    cur.close()
    con.commit()
    con.close()

def NseMasterUpdate():
    print("Inside NseMasterUpdate function ")
    if os.path.isfile('C:\\Users\\das\\Downloads\\EQUITY_L.csv'):
       con = cx_Oracle.connect('EQUITY/EQUITY@localhost/XE')
       cur = con.cursor()
       cur.execute("truncate table nse_master")
       with open(r'C:\\Users\\das\\Downloads\\EQUITY_L.csv', "r") as csv_file:

            csv_reader = csv.reader(csv_file, delimiter=',')
            next(csv_reader)
            for lines in csv_reader:
                cur.execute("INSERT INTO nse_master (symbol,name_of_comp,series,date_of_listing,paid_up_value,market_lot,isin,face_value)values (:1,:2,:3,:4,:5,:6,:7,:8)",(lines[0].strip(),lines[1].strip(),lines[2].strip(),lines[3].strip(),lines[4].strip(),lines[5].strip(),lines[6].strip(),lines[7].strip()))

       cur.close()
       con.commit()
       con.close()        
    else:
          #except IOError:
     print("File not accessible")

def HelpModule():
    print("The Method of execution of file")
    print("LoadFile.py -UM/LB --FileName")
    print(" UM -Upadate Master ")
    print(" LB -Load BhavCopy  ")
    print("---Example----")
    print("python c:\Migration\python\Scripts\loadbhavcopyNew2.py -LB sec_bhavdata_full.csv")
        
if __name__ == "__main__":


  
  #LoadBhavcopy(sys.argv[1])
  #NseMasterUpdate()
  print(" THe number of arguments " , len(sys.argv))
  print(" The name of first arguments ", sys.argv[0])
  print("The name of  second arguments ", sys.argv[1])
  print("The name of third arguments ", sys.argv[2])
  
if len(sys.argv) <3 and sys.argv[1].upper() == '-H' :

   HelpModule()
   
elif len(sys.argv) == 3 and sys.argv[1].upper() == '-LB' :

    
    LoadBhavcopy(sys.argv[2])

elif len(sys.argv) == 3 and sys.argv[1].upper() == '-UM' :  
    
    NseMasterUpdate()

else :

   HelpModule()

  




 

#EXECUTION python c:\Migration\python\Scripts\loadbhavcopytest.py C:\Users\das\Downloads\cm30SEP2020bhav.csv     
