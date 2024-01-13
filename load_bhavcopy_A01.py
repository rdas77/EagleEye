# This is beta version for loading file in EagleEye Database
# ....................................................
#Release date 03-oct-2020
#Newer verison of bhavcopy using python data frame
#Release date january 13 



import cx_Oracle
import csv
import sys
import os
import nsepython as np 

def LoadBhavcopy( ipdate  ):
    #print (ipfilename)
    
    con = cx_Oracle.connect('EQUITY/EQUITY@localhost/XE')
    cur = con.cursor()
    #Infile="C:\\Users\\Admin\\Downloads\\"
    #Infile=Infile+ipfilename
    #print(Infile)
    print("Inside LoadBhavcopy function ")
    bhav=np.get_bhavcopy(ipdate)
    bhav.columns = bhav.columns.str.strip()
    #Trim the white spaces 
    bhav = bhav.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
    bhav_1 = bhav[bhav['SERIES'] == 'EQ']
    #list(bhav.columns))
    #['AVG_PRICE', 'NO_OF_TRADES', 'DELIV_QTY', 'DELIV_PER'] this columns aren not required from data frame 
    for i , j in bhav_1.iterrows():
        cur.execute("INSERT INTO daily_nse_all ( symbol,series,open_price,high_price,low_price,last_price,close_price,prev_close,ttl_trd_qnty,turnover_lacs,date1) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)",(j['SYMBOL'],j['SERIES'],j['OPEN_PRICE'],j['HIGH_PRICE'],j['LOW_PRICE'],j['LAST_PRICE'],j['CLOSE_PRICE'],j['PREV_CLOSE'],j['TTL_TRD_QNTY'],j['TURNOVER_LACS']*100000,j['DATE1']))
        print('The symbol loaded~~',j['SYMBOL'])
     
           
    cur.callproc('eq_analysis_vol_up')
    cur.close()
    con.commit()
    con.close()

def NseMasterUpdate():
	#https://www.nseindia.com/market-data/securities-available-for-trading
    print("Inside NseMasterUpdate function ")
    if os.path.isfile('C:\\Users\\Admin\\Downloads\\EQUITY_L.csv'):
       con = cx_Oracle.connect('EQUITY/EQUITY@localhost/XE')
       cur = con.cursor()
       cur.execute("truncate table nse_master")
       with open(r'C:\\Users\\Admin\\Downloads\\EQUITY_L.csv', "r") as csv_file:

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
    print("python c:\Migration\python\Scripts\Load_Bahvcopy.py -LB Date in dd-mm-yyyy format")
        
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





 

