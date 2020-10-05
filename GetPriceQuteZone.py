# This is beta version for get price quoute 
# ....................................................
#Release date 03-oct-2020



import cx_Oracle
import csv
import sys
import os
from nsetools import Nse
import json

def GetPriceSymbol( ):
    nse = Nse()
    with open(r'C:\\Migration\\EagleEye\\Master\\Trading_Mast_zone2.csv',"r") as csv_file:
         csv_reader = csv.reader(csv_file, delimiter=',')
         #next(csv_reader)
         for lines in csv_reader:
             print("The symbol processing is",lines[0])
             q = nse.get_quote(lines[0].strip())
             parsed_json = json.loads(json.dumps(q))
             l_lastprice=parsed_json['lastPrice']
             #print("The last price is ",l_lastprice)
             l_averagePrice=parsed_json['averagePrice']
             #print("The average price is ",l_averagePrice)
             l_totaltradedvolume=parsed_json['totalTradedVolume']
             #print("The total traded volume is ",l_totaltradedvolume)
             l_max_price_n=parsed_json['dayHigh']
             #print("The max price for the symbol ",l_max_price_n)
             l_min_price_n=parsed_json['dayLow']
             #print("The min price for the symbol ",l_min_price_n)
             l_open_price_n=parsed_json['open']
             #print("The open price for the symbol ",l_open_price_n)
             con = cx_Oracle.connect('EQUITY/EQUITY@localhost/XE')
             cur = con.cursor()
             cur.execute("INSERT INTO daily_nse_movement_trans (symbol,lastprice_n,last_avg_price_n,totaltradedvol_n,max_price_n,min_price_n,open_price_n)  VALUES (:1,:2,:3,:4,:5,:6,:7)" ,(lines[0].strip(),l_lastprice, l_averagePrice,l_totaltradedvolume,l_max_price_n,l_min_price_n,l_open_price_n))
             statement = 'DELETE FROM daily_nse_movement_trans WHERE  EXCEPTION_FLAG_V=:type'
             cur.execute(cur.execute(statement, {'type':'Y'}))
             cur.close()
             con.commit()
             #print("End of the loop ---")
             con.close()
             #print("End of the loop ---")
             #except:
                 #rollback()

def DelException():
    print('This is inside of exception module')
    con = cx_Oracle.connect('EQUITY/EQUITY@localhost/XE')
    cur = con.cursor()
    statement = 'DELETE FROM daily_nse_movement_trans WHERE  EXCEPTION_FLAG_V=:type'
    cur.execute(cur.execute(statement, {'type':'Y'}))
    con.commit()
    con.close()
    
def HelpModule():
    print("The Method of execution of file")
    print("LoadFile.py -UM/LB --FileName")
    print(" UM -Upadate Master ")
    print(" LB -Load BhavCopy  ")
    print("---Example----")
    print("python c:\Migration\python\Scripts\loadbhavcopyNew2.py -LB sec_bhavdata_full.csv")
        
if __name__ == "__main__":

   DelException() 
   GetPriceSymbol()
   
   
   
  
  

  




 

#EXECUTION python c:\Migration\python\Scripts\loadbhavcopytest.py C:\Users\das\Downloads\cm30SEP2020bhav.csv     
