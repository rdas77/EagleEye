import schedule
import time
import cx_Oracle
import csv
import sys
import os
#from nsetools import Nse
import nsepython as np
import json
from datetime import datetime
  
# Functions setup
def sudo_placement():
    print("Get ready for Sudo Placement at Geeksforgeeks")
  
def good_luck():
    print("Good Luck for Test")
  
def work():
    print("Study and work hard")
  
def bedtime():
    print("It is bed time go rest")
      
def EagleEye_job1():
    #nse = Nse()
    list_symbol = np.fnolist()
    #with open(r'D:\\EagleEye\\Script\\Trading_Mast_zone2.csv',"r") as csv_file:
    #csv_reader = csv.reader(csv_file, delimiter=',')
    #next(csv_reader)
    for lines in list_symbol:
        #print("The symbol processing is",lines)
        try:
            if lines not in ('NIFTY','NIFTYIT','BANKNIFTY' ):
                q = np.nsetools_get_quote(lines)
                parsed_json = json.loads(json.dumps(q))
                l_lastprice=parsed_json['lastPrice']
                #print("The last price is ",l_lastprice)
                l_averagePrice=round( parsed_json['totalTradedValue']/parsed_json['totalTradedVolume'],2)
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
                cur.execute("INSERT INTO daily_nse_movement_trans (symbol,lastprice_n,last_avg_price_n,totaltradedvol_n,max_price_n,min_price_n,open_price_n)  VALUES (:1,:2,:3,:4,:5,:6,:7)" ,(lines,l_lastprice, l_averagePrice,l_totaltradedvolume,l_max_price_n,l_min_price_n,l_open_price_n))
                statement = 'DELETE FROM daily_nse_movement_trans WHERE  EXCEPTION_FLAG_V=:type'
                cur.execute(cur.execute(statement, {'type':'Y'}))
                cur.close()
                con.commit()
                #print("End of the loop ---")
                con.close()
        except Exception as exception:
            print("Exception~~",exception)
            continue
        
        #print("End of the loop ---")
    print("Successfully one batch is completed ~~~",datetime.now())
  
# Task scheduling
# After every 10mins geeks() is called. 
schedule.every(3).minutes.do(EagleEye_job1)
  
# After every hour geeks() is called.
#schedule.every().hour.do(geeks)
  
# Every day at 12am or 00:00 time bedtime() is called.
#schedule.every().day.at("00:00").do(bedtime)
  
# After every 5 to 10mins in between run work()
#schedule.every(5).to(10).minutes.do(work)
  
# Every monday good_luck() is called
#schedule.every().monday.do(good_luck)
  
# Every tuesday at 18:00 sudo_placement() is called
#schedule.every().tuesday.at("18:00").do(sudo_placement)
  
# Loop so that the scheduling task
# keeps on running all time.
while True:
  
    # Checks whether a scheduled task 
    # is pending to run or not
    schedule.run_pending()
    time.sleep(1)
