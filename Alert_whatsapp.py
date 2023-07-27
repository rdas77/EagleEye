import schedule
import time
import cx_Oracle
import csv
import sys
import os
from nsetools import Nse
import json
from datetime import datetime
import pyautogui as pag
import pywhatkit
from pynput.keyboard import Key, Controller
keyboard = Controller()
pywhatkit.sendwhatmsg_to_group_instantly( group_id="B7jDGP6RbwMAWAnoKEmvq4",message='Welcome to EagleEye decsion making performed using ML and RSI/BB indicator',tab_close=True) 
# Functions setup
def sudo_placement():
    print("Get ready for Sudo Placement at Geeksforgeeks")
  
def good_luck():
    print("Good Luck for Test")
  

    
  
def bedtime():
    print("It is bed time go rest")
      
def EagleEye_Alert():
    #import pyautogui as pag
    #pag.alert(text="Hello World", title="The Hello World Box")
    con = cx_Oracle.connect('EQUITY/EQUITY@localhost/XE')
    cur = con.cursor()
    cur.execute("select symbol , remark_v , cul_weight_n,short_long_flag_v,open_price_n,min_price_n,max_price_n,last_price ,whatsapp_flg from daily_shortsell_call where whatsapp_flg='N'")
    res = cur.fetchall()
    for symbol , remark_v , cul_weight_n,short_long_flag_v,open_price_n,min_price_n ,max_price_n,last_price,whatsapp_flg in res:
        if (whatsapp_flg=='N') :
            print ( 'inside whatsapp block');
            try:
                #remark_v = symbol +'price'+str(last_price)+'open price ~'+str(open_price_n)+'max price ~'+str(max_price_n)+'min price ~'+str(min_price_n)+remark_v
                #pywhatkit.sendwhatmsg_instantly(phone_no="+918105708878", message=remark_v,tab_close=True)
                pywhatkit.sendwhatmsg_to_group_instantly( group_id="B7jDGP6RbwMAWAnoKEmvq4",message=remark_v,tab_close=True)
                #time.sleep(1)
                #pyautogui.click()
                #time.sleep(1)
                keyboard.press(Key.enter)
                keyboard.release(Key.enter)
                print("Message sent!")
                sql = ('update daily_shortsell_call set whatsapp_flg = :whatsapp_flg  where symbol = :symbol')
                cur.execute(sql, ['Y', symbol])
            except Exception as e:
                print(str(e))        
           
            
    con.commit()
    con.close()
    print("Successfully one batch is completed ~~~",datetime.now())
  
# Task scheduling
# After every 10mins geeks() is called. 
schedule.every(1).minutes.do(EagleEye_Alert)
  
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
