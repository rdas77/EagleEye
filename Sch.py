"""
New Script for market data for Eagleeye

Date of development 30 Dec 2023

"""

import os,sys
# os.chdir(os.path.dirname(os.path.abspath(__file__)))
# sys.path.insert(1, os.path.join(sys.path[0], '..'))

import requests
import json
import datetime,time
import cx_Oracle
import schedule



headers = {
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'DNT': '1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36',
    'Sec-Fetch-User': '?1',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-Mode': 'navigate',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',
}

payload='https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O'

def running_status():
    import datetime
    start_now=datetime.datetime.now().replace(hour=9, minute=10, second=0, microsecond=0)
    end_now=datetime.datetime.now().replace(hour=15, minute=30, second=0, microsecond=0)
    return start_now<datetime.datetime.now()<end_now

#positions = requests.get(payload,headers=headers).json()
#print(positions['data'][0])


'''

for x in range(0, len(positions['data'])):
    print('symbol',positions['data'][x]['symbol'],'lastprice',positions['data'][x]['lastPrice'],'Volume',positions['data'][x]['totalTradedVolume'])

'''    

def get_market_data():
          
    try:
        
        if running_status():
            
            positions = requests.get(payload,headers=headers).json()
            #print(len(positions['data']))
            print (positions['marketStatus']['marketStatus'])
            con = cx_Oracle.connect('EQUITY/EQUITY@localhost/XE')
            for x in range(0, len(positions['data'])):
                l_lastprice=positions['data'][x]['lastPrice']
                #print("The last price is ",l_lastprice)
                l_averagePrice=round( positions['data'][x]['totalTradedValue']/positions['data'][x]['totalTradedVolume'],2)
                #print("The average price is ",l_averagePrice)
                l_totaltradedvolume=positions['data'][x]['totalTradedVolume']
                #print("The total traded volume is ",l_totaltradedvolume)
                l_max_price_n=positions['data'][x]['dayHigh']
                #print("The max price for the symbol ",l_max_price_n)
                l_min_price_n=positions['data'][x]['dayLow']
                #print("The min price for the symbol ",l_min_price_n)
                l_open_price_n=positions['data'][x]['open']
                #print("The open price for the symbol ",l_open_price_n)
                cur = con.cursor()
                cur.execute("INSERT INTO daily_nse_movement_trans (symbol,lastprice_n,last_avg_price_n,totaltradedvol_n,max_price_n,min_price_n,open_price_n)  VALUES (:1,:2,:3,:4,:5,:6,:7)" ,(positions['data'][x]['symbol'],l_lastprice, l_averagePrice,l_totaltradedvolume,l_max_price_n,l_min_price_n,l_open_price_n))
                statement = 'DELETE FROM daily_nse_movement_trans WHERE  EXCEPTION_FLAG_V=:type'
                cur.execute(cur.execute(statement, {'type':'Y'}))
                cur.close()
                con.commit()
            '''    
            print('Process start fro the NIFTY 50')
            payload='https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050'
            positions = requests.get(payload,headers=headers).json()
            l_lastprice=positions['data'][0]['lastPrice']
            l_averagePrice=round( positions['data'][0]['totalTradedValue']/positions['data'][0]['totalTradedVolume'],2)
            l_totaltradedvolume=positions['data'][0]['totalTradedVolume']
            l_max_price_n=positions['data'][0]['dayHigh']
            l_min_price_n=positions['data'][0]['dayLow']
            cur = con.cursor()
            cur.execute("INSERT INTO daily_nse_movement_trans (symbol,lastprice_n,last_avg_price_n,totaltradedvol_n,max_price_n,min_price_n,open_price_n)  VALUES (:1,:2,:3,:4,:5,:6,:7)" ,(positions['data'][x]['symbol'],l_lastprice, l_averagePrice,l_totaltradedvolume,l_max_price_n,l_min_price_n,l_open_price_n))
            statement = 'DELETE FROM daily_nse_movement_trans WHERE  EXCEPTION_FLAG_V=:type'
            cur.execute(cur.execute(statement, {'type':'Y'}))
            cur.close()
            con.commit()
            con.close()
            '''
            print("Successfully one batch is completed ~~~",datetime.datetime.now())    
        else:
            print("Market is closed ~~~",datetime.datetime.now())
                          
    except Exception as exception:
        print('The exception is',exception)

'''        main block started over here

'''

if __name__ == '__main__':

    schedule.every(1).minutes.do(get_market_data)
        
    while True:
        # Checks whether a scheduled task
        # is pending to run or not
        try :
            schedule.run_pending()
            time.sleep(1)
        except Exception as exception:
            print("Exception~~",exception)
            continue

   

    
    


