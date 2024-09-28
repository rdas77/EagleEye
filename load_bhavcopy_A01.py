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

def find_10_days_session():
    import pandas as pd
    # CSV file path
    csv_file_path = 'D:\\EagleEye\\MetaData\\Trading_Date.csv'
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)
    #date_format = '%d-%m-%Y'
    # Assuming 'DateColumn' is the name of the date column
    #df['Trading_Date'] = pd.to_datetime(df['Trading_Date'],format=date_format)
    # Sort the DataFrame based on the date column
    #df = df.sort_values(by='Trading_Date',format=date_format)
    #df = df.drop_duplicates()
    df_no_duplicates_subset = df.drop_duplicates(subset=['Trading_Date'])
    # Write the sorted DataFrame back to the CSV file
    df_no_duplicates_subset.to_csv(csv_file_path, index=False)
    print("CSV file sorted based on the date column.")
    # Specify the number of rows to read from the end of the file
    n_last_rows = 10  # Change this to the number of rows you want
    # Read the last N rows of the CSV file into a DataFrame
    #df_last_n_rows = pd.read_csv(csv_file_path, nrows=n_last_rows)
    #print(df_last_n_rows)
    #print(f"Last {n_last_rows} rows of the DataFrame:")
    #for i , j in df_last_n_rows.iterrows():
        #print("The date found ",j['Trading_Date'])
    with open(csv_file_path, 'r', newline='') as file:
        reader = csv.reader(file)
        lines = list(reader)
        #print(lines)
        #print(lines[-10:])
    #for i in lines[-10:]:
        #print(i[0])
    return(lines[-10:])
        
def last_10_days_one_min_vol(ip_list):
    import pandas as pd
    result_df = pd.DataFrame()
    for i in ip_list:
        bhav=np.get_bhavcopy(i[0])
        bhav.columns = bhav.columns.str.strip()
        bhav = bhav[bhav['SERIES'] == 'EQ']
        selected_columns = ['SYMBOL', 'TTL_TRD_QNTY','DATE1']
        new_df = bhav[selected_columns]
        result_df = pd.concat([result_df,new_df], ignore_index=True)                      
        
    print( result_df['SYMBOL']=='INFY')         
        

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

    if len(bhav_1) >0:
        
        # CSV file path
        csv_file_path = 'D:\\EagleEye\\MetaData\\Trading_Date.csv'
        # Open the CSV file in append mode
        with open(csv_file_path, mode='a', newline='') as file:
            writer = csv.writer(file)
            # Write the data to the CSV file
            writer.writerow([ipdate])
            print("Data appended successfully in trading date",ipdate)
        #find_10_days_session()
        #last_10_days_one_min_vol(find_10_days_session())
            
        
    cur.callproc('eq_analysis_vol_up')
    cur.close()
    con.commit()
    con.close()

def LoadIndex( ):
    '''
    https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20BANK
    https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050
    '''

    

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
    #Loading NIFTY 50 data into daily nse table 
    positions = requests.get(payload,headers=headers).json()
    con = cx_Oracle.connect('EQUITY/EQUITY@localhost/XE')
    cur = con.cursor()
    cur.execute("INSERT INTO daily_nse_all ( symbol,series,open_price,high_price,low_price,last_price,close_price,prev_close,ttl_trd_qnty,turnover_lacs,date1) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)",(positions['data'][0]['symbol'],'EQ',positions['data'][0]['open'],positions['data'][0]['dayHigh'],positions['data'][0]['dayLow'],positions['data'][0]['lastPrice'],positions['data'][0]['lastPrice'],positions['data'][0]['previousClose'],positions['data'][0]['totalTradedVolume'],positions['timestamp']))
    

    

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





 

