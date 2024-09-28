import streamlit as st
import cx_Oracle
st.set_page_config(layout="wide", initial_sidebar_state = "expanded")

# Version A02 -Date 07 jan 2024

# Version A03 --Date 06 April 2024

# Version A04 --Date 23 March 2024

# Version A05 -- Date 10 april 2024
# Version A06 -- Date 23 april 2024
def connect_to_oracle():
    connection = None
    try:
        connection = cx_Oracle.connect(
            user="EQUITY",
            password="EQUITY",
            dsn="localhost:1521/XE"
        )
        return connection
    except cx_Oracle.Error as error:
        st.error(f"Error connecting to Oracle: {error}")
        return None
    

connection = connect_to_oracle()
#note=st.text_input('Enter a value')

   
    

st.markdown("""
<style>
.big-font {
    font-size:300px !important;
}
</style>
""", unsafe_allow_html=True)
st.markdown("## " + 'All Symbol/Any Symbol/Top 5 Weight')	
st.markdown("#### " +"What Trends would you like to see?")

selected_metrics = st.selectbox( label="Choose...", options=['AllSymbol-2500','AllSymbol','Bank-nifty','Option_SL-Calc','Stock_SL-Calc','AnySymbol','Top5','Incranation','History'])

if selected_metrics=='Top5':

    def incranation(symbol):
        query = "SELECT count(*) FROM DAILY_NSE_MOVEMENT_TRANS where  BUYER_SELLER_PER_LAKH_N > 3000 and symbol='"+symbol+"'"
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            for row in results:
                return(row[0])
            
    symbol=[]
    weight=[]
    incra=[]
    query = "SELECT * FROM (SELECT symbol ,max(cul_weight_n) FROM BATCH_BUY_SELL_SEQ_NEW_V group by symbol order by 2 desc) WHERE ROWNUM <6"
    with connection.cursor() as cursor:
        
        cursor.execute(query)
        results = cursor.fetchall()
        
    for row in results:
        
        symbol.append(row[0])
        incra.append(incranation(row[0]))
        weight.append(row[1])
        
        
        
    #tuples = [(key, value) for i, (key, value) in enumerate(zip(symbol, weight))]
    # convert list of tuples to dictionary using dict()
    #symbol_dic = dict(tuples)
    #print(symbol_dic)
    #symbol_1 = list(symbol_dic.keys())
    #weight_1 = list(symbol_dic.values())
    import matplotlib.pyplot as plt
    fig = plt.figure(figsize = (10, 5))
    colors = ['r' if t > 1 else 'g' for t in incra]
    plt.barh(symbol , weight,color=colors)
    for index, value in enumerate(weight):
            plt.text(value, index, str(value))
    #from datetime import datetime
    #plt.xlabel(str(datetime.strftime(ip_date, "%d-%b-%Y")))
    #plt.title("Top 5 weighted symbol")
    plt.title('Top 5 weighted symbol', fontweight='bold', color = 'red', fontsize='18')
    st.pyplot(fig)
    symbol_index=[]
    XB =[]
    XS =[]
    SS =[]
    BB =[]
    BS =[]
    SB =[]
    query = "SELECT * FROM STACK_WEIGHT_V"
    with connection.cursor() as cursor:
        
        cursor.execute(query)
        results = cursor.fetchall()
        
    for row in results:
        
        symbol_index.append(row[0])
        XB.append(row[1])
        XS.append(row[2])
        SS.append(row[3])
        BB.append(row[4])
        BS.append(row[5])
        SB.append(row[6])
    import pandas as pd
    #import seaborn as sns
    df=pd.DataFrame({"XB":XB, "XS":XS,"SS":SS,"BB":BB,"BS":BS,"SB":SB},index= symbol_index)
    #st.pyplot(df.plot.bar(stacked=True).figure)
    #st.pyplot(sns.barplot(x='category', y='proportion', hue='subcat', data=df)plt.xlabel('Categories')plt.ylabel('Proportions')plt.legend(title='Subcategories'))
    st.bar_chart(df)    
if selected_metrics=='AllSymbol-2500':
    
    #st.set_page_config(layout="wide")
    #st.write("Connected to Oracle database.")

    # Execute a sample query
    #query = "SELECT to_char(last_update_dt,'hh24:mi:ss'), remark_v FROM daily_shortsell_call where BUYER_SELLER_PER_LAKH_N > 2500 order by last_update_dt"
    query = "SELECT to_char(last_update_dt,'hh24:mi:ss'), remark_v FROM daily_shortsell_call WHERE BUYER_SELLER_PER_LAKH_N > 2500 order by last_update_dt"
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

    # Display the query results
    st.write("Query Results:")
    for row in results:
        #st.write(row,f"<p style='font-size:40px;'>{label}</p>")
        st.write(f'<p style="background-color:#0066cc;color:#33ff33;font-size:20px;border-radius:2%;">{row}</p>', unsafe_allow_html=True)
        #st.write(f"<p style='font-size:60px; color:green;'>{row}</p>",unsafe_allow_html=False)
        #st.markdown(st.write(f"<p style='font-size:60px; color:red;'>{row}</p>"), unsafe_allow_html=True)
        #st.info(st.write(row))
        #st.write('<p style="font-size:26px; color:red;">Here is some red text</p>',
if selected_metrics=='AllSymbol':
    
    #st.set_page_config(layout="wide")
    #st.write("Connected to Oracle database.")

    # Execute a sample query
    #query = "SELECT to_char(last_update_dt,'hh24:mi:ss'), remark_v FROM daily_shortsell_call where BUYER_SELLER_PER_LAKH_N > 2500 order by last_update_dt"
    query = "SELECT to_char(last_update_dt,'hh24:mi:ss'), remark_v FROM daily_shortsell_call  order by last_update_dt"
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

    # Display the query results
    st.write("Query Results:")
    for row in results:
        #st.write(row,f"<p style='font-size:40px;'>{label}</p>")
        st.write(f'<p style="background-color:#0066cc;color:#33ff33;font-size:20px;border-radius:2%;">{row}</p>', unsafe_allow_html=True)
        #st.write(f"<p style='font-size:60px; color:green;'>{row}</p>",unsafe_allow_html=False)
        #st.markdown(st.write(f"<p style='font-size:60px; color:red;'>{row}</p>"), unsafe_allow_html=True)
        #st.info(st.write(row))
        #st.write('<p style="font-size:26px; color:red;">Here is some red text</p>',        

if selected_metrics=='Option_SL-Calc':
    
    lot = st.number_input("Lot Size")
    price = st.number_input("Price")
    status = st.radio('Select your SL amount: ',
                  ('2.5k', '6K', '10k'))
    
    if(st.button('Calculate SL')):
        if status=='2.5k':
            SL=price-(2500/lot)
            st.text("Your SL is {}.".format(SL))
            target = price+(5000/lot)
            st.text("Your target is {}.".format(target))
        elif status=='6k':
            SL=price-(6000/lot)
            st.text("Your SL is {}.".format(SL))
        else:
            SL=price-(10000/lot)
            st.text("Your SL is {}.".format(SL))
if selected_metrics=='Stock_SL-Calc':
    stockprice = st.number_input("Stock Price")
    
    if(st.button('Calculate stock SL')):
        
        
        try:
            number_stock= (50000/ int(stockprice))
        except ZeroDivisionError:
            number_stock =0
        target_price=(stockprice+ 1000/number_stock)
        sl_price =(stockprice- 1000/number_stock)
        st.text("Your number of stock {}.".format(number_stock))                 
        st.text("Your SL is {}.".format(sl_price))
        st.text("Your target is {}.".format(target_price))                  
if selected_metrics=='Bank-nifty':
    low_price =st.number_input("The High Price:- ",format="%.2f", value = 1.00, max_value=999999999999.00, min_value=0.00)
    high_price =st.number_input("The Low Price:- ",format="%.2f", value = 1.00, max_value=999999999999.00, min_value=0.00)
    
    def fibonacci_retracement(high, low):
        """
        Calculate the Fibonacci retracement levels.
        Args:
        high (float): The highest price.
        low (float): The lowest price.
        Returns:
        dict: A dictionary with Fibonacci levels.
        """
        diff = high - low
        levels =  high - 0.236 * diff
        #,'38.2%': high - 0.382 * diff,'50%': high - 0.5 * diff,'61.8%': high - 0.618 * diff }
        return levels

        
    
    #high_price =st.number_input("The Low Price:- ")

    #fibonacci_retracement(fibonacci_retracement(high_price, low_price), low_price)
    
    #st.text("fib level is", fibonacci_retracement(fibonacci_retracement(high_price, low_price), low_price))
            
    price = st.number_input('Price you want to deploy', value=10000)
    
    lot_price = st.number_input('Enter the lot price', value=fibonacci_retracement(fibonacci_retracement(high_price, low_price), high_price))
    lot=round((price/lot_price)/15,2) #15 lotsize of bank nifty
    st.text("Your lot is {}.".format(lot))
    sl_price_05=price-(price*.05)
    sl_price=(sl_price_05/lot)/15
    #sl_price=round(lot_price-lot_price*.05,2)
        
    st.text("Your SL 5ct {}.".format(sl_price))
    #sl_price_15=price-(price*.15)
    #sl_price_15=(sl_price_15/lot)/15
    #st.text("Your SL 15ct {}.".format(sl_price_15))
    target_price_5=((price+(price*.05))/lot)/15
    target_price_10=((price+(price*.1))/lot)/15
    target_price_20=((price+(price*.2))/lot)/15
    target_price_30=((price+(price*.3))/lot)/15
    st.text("Your Target 5% {}.".format(target_price_5))
    st.text("Your Target 10% {}.".format(target_price_10))
    st.text("Your Target 20% {}.".format(target_price_20))
    st.text("Your Target 30% {}.".format(target_price_30))
    st.text("Your lot 80pct {}.".format(round(lot*.8)))
    #st.text("Your Target_5 {}.".format(target_price_5)) 
    
    
     
        
if selected_metrics=='AnySymbol':
    #if st.button('Click me'):
        #st.toast('This is a notification!')
    note=st.text_input('Enter a value')
    #st.write("Connected to Oracle database.")
    #st.set_page_config(layout="wide")
    
    # Execute a sample query
    #query = "SELECT symbol||'~'||'lastprice_n~'||LASTPRICE_N||'~max_price_n~'||MAX_PRICE_N||'~min_price_n~'||MIN_PRICE_N||'~seq~'||seq_n , LAST_UPDATE_DT_TIME,BUYER_SELLER FROM BATCH_BUY_SELL_SEQ_NEW_V where symbol='"+note+"'"+" order by seq_n" 
    query="SELECT symbol||'~'||'lastprice_n~'||lastprice_n||'~max_price_n~' ||max_price_n||'~['||Trunc(max_price_n + max_price_n * .002, 2) ||',' ||Trunc(max_price_n - max_price_n * .002, 2)||']~min_price_n~'||min_price_n||'~['||Trunc(min_price_n + min_price_n * .002, 2)||',' ||Trunc(min_price_n - min_price_n * .002, 2)||']~seq~'||seq_n, last_update_dt_time, buyer_seller FROM  batch_buy_sell_seq_new_v  WHERE symbol='"+note+"'"+" order by seq_n"
    #st.write(query)
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

    # Display the query results
    st.write("Query Results:")
    for row in results:
        #st.write(row)
        #st.success(row)
        st.write(f'<p style="background-color:#0066cc;color:#33ff33;font-size:24px;border-radius:2%;">{row}</p>', unsafe_allow_html=True)
         

         
if selected_metrics=='Incranation':
        
    # Execute a sample query
    query = "SELECT symbol,cul_weight_n,RATIO_F_V FROM DAILY_NSE_MOVEMENT_TRANS where  BUYER_SELLER_PER_LAKH_N > 3000 order by 1"
 
    #st.write(query)
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

    # Display the query results
    symbol = []
    data = []
    ratio_v = []
    for row in results:
        #st.write(row)
        #st.success(row)
        symbol.append(row[0]+'~'+row[2])
        data.append(row[1])
        #ratio_v.append(row[2])
        #st.write(f'<p style="background-color:#0066cc;color:#33ff33;font-size:24px;border-radius:2%;">{row}</p>', unsafe_allow_html=True)
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    ax.pie(data, labels=symbol, autopct='%1.1f%%',  startangle=90, colors=plt.cm.Set3.colors)
    #ax.pie(data, labels=ratio_v, autopct='%1.1f%%', startangle=70, colors=plt.cm.Set3.colors)
    ax.set_title('Incranation')
    st.pyplot(fig)
         

if selected_metrics=='History':
    
    #Converting string date into datetime object
    #seq_n=st.text_input('Enter seq number 1')
    #symbol = st.text_input('Enter symbol Name')
    selected_metrics_1 = st.selectbox(
    label="Choose...", options=[ 'Firstbatch-2500', 'Firstbatch','Symbol','Top5-History','Incranation History']
    )
    
    ip_date =st.date_input("Any trading date", format="DD/MM/YYYY", disabled=False, label_visibility="visible")
    if selected_metrics_1=='Firstbatch':
        #from datetime import datetime 
    #dateTimeObj = str(datetime.strptime(ip_date, "%d-%b-%Y") )
        dateTimeObj=str(ip_date)
        st.write(dateTimeObj)
    # Execute a sample query
        #query = "SELECT 'CUL_WEIGHT_N~'||CUL_WEIGHT_N||'~OPEN_PRICE~'|| OPEN_PRICE ||'~MAX PRICE~'|| A.MAX_PRICE_N||'~MIN_PRICE~' || MIN_PRICE_N ||'~PREV_CLOSE~'||PREV_CLOSE ||'~LASTPRICE_N~'||LASTPRICE_N||'~BSPL~'||CASE WHEN LAST_AVG_PRICE_N > LASTPRICE_N THEN 'S->'||BUYER_SELLER_PER_LAKH_N  ELSE 'B->'||BUYER_SELLER_PER_LAKH_N  END|| CASE WHEN LAST_AVG_PRICE_N > LASTPRICE_N THEN 'S->'||BUYER_SELLER_PER_LAKH_N  ELSE  'B->'||BUYER_SELLER_PER_LAKH_N END||'~SYMBOL~'|| A.SYMBOL,'~TIME~'|| TO_CHAR(LAST_UPDATE_DT,'DD-MON-YY HH24:MI')||'~SEQ_N~'|| DAILY_BATCH_SEQ_N   FROM DAILY_NSE_MOVEMENT_TRANS_HIST A, FIVEDAYS_AVG_TBL_HIST B WHERE   BUYER_SELLER_PER_LAKH_N>0 AND BUYER_SELLER_PER_LAKH_N IS NOT NULL AND  A.SYMBOL=B.SYMBOL AND  to_char(LAST_UPDATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'"+" AND to_char(RECORD_DATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'"+"  AND DAILY_BATCH_SEQ_N=1 order by  DAILY_BATCH_SEQ_N"
        query ="SELECT  TO_CHAR(LAST_UPDATE_DT,'DD-MM-YY HH24:MI')||'~'||remark_v   FROM DAILY_SHORTSELL_CALL_HIST A WHERE TO_CHAR(LAST_UPDATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'"+"  order by  LAST_UPDATE_DT"
    #st.write(query)
        with connection.cursor() as cursor:
             cursor.execute(query)
             results = cursor.fetchall()
    #Display the query results
        st.write("Query Results:")
        for row in results:
            st.write(f'<p style="background-color:#0066cc;color:#33ff33;font-size:24px;border-radius:2%;">{row}</p>', unsafe_allow_html=True)
    if selected_metrics_1=='Firstbatch-2500':
        #from datetime import datetime 
    #dateTimeObj = str(datetime.strptime(ip_date, "%d-%b-%Y") )
        dateTimeObj=str(ip_date)
        st.write(dateTimeObj)
    # Execute a sample query
        #query = "SELECT 'CUL_WEIGHT_N~'||CUL_WEIGHT_N||'~OPEN_PRICE~'|| OPEN_PRICE ||'~MAX PRICE~'|| A.MAX_PRICE_N||'~MIN_PRICE~' || MIN_PRICE_N ||'~PREV_CLOSE~'||PREV_CLOSE ||'~LASTPRICE_N~'||LASTPRICE_N||'~BSPL~'||CASE WHEN LAST_AVG_PRICE_N > LASTPRICE_N THEN 'S->'||BUYER_SELLER_PER_LAKH_N  ELSE 'B->'||BUYER_SELLER_PER_LAKH_N  END|| CASE WHEN LAST_AVG_PRICE_N > LASTPRICE_N THEN 'S->'||BUYER_SELLER_PER_LAKH_N  ELSE  'B->'||BUYER_SELLER_PER_LAKH_N END||'~SYMBOL~'|| A.SYMBOL,'~TIME~'|| TO_CHAR(LAST_UPDATE_DT,'DD-MON-YY HH24:MI')||'~SEQ_N~'|| DAILY_BATCH_SEQ_N   FROM DAILY_NSE_MOVEMENT_TRANS_HIST A, FIVEDAYS_AVG_TBL_HIST B WHERE   BUYER_SELLER_PER_LAKH_N>0 AND BUYER_SELLER_PER_LAKH_N IS NOT NULL AND  A.SYMBOL=B.SYMBOL AND  to_char(LAST_UPDATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'"+" AND to_char(RECORD_DATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'"+"  AND DAILY_BATCH_SEQ_N=1 order by  DAILY_BATCH_SEQ_N"
        query ="SELECT  TO_CHAR(LAST_UPDATE_DT,'DD-MM-YY HH24:MI')||'~'||remark_v   FROM DAILY_SHORTSELL_CALL_HIST A WHERE BUYER_SELLER_PER_LAKH_N > 2500 AND TO_CHAR(LAST_UPDATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'"+"  order by  LAST_UPDATE_DT"
    #st.write(query)
        with connection.cursor() as cursor:
             cursor.execute(query)
             results = cursor.fetchall()
    #Display the query results
        st.write("Query Results:")
        for row in results:
            st.write(f'<p style="background-color:#0066cc;color:#33ff33;font-size:24px;border-radius:2%;">{row}</p>', unsafe_allow_html=True)        
                    
    if selected_metrics_1=='Symbol':        
        symbol_1 = st.text_input('Enter symbol Name')
        dateTimeObj=str(ip_date)
        st.write(dateTimeObj)
    # Execute a sample query
        if symbol_1:
            query = "SELECT 'SYMBOL~'|| A.SYMBOL,'~TIME~'|| TO_CHAR(LAST_UPDATE_DT,'DD-MON-YY HH24:MI')||'~LASTPRICE_N~'||LASTPRICE_N|| '~OPEN_PRICE~'|| OPEN_PRICE ||'~MAX PRICE~'|| A.MAX_PRICE_N||'~['||Trunc(a.max_price_n + a.max_price_n * .002, 2) ||',' ||Trunc(a.max_price_n - a.max_price_n * .002, 2)||']~MIN_PRICE~' || MIN_PRICE_N ||'~['||Trunc(min_price_n + min_price_n * .002, 2) ||',' ||Trunc(min_price_n - min_price_n * .002, 2)||']~PREV_CLOSE~'||PREV_CLOSE ||'~BSPL~'|| CASE WHEN LAST_AVG_PRICE_N > LASTPRICE_N THEN 'S->'||BUYER_SELLER_PER_LAKH_N  ELSE  'B->'||BUYER_SELLER_PER_LAKH_N END ||'CUL_WEIGHT_N~'||CUL_WEIGHT_N ||'~openpct~'||TRUNC(((b.prev_close - a.open_price_n)/(b.prev_close)*100),2) ||'~openpct~'||'~RSI~'|| TRUNC(((b.prev_close - a.open_price_n)/(b.prev_close)*100),2)*BUYER_SELLER_PER_LAKH_N FROM DAILY_NSE_MOVEMENT_TRANS_HIST A, FIVEDAYS_AVG_TBL_HIST B WHERE   BUYER_SELLER_PER_LAKH_N>0 AND BUYER_SELLER_PER_LAKH_N IS NOT NULL AND  A.SYMBOL=B.SYMBOL AND  to_char(LAST_UPDATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'"+" AND to_char(RECORD_DATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'"+"  AND a.symbol='"+symbol_1+"'"+" order by  a.DAILY_BATCH_SEQ_N"
            #st.write(query)
            with connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            #Display the query results
            st.write("Query Results:")
            for row in results:
                st.write(f'<p style="background-color:#0066cc;color:#33ff33;font-size:24px;border-radius:2%;">{row}</p>', unsafe_allow_html=True)
    if selected_metrics_1=='Top5-History':
        
        def incranation(symbol):
            query = " SELECT * FROM (SELECT DISTINCT RATIO_F_V FROM INCR_VW_HIST where BUYER_SELLER_PER_LAKH_N >3000 AND TO_CHAR(LAST_UPDATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'"+" and symbol='"+symbol+"' )"
            with connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                for row in results:
                    if row[0]=='S':
                        return(3)
                    else:
                        return(2)
                return(1)    
        #symbol_1 = st.text_input('Enter symbol Name')
        dateTimeObj=str(ip_date)
        st.write(dateTimeObj)
        query = "SELECT * FROM (SELECT A.symbol ,max(a.cul_weight_n)  weight FROM DAILY_NSE_MOVEMENT_TRANS_HIST A , FIVEDAYS_AVG_TBL_HIST B WHERE a.BUYER_SELLER_PER_LAKH_N>0 AND a.BUYER_SELLER_PER_LAKH_N IS NOT NULL  AND  A.SYMBOL=B.SYMBOL AND  to_char(LAST_UPDATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"' AND to_char(RECORD_DATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'  GROUP BY a.SYMBOL order by 2 DESC) WHERE ROWNUM <6"
        #st.write(query)
        symbol=[]
        weight=[]
        incra=[]
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        for row in results:
            symbol.append(row[0])
            weight.append(row[1])
            incra.append(incranation(row[0]))
        #tuples = [(key, value) for i, (key, value) in enumerate(zip(symbol, weight))]
        # convert list of tuples to dictionary using dict()
        #symbol_dic = dict(tuples)
        #print(symbol_dic)
        #symbol_1 = list(symbol_dic.keys())
        #weight_1 = list(symbol_dic.values())
        import matplotlib.pyplot as plt
        fig = plt.figure(figsize = (20, 8))
        #print(incra)
        colors = ['r' if t > 2 else ( 'y' if t>1 else 'g' )for t in incra]
        plt.margins(x=0)
        fig, ax = plt.subplots()
        width = 0.75
           #plt.bar(symbol,weight,width=.8,color=colors,align='center')
        plt.barh(symbol,weight,color=colors,align='center')
        from datetime import datetime
        plt.xlabel(str(datetime.strftime(ip_date, "%d-%b-%Y")))
        #plt.xticks(range(len(symbol)), symbol, rotation='vertical',fontsize='15')
        for index, value in enumerate(weight):
            plt.text(value, index, str(value))
            
        plt.ylabel("Symbol")
        plt.title('Top 5 weighted symbol', fontweight='bold', color = '#7FFFD4', fontsize='18')
        st.pyplot(fig)
    if selected_metrics_1=='Incranation History':
              
        #Converting string date into datetime object
        #ip_date =st.date_input("Any trading date", format="DD/MM/YYYY", disabled=False, label_visibility="visible")
        dateTimeObj=str(ip_date)
        st.write(dateTimeObj)
        # Execute a sample query
        query = "SELECT symbol,RATIO_F_V,sum(weight_n) cul_weight_n FROM DAILY_NSE_MOVEMENT_TRANS_HIST where  BUYER_SELLER_PER_LAKH_N > 3000 and to_char(LAST_UPDATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"' group by symbol,RATIO_F_V order by 1"
        #st.write(query)
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        #Display the query results
        st.write("Query Results:")
        symbol = []
        data = []
        ratio_v=[]
        legend=[]
        for row in results:
            #st.write(row)
            #st.success(row)
            symbol.append(row[0]+'~'+row[1])
            data.append(row[2])
            legend.append(row[0]+'~'+row[1]+str(row[2]))
            #ratio_v.append(row[2])
            #st.write(f'<p style="background-color:#0066cc;color:#33ff33;font-size:24px;border-radius:2%;">{row}</p>', unsafe_allow_html=True)
        import matplotlib.pyplot as plt
        
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.pie(data, labels=symbol, autopct='%1.1f%%',startangle=90, colors=plt.cm.Set3.colors )
        #wedges, texts, autotexts = ax.pie(data, labels=symbol, autopct='%1.1f%%', startangle=90, colors=plt.cm.Set3.colors)
        #for i, (text, autotext) in enumerate(zip(texts, autotexts)):
            #percentage_value = data[i]
            #text.set_text(f'{percentage_value}')
            #text.set_fontsize(8)
            #text.set_color('red')
            #autotext.set_text('')  # Clear the autopct text, which is now duplicated in the labels
        #ax.pie(data, labels=ratio_v, autopct='%1.1f%%', startangle=70, colors=plt.cm.Set3.colors)
        #ax.set_title('Incranation History')
        ax.legend(legend, title='Incranation History', loc='upper left', bbox_to_anchor=(1, 0.5))
        st.pyplot(fig)
        #fig1, ax = plt.subplots()
        #ax.pie(data, labels=ratio_v, autopct='%1.1f%%', startangle=70, colors=plt.cm.Set3.colors)
        #st.pyplot(fig1)

