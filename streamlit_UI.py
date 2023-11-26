import streamlit as st
import cx_Oracle
st.set_page_config(layout="wide", initial_sidebar_state = "expanded")

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

selected_metrics = st.selectbox(
    label="Choose...", options=['AllSymbol','AnySymbol','Top5','Incranation','Incranation History','History']
)

if selected_metrics=='Top5':
    
    
    symbol=[]
    weight=[]    
    query = "SELECT * FROM (SELECT symbol ,max(cul_weight_n) FROM BATCH_BUY_SELL_SEQ_NEW_V group by symbol order by 2 desc) WHERE ROWNUM <6"
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        
    for row in results:
        symbol.append(row[0])
        weight.append(row[1])
        
        
        
    tuples = [(key, value) for i, (key, value) in enumerate(zip(symbol, weight))]
    # convert list of tuples to dictionary using dict()
    symbol_dic = dict(tuples)
    #print(symbol_dic)
    symbol_1 = list(symbol_dic.keys())
    weight_1 = list(symbol_dic.values())
    import matplotlib.pyplot as plt
    fig = plt.figure(figsize = (10, 5))
    plt.barh(symbol_1 , weight_1)
    plt.xlabel("Symbol")
    plt.ylabel("Weight")
    plt.title("Top 5 weighted symbol")
    st.pyplot(fig)


         
if selected_metrics=='AllSymbol':
    
    #st.set_page_config(layout="wide")
    #st.write("Connected to Oracle database.")

    # Execute a sample query
    query = "SELECT to_char(last_update_dt,'hh24:mi:ss'), remark_v FROM daily_shortsell_call order by last_update_dt"
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
        
if selected_metrics=='AnySymbol':
    if st.button('Click me'):
        st.toast('This is a notification!')
    note=st.text_input('Enter a value')
    #st.write("Connected to Oracle database.")
    #st.set_page_config(layout="wide")
    
    # Execute a sample query
    query = "SELECT symbol||'~'||'lastprice_n~'||LASTPRICE_N||'~max_price_n~'||MAX_PRICE_N||'~min_price_n~'||MIN_PRICE_N||'~seq~'||seq_n , LAST_UPDATE_DT_TIME,BUYER_SELLER FROM BATCH_BUY_SELL_SEQ_NEW_V where symbol='"+note+"'"+" order by seq_n" 
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
         
if selected_metrics=='Incranation History':
#Converting string date into datetime object
       
    ip_date =st.date_input("Any trading date", format="DD/MM/YYYY", disabled=False, label_visibility="visible")
    #from datetime import datetime 
    #dateTimeObj = str(datetime.strptime(ip_date, "%d-%b-%Y") )
    dateTimeObj=str(ip_date)
    st.write(dateTimeObj)
    # Execute a sample query
    query = "SELECT symbol,cul_weight_n,RATIO_F_V FROM DAILY_NSE_MOVEMENT_TRANS_HIST where  BUYER_SELLER_PER_LAKH_N > 3000 and to_char(LAST_UPDATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'"+""
 
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
    query = "select * from  BATCH_BUY_SELL_SEQ_NEW_V where substr(BUYER_SELLER,4) >3000 order by to_number(trim(CUL_WEIGHT_N)) desc"
 
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
         

if selected_metrics=='History':
    
#Converting string date into datetime object
    #seq_n=st.text_input('Enter seq number 1')
    #symbol = st.text_input('Enter symbol Name')
    selected_metrics_1 = st.selectbox(
    label="Choose...", options=['Firstbatch','Symbol']
    )
    
    ip_date =st.date_input("Any trading date", format="DD/MM/YYYY", disabled=False, label_visibility="visible")
    if selected_metrics_1=='Firstbatch':
        #from datetime import datetime 
    #dateTimeObj = str(datetime.strptime(ip_date, "%d-%b-%Y") )
        dateTimeObj=str(ip_date)
        st.write(dateTimeObj)
    # Execute a sample query
        #query = "SELECT 'CUL_WEIGHT_N~'||CUL_WEIGHT_N||'~OPEN_PRICE~'|| OPEN_PRICE ||'~MAX PRICE~'|| A.MAX_PRICE_N||'~MIN_PRICE~' || MIN_PRICE_N ||'~PREV_CLOSE~'||PREV_CLOSE ||'~LASTPRICE_N~'||LASTPRICE_N||'~BSPL~'||CASE WHEN LAST_AVG_PRICE_N > LASTPRICE_N THEN 'S->'||BUYER_SELLER_PER_LAKH_N  ELSE 'B->'||BUYER_SELLER_PER_LAKH_N  END|| CASE WHEN LAST_AVG_PRICE_N > LASTPRICE_N THEN 'S->'||BUYER_SELLER_PER_LAKH_N  ELSE  'B->'||BUYER_SELLER_PER_LAKH_N END||'~SYMBOL~'|| A.SYMBOL,'~TIME~'|| TO_CHAR(LAST_UPDATE_DT,'DD-MON-YY HH24:MI')||'~SEQ_N~'|| DAILY_BATCH_SEQ_N   FROM DAILY_NSE_MOVEMENT_TRANS_HIST A, FIVEDAYS_AVG_TBL_HIST B WHERE   BUYER_SELLER_PER_LAKH_N>0 AND BUYER_SELLER_PER_LAKH_N IS NOT NULL AND  A.SYMBOL=B.SYMBOL AND  to_char(LAST_UPDATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'"+" AND to_char(RECORD_DATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'"+"  AND DAILY_BATCH_SEQ_N=1 order by  DAILY_BATCH_SEQ_N"
        query ="SELECT remark_v   FROM DAILY_SHORTSELL_CALL_HIST A WHERE TO_CHAR(LAST_UPDATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'"+"  order by  LAST_UPDATE_DT"
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
            query = "SELECT 'SYMBOL~'|| A.SYMBOL,'~TIME~'|| TO_CHAR(LAST_UPDATE_DT,'DD-MON-YY HH24:MI')||'~LASTPRICE_N~'||LASTPRICE_N|| '~OPEN_PRICE~'|| OPEN_PRICE ||'~MAX PRICE~'|| A.MAX_PRICE_N||'~MIN_PRICE~' || MIN_PRICE_N ||'~PREV_CLOSE~'||PREV_CLOSE ||'~BSPL~'|| CASE WHEN LAST_AVG_PRICE_N > LASTPRICE_N THEN 'S->'||BUYER_SELLER_PER_LAKH_N  ELSE  'B->'||BUYER_SELLER_PER_LAKH_N END ||'CUL_WEIGHT_N~'||CUL_WEIGHT_N FROM DAILY_NSE_MOVEMENT_TRANS_HIST A, FIVEDAYS_AVG_TBL_HIST B WHERE   BUYER_SELLER_PER_LAKH_N>0 AND BUYER_SELLER_PER_LAKH_N IS NOT NULL AND  A.SYMBOL=B.SYMBOL AND  to_char(LAST_UPDATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'"+" AND to_char(RECORD_DATE_DT,'YYYY-MM-DD')='"+dateTimeObj+"'"+"  AND a.symbol='"+symbol_1+"'"+" order by  a.DAILY_BATCH_SEQ_N"
            #st.write(query)
            with connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            #Display the query results
            st.write("Query Results:")
            for row in results:
                st.write(f'<p style="background-color:#0066cc;color:#33ff33;font-size:24px;border-radius:2%;">{row}</p>', unsafe_allow_html=True)
        
        
