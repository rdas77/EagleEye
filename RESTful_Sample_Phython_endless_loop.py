import asyncio
import websockets
import datetime
import json
import time
import requests

"""
GFDL TODO - please enter below the endpoint received from GFDL team. 
If you dont have one, please contact us on sales@globaldatafeeds.in 
"""  
endpoint = "http://test.lisuns.com:4531/"

"""
//GFDL TODO - please enter below the API Key received from GFDL team. 
If you dont have one, please contact us on sales@globaldatafeeds.in 
"""
accesskey = "ad3d6da1-cd49-41cb-aad3-5b6090616399"

"""
GFDL TODO : All the functions supported by API are listed below. 
You can uncomment any function (one at a time) to see the flow of request and response
"""

#*****List of functions*****#

#function = "GetLastQuote"                          #GFDL : Returns LastTradePrice of Single Symbol (detailed)
#function = "GetLastQuoteShort"                     #GFDL : Returns LastTradePrice of Single Symbol (short)
#function = "GetLastQuoteShortWithClose"            #GFDL : Returns LastTradePrice of Single Symbol (short) with Close of Previous Day
function = "GetLastQuoteArray"                     #GFDL : Returns LastTradePrice of multiple Symbols – max 25 in single call (detailed)
#function = "GetLastQuoteArrayShort"                #GFDL : Returns LastTradePrice of multiple Symbols – max 25 in single call (short)
#function = "GetLastQuoteArrayShortwithClose"       #GFDL : Returns LastTradePrice of multiple Symbols – max 25 in single call (short) with Previous Close


"""
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/* 	GFDL : 	1. 	Below 3 functions return the data of SINGLE SYMBOL - whenever requested.
				2. 	So you will need to send these requests EVERY TIME when you need latest data.
				3. 	GetLastQuote : returns single record of realtime data of single symbol. Contains many fields in response
				4. 	GetLastQuoteShort : returns single record of realtime data of single symbol. Contains limited fields in response
				5. 	GetLastQuoteShortWithClose : returns single record of realtime data of single symbol. Contains limited fields in response
				6.	If you want to get data of multiple symbols, you will need to send 1 request each - for each symbol
				
                //This example shows how to request data using Continuous Format
                //Similarly, you can send NIFTY-II (Near month), NIFTY-III (Far month). 
                //Below Symbol is Continuous Format of NIFTY Futures. It will never expire. So no change in code will be necessary.
                //You can use same naming convention for Futures of Instruments from NFO, CDS, MCX Exchanges
                //CDS Examples : USDINR-I, USDINR-II, USDINR-III
                //MCX Examples : NATURALGAS-I, NATURALGAS-II, NATURALGAS-III
                
                //Similarly, you can send NIFTY20AUGFUT (near month), NIFTY20SEPFUT (far month). 
                //You can use same naming convention for Futures of Instruments from NFO, CDS, MCX Exchanges
                //NFO Options Examples : NIFTY02JUL2010000CE, RELIANCE30JUL201700CE
                //CDS Futures Examples : USDINR20JULFUT, USDINR20AUGFUT, USDINR20SEPFUT
                //CDS Options Examples : USDINR29JUL2075.5CE, EURINR29JUL2080CE
                //MCX Options Examples : CRUDEOIL20JULFUT, CRUDEOIL20AUGFUT, CRUDEOIL20SEPFUT
                //MCX Options Examples : CRUDEOIL20JUL2050PE, GOLD20JUL43700PE	
                //Important : Replace it with appropriate expiry date if this contract is expired

                //Similarly, you can send FUTIDX_NIFTY_27AUG2020_XX_0 (near month), FUTIDX_NIFTY_24SEP2020_XX_0 (far month). 
                //You can use same naming convention for Futures of Instruments from NFO, CDS, MCX Exchanges
                //NFO Options Examples : OPTIDX_NIFTY_02JUL2020_CE_10000, OPTSTK_RELIANCE_30JUL2020_CE_1700
                //CDS Futures Examples : FUTCUR_USDINR_26JUN2020_XX_0, FUTCUR_USDINR_29JUL2020_XX_0, FUTCUR_USDINR_27AUG2020_XX_0
                //CDS Options Examples : OPTCUR_USDINR_29JUL2020_CE_75.5, OPTCUR_EURINR_29JUL2020_CE_80
                //MCX Futures Examples : FUTCOM_CRUDEOIL_20JUL2020__0, FUTCOM_CRUDEOIL_19AUG2020__0, FUTCOM_CRUDEOIL_21SEP2020__0
                //MCX Options Examples : OPTFUT_CRUDEOIL_16JUL2020_PE_2050, OPTFUT_GOLD_27JUL2020_PE_43700
                //Important : Replace it with appropriate expiry date if this contract is expired

                Requesting realtime data of NSE Indices
                -------------------------------------------
                //Use InstrumentIdenfier value "NIFTY 50", "NIFTY BANK", "NIFTY 100", etc.
                //Use NSE_IDX as Exchange
                //Please note that Indices Symbols have white space. For example, between NIFTY & 50, NIFTY & BANK above

                Requesting realtime data of NSE Stocks 
                ------------------------------------------
                //For EQ Series, use InstrumentIdentifier value BAJAJ-AUTO, RELIANCE, AXISBANK, LT, etc..
                //To subscribe to realtime data of any other series, append the series name to symbol name 
                //for example, to request data of RELIANCE CAPITAL from BE Series, use RELCAPITAL.BE
                //EQ Series Symbols are mentioned without any suffix

                // Please see symbol naming conventions here : 
                // https://globaldatafeeds.in/global-datafeeds-apis/global-datafeeds-apis/documentation-support/symbol-naming-convention/
	*/
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
"""
async def GetLastQuote():
    print("----------------------------------------------------")
    print("Work in progress... sending request for GetLastQuote")
    print("----------------------------------------------------")
    ExchangeName = "NFO"
    InstIdentifier = "NIFTY-I"
    isShortIdentifier = "false"         #GFDL : When using contractwise symbol like NIFTY20JULFUT, 
                                        #this argument must be sent with value "true" 
    xml="false"                                        
    response = ""
    count = 1
    while(count==1):
        strMessage = endpoint+"GetLastQuote/?accessKey="+accesskey+"&exchange="+ExchangeName+"&instrumentIdentifier="+InstIdentifier+"&xml="+xml
        response = requests.get(strMessage)
        print("Message sent : "+strMessage)
        print("Waiting for response...")
        print("Response :\n" + response.text)
        print("----------------------------------------------------")
        time.sleep(1.5)


async def GetLastQuoteShort():
    print("----------------------------------------------------")
    print("Work in progress... sending request for GetLastQuoteShort")
    print("----------------------------------------------------")
    ExchangeName = "NFO"
    InstIdentifier = "NIFTY-I"
    isShortIdentifier = "false"         #GFDL : When using contractwise symbol like NIFTY20JULFUT, 
                                        #this argument must be sent with value "true" 
    xml="false"                                        
    response = ""
    count = 1
    while(count==1):
        strMessage = endpoint+"GetLastQuoteShort/?accessKey="+accesskey+"&exchange="+ExchangeName+"&instrumentIdentifier="+InstIdentifier+"&xml="+xml
        response = requests.get(strMessage)
        print("Message sent : "+strMessage)
        print("Waiting for response...")
        print("Response :\n" + response.text)
        print("----------------------------------------------------")
        time.sleep(1.5)


async def GetLastQuoteShortWithClose():
    print("----------------------------------------------------")
    print("Work in progress... sending request for GetLastQuoteShortWithClose")
    print("----------------------------------------------------")
    ExchangeName = "NFO"
    InstIdentifier = "NIFTY-I"
    isShortIdentifier = "false"         #GFDL : When using contractwise symbol like NIFTY20JULFUT, 
                                        #this argument must be sent with value "true" 
    xml="false"                                        
    response = ""
    count = 1
    while(count==1):
        strMessage = endpoint+"GetLastQuoteShortWithClose/?accessKey="+accesskey+"&exchange="+ExchangeName+"&instrumentIdentifier="+InstIdentifier+"&xml="+xml
        response = requests.get(strMessage)
        print("Message sent : "+strMessage)
        print("Waiting for response...")
        print("Response :\n" + response.text)
        print("----------------------------------------------------")
        time.sleep(1.5)


"""
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/* 	GFDL : 	1. 	Below 3 functions return the data of MULTIPLE SYMBOLS (MAX 25IN SINGLE CALL) - whenever requested.
				2. 	So you will need to send these requests EVERY TIME when you need latest data.
				3. 	GetLastQuoteArray : returns array of realtime data of multiple symbols. Contains many fields in response
				4. 	GetLastQuoteArrayShort : returns array of realtime data of multiple symbols. Contains limited fields in response
				5. 	GetLastQuoteArrayShortWithClose : returns array of realtime data of multiple symbols. Contains limited fields in response
				6.	If you want to get data of multiple symbols (more than 25), you will need to send more requests - 1 each for 25 symbols

                //This example shows how to request data using Continuous Format
                //Similarly, you can send NIFTY-II (Near month), NIFTY-III (Far month). 
                //Below Symbol is Continuous Format of NIFTY Futures. It will never expire. So no change in code will be necessary.
                //You can use same naming convention for Futures of Instruments from NFO, CDS, MCX Exchanges
                //CDS Examples : USDINR-I, USDINR-II, USDINR-III
                //MCX Examples : NATURALGAS-I, NATURALGAS-II, NATURALGAS-III
                
                //Similarly, you can send NIFTY20AUGFUT (near month), NIFTY20SEPFUT (far month). 
                //You can use same naming convention for Futures of Instruments from NFO, CDS, MCX Exchanges
                //NFO Options Examples : NIFTY02JUL2010000CE, RELIANCE30JUL201700CE
                //CDS Futures Examples : USDINR20JULFUT, USDINR20AUGFUT, USDINR20SEPFUT
                //CDS Options Examples : USDINR29JUL2075.5CE, EURINR29JUL2080CE
                //MCX Options Examples : CRUDEOIL20JULFUT, CRUDEOIL20AUGFUT, CRUDEOIL20SEPFUT
                //MCX Options Examples : CRUDEOIL20JUL2050PE, GOLD20JUL43700PE	
                //Important : Replace it with appropriate expiry date if this contract is expired

                //Similarly, you can send FUTIDX_NIFTY_27AUG2020_XX_0 (near month), FUTIDX_NIFTY_24SEP2020_XX_0 (far month). 
                //You can use same naming convention for Futures of Instruments from NFO, CDS, MCX Exchanges
                //NFO Options Examples : OPTIDX_NIFTY_02JUL2020_CE_10000, OPTSTK_RELIANCE_30JUL2020_CE_1700
                //CDS Futures Examples : FUTCUR_USDINR_26JUN2020_XX_0, FUTCUR_USDINR_29JUL2020_XX_0, FUTCUR_USDINR_27AUG2020_XX_0
                //CDS Options Examples : OPTCUR_USDINR_29JUL2020_CE_75.5, OPTCUR_EURINR_29JUL2020_CE_80
                //MCX Futures Examples : FUTCOM_CRUDEOIL_20JUL2020__0, FUTCOM_CRUDEOIL_19AUG2020__0, FUTCOM_CRUDEOIL_21SEP2020__0
                //MCX Options Examples : OPTFUT_CRUDEOIL_16JUL2020_PE_2050, OPTFUT_GOLD_27JUL2020_PE_43700
                //Important : Replace it with appropriate expiry date if this contract is expired

                Requesting realtime data of NSE Indices
                -------------------------------------------
                //Use InstrumentIdenfier value "NIFTY 50", "NIFTY BANK", "NIFTY 100", etc.
                //Use NSE_IDX as Exchange
                //Please note that Indices Symbols have white space. For example, between NIFTY & 50, NIFTY & BANK above

                Requesting realtime data of NSE Stocks 
                ------------------------------------------
                //For EQ Series, use InstrumentIdentifier value BAJAJ-AUTO, RELIANCE, AXISBANK, LT, etc..
                //To subscribe to realtime data of any other series, append the series name to symbol name 
                //for example, to request data of RELIANCE CAPITAL from BE Series, use RELCAPITAL.BE
                //EQ Series Symbols are mentioned without any suffix

                // Please see symbol naming conventions here : 
                // https://globaldatafeeds.in/global-datafeeds-apis/global-datafeeds-apis/documentation-support/symbol-naming-convention/
	*/
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
"""
async def GetLastQuoteArray():
    print("----------------------------------------------------")
    print("Work in progress... sending request for GetLastQuoteArray")
    print("----------------------------------------------------")
    ExchangeName = "NSE"
    InstIdentifiers = "BEL+TECHM+EQUITAS+M%26MFIN"
    isShortIdentifier = "false"         #GFDL : When using contractwise symbol like NIFTY20JULFUT, 
                                        #this argument must be sent with value "true" 
    xml="false"                                        
    response = ""
    count = 1
    while(count==1):
        strMessage = endpoint+"GetLastQuoteArray/?accessKey="+accesskey+"&exchange="+ExchangeName+"&instrumentIdentifiers="+InstIdentifiers+"&xml="+xml
        response = requests.get(strMessage)
        print("Message sent : "+strMessage)
        print("Waiting for response...")
        print("Response :\n" + response.text)
        time.sleep(300)


async def GetLastQuoteArrayShort():
    print("----------------------------------------------------")
    print("Work in progress... sending request for GetLastQuoteArrayShort")
    print("----------------------------------------------------")
    ExchangeName = "NSE"
    InstIdentifiers = "BEL+TECHM+EQUITAS+M%26MFIN"
    isShortIdentifier = "false"         #GFDL : When using contractwise symbol like NIFTY20JULFUT, 
                                        #this argument must be sent with value "true" 
    xml="false"                                        
    response = ""
    count = 1
    while(count==1):
        strMessage = endpoint+"GetLastQuoteArrayShort/?accessKey="+accesskey+"&exchange="+ExchangeName+"&instrumentIdentifiers="+InstIdentifiers+"&xml="+xml
        response = requests.get(strMessage)
        print("Message sent : "+strMessage)
        print("Waiting for response...")
        print("Response :\n" + response.text)
        print("----------------------------------------------------")
        time.sleep(1.5)


async def GetLastQuoteArrayShortwithClose():
    print("----------------------------------------------------")
    print("Work in progress... sending request for GetLastQuoteArrayShortWithClose")
    print("----------------------------------------------------")
    ExchangeName = "NSE"
    InstIdentifiers = "BEL+TECHM+EQUITAS+M%26MFIN"
    isShortIdentifier = "false"         #GFDL : When using contractwise symbol like NIFTY20JULFUT, 
                                        #this argument must be sent with value "true" 
    xml="false"                                        
    response = ""
    count = 1
    while(count==1):
        strMessage = endpoint+"GetLastQuoteArrayShortWithClose/?accessKey="+accesskey+"&exchange="+ExchangeName+"&instrumentIdentifiers="+InstIdentifiers+"&xml="+xml
        response = requests.get(strMessage)
        print("Message sent : "+strMessage)
        print("Waiting for response...")
        print("Response :\n" + response.text)
        print("----------------------------------------------------")
        time.sleep(300)


"""
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
"""

async def functionCall():
    if function == "GetLastQuote":
        await GetLastQuote()
    elif function == "GetLastQuoteShort":
        await GetLastQuoteShort()
    elif function == "GetLastQuoteShortWithClose":
        await GetLastQuoteShortWithClose()
    elif function == "GetLastQuoteArray":
        await GetLastQuoteArray()
    elif function == "GetLastQuoteArrayShort":
        await GetLastQuoteArrayShort()
    elif function == "GetLastQuoteArrayShortwithClose":
        await GetLastQuoteArrayShortwithClose()

asyncio.get_event_loop().run_until_complete(functionCall())
