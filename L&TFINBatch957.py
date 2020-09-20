from nsetools import Nse
import json
import cx_Oracle
import sys
nse = Nse()
q = nse.get_quote('L&TFH')
parsed_json = json.loads(json.dumps(q))
v_open=parsed_json['open']
v_high=parsed_json['dayHigh']
v_low=parsed_json['dayLow']
v_quantitytraded=parsed_json['quantityTraded']
v_lastprice=parsed_json['lastPrice']
v_averageprice=parsed_json['averagePrice']
v_previousClose=parsed_json['previousClose']
con = cx_Oracle.connect('EQUITY/EQUITY@127.0.0.1/XE')
cur = con.cursor()
cur.callproc('DILLIP_FORMULA_PRC', ('L&TFH',v_open,v_high,v_low,v_quantitytraded,v_lastprice,v_averageprice,v_previousClose))
cur.close()
