import re
import six
import json
import cx_Oracle
from datetime import datetime
from urllib.request import build_opener, HTTPCookieProcessor, Request
from http.cookiejar import CookieJar
from urllib.parse import urlencode




class Nse_new():
    """
    class which implements all the functionality for
    National Stock Exchange
    """
    __CODECACHE__ = None
    

    def __init__(self):
        self.opener = build_opener(HTTPCookieProcessor(CookieJar()))
        self.headers = {'Accept': '*/*',
                        'Accept-Language': 'en-US,en;q=0.5',
                        'Host': 'nseindia.com',
                        'Referer': "https://www.nseindia.com/live_market\
                        /dynaContent/live_watch/get_quote/GetQuote.jsp?symbol=INFY&illiquid=0&smeFlag=0&itpFlag=0",
                        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:28.0) Gecko/20100101 Firefox/28.0',
                        'X-Requested-With': 'XMLHttpRequest'
                        }
                        
        
        
        # URL list
        self.get_quote_url = 'https://www.nseindia.com/live_market/dynaContent/live_watch/get_quote/GetQuote.jsp?'
        self.stocks_csv_url = 'http://www.nseindia.com/content/equities/EQUITY_L.csv'

    def build_url_for_quote(self, code):
        """
        builds a url which can be requested for a given stock code
        :param code: string containing stock code.
        :return: a url object
        """
        if code is not None and type(code) is str:
            encoded_args = urlencode([('symbol', code), ('illiquid', '0'), ('smeFlag', '0'), ('itpFlag', '0')])
            return self.get_quote_url + encoded_args
        else:
            raise Exception('code must be string')
            
    def get_stock_codes(self, cached=True, as_json=False):
        """
        returns a dictionary with key as stock code and value as stock name.
        It also implements cache functionality and hits the server only
        if user insists or cache is empty
        :return: dict
        """
        url = self.stocks_csv_url
        req = Request(url, None, self.headers)
        res_dict = {}
        if cached is not True or self.__CODECACHE__ is None:
            # raises HTTPError and URLError
            res = self.opener.open(req)
            if res is not None:
                # for py3 compat covert byte file like object to
                # string file like object
                # res = byte_adaptor(res)
                temps = res.read().decode('latin-1')
                res = six.StringIO(temps)
                for line in res.read().split('\n'):
                    if line != '' and re.search(',', line):
                        (code, name) = line.split(',')[0:2]
                        res_dict[code] = name
                    # else just skip the evaluation, line may not be a valid csv
            else:
                raise Exception('no response received')
            self.__CODECACHE__ = res_dict
        return self.render_response(self.__CODECACHE__, as_json)

    def is_valid_code(self, code):
        """
        :param code: a string stock code
        :return: Boolean
        """
        if code:
            stock_codes = self.get_stock_codes()
            if code.upper() in stock_codes.keys():
                return True
            else:
                return False
                
    def clean_server_response(self, resp_dict):
        """cleans the server reponse by replacing:
            '-'     -> None
            '1,000' -> 1000
        :param resp_dict:
        :return: dict with all above substitution
        """

        # change all the keys from unicode to string
        d = {}
        for key, value in resp_dict.items():
            d[str(key)] = value
        resp_dict = d
        for key, value in resp_dict.items():
            if type(value) is str:
                if re.match('-', value):
                    try:
                        if float(value) or int(value):
                            dataType = True
                    except ValueError:
                        resp_dict[key] = None
                elif re.search(r'^[0-9,.]+$', value):
                    # replace , to '', and type cast to int
                    resp_dict[key] = float(re.sub(',', '', value))
                else:
                    resp_dict[key] = str(value)
        return resp_dict

    def render_response(self, data, as_json=False):
        if as_json is True:
            return json.dumps(data)
        else:
            return data


    def get_quote(self, code, as_json=False):
        """
        gets the quote for a given stock code
        :param code:
        :return: dict or None
        :raises: HTTPError, URLError
        """
        #code = code.upper()
        if code is not None:
            url = self.build_url_for_quote(code)
            req = Request(url, None, self.headers)
            # this can raise HTTPError and URLError, but we are not handling it
            # north bound APIs should use it for exception handling
            res = self.opener.open(req)

            # for py3 compat covert byte file like object to
            # string file like object
            temps = res.read().decode('latin-1')
            res = six.StringIO(temps)
            res = res.read()
            # Now parse the response to get the relevant data
            match = re.search(\
                        r'<div\s+id="responseDiv"\s+style="display:none">(.*?)</div>',
                        res, re.S
                    )
            try:
                buffer = match.group(1).strip()
               
                response = self.clean_server_response(json.loads(buffer)['data'][0])
            except SyntaxError as err:
                raise Exception('ill formatted response')
            else:
                return self.render_response(response, as_json)
        else:
            return None


print ("Data Extraction Started : ",datetime.now())
n = Nse_new()

dsn_tns = cx_Oracle.makedsn('LAPTOP-371KP9RT', '1521', service_name='XE') 
conn = cx_Oracle.connect(user='EQUITY', password='EQUITY', dsn=dsn_tns) 
cur = conn.cursor()
all_stock_codes=['MIRCELECTR','MPHASIS','MTNL','MUTHOOTFIN','NBCC','NBIFIN','NH','NLCINDIA','NSIL','NUCLEUS','OCCL','OMAXE','OPTOCIRCUI','ORBTEXP','ORIENTPPR','PANAMAPET','PENIND','PENINLAND','PFOCUS','PLASTIBLEN','PNBGILTS','PRECWIRE','QUICKHEAL','RADIOCITY','RAJESHEXPO','RELAXO','RELIANCE','RIIL','RML','RNAM','RPPINFRA','RSYSTEMS','RUCHIRA','SAIL','SAKUMA','SANCO','SANGAMIND','SEQUENT','SETCO','SFL','SHALBY','SHOPERSTOP','SHREYANIND','SOLARA','SUNPHARMA','TAINWALCHM','TATACOFFEE','TFCILTD','TINPLATE','TORNTPHARM','TRIL','TRIVENI','UGARSUGAR','UPL','VADILALIND','VENKEYS','VIPULLTD','WEBELSOLAR','ZEELEARN','ICICIBANK','GALLISPAT','8KMILES','AVTNPL','BEML','MAGMA','ENIL','BRNL','JCHAC','JSL','HCLTECH','DYNAMATECH','PNCINFRA','NIACL','SALONA','SHREECEM','MANINDS','SMSLIFE','PGIL','PHILIPCARB','STEELCITY','VOLTAMP','RGL','UNIVCABLES','RAMCOIND','KANORICHEM','TCI','INDSWFTLTD','KREBSBIO','TREJHARA','JAIBALAJI','TTL','ORIENTCEM','AUBANK','GANGESSECU','COROMANDEL','BAJAJHIND','COSMOFILMS','CINELINE',
'DLINKINDIA','EXCELCROP','INDOWIND','SHRIRAMCIT','GROBTEA','UNICHEMLAB','HIGHGROUND','RANEHOLDIN','SUNDARMHLD','RSSOFTWARE','SURANASOL','SURANAT&P','SWELECTES','ZODJRDMKJ','INDHOTEL','RALLIS','RAMANEWS','SHK','SKIPPER','PAISALO','NAGAFERT','RADAAN','TVSELECT','JTEKTINDIA','ESTER','JKTYRE','TCS','XELPMOC','KTIL','TI','AJMERA','PRABHAT','3MINDIA','AARVEEDEN','AKSHARCHEM','ANDHRSUGAR','ASTEC',
'AVADHSUGAR',
'BEDMUTHA',
'BHAGYANGR',
'BPL',
'CANFINHOME',
'COX&KINGS',
'DCM',
'DEN',
'DGCONTENT',
'DHUNINV',
'ELAND',
'FDC',
'FRETAIL',
'GAL',
'GENUSPOWER',
'GET&D',
'GRAPHITE',
'GTL',
'HFCL',
'HINDCOMPOS',
'HONAUT',
'HPL',
'INDBANK',
'INDLMETER',
'INSPIRISYS',
'INTENTECH',
'IRB',
'ISFT',
'IZMO',
'JAYAGROGN',
'JMCPROJECT',
'JPASSOCIAT',
'KABRAEXTRU',
'KARDA',
'KSK',
'KTKBANK',
'MADHAV',
'MANGLMCEM',
'MBLINFRA',
'MEP',
'MMTC',
'MOTILALOFS',
'MPSLTD',
'MUKANDLTD',
'NELCAST',
'NESTLEIND',
'NEWGEN',
'NIPPOBATRY',
'ONWARDTEC',
'ORICONENT',
'ORIENTBELL',
'PPAP',
'RAMKY',
'RVNL',
'SAKSOFT',
'SATIN',
'SCHNEIDER',
'SHRENIK',
'SICAGEN',
'SKFINDIA',
'SOBHA',
'STARPAPER',
'SURYALAXMI',
'SUTLEJTEX',
'TARAPUR',
'TCIEXP',
'TEXINFRA',
'TFL',
'TRIGYN',
'TTKHLTCARE',
'TVSMOTOR',
'UNITECH',
'UTTAMSTL',
'VIMTALABS',
'WONDERLA',
'GICHSGFIN',
'CONTROLPR',
'EXPLEOSOL',
'GREENLAM',
'JINDRILL',
'BASML',
'BEPL',
'MUNJALAU',
'DAAWAT',
'KAYA',
'KCP',
'DBL',
'GUFICBIO',
'LGBBROSLTD',
'DYNPRO',
'HGINFRA',
'HGS',
'ORTEL',
'KPRMILL',
'PFS',
'PTL',
'RBLBANK',
'JYOTHYLAB',
'KAKATCEM',
'ZEEL',
'HOTELEELA',
'BHARATWIRE',
'ASHIMASYN',
'ELGIEQUIP',
'ASIANHOTNR',
'ESCORTS',
'ESSARSHPNG',
'CARBORUNIV',
'GANDHITUBE',
'ADANITRANS',
'AHLEAST',
'AKZOINDIA',
'BALMLAWRIE',
'BANARBEADS',
'HITECHCORP',
'FINPIPE',
'BDL',
'BEARDSELL',
'ADANIGREEN',
'ASTRON',
'EIDPARRY',
'AHLWEST',
'ALBK',
'ALLCARGO',
'GRASIM',
'INDOTHAI',
'TORNTPOWER',
'SOMANYCERA',
'VINDHYATEL',
'GIPCL',
'SHIVAMAUTO',
'NRBBEARING',
'MARICO',
'PALASHSECU',
'SYNDIBANK',
'SAKHTISUG',
'LASA',
'POLYPLEX',
'NECLIFE',
'PRESSMN',
'SHAHALLOYS',
'MEGH',
'MSTCLTD',
'DCAL',
'JAYBARMARU',
'TCIFINANCE',
'FSL',
'TECHM',
'ZENTEC',
'KSL',
'TIMETECHNO',
'MADHUCON',
'UCALFUEL',
'PNB',
'20MICRONS',
'63MOONS',
'ABB',
'ACCELYA',
'AGARIND',
'ALKEM',
'APLLTD',
'APOLLOTYRE',
'ARIES',
'ASHIANA',
'ATLANTA',
'AURIONPRO',
'BLISSGVS',
'CASTROLIND',
'CHAMBLFERT',
'COALINDIA',
'COLPAL',
'CROMPTON',
'CYIENT',
'DALBHARAT',
'DCW',
'DHARSUGAR',
'GARDENSILK',
'GRANULES',
'GREAVESCOT',
'GULFOILLUB',
'HARITASEAT',
'HATHWAY',
'HBLPOWER',
'HESTERBIO',
'HILTON',
'HMVL',
'IDFC',
'INDOTECH',
'INFY',
'JOCIL',
'KNRCON',
'KRBL',
'LINDEINDIA',
'LOKESHMACH',
'LSIL',
'LTTS',
'MAHSEAMLES',
'MCDOWELL-N',
'MMFL',
'MOLDTECH',
'MOLDTKPAC',
'MORARJEE',
'NAGAROIL',
'NAHARINDUS',
'NECCLTD',
'OFSS',
'ORIENTREF',
'PARACABLES',
'PATELENG',
'PHOENIXLTD',
'PILANIINVS',
'PONNIERODE',
'RCOM',
'RELCAPITAL',
'REPRO',
'RUBYMILLS',
'SARDAEN',
'SHARDAMOTR',
'SINTEX',
'SIRCA',
'SNOWMAN',
'SPIC',
'STAMPEDE',
'STEELXIND',
'SUBEX',
'SUDARSCHEM',
'SUNDARMFIN',
'SUNDRMFAST',
'TCPLPACK',
'TEAMLEASE',
'TIIL',
'TRENT',
'UCOBANK',
'UNITEDBNK',
'VENUSREM',
'VSTIND',
'WIPRO',
'XPROINDIA',
'YESBANK',
'FACT',
'USHAMART',
'KESORAMIND',
'INDIANB',
'EDELWEISS',
'BATAINDIA',
'ELECON',
'EMKAY',
'BROOKS',
'MCLEODRUSS',
'BSOFT',
'GODREJAGRO',
'JSLHISAR',
'KIRIINDUS',
'DCMSHRIRAM',
'GTPL',
'HEIDELBERG',
'LFIC',
'ASAL',
'SUNCLAYLTD',
'SELAN',
'MANAKCOAT',
'SMSPHARMA',
'MINDACORP',
'RBL',
'TATAPOWER',
'OMKARCHEM',
'VGUARD',
'PRSMJOHNSN',
'KAMATHOTEL',
'KIRLOSENG',
'INDIGO',
'NDL',
'INDUSINDBK',
'MANUGRAPH',
'HSCL',
'ASIANPAINT',
'ASPINWALL',
'AXISCADES',
'AEGISCHEM',
'AKSHOPTFBR',
'DFMFOODS',
'DHANBANK',
'FIEMIND',
'FMGOETZE',
'BHAGYAPROP',
'DEEPAKFERT',
'GILLETTE',
'BLKASHYAP',
'ARVSMART',
'AIAENG',
'ALKYLAMINE',
'JAMNAAUTO',
'VAKRANGEE',
'JKIL',
'SAMBHAAV',
'TALWGYM',
'TATASTEEL',
'TEXRAIL',
'THEMISMED',
'MAJESCO',
'RMCL',
'RPGLIFE',
'RPOWER',
'MUKANDENGG',
'PRAKASH',
'LIBERTSHOE',
'NETWORK18',
'BRIGADE',
'NMDC',
'NOCIL',
'J&KBANK',
'TCNSBRANDS',
'VSTTILLERS',
'KHADIM',
'ZUARIGLOB',
'TIDEWATER',
'TIMKEN',
'SADBHAV',
'21STCENMGM',
'ADANIPORTS',
'ADFFOODS',
'AGRITECH',
'AIONJSW',
'ALANKIT',
'ALLSEC',
'ARIHANT',
'AROGRANITE',
'BAJAJHLDNG',
'BALAMINES',
'BANG',
'BAYERCROP',
'BFUTILITIE',
'BHARATRAS',
'BSE',
'CANTABIL',
'CAPLIPOINT',
'CEREBRAINT',
'CGPOWER',
'CHEMFAB',
'CORPBANK',
'CUMMINSIND',
'DEEPAKNTR',
'DIXON',
'DONEAR',
'DREDGECORP',
'ENGINERSIN',
'EVEREADY',
'EXIDEIND',
'GAMMNINFRA',
'GAYAPROJ',
'GEOJITFSL',
'GICRE',
'GLOBOFFS',
'GMDCLTD',
'GODREJCP',
'GPIL',
'GREENPOWER',
'GSCLCEMENT',
'GUJAPOLLO',
'GULPOLY',
'HAL',
'HCL-INSYS',
'HEROMOTOCO',
'HIL',
'HINDMOTORS',
'HINDZINC',
'IDBI',
'IDFCFIRSTB',
'INDIANHUME',
'INDRAMEDCO',
'INDTERRAIN',
'INFIBEAM',
'INSECTICID',
'IOLCP',
'JAGSNPHARM',
'JBFIND',
'JHS',
'JINDALPOLY',
'JISLDVREQS',
'JKCEMENT',
'JKPAPER',
'JPOLYINVST',
'KAVVERITEL',
'KIRLOSBROS',
'KOLTEPATIL',
'LAKSHVILAS',
'LOTUSEYE',
'MAHLIFE',
'MAHLOG',
'MARALOVER',
'NAGREEKCAP',
'NAHARCAP',
'NIITTECH',
'NITINFIRE',
'ORIENTHOT',
'PNBHOUSING',
'PRAENG',
'PTC',
'PVR',
'RCF',
'REMSONSIND',
'REPCOHOME',
'SHAKTIPUMP',
'SHREYAS',
'SITINET',
'SOMICONVEY',
'SPENCERS',
'SUNTV',
'SUPERHOUSE',
'TARMAT',
'TNPL',
'V2RETAIL',
'VASWANI',
'WELSPUNIND',
'WHIRLPOOL',
'ZENITHEXPO',
'BKMINDST',
'CESC',
'CESCVENT',
'DELTAMAGNT',
'DICIND',
'GLOBUSSPR',
'IRCON',
'HISARMETAL',
'A2ZINFRA',
'ELECTCAST',
'BOSCHLTD',
'EON',
'DABUR',
'KIOCL',
'ALKALI',
'APARINDS',
'IDEA',
'RITES',
'NESCO',
'ROLTA',
'SASTASUNDR',
'OAL',
'SCHAND',
'SUZLON',
'KPITTECH',
'SHREEPUSHK',
'POWERGRID',
'MRF',
'SUPPETRO',
'QUESS',
'OLECTRA',
'TATAMTRDVR',
'VMART',
'PARAGMILK',
'JPINFRATEC',
'MOIL',
'MOREPENLAB',
'NAVKARCORP',
'NAVNETEDUL',
'NDGL',
'TIPSINDLTD',
'EIMCOELECO',
'ASHOKA',
'CALSOFT',
'FORTIS',
'COCHINSHIP',
'BAJAJFINSV',
'HINDCOPPER',
'BANDHANBNK',
'HINDOILEXP',
'BGRENERGY',
'HITECHGEAR',
'ALMONDZ',
'ALOKTEXT',
'BBTC',
'COMPINFO',
'CUB',
'BLS',
'DISHTV',
'ASTERDM',
'CAPACITE',
'CEBBCO',
'CENTEXT',
'EMAMIREAL',
'CHENNPETRO',
'THOMASCOOK',
'INOXWIND',
'HDFCBANK',
'VIKASECO',
'HIKAL',
'HIMATSEIDE',
'HUBTOWN',
'ICIL',
'WEIZFOREX',
'SWARAJENG',
'IGARASHI',
'WILLAMAGOR',
'IMFA',
'IMPAL',
'SESHAPAPER',
'GOLDIAM',
'NRAIL',
'SKIL',
'SONATSOFTW',
'ORIENTLTD',
'MAXINDIA',
'OSWALAGRO',
'SARLAPOLY',
'NAUKRI',
'PRESTIGE',
'JAGRAN',
'VTL',
'KEC',
'ZEEMEDIA',
'KKCL',
'KMSUGAR',
'ZENSARTECH',
'TRF',
'ONEPOINT']
statement = 'insert into EQUITY.nse_3minute_data (SYMBOL,AS_DATE,ISIN,LAST_PRICE,PREVIOUS_CLOSE,DAY_HIGH,DAY_LOW,VWAP,TOTAL_VOLUME,PRICE_BAND_UPPER,PRICE_BAND_LOWER) values (:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11)'        
for stock_code in all_stock_codes:            
    q=n.get_quote(stock_code)

    #cur.execute(statement, (q['symbol'], datetime.now(), q['isinCode'],q['lastPrice'].replace(',',''),q['previousClose'].replace(',',''),q['dayHigh'].replace(',',''),q['dayLow'].replace(',',''),q['averagePrice'].replace(',',''),q['totalTradedVolume'].replace(',',''),q['pricebandupper'].replace(',',''),q['pricebandlower'].replace(',','')))
    cur.execute(statement, (q['symbol'], datetime.now(), q['isinCode'],q['lastPrice'],q['previousClose'],q['dayHigh'],q['dayLow'],q['averagePrice'],q['totalTradedVolume'],q['pricebandupper'],q['pricebandlower']))
    print ("The script name is  : ",stock_code)
    conn.commit()
cur.close()
conn.close
print ("Data Extraction Completed : ",datetime.now())
