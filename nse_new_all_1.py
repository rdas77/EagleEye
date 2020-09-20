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
all_stock_codes=['3IINFOTECH','ABFRL','ACC','ADANIPOWER','ALPHAGEO','ASAHISONG','ASHOKLEY','ASTRAMICRO','AUROPHARMA','AUTOIND','BAJAJELEC','BCG','BIRLACORPN','BIRLAMONEY','BRITANNIA','CARERATING','CHOLAHLDNG','CLNINDIA','CONFIPET','DCBBANK','DQE','EMAMILTD','ESSELPACK','FINCABLES','FMNL','GAEL','GALAXYSURF','GODFRYPHLP','GSFC','GUJALKALI','GULFPETRO','HEG','HINDUJAVEN','IBULISL','IPCALAB','IVP','JAYSREETEA','KAJARIACER','KANANIIND','KANSAINER','KINGFA','KITEX','KOPRAN','LICHSGFIN','LT','MAHABANK','MALUPAPER','MANAPPURAM','MARKSANS','MAXVIL','NCC','NILAINFRA','OILCOUNTUB','ONGC','PGHH','PREMIERPOL','RADICO','RAMASTEEL','RELIGARE','RSWM','RUCHISOYA','SADBHIN','SBILIFE','SEAMECLTD','SEPOWER','SPMLINFRA','SREEL','STARCEMENT','STRTECH','SUMIT','SURYAROSNI','TANLA','THYROCARE','TIINDIA','TNPETRO','UNIENTER','VAIBHAVGBL','VISAKAIND','VISHNU','WOCKPHARMA','SEYAIND','LUPIN','IFBIND','MAANALU','IIFL','ERIS','CUPID','ADSL','LPDC','SALASAR','SOTL','NATCOPHARM','NITESHEST','TATACOMM','OMMETALS','PATSPINLTD','UTTAMSUGAR','MHRIL','PIDILITIND','WELENT','WSTCSTPAPR','NAGREEKEXP','NAHARPOLY','TIL','NTPC','MANAKSTEEL','MANALIPETC','ORIENTALTL','UNIONBANK','ELGIRUBCO','TAKE','COFFEEDAY','BAJAJCON','AFFLE','ANIKINDS','FINEORG','FLFL','BIRLACABLE','DIVISLAB','ARIHANTSUP','ADHUNIKIND','AGLSL','ENDURANCE','ALBERTDAVD','BALKRISHNA','FEL','GRINDWELL','GVKPIL','INOXLEISUR','JSWHL','STEL','VRLLOG','WALCHANNAG','RELINFRA','MIRZAINT','RTNINFRA','MONSANTO','PODDARMENT','POKARNA','LYPSAGEMS','CHALET','IPAPPM','DUCON','VIDHIING','EASUNREYRL','TDPOWERSYS','GLENMARK','TEJASNET','GPTINFRA','TMRVL','TRIDENT','PSPPROJECT','PURVA','SABTN','SPTL','ADVANIHOTR','ALEMBICLTD','ANDHRACEMT','ARSHIYA','ARVIND','ATFL','BAGFILMS','BALKRISIND','BALLARPUR','BALRAMCHIN','BANKINDIA','BILENERGY','BUTTERFLY','CADILAHC','CENTRALBK','CENTRUM','CREDITACC','CTE','DLF','DTIL','EICHERMOT','ENERGYDEV','EUROTEXIND','EVERESTIND','FCL','FELDVR','FLEXITUFF','GABRIEL','GANESHHOUC','GINNIFILA','GRAVITA','GRPLTD','HDIL','HEXAWARE','HINDPETRO','HINDUNILVR','HITECH','IBULHSGFIN','IFBAGRO','INDOCO','IOC','ITDC','ITDCEM','JBMA','JINDALSTEL','JMFINANCIL','JSWENERGY','KARURVYSYA','KEI','KHAITANLTD','KWALITY','LUMAXIND','LUXIND','MASTEK','MATRIMONY','MENONBE','MOHITIND','MOHOTAIND','MOTHERSUMI','MTEDUCARE','NATIONALUM','NOIDATOLL','ONMOBILE','PANACEABIO','PIONEEREMB','PODDARHOUS','PRAXIS','RANASUG','RANEENGINE','RAYMOND','REVATHI','ROHLTD','SANGHIIND','SANGHVIMOV','SHILPAMED','SHIVAMILLS','SIMBHALS','SOLARINDS','SORILINFRA','SRTRANSFIN','SUMEETINDS','SUNDARAM','SUPRAJIT','SUPREMEIND','SWANENERGY','SYNGENE','TALBROAUTO','TATACHEM','TATAGLOBAL','TATAMETALI','TATASPONGE','TECHNOFAB','TIMESGTY','TITAN','VESUVIUS','VLSFINANCE','XCHANGING','HIRECT','JINDWORLD','INDIAMART','INDOSTAR','FCONSUMER',
'ISEC','CCL','CDSL','ITI','IVC','CNOVAPETRO','3PLAND','5PAISA','GSPL','RKDL','SRF','NEOGEN','PREMEXPLN','NFL','RUPA','NIBL','SCHAEFFLER','SELMCL','SHARDACROP','MANAKALUCO','PGEL','SIEMENS','SIMPLEXINF','POWERMECH','SIYSIL','SJVN','STCINDIA','PATINTLOG','PDMJEPAPER','TASTYBITE','TCIDEVELOP','TREEHOUSE','BINDALAGRO','BLUEDART','FOSECOIND','CIGNITITEC','GMMPFAUDLR','GMRINFRA','CORDSCABLE','GOODLUCK','BAJFINANCE','BALAJITELE','DALMIASUG','BHARATGEAR','BANSWRAS','CLEDUCATE','APEX','ABCAPITAL','ASTRAL','CENTENKA','FEDERALBNK','SHYAMCENT','HARRMALAYA','HDFCLIFE','HERITGFOOD','VIKASMCORP','VISASTEEL','RECLTD','WINDMACHIN','PILITA','SAREGAMA','SCI','LTI','LYKALABS','SHEMAROO','TV18BRDCST','TVSSRICHAK','TVTODAY','UFLEX','VARROC','ISMTLTD','GAIL','AIRAN','GRSE','GRUH','UBL','RNAVAL','PEL','ADANIENT','ADANIGAS','AJANTPHARM','AMARAJABAT','AMBUJACEM','AMRUTANJAN','ANANTRAJ','ANUP','ARCHIDPLY','ARROWTEX','ASAHIINDIA','ATUL','AVANTIFEED','AXISBANK','BALPHARMA','BANARISUG','BBL','BHEL','BIOCON','BOMDYEING','BYKE','CHOLAFIN','CIMMCO','CMICABLES','CREST','CUBEXTUB','CYBERTECH','DECCANCE','DHANUKA','DIGJAMLTD','DMART','DOLPHINOFF','EIHAHOTELS','EIHOTEL','GEECEE','GNFC','GOCLCORP','GODREJIND','GTNIND','GTNTEX','GUJGASLTD','HCG','HTMEDIA','IEX','INDIACEM','INDIAGLYCO','INDORAMA','INFOBEAN','JINDCOT','JUBLINDS','JUSTDIAL','KALPATPOWR','KAMDHENU','KCPSUGIND','KILITCH','KOHINOOR','KOTHARIPET','KSB','LAMBODHARA','MANGALAM','MANGCHEFER','MAWANASUG','MONTECARLO','MUNJALSHOW','NACLIND','NAVINFLUOR','NIITLTD','NILKAMAL','ORIENTABRA','ORIENTBANK','ORIENTELEC','PAPERPROD','PDSMFL','PFC','POLYCAB','PRAJIND','PRIMESECU','PSB','PSL','PUNJLLOYD','RAMCOSYS','RATNAMANI','RKFORGE','ROSSELLIND','RTNPOWER','SALSTEEL','SKMEGGPROD','SMLISUZU','SPAL','SRHHYPOLTD','SUPREMEINF','TATASTLBSL','THEINVEST','THERMAX','TWL','UMANGDAIRY','UNITEDTEA','VINATIORGA','VINYLINDIA','VIVIMEDLAB','WABAG','WABCOINDIA','WELCORP','GHCL','CONSOFINVT','DELTACORP','ICICIGI','GMBREW','KEYFINSERV','MAZDA','INGERRAND','GENUSPAPER','GEPIL','GESHIP','HDFC','ARSSINFRA','DPSCLTD','NEXTMEDIA','SANOFI','SANWARIA','ORISSAMINE','SHANTIGEAR','SREINFRA','SOUTHBANK','NATHBIOGEN','NITCO','JISLJALEQS','MCDHOLDING','VHL','MINDTECK','PIIND','WELINV','MURUDCERA','KELLTONTEC','KIRLOSIND','INDSWFTLAB','TIJARIA','KOTARISUG','INEOSSTYRO','TOKYOPLAST','TTKPRESTIG','MANPASAND','HSIL','ICICIPRULI','AAVAS','GODREJPROP','BAJAJ-AUTO','ADVENZYMES','DAMODARIND','DENORA','ANDHRABANK','BHARATFORG','CINEVISTA','COMPUSOFT','BFINVEST','GATI','ANSALHSG','APLAPOLLO','DHFL','APOLLO','AARTIIND','APOLSINHOT','APTECHT','BODALCHEM','CELEBRITY','AGCNET','AUTOLITIND','CERA','SPARC','JKLAKSHMI','STERTOOLS','HUDCO','VSSL','RESPONIND','SUNFLAG','ICRA','WENDT','IGL','SALZERELEC','TEXMOPIPES','GKWLIMITED','INDNIPPON','SHIRPUR-G',
'GOLDENTOBC','NILASPACES','REDINGTON','MARATHON','RENUKA','PIONDIST','KARMAENG','PITTIENG','SANDHAR','KOTAKBANK','POLYMED','NBVENTURES','SHALPAINTS','HCC','CREATIVE','UFO','MVL','UJAAS','VASCONEQ','ITC','OIL','JUBILANT','TBZ','VIPIND','JMA','FSC','TECHNOE','TERASOFT','GOACARBON','TRITURBINE','VBL','VEDL','SIS','ADLABS','ADORWELD','AHLUCONT','AMBER','ANSALAPI','ARCHIES','ASTRAZEN','AUTOAXLES','BANKBARODA','BARTRONICS','BASF','BERGEPAINT','BHANDARI','BSL','CAMLINFINE','CAPTRUST','CENTURYPLY','CGCL','CIPLA','CONCOR','CORALFINAC','DATAMATICS','DBREALTY','DCMNVL','DIAMONDYD','EKC','EQUITAS','ESABINDIA','GAYAHWS','GLAXO','GLOBALVECT','GPPL','GSS','HATSUN','HDFCAMC','HERCULES','HONDAPOWER','HOVS','IBREALEST','IBVENTURES','INDIANCARD','JINDALSAW','JPPOWER','KECL','LALPATHLAB','LINCOLN','M&MFIN','MASFIN','MAYURUNIQ','METROPOLIS','MFSL','MINDAIND','MRPL','NAHARSPING','NCLIND','NDTV','NELCO','NEULANDLAB','NHPC','PAGEIND','PALREDTEC','PCJEWELLER','PERSISTENT','PFIZER','PGHL','PRECAM','PRECOT','PRICOLLTD','RAMCOCEM','RICOAUTO','SAGCEM','SANDESH','SANGINITA','SASKEN','SATIA','SBIN','SCAPDVR','SHANKARA','SHRIRAMEPC','SICAL','SPECIALITY','SUBROS','SUNDRMBRAK','SUNTECK','SUVEN','SYMPHONY','TATAINVEST','TATAMOTORS','THANGAMAYL','TPLPLASTEH','ULTRACEMCO','VETO','VIJIFIN','VOLTAS','WEIZMANIND','ZYDUSWELL','DRREDDY','SETUINFRA','GALLANTT','INTELLECT','GREENPLY','LUMAXTECH','IFCI','EMMBI','BPCL','EROSMEDIA','MCX','MUKTAARTS','ACE','ADROITINFO','DBCORP','GSKCONS','ALICON','KSCL','APCL','LOVABLE','PNC','RUSHIL','SSWL','NITINSPIN','SUMMITSEC','OBEROIRLTY','L&TFH','SIGIND','PUNJABCHEM','SUPERSPIN','SPLIL','TATAELXSI','OMAXAUTO','RHFL','MARUTI','MINDTREE','MUTHOOTCAP','TIRUMALCHM','JAICORPLTD','JBCHEPHARM','BHARTIARTL','GANECOS','AYMSYNTEX','ABAN','TAJGVK','GOLDTECH','HAVELLS','CRISIL','AMJLAND','ARCOTECH','FILATEX','AMBIKCO','GARFIBRES','GDL','GILLANDERS','APOLLOHOSP','AARTIDRUGS','CANBK','DOLLAR','AUSOMENT','CENTUM','SILINV','INVENTURE','IOB','VARDHACRLC','SRIPIPES','JSWSTEEL','WHEELS','TALWALKARS','ZODIACLOTH','SHIVATEX','MAHSCOOTER','OPTIEMUS','PETRONET','KOKUYOCMLN','LAXMIMACH','RAIN','MSPL','UJJIVAN','VIPCLOTHNG','KDDL','TGBHOTELS','ZUARI','GTLINFRA','BIGBLOC','STAR','ABBOTINDIA','ADHUNIK','ALPA','AMDIND','APCOTEXIND','ARMANFIN','ARVINDFASN','ASIANTILES','ATULAUTO','BANCOINDIA','BEL','BHAGERIA','BLUESTARCO','BOROSIL','BRFL','BSELINFRA','CAREERP','CEATLTD','CELESTIAL','CENTURYTEX','CKFSL','DEEPIND','DHAMPURSUG','DSSL','DVL','DWARKESH','ECLERX','ELECTHERM','EMAMIPAP','EXCELINDUS','FAIRCHEM','GENESYS','GNA','GOKEX','HINDALCO','IFGLEXPOR','IGPL','INFRATEL','JUBLFOOD','KICL','KOTHARIPRO','LAOPALA','LAURUSLABS','LEMONTREE','LINCPEN','M&M','MADRASFERT','MAGADSUGAR','MAHESHWARI','MAHINDCIE','MAITHANALL','MANAKSIA','MANINFRA','MGL','MIDHANI']


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

