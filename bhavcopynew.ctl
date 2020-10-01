LOAD DATA INFILE 'C:\Users\das\Downloads\sec_bhavdata_full.csv'
BADFILE 'C:\Users\das\Downloads\sec_bhavdata_full.bad'
DISCARDFILE 'C:\Users\das\Downloads\sec_bhavdata_full.dsc'
APPEND INTO TABLE daily_nse_all FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY '"' TRAILING NULLCOLS
(SYMBOL,SERIES,OPEN_PRICE, HIGH_PRICE,LOW_PRICE,CLOSE_PRICE,LAST_PRICE,PREV_CLOSE,TTL_TRD_QNTY,TURNOVER_LACS,DATE1 date 'dd-mon-yyyy',NO_OF_TRADES)

