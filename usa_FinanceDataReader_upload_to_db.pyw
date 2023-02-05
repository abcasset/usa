# -*- coding: utf-8 -*-

import time

import matplotlib.pyplot as plt
import numpy as np
import six
from PIL import ImageFont
from numpy.random import randn
from bs4 import BeautifulSoup as bs
from urllib.request import urlopen
import matplotlib.pyplot as plt
import requests
import matplotlib.font_manager as fm
import matplotlib
from datetime import datetime
from PIL import Image
import socket
import logging
import pymssql
import pandas as pd
import numpy as np
from functools import reduce
import os
import dataframe_image as dfi
import pandas as pd
import pymssql
import requests
import os
# ===============================================================================
import FinanceDataReader as fdr

"""
df = fdr.DataReader('AAPL', '2018-01-01', '2018-03-30')

df = fdr.DataReader('CL', '2022-12-22', '2030-03-30')
df.tail()

<거래소별 전체 종목 코드 - StockListing() 함수>
- 한국 거래소 : KRX(KOSPI, KOSDAQ, KONEX)
- 미국 거래소 : NASDAQ, NYSE, AMEX, S&P500

<가격 데이터 - DataReader() 함수>
- 국내 주식 : 005930(삼성전자), 091990(셀트리온헬스케어) 등
- 해외 주식 : AAPL(애플), AMZN(아마존), GOOG(구글) 등
- 각종 지수 : KS11(코스피 지수), KQ11(코스닥 지수), DJI(다우 지수), IXIC(나스닥 지수), US500(S&P5000)
- 환율 데이터 : USD/KRX(원달러 환율), USD/EUR(달러당 유로화 환율), CNY/KRX(위완화 원화 환율)
- 암호 화폐 가격 : BTC/USD(비트코인 달러 가격, 비트파이넥스), BTC/KRW(비트코인 원화 가격, 빗썸)

POILWTIUSDM

이외 상세한 옵션은 API 사용자 메뉴얼을 참고해주세요.
첨고 : https://github.com/FinanceData/FinanceDataReader/wiki/Users-Guide


"""



gs_host_ip = '128.50.245.140'

# conn = pymssql.connect(server=gs_host_ip, user='fuser', password='mada3787', database='fnguide', autocommit=True, charset='cp949') # 한글 깨짐 해결 
conn = pymssql.connect(server=gs_host_ip, user='fuser', password='mada3787', database='fnguide', autocommit=True) #   charset='cp949' 한글 깨짐 해결 
# conn = pymssql.connect(host=gs_host_ip, user='fuser', password='mada3787', database='fnguide', autocommit=True)
# cursor = conn.cursor()

# sql_top10_days = 'SELECT top 10 stk_cd, stk_nm FROM A_STK_US  '
sql_top10_days = 'SELECT stk_cd, stk_nm FROM A_STK_USA  ;'

df_stk_cd = pd.read_sql(sql_top10_days, conn)

df_stk_cd = df_stk_cd.replace('-US', '', regex=True)

# for 
df_main = pd.DataFrame(columns=['Date', 'stk_cd', 'stk_nm', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])

for i in df_stk_cd.index:
    try:
        ls_cd = df_stk_cd.loc[i, 'stk_cd']
        ls_nm = df_stk_cd.loc[i, 'stk_nm']


        df_temp = fdr.DataReader(ls_cd, '2021-10-23', '2029-12-31')

        df_temp['Date'] = df_temp.index.strftime("%Y%m%d")  ###################################

        df_temp['stk_cd'] = ls_cd
        df_temp['stk_nm'] = ls_nm
        df_main = pd.concat([df_main, df_temp], axis=0)
        time.sleep(0.01)
    except:
        print ("error")
        

 
 



### 6) sql_server insert ############################
# conn = pymssql.connect(host=gs_host_ip, user='fuser', password='mada3787', database='fnguide', autocommit=True)
cur = conn.cursor()
cur.execute("delete from A_STK_DAILY_USA;")

# sql = "insert into A_STK_DAILY_USA (TRD_DT, STK_CD, STK_NM) values(%s, %s, %s);"
# line_data = [tuple(x) for x in df_main.values]
# cur.executemany(sql, line_data)  # many도 auto commit 되는구나

df_main2 = df_main[['Date', 'stk_cd', 'stk_nm', 'Adj Close']]


# for i in df_main2.index:
#     try:
#         fs_date = df_main2.loc[i, 'Date']
#         fs_stk_cd = df_main2.loc[i, 'stk_cd']
#         fs_stk_nm = df_main2.loc[i, 'stk_nm']
#         fs_adj_close = df_main2.loc[i, 'Adj Close']
        
#         query = f"INSERT INTO A_STK_DAILY_USA (TRD_DT, STK_CD, STK_NM, ADJ_CLOSE) VALUES ( {fs_date}, {fs_stk_cd}, {fs_stk_nm}, {fs_adj_close});"
#         print( query )
#         cur.execute(query)
#     except:
#         print ("error for")
        


sql = "insert into A_STK_DAILY_USA (TRD_DT, STK_CD, STK_NM, ADJ_CLOSE) values(%s, %s, %s,   %d);"
line_data = [tuple(x) for x in df_main2.values]
cur.executemany(sql, line_data)  # many도 auto commit 되는구나

print('=== end ===')



