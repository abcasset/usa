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
import re


def p_init():
    fm.get_fontconfig_fonts()
    font_location = 'C:/Windows/Fonts/malgunbd.ttf'  # For Windows
    font_name = fm.FontProperties(fname=font_location).get_name()
    matplotlib.rc('font', family=font_name)
    return


def f_buho_color(ls0):
    ls = str(ls0)
    try:
        p = re.compile('[-+][0-9]+[.][0-9]+[%]')
        ls_ret = p.findall(ls)  # list로 출력을 하니깐 아래 [0]을 해야 함

        ls_ret2 = ls_ret[0].replace('%', '')
        lf = float(ls_ret2)
    except:
        lf = 9.99
    return lf


# col_width =0  하면 에러
# 스택오버의 소스 : https://stackoverflow.com/questions/19726663/how-to-save-the-pandas-dataframe-series-data-as-a-figure
def ft_render_mpl_table(df_data, as_choose, col_width=1.5, row_height=0.5, font_size=14,
                        header_color='#40466e', row_colors=['#f1f1f2', 'w'], edge_color='w',
                        bbox=[0, 0, 1, 1], header_columns=0, ax=None, **kwargs):  # col_width=1.5 해서 crop필요해짐
    # def render_mpl_table_new(data, col_width=1.0, row_height=0.5, font_size=12,
    #                      header_color='#40466e', row_colors=['#f1f1f2', 'w'], edge_color='w',
    #                      bbox=[0, 0, 1, 1], header_columns=0, ax=None):
    if ax is None:
        size = (np.array(df_data.shape[::-1]) + np.array([0, 1])) * np.array([col_width, row_height])
        fig, ax = plt.subplots(figsize=size)
        ax.axis('off')
    mpl_table = ax.table(cellText=df_data.values, bbox=bbox, colLabels=df_data.columns, **kwargs)
    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)

    # 한줄 추가 성공.  https://stackoverflow.com/questions/25896964/centered-text-in-matplotlib-tables
    mpl_table.auto_set_column_width(col=list(range(len(df_data.columns))))

    for xy01, content in mpl_table._cells.items():
        x_posi = xy01[0]
        y_posi = xy01[1]

        content.set_edgecolor(edge_color)
        if x_posi == 0 or y_posi < header_columns:
            content.set_text_props(weight='bold', color='w')  # w : 화이트
            content.set_facecolor(header_color)  # header_color : 진한파랑
        else:
            content.set_facecolor(row_colors[xy01[0] % len(row_colors)])
            content.set_text_props(ha='center')

        if as_choose == 'USA_STOCK':
            li_row_10 = 10
            if (y_posi == 2):
                if (1 <= x_posi and x_posi <= li_row_10):
                    content.set_facecolor('violet')  # violet lightsteelblue
                elif (x_posi > li_row_10):
                    content.set_facecolor('lightsteelblue')  # violet plum  lightsteelblue

        elif as_choose == 'USA_JISU':
            if (x_posi != 0 and y_posi == 2):
                lf_temp = f_buho_color(content.get_text())
                if (lf_temp > 0):
                    content.get_text().set_color('red')
                elif (lf_temp < 0):
                    content.get_text().set_color('blue')

    return ax.get_figure(), ax


def p_send_stickies2(gs_now_unique_c12):
    print('p_send_stickies_only===============', gs_now_unique_c12)
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((gs_tcp_host_ip, gi_PORT))
    client_socket.sendall(gs_now_unique_c12.encode())
    client_socket.close()
    return


def f_div(lf_now, lf_3d):
    try:
        lf_ret = lf_now / lf_3d - 1
        if (lf_ret < -0.99):
            lf_ret = 0
    except:
        lf_ret = 0
    return lf_ret


def f_lookup(search_val, col_from, col_to, a_df_total):
    try:
        ls_ret = a_df_total.loc[a_df_total[col_from] == search_val, col_to].values[0]  # 좀 더 빠름
        # ls_ret = df.loc[df[col_from] == search_val, col_to].item()
        # df[  df['c1'].isin(['bb']) ] ['c2'].values[0]  # 완전 느림
    except:
        ls_ret = '0'
    return ls_ret


def f_make_df_stock(as_pre_day_N):
    '''
    as_pre_day_N = pd_2d
    '''

    sql_Nd_val = f"SELECT  *  FROM A_STK_DAILY_USA  where TRD_DT = '{as_pre_day_N}'"
    df_pre_day_Nd = pd.read_sql(sql_Nd_val, conn)

    sql_hoga_main = f" SELECT a.*, b.WI26_NM  FROM A_STK_DAILY_USA  AS a     JOIN A_STK_USA  AS b     ON a.STK_CD = b.STK_CD WHERE a.TRD_DT = '{pd_1}'; "

    df_pre_day_1d = pd.read_sql(sql_hoga_main, conn)

    df_pre_day_1d["ND_PRC"] = 0
    df_pre_day_1d["ND_YLD"] = 0
    df_pre_day_1d["RNK_YLD"] = 0

    for i in range(len(df_pre_day_1d)):
        #  i = 1
        ls_stk_cd = df_pre_day_1d.loc[i, "STK_CD"]
        ls_prc = df_pre_day_1d.loc[i, "ADJ_CLOSE"]
        li_temp_Nd = f_lookup(ls_stk_cd, 'STK_CD', 'ADJ_CLOSE', df_pre_day_Nd)
        df_pre_day_1d.loc[i, "ND_PRC"] = li_temp_Nd
        df_pre_day_1d.loc[i, "ND_YLD"] = f_div(ls_prc, li_temp_Nd)
        # ls_name = df_pre_day_1d.loc[i, "STK_NM"]
        # df_pre_day_1d.loc[i, "STK_NM"] = ls_name

    df_pre_day_1d['RNK_YLD'] = df_pre_day_1d['ND_YLD'].rank(method='first')

    df_desc = df_pre_day_1d.sort_values('RNK_YLD', ascending=True)
    df_asc = df_pre_day_1d.sort_values('RNK_YLD', ascending=False)
    df_head = df_desc.head(10)
    df_head = df_head.sort_values('RNK_YLD', ascending=False)
    df_tail = df_asc.head(10)
    df_union = pd.concat([df_tail, df_head])

    df_two_col = df_union[['STK_NM', 'ND_YLD', 'WI26_NM']]
    # df_two_col['전일'] = df_two_col['ND_YLD'].apply('{:.1%}'.format)
    # df_two_col['ND_YLD'] = df_two_col['ND_YLD'].apply('{:.1%}'.format, inplace=True)
    df_two_col['ND_YLD'] = df_two_col['ND_YLD'].apply('{:.1%}'.format)

    # df_two_col_ret = df_two_col[['STK_NM', '전일', '업종']]

    return df_two_col


def f_make_df_jisu(as_pre_day_N, as_stk_cd):
    """
    as_pre_day_N = pd_2d
    as_stk_cd = 'us500'
    """

    sql_Nd_val = f"SELECT  *  FROM A_STK_DAILY_USA  where TRD_DT = '{as_pre_day_N}' and  STK_CD = '{as_stk_cd}'   "
    df_pre_day_Nd = pd.read_sql(sql_Nd_val, conn)

    sql_hoga_main = f"SELECT  *  FROM A_STK_DAILY_USA  where TRD_DT = '{pd_1}'  and  STK_CD = '{as_stk_cd}'   "
    df_pre_day_1d = pd.read_sql(sql_hoga_main, conn)

    ldf_temp = df_pre_day_1d["ADJ_CLOSE"] / df_pre_day_Nd["ADJ_CLOSE"] - 1

    df_pre_day_1d["ND_YLD"] = ldf_temp.apply('{:.2%}'.format)

    df_pre_day_1d.drop(['TRD_DT', 'STK_CD'], axis=1, inplace=True)

    df_pre_day_1d.columns = ['명', '종가', '율']

    return df_pre_day_1d


def p_send_table_to_line(df_send_show, as_title, as_choose):
    fig, ax = ft_render_mpl_table(df_send_show, as_choose)
    fig.savefig(gs_image_file_path_NEW, dpi=80 * 2, bbox_inches='tight', pad_inches=.1)  # pad_inches로 crop할 필요 없음

    LINE_HEADERS = {"Authorization": "Bearer " + gs_bearer_key}
    # line_data = ({'message': '미국 전일'})
    line_data = ({'message': as_title})
    line_file = {'imageFile': open(gs_image_file_path_NEW, 'rb')}

    requests.post(gs_line_url, headers=LINE_HEADERS, data=line_data, files=line_file, verify=False)  # response_not_use 리턴값 있음
    line_file.clear()

    # p_send_stickies2(gs_stickies_png_file_name)
    print(df_send_show)
    print('=== end ===')

    return


#######################################
p_init()

gs_image_file_path_NEW = 'C:\\abc\\stickies_send_file\\stickies_send_999999999999.png'
gs_line_url = "https://notify-api.line.me/api/notify"
# lg_db_server_ip = '128.50.245.140'
gs_db_server_ip = '128.50.245.140'
gs_tcp_host_ip = '127.0.0.1'
gi_PORT = 9999

gi_TEST_R_REAL = 'T'

if gi_TEST_R_REAL == 'R':
    gs_bearer_key = "8oDcRiCemSflMHLrxPrkfGSOcSEmoDoxGdtB8050BXA"  # 마이챗
else:  # T
    gs_bearer_key = "H0Yv4GK8M2MPLuXM77yS4Kx7pWsuiQ00Py5q7xAleHK"  # 김지원, 이천주 두번재    

# gs_bearer_key = "ROLFJPV3XFQprDOxlGfyHcPjepoc8OS5mSJRMUHdk75"  #자동공지방
# gs_bearer_key = "H0Yv4GK8M2MPLuXM77yS4Kx7pWsuiQ00Py5q7xAleHK"  # 김지원, 이천주 두번재

# conn = pymssql.connect(server=gs_db_server_ip, user='fuser', password='mada3787', database='fnguide', autocommit=True, charset='cp949')  # 한글 깨짐 해결
conn = pymssql.connect(server=gs_db_server_ip, user='fuser', password='mada3787', database='fnguide', autocommit=True)  # 한글 깨짐 해결
# conn = pymssql.connect(host=gs_host_ip, user='fuser', password='mada3787', database='fnguide', autocommit=True)
# cursor = conn.cursor()

sql_top10_days = "SELECT  top 999 trd_dt FROM TS_STK_DAILY where stk_cd = '005930' order by trd_dt desc"
df_pre_days = pd.read_sql(sql_top10_days, conn)
pd_1   = df_pre_days.iat[1-1, 0]
pd_2   = df_pre_days.iat[2-1, 0]
pd_3   = df_pre_days.iat[3-1, 0]
pd_4   = df_pre_days.iat[4-1, 0]
pd_5   = df_pre_days.iat[5-1, 0]

pd_10  = df_pre_days.iat[10-1, 0]
pd_15  = df_pre_days.iat[15-1, 0]
pd_25  = df_pre_days.iat[25-1, 0]
pd_37  = df_pre_days.iat[37-1, 0]
pd_50  = df_pre_days.iat[50-1, 0]
pd_62  = df_pre_days.iat[62-1, 0]
pd_75  = df_pre_days.iat[75-1, 0]
pd_87  = df_pre_days.iat[87-1, 0]
pd_99  = df_pre_days.iat[99-1, 0]

pd_111 = df_pre_days.iat[111-1, 0]
pd_123 = df_pre_days.iat[123-1, 0]
pd_135 = df_pre_days.iat[135-1, 0]
pd_147 = df_pre_days.iat[147-1, 0]
pd_159 = df_pre_days.iat[159-1, 0]
pd_171 = df_pre_days.iat[171-1, 0]
pd_183 = df_pre_days.iat[183-1, 0]
pd_195 = df_pre_days.iat[195-1, 0]
pd_207 = df_pre_days.iat[207-1, 0]
pd_219 = df_pre_days.iat[219-1, 0]
pd_231 = df_pre_days.iat[231-1, 0]
pd_243 = df_pre_days.iat[243-1, 0]
pd_255 = df_pre_days.iat[255-1, 0]
pd_267 = df_pre_days.iat[267-1, 0]


sql_days_stock = f"SELECT TRD_DT, STK_CD, VAL FROM TS_STK_DAILY where TRD_DT IN  ('{pd_1}','{pd_2}','{pd_3}','{pd_4}','{pd_5}','{pd_10}','{pd_15}','{pd_25}','{pd_37}','{pd_50}','{pd_62}','{pd_75}','{pd_87}','{pd_99}','{pd_111}','{pd_123}','{pd_135}','{pd_147}','{pd_159}','{pd_171}','{pd_183}','{pd_195}','{pd_207}','{pd_219}','{pd_231}','{pd_243}','{pd_255}','{pd_267}') order by stk_cd, trd_dt "
df_days_stock = pd.read_sql(sql_days_stock, conn)

sql_days_jisu = f"SELECT TRD_DT, STK_CD, VAL FROM TS_STK_DAILY where STK_CD = 'IKS001' and TRD_DT IN  ('{pd_1}','{pd_2}','{pd_3}','{pd_4}','{pd_5}','{pd_10}','{pd_15}','{pd_25}','{pd_37}','{pd_50}','{pd_62}','{pd_75}','{pd_87}','{pd_99}','{pd_111}','{pd_123}','{pd_135}','{pd_147}','{pd_159}','{pd_171}','{pd_183}','{pd_195}','{pd_207}','{pd_219}','{pd_231}','{pd_243}','{pd_255}','{pd_267}') order by trd_dt desc"
df_days_jisu = pd.read_sql(sql_days_jisu, conn)

for i in df_days_stock.index:
    '''
    i= 0
    '''
    fs_temp = df_days_stock.loc[i, 'TRD_DT']
    df_days_stock.loc[i, 'JISU_VAL'] = f_lookup(fs_temp, 'TRD_DT', 'VAL', df_days_jisu)


df_days_stock['STK_YLD'] = df_days_stock['VAL'].pct_change()
df_days_stock['JISU_YLD'] = df_days_stock['JISU_VAL'].pct_change()
df_days_stock['CHA_YLD'] = df_days_stock['STK_YLD'] - df_days_stock['JISU_YLD']

gi_count = 1
# for i in range(0, len(df_days_stock)): 
for i in df_days_stock.index:
    if i == 0:
        df_days_stock.loc[i, 'CHA_YLD'] = 0  # df_days_stock.replace(np.nan, 0)        
        df_days_stock.loc[i, 'NO'] = 1
    else:
        if (df_days_stock.loc[i, 'STK_CD'] != df_days_stock.loc[i - 1, 'STK_CD']):
            df_days_stock.loc[i, 'CHA_YLD'] = 0
            gi_count = 1
            df_days_stock.loc[i, 'NO'] = gi_count
        else:
            gi_count = gi_count + 1
            df_days_stock.loc[i, 'NO'] = gi_count

df_days_stock2 = df_days_stock[['TRD_DT', 'STK_CD', 'CHA_YLD', 'NO']]

cur = conn.cursor()
cur.execute("delete from A_STK_DAILY_USA_1Y_YLD;")

sql = "insert into A_STK_DAILY_USA_1Y_YLD (TRD_DT, STK_CD, CHA_YLD, NO) values(%s, %s, %s,  %s);"
line_data = [tuple(x) for x in df_days_stock2.values]
cur.executemany(sql, line_data)  # many도 auto commit 되는구나

print('=== end ===')



















