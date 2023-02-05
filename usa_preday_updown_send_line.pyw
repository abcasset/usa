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


def p_init():
    fm.get_fontconfig_fonts()
    font_location = 'C:/Windows/Fonts/malgunbd.ttf'  # For Windows
    font_name = fm.FontProperties(fname=font_location).get_name()
    matplotlib.rc('font', family=font_name)
    return

# col_width =0  하면 에러
# 스택오버의 소스 : https://stackoverflow.com/questions/19726663/how-to-save-the-pandas-dataframe-series-data-as-a-figure
def render_mpl_table(df_data, col_width=1.5, row_height=0.5, font_size=14,
                     header_color='#40466e', row_colors=['#f1f1f2', 'w'], edge_color='w',
                     bbox=[0, 0, 1, 1], header_columns=0, ax=None, **kwargs):    # col_width=1.5 해서 crop필요해짐
    # def render_mpl_table(data, col_width=1.0, row_height=0.5, font_size=12,
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

    for k, cell in mpl_table._cells.items():
        cell.set_edgecolor(edge_color)
        if k[0] == 0 or k[1] < header_columns:
            cell.set_text_props(weight='bold', color='w')
            cell.set_facecolor(header_color)
        else:
            cell.set_facecolor(row_colors[k[0] % len(row_colors)])
            # k[3,1] .set_facecolor('violet')
            cell.set_text_props(ha='center')

        #아래 6줄 추가, 성공
        li_row = 10
        if (  k[1] == 2  or  k[1] == 4 or  k[1] == 6  ):
            if (k[0] > li_row  ):
                cell.set_facecolor('lightsteelblue') #  violet plum  lightsteelblue
            elif (k[0] >=1  and  k[0] <= li_row ):
                cell.set_facecolor('violet')  #  violet lightsteelblue
    return ax.get_figure(), ax

def p_send_stickies2(gs_now_unique_c12):
    print ( 'p_send_stickies_only===============' , gs_now_unique_c12 )
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

def f_make_df(as_pre_day_N):
    # as_pre_day = ls_pre_day_2d
    # ls_Nd = ls_pre_day_2d

    sql_Nd_val = f"SELECT  *  FROM A_STK_DAILY_USA  where TRD_DT = '{as_pre_day_N}'"
    df_pre_day_Nd = pd.read_sql(sql_Nd_val, conn)

    sql_hoga_main = f"SELECT  *  FROM A_STK_DAILY_USA  where TRD_DT = '{ls_pre_day_1d}'"
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

    df_two_col = df_union[['STK_NM', 'ND_YLD']]
    df_two_col['전일'] = df_two_col['ND_YLD'].apply('{:.1%}'.format)

    df_two_col_ret = df_two_col[['STK_NM', '전일']]

    return df_two_col_ret


def f_make_df_us500(as_pre_day_N, as_stk_cd):
    """
    as_pre_day_N = ls_pre_day_2d
    as_stk_cd = 'us500'
    """
    

    sql_Nd_val = f"SELECT  *  FROM A_STK_DAILY_USA  where TRD_DT = '{as_pre_day_N}' and  STK_CD = '{as_stk_cd}'   "
    df_pre_day_Nd = pd.read_sql(sql_Nd_val, conn)

    sql_hoga_main = f"SELECT  *  FROM A_STK_DAILY_USA  where TRD_DT = '{ls_pre_day_1d}'  and  STK_CD = '{as_stk_cd}'   "
    df_pre_day_1d = pd.read_sql(sql_hoga_main, conn)

    df_pre_day_1d["ND_YLD"] = df_pre_day_1d["ADJ_CLOSE"] / df_pre_day_Nd["ADJ_CLOSE"] - 1


    return df_pre_day_1d


#######################################

p_init()

gs_image_file_path_NEW = 'C:\\abc\\stickies_send_file\\stickies_send_999999999999.png'
gs_line_url = "https://notify-api.line.me/api/notify"
# lg_db_server_ip = '128.50.245.140'
gs_db_server_ip = '128.50.245.140'
gs_tcp_host_ip = '127.0.0.1'
gi_PORT = 9999

# gs_bearer_key = "ROLFJPV3XFQprDOxlGfyHcPjepoc8OS5mSJRMUHdk75"
gs_bearer_key = "H0Yv4GK8M2MPLuXM77yS4Kx7pWsuiQ00Py5q7xAleHK" # 김지원, 이천주 두번재

conn = pymssql.connect(server=gs_db_server_ip, user='fuser', password='mada3787', database='fnguide', autocommit=True, charset='cp949')  # 한글 깨짐 해결
# conn = pymssql.connect(host=gs_host_ip, user='fuser', password='mada3787', database='fnguide', autocommit=True)
# cursor = conn.cursor()

sql_top10_days = "SELECT  top 10 trd_dt FROM A_STK_DAILY_USA where stk_cd = 'AAPL' order by trd_dt desc"
df_pre_days = pd.read_sql(sql_top10_days, conn)
ls_pre_day_1d = df_pre_days.iat[0, 0]  # 3일 또는 5일
ls_pre_day_2d = df_pre_days.iat[1, 0]  # 3일 또는 5일
ls_pre_day_3d = df_pre_days.iat[2, 0]  # 3일 또는 5일
ls_pre_day_4d = df_pre_days.iat[3, 0]  # 3일 또는 5일
ls_pre_day_5d = df_pre_days.iat[4, 0]  # 3일 또는 5일


# df_send_show = f_make_df(ls_pre_day_2d)
df_send_show = f_make_df_us500(ls_pre_day_2d, 'US500')
df_cum   = df_send_show

df_send_show2 = f_make_df_us500(ls_pre_day_2d, 'DJI')

df_cum.loc[len(df_cum)] = [df_send_show2]



df_send_show.insert(0, "no", df_send_show.reset_index().index + 1)
df_send_show.loc[len(df_send_show)] = [' ', ' ', ' ']  # 휴대폰에서 20번째행 가리지 않게 하기 위해

fig, ax = render_mpl_table(df_send_show)
fig.savefig(gs_image_file_path_NEW, dpi=80*2, bbox_inches='tight', pad_inches=.1 )  #pad_inches로 crop할 필요 없음

LINE_HEADERS = {"Authorization": "Bearer " + gs_bearer_key}
line_data = ({'message': '미국 전일'})
line_file = {'imageFile': open(gs_image_file_path_NEW, 'rb')}

requests.post(gs_line_url, headers=LINE_HEADERS, data=line_data, files=line_file, verify=False) # response_not_use 리턴값 있음
line_file.clear()

# p_send_stickies2(gs_stickies_png_file_name)
print(df_send_show)
print('=== end ===')






