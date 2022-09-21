import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns
from mpl_finance import candlestick_ohlc
from matplotlib.dates import DateFormatter, WeekdayLocator, DayLocator, MONDAY,date2num
##sns.set_style('whitegrid')
plt.style.use("fivethirtyeight")

# For reading stock data from yahoo
from pandas_datareader.data import DataReader
import yfinance as yf

# For time stamps
from datetime import datetime
import time

import pymysql

import warnings
warnings.filterwarnings("ignore")

DATABASE = 'StockDatabase'


def collect_data():
    tech_list = ['AAPL', 'GOOG', 'MSFT', 'AMZN']
    tech_list = ['AAPL', 'GOOG', 'MSFT', 'AMZN']
    end = datetime.now()
    start = datetime(end.year, end.month - 1, 30 - end.day)
    print('FETCHING DATA...')
    for stock in tech_list:
        globals()[stock] = yf.download(stock, start, end)
    company_list = [AAPL, GOOG, MSFT, AMZN]
    company_name = ["APPLE", "GOOGLE", "MICROSOFT", "AMAZON"]
    for company, com_name in zip(company_list, company_name):
        company["company_name"] = com_name
    df = pd.concat(company_list, axis=0)
    print('FETCHING SUCCEED.')
    return df.reset_index()


def pandas_candlestick_ohlc(stock_data,  filename,otherseries=None):
    # 设置绘图参数，主要是坐标轴
    mondays = WeekdayLocator(MONDAY)
    alldays = DayLocator()
    dayFormatter = DateFormatter('%d')

    fig, ax = plt.subplots()
    fig.subplots_adjust(bottom=0.2)
    if stock_data.index[-1] - stock_data.index[0] < 730:
        weekFormatter = DateFormatter('%b %d')
        ax.xaxis.set_major_locator(mondays)
        ax.xaxis.set_minor_locator(alldays)
    else:
        weekFormatter = DateFormatter('%b %d, %Y')
    ax.xaxis.set_major_formatter(weekFormatter)
    ax.grid(True)

    # 创建K线图
    stock_array = np.array(stock_data[['Date', 'Open', 'High', 'Low', 'Close']])
    stock_array[:, 0] = date2num(stock_array[:, 0])
    candlestick_ohlc(ax, stock_array, colorup="red", colordown="green", width=0.6)

    # 可同时绘制其他折线图
    if otherseries is not None:
        for each in otherseries:
            plt.plot(stock_data['Date'], stock_data[each], linewidth=1.5, label=each)
        plt.legend()

    ax.xaxis_date()
    ax.autoscale_view()
    plt.rcParams['axes.facecolor'] = 'white'
    plt.setp(plt.gca().get_xticklabels(), rotation=45, horizontalalignment='right')
    print('SAVING...')
    plt.savefig('./'+filename+'.png')
    print('SAVING SUCCEED.')
    #plt.show()

def store_pic(filename):
    f = open(file='./'+filename+'.png', mode='rb')
    dataimg = f.read()
    f.close()

    nowtime = datetime.now().strftime("%Y-%m-%d")
    argdata = pymysql.Binary(dataimg)

    CREATE_PIC_SQL = '''
        CREATE TABLE IF NOT EXISTS `pic` (
        `pic_Id` int NOT NULL auto_increment,
        `pic_name` varchar(20) NOT NULL,
        `date` datetime DEFAULT NULL,
        `imgdata` longblob NOT NULL,
        PRIMARY KEY (`pic_Id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
    '''
    DELETE_SQL = "drop table if exists pic;"
    sqlone = "insert into pic(pic_name,date,imgdata) values(%s,%s,%s)"

    try:
        DB = pymysql.connect(host='localhost', user='root', password='c0nygre', charset='utf8', database=DATABASE)
        cursor = DB.cursor()
        #cursor.execute(DELETE_SQL)
        cursor.execute(CREATE_PIC_SQL)
        print('START INSERTING PLOT.')
        cursor.execute(sqlone, (filename,nowtime, argdata))
        DB.commit()
        print('INSERTING SUCCEED %s.'%filename)
    except Exception as e:
        print(e)
        DB.rollback()
    finally:
        cursor.close()
        DB.close()
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    df_new = collect_data()
    filenames = ["'APPLE'", "'GOOGLE'", "'MICROSOFT'", "'AMAZON'"]
    for i in range(len(filenames)):
        df_Company =df_new.query("company_name ==" + filenames[i])
        #df_Company = df_new.query("company_name == 'APPLE'")
        df_Company['close_mean3'] = np.round(df_Company['Close'].rolling(window=3, center=False).mean(), 2)
        df_Company['close_mean10'] = np.round(df_Company['Close'].rolling(window=10, center=False).mean(), 2)
        pandas_candlestick_ohlc(df_Company, filenames[i],['close_mean3', 'close_mean10'])
        store_pic(filenames[i])


