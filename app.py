from flask import Flask, render_template, url_for, request, redirect

import google.auth
from google.cloud import storage
from google.cloud import bigquery, bigquery_storage
from google.oauth2 import service_account
from googleapiclient import discovery

import json, csv
import math
import numpy as np
import pandas as pd
import pandas_datareader as pdr
from datetime import datetime, date, timedelta
import yfinance as yf
#from yahoofinancials import YahooFinancials
import random

from time import time

# %% Global variables and functions

quantity_threshold = 256
customer_id = 'C2193'

def execute_orders(order_dict, client):
    execute_start = time()
    
    print("Running in execute_orders func:")
    
    for ticker, action in order_dict.items():
        print('-'*10)
        print(ticker, action)
        
        # Get Accumulated Quantity in normal table
        sql_normal = "SELECT SUM(Quantity) AS Quantity FROM final-year-project-ftec.pending_orders.normal WHERE Ticker = \""+ticker+"\" AND BuySell = \""+action+"\""
        # print(sql_normal)
        df_normal = client.query(sql_normal).to_dataframe()
        df_normal['Quantity'] = df_normal['Quantity'].fillna(0)
        # print(df_normal)
        accumulated_quantity_normal = df_normal.at[0, 'Quantity']
        # print(accumulated_quantity_normal)
        
        # Get Accumulated Quantity in etf table
        sql_etf = "SELECT SUM(Volume) AS Volume FROM final-year-project-ftec.pending_orders.etf WHERE Ticker = \""+ticker+"\" AND BuySell = \""+action+"\""
        # print(sql_etf)
        df_etf = client.query(sql_etf).to_dataframe()
        df_etf['Volume'] = df_etf['Volume'].fillna(0)
        accumulated_volume_etf = df_etf.at[0, 'Volume']
        # print(accumulated_volume_etf)
        
        # stock_info = yf.Ticker(ticker).info
        # market_price = stock_info['regularMarketPrice']
        market_price = yf.Ticker(ticker).info['regularMarketPrice']
        accumulated_quantity_etf = accumulated_volume_etf / market_price
        
        # Get Total Accumulated Quantity in two tables
        accumulated_quantity_sum = accumulated_quantity_normal + accumulated_quantity_etf
        print(f'Sum of accumulated quantity: {accumulated_quantity_sum}')
        
        # Check for execution
        if accumulated_quantity_sum >= quantity_threshold:
            print("Execute Orders!")
            
            # Move completed normal orders to completed_orders table
            
            # Get executable orders from normal table 
            sql_get_normal = "SELECT * FROM final-year-project-ftec.pending_orders.normal WHERE Ticker = \""+ticker+"\" AND BuySell = \""+action+"\""
            df_get_normal = client.query(sql_get_normal).to_dataframe()
            
            # Add columns for completed_orders table
            df_get_normal["ExecutionTime"] = datetime.now()
            df_get_normal["ExecutionPrice"] = market_price
            ratio = math.floor(accumulated_quantity_sum) / accumulated_quantity_sum
            df_get_normal["ExecutionQuantity"] = df_get_normal['Quantity'] * ratio
            df_get_normal["ExecutionVolume"] = df_get_normal["ExecutionQuantity"] * df_get_normal["ExecutionPrice"]
            df_get_normal["ETF"] = False
            print(f'Executable normal orders: \n{df_get_normal.head()}')

            # Upload to completed_orders table
            table_id = "final-year-project-ftec.completed_orders.completed_orders"

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # WRITE_TRUNCATE
                autodetect=True,
            )
            
            job = client.load_table_from_dataframe(
                df_get_normal, table_id, job_config=job_config
            )  # Make an API request.
            job.result()
            print("Uploaded completed normal orders to completed_orders")
            
            # Delete completed normal orders from normal table
            sql_del_normal = "SELECT * FROM final-year-project-ftec.pending_orders.normal WHERE NOT (Ticker = \"" + ticker + "\" AND BuySell = \"" + action+"\")"
            remaining_orders_normal = client.query(sql_del_normal).to_dataframe()
            table_id = "final-year-project-ftec.pending_orders.normal"

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # WRITE_APPEND
                autodetect=True,
            )
            job = client.load_table_from_dataframe(
                remaining_orders_normal, table_id, job_config=job_config
            )  # Make an API request.
            job.result()
            print("Deleted completed orders from normal database \n")
            
            
            # Move completed etf orders to completed_orders table
            
            # Get executable orders from normal table 
            sql_get_etf = "SELECT * FROM final-year-project-ftec.pending_orders.etf WHERE Ticker = \""+ticker+"\" AND BuySell = \""+action+"\""
            df_get_etf = client.query(sql_get_etf).to_dataframe()
            
            # Add columns for completed_orders table
            df_get_etf["ExecutionTime"] = datetime.now()
            df_get_etf["ExecutionPrice"] = market_price
            df_get_etf["ExecutionQuantity"] = (df_get_etf["Volume"] / df_get_etf["ExecutionPrice"]) * ratio
            df_get_etf["ExecutionVolume"] = df_get_etf["ExecutionPrice"] * df_get_etf["ExecutionQuantity"]
            df_get_etf["ETF"] = True
            print(f'Executable etf orders: \n{df_get_etf.head()}')
            
            # Upload to completed_orders table
            table_id = "final-year-project-ftec.completed_orders.completed_orders"

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # WRITE_TRUNCATE
                autodetect=True,
            )
            job = client.load_table_from_dataframe(
                df_get_etf, table_id, job_config=job_config
            )  # Make an API request.
            job.result()
            print("Uploaded completed etf orders to completed_orders")
            
            # Delete completed etf orders from etf table
            sql_del_etf = "SELECT * FROM final-year-project-ftec.pending_orders.etf WHERE NOT (Ticker = \"" + ticker + "\" AND BuySell = \"" + action+"\")"
            remaining_orders_etf = client.query(sql_del_etf).to_dataframe()
            table_id = "final-year-project-ftec.pending_orders.etf"

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # WRITE_APPEND
                autodetect=True,
            )
            job = client.load_table_from_dataframe(
                remaining_orders_etf, table_id, job_config=job_config
            )  # Make an API request.
            job.result()
            print("Deleted completed orders from etf database")
            
        else: 
            print("Do nothing.")
        
    print(f'* Time used by execute_orders: {time() - execute_start}')
        
    return

# %%

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True
 
@app.route('/')
@app.route('/home')
def home():
    
    # Oil Price  
    oil = yf.download('CL=F', period='7d')['Close']
    CurdeOilPrice = "{:.2f}".format(oil[-1])
    change_CurdeOilPrice = "{:.2f}".format((oil[-1]-oil[-2])/oil[-2]*100)
    
    # S&P 500  
    sp_2 = yf.download('^GSPC', period='7d')['Close']
    sp = "{:.2f}".format(sp_2[-1])
    change_sp = "{:.2f}".format((sp_2[-1]-sp_2[-2])/sp_2[-2]*100)
    
    # Dow Jones 
    dow_2 = yf.download('^DJI', period='7d')['Close']
    dow = "{:.2f}".format(dow_2[-1])
    change_dow = "{:.2f}".format((dow_2[-1]-dow_2[-2])/dow_2[-2]*100)
    
    # Nasdaq Futures
    nasdaq_2 = yf.download('^IXIC', period='7d')['Close']
    nasdaq = "{:.2f}".format(nasdaq_2[-1])
    change_nasdaq = "{:.2f}".format((nasdaq_2[-1]-nasdaq_2[-2])/nasdaq_2[-2]*100)
        
    return render_template("index.html", CurdeOilPrice=CurdeOilPrice, change_CurdeOilPrice=change_CurdeOilPrice, 
                           sp=sp, change_sp=change_sp, dow=dow, change_dow=change_dow, nasdaq=nasdaq, change_nasdaq=change_nasdaq)



@app.route("/index.html")
def index():
    
    # Oil Price  
    oil = yf.download('CL=F', period='7d')['Close']
    CurdeOilPrice = "{:.2f}".format(oil[-1])
    change_CurdeOilPrice = "{:.2f}".format((oil[-1]-oil[-2])/oil[-2]*100)
    
    # S&P 500  
    sp_2 = yf.download('^GSPC', period='7d')['Close']
    sp = "{:.2f}".format(sp_2[-1])
    change_sp = "{:.2f}".format((sp_2[-1]-sp_2[-2])/sp_2[-2]*100)
    
    # Dow Jones 
    dow_2 = yf.download('^DJI', period='7d')['Close']
    dow = "{:.2f}".format(dow_2[-1])
    change_dow = "{:.2f}".format((dow_2[-1]-dow_2[-2])/dow_2[-2]*100)
    
    # Nasdaq Futures
    nasdaq_2 = yf.download('^IXIC', period='7d')['Close']
    nasdaq = "{:.2f}".format(nasdaq_2[-1])
    change_nasdaq = "{:.2f}".format((nasdaq_2[-1]-nasdaq_2[-2])/nasdaq_2[-2]*100)
        
        
    return render_template("index.html", CurdeOilPrice=CurdeOilPrice, change_CurdeOilPrice=change_CurdeOilPrice, 
                           sp=sp, change_sp=change_sp, dow=dow, change_dow=change_dow, nasdaq=nasdaq, change_nasdaq=change_nasdaq)

@app.route("/portfolio-view.html")
def port_view():
    return render_template("portfolio-view.html")

@app.route("/data/overview-holdings.json")
def overview_holdings():
    return render_template("/data/overview-holdings.json")

@app.route("/data/portfolio-summary.csv")
def port_sum():
    return render_template("/data/portfolio-summary.csv")

@app.route("/portfolio-transfer.html")
def port_transfer():
    return render_template("portfolio-transfer.html")

@app.route("/trading.html")
def trading():
    return render_template("trading.html")

@app.route("/history.html")
def history():
    return render_template("history.html")

@app.route("/news.html")
def news():
    return render_template("news.html")

@app.route("/data/portfolio-holdings.json")
def portfolio_holdings():
    return render_template("/data/portfolio-holdings.json")

@app.route("/data/portfolio-allocation.json")
def portfolio_allocation():
    return render_template("/data/portfolio-allocation.json")

@app.route("/data/transaction-record.json")
def transaction_record():
    return render_template("/data/transaction-record.json")

@app.route("/filter-transaction-record",methods=['POST', 'GET'])
def filter_transaction_record():
    
    output = request.form.to_dict()
    print(output)
    
    output = pd.DataFrame.from_dict(output, orient='index').T
    output['type'] = output['type'].map({'All':'All','Normal':False,'ETF':True})
    
    # Connect to GCP
    credentials = service_account.Credentials.from_service_account_file('./google_cloud/credential.json')
    project_id = 'final-year-project-ftec'
    client = bigquery.Client(credentials=credentials, project=project_id)
    
    sql_transaction_record = "SELECT ExecutionTime,Ticker,BuySell,ExecutionPrice,ExecutionQuantity,ExecutionVolume,ETF \
        FROM final-year-project-ftec.completed_orders.completed_orders WHERE CustomerId = \"" + customer_id+"\"" 
    
    if output.at[0,'buysell'] in ('Buy', 'Sell'):
        sql_transaction_record = sql_transaction_record + " AND BuySell = \"" + output.at[0,'buysell'] + "\"" 
    if output.at[0,'type'] != 'All':
        sql_transaction_record = sql_transaction_record + " AND ETF = " + str(output.at[0,'type'])
    if output.at[0,'date-start']:
        sql_transaction_record = sql_transaction_record + " AND ExecutionTime >= \"" + str(output.at[0,'date-start']) + "\"" 
    if output.at[0,'date-end']:
        sql_transaction_record = sql_transaction_record + " AND ExecutionTime <= \"" + str(output.at[0,'date-end']) + "\""   
    sql_transaction_record = sql_transaction_record + " ORDER BY ExecutionTime DESC"
    
    print(sql_transaction_record, "\n")
    
    
    df_transaction_record = client.query(sql_transaction_record).to_dataframe()
    print(df_transaction_record)
    # df_transaction_record = df_transaction_record[['ExecutionTime','Ticker','BuySell','ExecutionPrice','ExecutionQuantity','ExecutionVolume','ETF']]
    if not df_transaction_record.empty:
        df_transaction_record['ExecutionTime'] = df_transaction_record['ExecutionTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_transaction_record['ETF'] = df_transaction_record['ETF'].map({True: 'ETF', False: 'Normal'})  

        df_transaction_record = df_transaction_record.rename(columns={'ExecutionTime':'Execution Time', 'Ticker':'Symbol', 'BuySell':'Action', 'ExecutionPrice':'Price', 'ExecutionQuantity':'Quantity', 'ExecutionVolume':'Volume', 'ETF':'Type'})
    print(df_transaction_record.head())
    
    df_transaction_record.to_json("./templates/data/transaction-record.json", orient ='records', indent=4)
    
    return redirect(request.referrer)

@app.route('/gen-normal-order',methods=['POST', 'GET'])
def gen_normal_order():
    
    gen_normal_start = time()
    
    # Create dataframe for gathering the output 
    output = request.form.to_dict() 
    print(output)
    ticker = output["ticker"] #TSLA
    buysell = output["buysell"] #Buy
    quantity = output["quantity"]
    OrderNo = np.random.randint(10000000,99999999)
    OrderTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    dict = {'OrderNo': [OrderNo],
            'CustomerId': [customer_id],
            'Ticker': ticker,
            'OrderTime': [OrderTime],
            'BuySell': buysell,
            'Quantity': quantity}
    result_df = pd.DataFrame(dict)
    print(result_df.dtypes)
    
    # Change data type of the output
    result_df = result_df.astype({"OrderNo": "int32", "CustomerId": "str", "Ticker": 'str', 'BuySell': 'str', 'Quantity': 'float'}, errors='ignore')
    print(result_df.dtypes)
    #result_df.to_csv('output.csv', index=False)
    
    # Generate etf dict {Ticker: Buy/Sell}
    order_dict = {ticker : buysell}
    print(f'result dict: {order_dict}')

    # Generate etf json
    result_json = result_df[['OrderNo','CustomerId','Ticker','OrderTime','BuySell','Quantity']].to_dict(orient="records")
    print(f'result json: \n{result_json}')
    
    # Upload orders to pending_orders table
    credentials = service_account.Credentials.from_service_account_file('google_cloud/credential.json')

    project_id = 'final-year-project-ftec'

    client = bigquery.Client(credentials=credentials, project=project_id)

    result = [json.dumps(record) for record in result_json]
    with open('google_cloud/format.json', 'w') as obj:
        for i in result:
            obj.write(i + '\n')

    table_id = "final-year-project-ftec.pending_orders.normal"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # WRITE_TRUNCATE
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
    )

    with open("google_cloud/format.json", "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config
                                           )
                                      
    job.result()  # Waits for the job to complete.
    print("Normal order uploaded to pending_orders table")
    
    print(f'* Time used by gen_normal_order: {time() - gen_normal_start}')
    
    execute_orders(order_dict, client)
    
    return redirect(request.referrer)
    
@app.route('/gen-etf-order',methods=['POST', 'GET'])
def gen_etf_order():
    gen_etf_start = time()
    
    from_html = request.form.to_dict() 
    
    from_html = list(from_html.values())

    ticker_list = from_html[::3] # Elements from list starting from 0 iterating by 3
    cpct_list = from_html[1::3] # Elements from list starting from 1 iterating by 3
    tpct_list = from_html[2::3] # Elements from list starting from 2 iterating by 3

    portfolio_value = 1000 # get from db, haven't thought
    print(f'from html: \n{from_html}')
    
    # Make a dataframe of details of orders
    etf_df = pd.DataFrame({'Ticker': ticker_list,'cpct': cpct_list, 'tpct':tpct_list})
    etf_df = etf_df.replace(r'^\s*$', 0, regex=True)
    etf_df['OrderNo'] = np.random.randint(10000000,99999999, etf_df.shape[0])
    etf_df['CustomerId'] = customer_id
    etf_df['change_pct'] = etf_df['tpct'].astype('float') - etf_df['cpct'].astype('float')
    etf_df['BuySell'] = etf_df['change_pct'].apply(lambda x : "Buy" if x > 0 else "Sell")
    etf_df['Volume'] = abs(etf_df['change_pct']*portfolio_value/100)
    etf_df['OrderTime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Generate etf json
    etf_json = etf_df[['OrderNo','CustomerId','Ticker','OrderTime','BuySell','Volume']].to_dict(orient="records")
    print(f'ETF json: \n{etf_json}')
    
    # Generate etf dict {Ticker: Buy/Sell}
    order_dict = dict(zip(etf_df['Ticker'], etf_df['BuySell']))
    print(f'ETF dict: {order_dict}')
    for key, value in order_dict.items():
        print(key, ': ', value)

    # Upload orders to pending_orders table
    credentials = service_account.Credentials.from_service_account_file('google_cloud/credential.json')

    project_id = 'final-year-project-ftec'

    client = bigquery.Client(credentials=credentials, project=project_id)

    # format json cater for GCP upload
    result = [json.dumps(record) for record in etf_json]
    with open('google_cloud/format.json', 'w') as obj:
        for i in result:
            obj.write(i + '\n')

    table_id = "final-year-project-ftec.pending_orders.etf"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # WRITE_TRUNCATE
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
    )

    with open("google_cloud/format.json", "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
                                      
    job.result()  # Waits for the job to complete.
    print("ETF orders uploaded to pending_orders table")
    
    print(f'* Time used by gen_etf_order: {time() - gen_etf_start}')
    
    execute_orders(order_dict, client)
        
    return redirect(request.referrer)

if __name__ == "__main__":
    app.run(debug=False,port=5002)
    