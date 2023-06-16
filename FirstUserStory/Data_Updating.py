import os
from binance.client import Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import talib
from talib import MA_Type
import re
import threading
from google.cloud import bigquery
import pandas_gbq
from google.cloud import secretmanager
from google.oauth2 import service_account
import json 
client=Client(api_key='VvriEf3Lvl74QqdG8XnxeaUV2YyJ2u56ecRg8gh1Mb7mx7BAWUlrDC87nHEBIi40',api_secret='LwfOr7t6E9eqqG9JtxNacWTkvoKkI9Ftmr7JbYQ29MOWCL33x6nSdCbYbv04Ya8A')
###################################################
Interval="1m"
Interval=str(Interval)
#InitiateTime=input("Do you want to assign start time? \n ")
#os.chdir("/Users/shahriar/desktop/binance/Trading")
digit1=re.findall('\d+',Interval)
string1=re.findall('[a-zA-z]',Interval)
digit1[0]=float(digit1[0])
today = datetime.now()
d1 = today.strftime("%Y-%m-%d  %H:%M:%S")
d2=today-timedelta(days=2)
d2=d2.strftime("%Y-%m-%d  %H:%M:%S")
Start=str(d2)
print("The date that we collecting our data from: ",Start,"to ",str(d1))
#print(Start)
if string1[0]=='h' or string1[0]=='H':
    digit1[0]=digit1[0]*60
listOfCoins={"XRPUSDT","BTCUSDT","ETHUSDT","ADAUSDT","LTCUSDT","HBARUSDT","BNBUSDT","SOLUSDT","DOTUSDT","DOGEUSDT","UNIUSDT","XLMUSDT","MATICUSDT","AVAXUSDT","LINKUSDT","ALGOUSDT","TRXUSDT"}
secret_client = secretmanager.SecretManagerServiceClient()
project_id = 'data-engineering-2-2023'
dataset_id = 'dataframes'
location = 'EU' 
secret_id ="ShahriBabaki"
secret_version =1
secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/{secret_version}"

# Access the secret value
response = secret_client.access_secret_version(name=secret_name)
data = json.loads(response.payload.data.decode("utf-8"))
credentials = service_account.Credentials.from_service_account_info(data)
client_Gcp = bigquery.Client(credentials=credentials)
# Check if the project already exists
try:
    projects = client_Gcp.list_projects()
    for project in projects:
        #print(f"Project ID: {project.project_id}")
        #print("---")
        if str(project.project_id)==project_id:
            None
            #print(f"Project '{project.project_id}' already exists.")
except Exception as e:
    # Create the new project
    project = client_Gcp.create_project(project_id)

# Check if the dataset already exists
try:
    dataset_ref = client_Gcp.dataset(dataset_id, project=project_id)
    dataset = client_Gcp.get_dataset(dataset_ref)
except Exception as e:
    # Create the new dataset
    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset.location = 'EU'
    dataset.default_table_expiration_ms = None
    dataset = client_Gcp.create_dataset(dataset)




def process_coin_data(coinData):
    try:
        # request historical candle (or klines) data
        bars = client.futures_historical_klines(symbol=coinData,interval=Interval,start_str= str(Start),end_str=None, limit=500)
        for x in bars:
            x[0]=datetime.fromtimestamp(x[0]/1000)
        for line in bars:
            del line[6:]
        # option 4 - create a Pandas DataFrame and export to CSV
        btc_df = pd.DataFrame(bars, columns=['date', 'open', 'high', 'low', 'close','volume'])
        #btc_df.set_index('date', inplace=True)
        btc_df['date'] = btc_df['date'].astype('str')
        btc_df['open'] = btc_df['open'].astype('float') 
        btc_df['high'] = btc_df['high'].astype('float')
        btc_df['low'] = btc_df['low'].astype('float')
        btc_df['close'] = btc_df['close'].astype('float')
        pd.options.display.max_columns=None
        #btc_df['sma'] = talib.SMA(btc_df.close, timeperiod=20)
        #RSI
        btc_df['rsi'] = talib.RSI(btc_df.close, timeperiod=14)
        btc_df["rsi"] = btc_df['rsi'].astype('float') 
        btc_df["rsinp"] = btc_df['rsi'].values
        #btc_df["rsinp"] = rsinp[np.logical_not(np.isnan(rsinp))]
        #MACD
        btc_df['macd'],btc_df['macdsignal'],btc_df['macdhist'] = talib.MACD(btc_df.close, fastperiod=12, slowperiod=26, signalperiod=9)
                    #MACD
        btc_df['macd612'],btc_df['macdsignal612'],btc_df['macdhist612'] = talib.MACD(btc_df.close, fastperiod=6, slowperiod=12, signalperiod=9)
        #try:
            #btc_df['macd'] = btc_df['macd'].astype('float')
            #btc_df['macdsignal'] = btc_df['macdsignal'].astype('float')
        #except:
            #pass
        #ADX
        btc_df['ADX']=talib.ADX(btc_df.high, btc_df.low, btc_df.close, timeperiod=14)
        btc_df["ADX"] = btc_df['ADX'].astype('float')
        #DMI-
        btc_df['DMIM'] = talib.MINUS_DI(btc_df.high, btc_df.low, btc_df.close,timeperiod=14)
        btc_df['DMIM'] = btc_df['DMIM'].astype('float')
        #DMI+
        btc_df['DMIP'] = talib.PLUS_DI(btc_df.high, btc_df.low, btc_df.close,timeperiod=14)
        btc_df['DMIP'] = btc_df['DMIP'].astype('float')
        #MFI
        btc_df['MFI'] = talib.MFI(btc_df.high, btc_df.low, btc_df.close, btc_df.volume, timeperiod=14)
        btc_df['MFI'] = btc_df['MFI'].astype('float')
        #CMO
        btc_df['CMO']= talib.CMO(btc_df.close, timeperiod=14)
               #EMA (3)
        btc_df['EMA3']= talib.EMA(btc_df.close, timeperiod=3)
        btc_df['EMA3'] = btc_df['EMA3'].astype('float')  
        #EMA (5)
        btc_df['EMA5']= talib.EMA(btc_df.close, timeperiod=5)
        btc_df['EMA5'] = btc_df['EMA5'].astype('float')
                #EMA (6)
        btc_df['EMA6']= talib.EMA(btc_df.close, timeperiod=6)
        btc_df['EMA6'] = btc_df['EMA6'].astype('float')  
        #EMA (20)
        btc_df['EMA10']= talib.EMA(btc_df.close, timeperiod=10)
        btc_df['EMA10'] = btc_df['EMA10'].astype('float') 
        #EMA (50)
        #btc_df['EMA(50)']= talib.EMA(btc_df.close, timeperiod=50)
        #EMA (200)
        btc_df['EMA150']= talib.EMA(btc_df.close, timeperiod=150)
        btc_df['EMA150'] = btc_df['EMA150'].astype('float')
        btc_df['EMA200']= talib.EMA(btc_df.close, timeperiod=200)
        btc_df['EMA200'] = btc_df['EMA200'].astype('float')
        btc_df['EMA100']= talib.EMA(btc_df.close, timeperiod=100)
        btc_df['EMA100'] = btc_df['EMA100'].astype('float')
        btc_df['EMA13']= talib.EMA(btc_df.close, timeperiod=13)
        btc_df['EMA13'] = btc_df['EMA13'].astype('float')
        #BOLLINGER
        btc_df['upper'],btc_df[' middle'], btc_df['lower'] = talib.BBANDS(btc_df.close,timeperiod=20,nbdevup=2,nbdevdn=2,matype=0)
        #RSI Stochastic
        #btc_df["fastk"],btc_df["fastd"] = ti.stoch(btc_df["high"].values,btc_df["low"].values,btc_df["close"].values, 14, 3, 3)
        btc_df["fastk"],btc_df["fastd"]=talib.STOCH(btc_df.rsi, btc_df.rsi, btc_df.rsi,fastk_period=14,slowk_period=3,slowk_matype=MA_Type.EMA,slowd_period=3,slowd_matype=MA_Type.EMA)
        btc_df['fastk'] = btc_df['fastk'].astype('float')
        btc_df['fastd'] = btc_df['fastd'].astype('float')
        #Momentum
        btc_df["MOMENTUM"]=talib.MOM(btc_df.close, timeperiod=10)
        #Rate Of change
        btc_df["ROC"]= talib.ROC(btc_df.close, timeperiod=5)
        #William R%
        btc_df["WILLR"]=talib.WILLR(btc_df.high,btc_df.low,btc_df.close, timeperiod=14)
        #Mean of change of candle sticks
        btc_df["SLTP"]=btc_df.high/btc_df.low-1
        btc_df["CHANGE"]=btc_df.high-btc_df.low
        btc_df["SUPPORT"]=(btc_df.low<btc_df.low.shift(+1))&(btc_df.low<btc_df.low.shift(-1))&(btc_df.low.shift(+1)<btc_df.low.shift(+2))&(btc_df.low.shift(-1)<btc_df.low.shift(-2))
        btc_df["RESISTANCE"]=(btc_df.high>btc_df.high.shift(+1))&(btc_df.high>btc_df.high.shift(-1))&(btc_df.high.shift(+1)>btc_df.high.shift(+2))&(btc_df.high.shift(-1)>btc_df.high.shift(-2))
        btc_df["PriceMax9"]=talib.MAX(btc_df.high , timeperiod=9)
        btc_df["PriceMin9"]=talib.MIN(btc_df.low , timeperiod=9)
        btc_df["PriceMax20"]=talib.MAX(btc_df.high , timeperiod=26)
        btc_df["PriceMin20"]=talib.MIN(btc_df.low , timeperiod=26)
        btc_df['PriceMax9'] = btc_df['PriceMax9'].astype('float')
        btc_df['PriceMin9'] = btc_df['PriceMin9'].astype('float')
        btc_df['PriceMax20'] = btc_df['PriceMax20'].astype('float')
        btc_df['PriceMin20'] = btc_df['PriceMin20'].astype('float')
        btc_df["ConversionLine"]=(btc_df.PriceMax9+btc_df.PriceMin9)/2
        btc_df["BaseLine"]=(btc_df.PriceMax20+btc_df.PriceMin20)/2
        #display(btc_df.tail())
        #btc_df.dropna(inplace=True)
        btc_df["macdLong"]=(btc_df.macd<0)&(btc_df.macdsignal<0)&(btc_df.macd>btc_df.macdsignal)
        btc_df["macdShort"]=(btc_df.macd>0)&(btc_df.macdsignal>0)&(btc_df.macd<btc_df.macdsignal)
        btc_df["macdLong100"]=(btc_df.macd<0)&(btc_df.macdsignal<0)&(btc_df.macd>btc_df.macdsignal)&(btc_df.close>btc_df.EMA100)
        btc_df["macdShort100"]=(btc_df.macd>0)&(btc_df.macdsignal>0)&(btc_df.macd<btc_df.macdsignal)&(btc_df.close<btc_df.EMA100)
        btc_df["DirectionMovingLong"]=((btc_df.DMIP>btc_df.DMIM)&(btc_df.DMIP.shift(+1)<=btc_df.DMIM.shift(+1))&(btc_df.ADX>btc_df.ADX.shift(+1))&(btc_df.macd>btc_df.macdsignal))|((btc_df.DMIP>btc_df.DMIM)&(btc_df.DMIP.shift(+1)>btc_df.DMIM.shift(+1))&(btc_df.DMIP.shift(+2)<=btc_df.DMIM.shift(+2))&(btc_df.ADX>btc_df.ADX.shift(+1))&(btc_df.macd>btc_df.macdsignal))|((btc_df.DMIP>btc_df.DMIM)&(btc_df.DMIP.shift(+2)>btc_df.DMIM.shift(+2))&(btc_df.DMIP.shift(+3)<=btc_df.DMIM.shift(+3))&(btc_df.ADX>btc_df.ADX.shift(+1))&(btc_df.macd>btc_df.macdsignal))
        btc_df["DirectionMovingShort"]=((btc_df.DMIP<btc_df.DMIM)&(btc_df.DMIP.shift(+1)>=btc_df.DMIM.shift(+1))&(btc_df.ADX>btc_df.ADX.shift(+1))&(btc_df.macd<btc_df.macdsignal))|((btc_df.DMIP<btc_df.DMIM)&(btc_df.DMIP.shift(+1)<btc_df.DMIM.shift(+1))&(btc_df.DMIP.shift(+2)>=btc_df.DMIM.shift(+2))&(btc_df.ADX>btc_df.ADX.shift(+1))&(btc_df.macd<btc_df.macdsignal))|((btc_df.DMIP<btc_df.DMIM)&(btc_df.DMIP.shift(+2)<btc_df.DMIM.shift(+2))&(btc_df.DMIP.shift(+3)>=btc_df.DMIM.shift(+3))&(btc_df.ADX>btc_df.ADX.shift(+1))&(btc_df.macd<btc_df.macdsignal))
        btc_df["Short"]=(btc_df.macd<btc_df.macdsignal)
        btc_df["Long"]=(btc_df.macd>btc_df.macdsignal)
        btc_df["rsiLong30"]=(btc_df.rsi>30)&(btc_df.rsi.shift(+1)<30)  
        #btc_df["rsiLong50"]=(btc_df.rsi>50)&(btc_df.rsi.shift(+1)<50)  
        btc_df["rsiShort70"]=(btc_df.rsi<70)&(btc_df.rsi.shift(+1)>70)  
        #btc_df["rsiShort50"]=(btc_df.rsi<50)&(btc_df.rsi.shift(+1)>50)  
        btc_df["EmaShort"]=(btc_df.EMA13>btc_df.EMA5)&(btc_df.EMA13.shift(+1)<=btc_df.EMA5.shift(+1))
        btc_df["EmaLong"]=(btc_df.EMA5>btc_df.EMA13)&(btc_df.EMA5.shift(+1)<=btc_df.EMA13.shift(+1))	
        btc_df["EmaLong612"]=(btc_df.EMA3>btc_df.EMA6)&(btc_df.EMA3.shift(+1)<=btc_df.EMA6.shift(+1))&(btc_df.macdsignal612<0)&(btc_df.macd612>btc_df.macdsignal612)&(btc_df.close>btc_df.EMA200) 
        btc_df["EmaShort612"]=(btc_df.EMA3<btc_df.EMA6)&(btc_df.EMA3.shift(+1)>=btc_df.EMA6.shift(+1))&(btc_df.macdsignal612>0)&(btc_df.macd612<btc_df.macdsignal612)&(btc_df.close<btc_df.EMA200) 
        btc_df["bbShort"]=(btc_df.fastk<90)&(btc_df.fastk.shift(+1)>90)&(btc_df.high>=btc_df.upper)&(btc_df.close<btc_df.EMA200)
        btc_df["bbLong"]=(btc_df.fastk>10)&(btc_df.fastk.shift(+1)<10)&(btc_df.low<=btc_df.lower)&(btc_df.close>btc_df.EMA200)
        btc_df["IchiLong"]=(btc_df.ConversionLine>btc_df.BaseLine)&(btc_df.ConversionLine.shift(+1)==btc_df.BaseLine.shift(+1))&((btc_df.ConversionLine>btc_df.ConversionLine.shift(+1))|(btc_df.BaseLine>btc_df.BaseLine.shift(+1)))&(btc_df.BaseLine>btc_df.BaseLine.shift(+1))&(btc_df.ConversionLine>btc_df.ConversionLine.shift(+1))
        btc_df["IchiShort"]=(btc_df.ConversionLine<btc_df.BaseLine)&(btc_df.ConversionLine.shift(+1)==btc_df.BaseLine.shift(+1))&((btc_df.ConversionLine<btc_df.ConversionLine.shift(+1))|(btc_df.BaseLine<btc_df.BaseLine.shift(+1)))&(btc_df.BaseLine<btc_df.BaseLine.shift(+1))&(btc_df.ConversionLine<btc_df.ConversionLine.shift(+1))
        btc_df["MfiStochLong"]=(btc_df.MFI<20)&((btc_df.fastk<20)|(btc_df.fastd<20))
        btc_df["MfiStochShort"]=(btc_df.MFI>80)&((btc_df.fastk>80)|(btc_df.fastd>80))
        btc_df["SLTP"]=btc_df.high/btc_df.low-1
        btc_df["CHANGE"]=btc_df.high-btc_df.low
        btc_df["SUPPORT"]=(btc_df.low<btc_df.low.shift(+1))&(btc_df.low<btc_df.low.shift(-1))&(btc_df.low.shift(+1)<btc_df.low.shift(+2))&(btc_df.low.shift(-1)<btc_df.low.shift(-2))
        btc_df["RESISTANCE"]=(btc_df.high>btc_df.high.shift(+1))&(btc_df.high>btc_df.high.shift(-1))&(btc_df.high.shift(+1)>btc_df.high.shift(+2))&(btc_df.high.shift(-1)>btc_df.high.shift(-2))

        #btc_df.dropna(inplace=True)
        btc_df["macdLong"]=(btc_df.macd<0)&(btc_df.macdsignal<0)&(btc_df.macd>btc_df.macdsignal)
        btc_df["macdShort"]=(btc_df.macd>0)&(btc_df.macdsignal>0)&(btc_df.macd<btc_df.macdsignal)
        btc_df["DirectionMovingLong"]=((btc_df.DMIP>btc_df.DMIM)&(btc_df.DMIP.shift(+1)<=btc_df.DMIM.shift(+1))&(btc_df.ADX>btc_df.ADX.shift(+1))&(btc_df.macd>btc_df.macdsignal))|((btc_df.DMIP>btc_df.DMIM)&(btc_df.DMIP.shift(+1)>btc_df.DMIM.shift(+1))&(btc_df.DMIP.shift(+2)<=btc_df.DMIM.shift(+2))&(btc_df.ADX>btc_df.ADX.shift(+1))&(btc_df.macd>btc_df.macdsignal))|((btc_df.DMIP>btc_df.DMIM)&(btc_df.DMIP.shift(+2)>btc_df.DMIM.shift(+2))&(btc_df.DMIP.shift(+3)<=btc_df.DMIM.shift(+3))&(btc_df.ADX>btc_df.ADX.shift(+1))&(btc_df.macd>btc_df.macdsignal))
        btc_df["DirectionMovingShort"]=((btc_df.DMIP<btc_df.DMIM)&(btc_df.DMIP.shift(+1)>=btc_df.DMIM.shift(+1))&(btc_df.ADX>btc_df.ADX.shift(+1))&(btc_df.macd<btc_df.macdsignal))|((btc_df.DMIP<btc_df.DMIM)&(btc_df.DMIP.shift(+1)<btc_df.DMIM.shift(+1))&(btc_df.DMIP.shift(+2)>=btc_df.DMIM.shift(+2))&(btc_df.ADX>btc_df.ADX.shift(+1))&(btc_df.macd<btc_df.macdsignal))|((btc_df.DMIP<btc_df.DMIM)&(btc_df.DMIP.shift(+2)<btc_df.DMIM.shift(+2))&(btc_df.DMIP.shift(+3)>=btc_df.DMIM.shift(+3))&(btc_df.ADX>btc_df.ADX.shift(+1))&(btc_df.macd<btc_df.macdsignal))
        btc_df["rsiLong30"]=((btc_df.rsi>30)&(btc_df.rsi.shift(+1)<30))|((btc_df.rsi.shift(+1)<30)&(btc_df.rsi.shift(+1)>btc_df.rsi))
        btc_df["rsiShort70"]=((btc_df.rsi<70)&(btc_df.rsi.shift(+1)>70))|((btc_df.rsi.shift(+1)>70)&(btc_df.rsi.shift(+1)<btc_df.rsi))
        btc_df["StochShort"]=((btc_df.fastk<90)&(btc_df.fastk.shift(+1)>90))|((btc_df.fastk.shift(+1)>90)&(btc_df.fastk.shift(+1)<btc_df.fastk))
        btc_df["StochLong"]=((btc_df.fastk>10)&(btc_df.fastk.shift(+1)<10))|((btc_df.fastk.shift(+1)<10)&(btc_df.fastk.shift(+1)>btc_df.fastk))
        btc_df["MfiStochLong"]=(btc_df.MFI<20)&((btc_df.fastk<20)|(btc_df.fastd<20))
        btc_df["MfiStochShort"]=(btc_df.MFI>80)&((btc_df.fastk>80)|(btc_df.fastd>80))
        btc_df.dropna(subset=["DirectionMovingLong","DirectionMovingShort","rsiLong30","rsiShort70","macdShort","macdLong","MfiStochShort","MfiStochLong","StochShort","StochLong"],inplace=True)
        for index in btc_df.index:
            if 6<index<len(btc_df)-6:
                n=0
                if   btc_df.loc[index,'close']>1.0028*btc_df.loc[index+3,'close']or btc_df.loc[index,'close']>1.0018*btc_df.loc[index+2,'close'] or  btc_df.loc[index,'close']>1.0015*btc_df.loc[index+1,'close']:
                    btc_df.loc[index,'DECISION']="SELL1"
                    n=1
                if n==0:
                    if 1.0028*btc_df.loc[index,'close']<btc_df.loc[index+3,'close'] or 1.0018*btc_df.loc[index,'close']<btc_df.loc[index+2,'close'] or 1.0015*btc_df.loc[index,'close']<btc_df.loc[index+1,'close']:
                        btc_df.loc[index,'DECISION']="BUY1"
                        n=1
                if n==0:
                    if  btc_df.loc[index,'close']>1.0045*btc_df.loc[index+5,'close']or btc_df.loc[index,'close']>1.0035*btc_df.loc[index+4,'close'] or  btc_df.loc[index,'close']>1.0028*btc_df.loc[index+3,'close']:
                        btc_df.loc[index,'DECISION']="SELL2"
                        n=1
                if n==0:
                    if  1.0045*btc_df.loc[index,'close']<btc_df.loc[index+5,'close'] or 1.0035*btc_df.loc[index,'close']<btc_df.loc[index+4,'close'] or 1.0028*btc_df.loc[index,'close']<btc_df.loc[index+3,'close']:
                        btc_df.loc[index,'DECISION']="BUY2"
                        n=1
                if n==0:
                    btc_df.loc[index,'DECISION']="NEXT"
            else:
                continue


        try:
            #btc_df.to_csv("DataT%s/%s%s.csv"%(Interval,coinData,Interval),index=False)
            table_name=f'{coinData}{Interval}'
            table_id = f'{project_id}.{dataset_id}.{table_name}'
            #btc_df.to_gbq(destination_table=table_id, project_id=project_id, if_exists='replace', progress_bar=True)
            pandas_gbq.to_gbq(btc_df,destination_table=table_id, project_id=project_id, if_exists='replace', progress_bar=False)

        except:
            #if not os.path.exists("DataT%s"%Interval): 
                #os.mkdir("DataT%s"%Interval)
            #btc_df.to_csv("DataT%s/%s%s.csv"%(Interval,coinData,Interval),index=False)
            table_name=f'{coinData}{Interval}'
            table_id = f'{project_id}.{dataset_id}.{table_name}'
            pandas_gbq.to_gbq(btc_df,destination_table=table_id, project_id=project_id, if_exists='replace', progress_bar=False)
    except:
        print(" There is an error in ",coinData)
threads = []
for coinData in listOfCoins:
    thread = threading.Thread(target=process_coin_data, args=(coinData,))
    thread.start()
    threads.append(thread)
for thread in threads:
    thread.join()








