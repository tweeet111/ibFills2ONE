###### Using ib_insync with ib.fills() to extract filled (option) trades instantly and create IB-FlexQuery formatted import file for OptionNetExplorer
###### Requirements: TWS, TWS API installation, Python >3.6, ib_insync library

from ib_insync import *
import pandas as pd
import numpy as np
from datetime import datetime as dt
from dateutil import tz
import logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
										 level=logging.INFO)
#util.patchAsyncio()



def import_fills():
	
	
	# connection function for ib_insync --- try until connected or timeout
	# argument client number
	def conn_tws(client=np.random.randint(0,10)):
		i=0
		while ib.isConnected() == False and i<20:
			try:
				print("connecting...")
				ib.connect('127.0.0.1', 7496, clientId=client, timeout=None)#tws 7496 gateway 4001
				ib.sleep(1)
			except ConnectionRefusedError:
				ib.sleep(3)
				i +=1
		if ib.isConnected() == True:
			print("Connection established")
		else:
			print ("Timeout")


# create ib_insync IB() instance and connect to TWS
	
	ib=IB()
	ib.sleep(0.1)
	conn_tws(0)
	ib.sleep(0.1)
	# call ib.fills() and get all recorded fills (usually last 7days ??)

	fills = ib.fills()
	ib.sleep(0.1)

# settings to convert timezone in import file  to local
	to_local = tz.gettz('Europe/Berlin')
	to_chicago = tz.gettz('America/Chicago')
	t_str = dt.today().strftime('%Y%m%d')

		# creating dataframe from fills list
	cols =["TradeDate" , "TradeTimeUTC" , "TradeTimeCDT", "TradeTime", "Buy/Sell" , "conID", "AssetClass" , "Multiplier", "Symbol" , "Quantity" , "TradePrice" , "IBCommission" , "Currency", "UnderlyingSymbol", "Right", "permId", "execId", "orderRef","Expiration", "Description"]
	fills_df = pd.DataFrame([[dt.strftime(fill.execution.time.astimezone(to_local),'%Y%m%d'),
													dt.strftime(fill.execution.time,'%H%M%S'),
													dt.strftime(fill.execution.time.astimezone(to_chicago),'%H%M%S'),
													dt.strftime(fill.execution.time.astimezone(to_local),'%H%M%S'),
													fill.execution.side,
													fill.contract.conId,
													fill.contract.secType,
                                                    fill.contract.multiplier,
													fill.contract.localSymbol,
													fill.execution.shares,
													fill.execution.price,
													fill.commissionReport.commission,
													fill.contract.currency,
													fill.contract.symbol,
													fill.contract.right,
													fill.execution.permId,
													fill.execution.execId,
													fill.execution.orderRef,
													fill.contract.lastTradeDateOrContractMonth,
													"IB Import"]
												 for fill in fills if fill.contract.secType not in ['BAG','CASH']], columns=cols)

	#disconnect from TWS
	ib.disconnect()
	ib.waitOnUpdate(timeout=0.1)

	# for stocks multiplier is 1 but is  provided by IB as ""
	fills_df['Multiplier'] = fills_df['Multiplier'].replace("",1).astype(float)
	# Add PnL manually beacause TWS reports inconsistantly
	fills_df['NetCash'] = -(fills_df['Quantity']*fills_df['Multiplier']*fills_df['TradePrice'])-fills_df['IBCommission'].where(fills_df['Buy/Sell'] == "BOT") 
	fills_df['NetCash'] = fills_df['NetCash'].fillna(
                       (fills_df['Quantity']*fills_df['Multiplier']*fills_df['TradePrice'])-fills_df['IBCommission'])

	#######################################################################################################
	# ONE import file with FlexQueryImport formatted headers
	# filter for Option Trades only and SPX UL ( European options e.g. Covered Calls throw error in ONE)
	filter_list = ['SPX', 'SPY']
	df_one = fills_df.query('AssetClass == "OPT" & UnderlyingSymbol in @filter_list')
	# filter for today's trades only if needed to keep data short and clear, assuming one usually imports during the same session
	#df_one = df_one.query('TradeDate == @t_str')

	df_one = df_one[['TradeDate','TradeTime','Buy/Sell','AssetClass','Symbol','Quantity','TradePrice','IBCommission','UnderlyingSymbol','NetCash']].replace("BOT","BUY").replace("SLD","SELL")   # replace signal words "buy/sell" from ib.fills to import format

	# create filename with date/time suffix
	t = dt.now().strftime('%Y%m%d-%H%M%S')
	filename= "ONE_import_"+t+".csv"

#export to same folder where the script is running
	if df_one.empty == False:
		df_one.to_csv(filename, index=False)


	#############################################################################################################
	# Make Quantity values negative for Sells and positive for Buys ---- easier to calculate positions from tradelog.csv
	
	fills_df['Quantity'] = np.where(fills_df['Buy/Sell'] =='SLD', fills_df['Quantity']*(-1), fills_df['Quantity'])

	############################################################################################################

	df_log = pd.read_csv("trade_log.csv")
	df = pd.concat([df_log, fills_df])
	df = df.drop_duplicates(subset=['execId'])
	df['Expiration'] = df['Expiration'].astype(str)
	df = df.sort_values(by=['TradeDate', 'TradeTime'], ascending=False)
	df.to_csv("trade_log.csv", index=False)



if __name__ == "__main__":
	 
	import_fills()