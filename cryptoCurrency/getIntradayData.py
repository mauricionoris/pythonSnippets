
import urllib2, json, time, pandas

def getData(currencyPair, start, period):
    #to get the information from polinex
    url = "https://poloniex.com/public?command=returnChartData&currencyPair=" + currencyPair + "&start=" + str(start) + "&end=9999999999&period=" + str(period)
    return pandas.read_json((urllib2.urlopen(urllib2.Request(url)).read())).set_index('date');

#parameters
start_time = time.time()-60*24*60*60 # current time - 60 days
curr_Pair = "USDT_BTC"
period=12

ts1 = getData(curr_Pair,start_time,"300").resample('10Min').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}) # data feed only available in 5 minutes interval, resample as a frequency of 10 minutes
mvats1 = ts1.rolling(window=period,center=False).mean()

print mvats1
