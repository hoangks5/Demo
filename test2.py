import requests
import time
import pymongo
import threading
import certifi
# coinmarketcap fe
# uniswap
# BTC-USD, ETH-USD, BNB-USD, DOGE-USD, LINK-USD, UNI-USD, SOL-USD, MATIC-USD, LUNA-USD, DOT-USD, ATOM-USD

# binace
#coinbase
#gateio
#kucoin
#chainlink
#coingecko


# Connection MongoDB


#myclient = pymongo.MongoClient("mongodb+srv://hoangks5:YrfvDz4Mt8xrrHxi@cluster0.tcbxc.mongodb.net/?retryWrites=true&w=majority")
#mydb = myclient["price"]


client = pymongo.MongoClient("mongodb+srv://hoangks5:YrfvDz4Mt8xrrHxi@cluster0.tcbxc.mongodb.net/",tlsCAFile=certifi.where())
mydb = client['price']
dict_test = ['BTC-USD', 'ETH-USD', 'BNB-USD', 'DOGE-USD', 'LINK-USD', 'UNI-USD', 'SOL-USD', 'MATIC-USD', 'LUNA-USD', 'DOT-USD', 'ATOM-USD']

# binace
def get_binance_list():
    url = "https://api.binance.com/api/v3/ticker/price"
    payload={}
    headers = {}
    
    binance_coin_list = requests.request("GET", url, headers=headers, data=payload)
    binance_coin_list = binance_coin_list.json()
    return binance_coin_list


binance = get_binance_list()
def get_binance_price(symbol):
    
    symbol = symbol.split("-")[0]
    
    url = "https://api.binance.com/api/v3/ticker/price?symbol="+symbol+"USDT"

    payload={}
    headers = {}
    
    time1 = time.time()
    response = requests.request("GET", url, headers=headers, data=payload)
    avg = {
        'token' : symbol,
        'source' : 'binace',
        'price' : float(response.json()['price']),
        'timestamp' : time1
        
    }
    mycol = mydb['data']
    mycol.insert_one(avg)


#coinbase

def get_coinbase_price(symbol):
    #url = "https://api.coinbase.com/v2/prices/"+pair+"/ticker"
    url = "https://api.coinbase.com/v2/prices/"+symbol+"/spot"
    
    payload={}
    headers = {
      'Cookie': 'cb_dm=6aa55183-86f4-41e5-8733-439d4f6a288f'
    }
    time1 = time.time()
    response = requests.request("GET", url, headers=headers, data=payload)
    try:
        avg = {
          'token' : symbol.split('-')[0],
          'source' : 'coinbase',
          'price' :float(response.json()['data']['amount']),
          'timestamp' : time1
      }
        mycol = mydb['data']
        mycol.insert_one(avg)
    except:
      pass
    
    
# gateio

def get_gateio_price(symbol):
    
    pair = symbol.replace("-","_")
    host = "https://api.gateio.ws"
    prefix = "/api/v4"
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    url = '/spot/trades'
    query_param = 'currency_pair='+pair
    time1 = time.time()
    response = requests.request('GET', host + prefix + url + "?" + query_param, headers=headers).json()
    try:
        avg = {
            'token' : symbol.split('-')[0],
            'source' : 'gateio',
            'price' : float(response[0]['price']),
            'timestamp' : time1
        }
        mycol = mydb['data']
        mycol.insert_one(avg)
    except:
        pass
    

#kucoin 


def get_kucoin_price(symbol) :
    
    time1 = time.time()
    response = requests.get('https://api.kucoin.com/api/v1/prices').json()['data'][symbol.split("-")[0]]
    avg = {
        'token' : symbol.split('-')[0],
        'source' : 'kucoin',
        'price' : float(response),
        'timestamp' : time1
        
    }
    mycol = mydb['data']
    mycol.insert_one(avg)

#coingecko

def get_coingecko_list_symbol():
    import requests

    url = "https://api.coingecko.com/api/v3/coins/list"

    payload={}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    return response.json()

coingecko_map = {}
coingecko_list_symbol = get_coingecko_list_symbol()
for i in coingecko_list_symbol:
    coingecko_map[i["symbol"].upper()] = i["id"]

def get_coingecko_list():
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"

    payload={}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json()

coingecko = get_coingecko_list()

def get_coingecko_price(symbol) :
    token = symbol.split('-')[0]
    symbol = coingecko_map[symbol.split("-")[0]]
    url = "https://api.coingecko.com/api/v3/simple/price?ids="+symbol+"&vs_currencies=usd"

    payload = ""
    headers = {}
    time1 = time.time()
    response = requests.request("GET", url, headers=headers, data=payload).json()
    for d in list(response.keys()):
        price = response[d]['usd']
    avg = {
        'token' : token,
        'source' : 'coingecko',
        'price' : float(price),
        'timestamp' : time1
    }
    mycol = mydb['data']
    mycol.insert_one(avg)

# chainlink


def get_chainlink_price(symbol):
    
    url = 'https://min-api.cryptocompare.com/data/price?fsym='+symbol.split('-')[0]+'&tsyms='+symbol.split('-')[1]
    time1 = time.time()
    response = requests.get(url)

    avg = {
        'token' : symbol.split('-')[0],
        'source' : 'chainlink',
        'price' : float(response.json()['USD']),
        'timestamp' : time1
        
    }
    mycol = mydb['data']
    mycol.insert_one(avg)

# coinmarketcap

def get_coinmarketcap_price():
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
    parameters = {
        'symbol' : 'BTC,ETH,BNB,DOGE,LINK,UNI,SOL,MATIC,LUNA,DOT,ATOM'
    }
    header = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY' : '328e6159-90a7-4a16-8ecf-49983a9d76dd'}
    time1 = time.time()
    response = requests.get(url=url,params=parameters,headers=header)
    avg_s = 'BTC,ETH,BNB,DOGE,LINK,UNI,SOL,MATIC,LUNA,DOT,ATOM'
    list_symbol = avg_s.split(',')
    for symbol in list_symbol:
        price = response.json()['data'][symbol]['quote']['USD']['price']
        avg = {
            'token' : symbol.split('-')[0],
            'source' : 'coinmarketcap',
            'price' : float(price),
            'timestamp' : time1,
        }
        mycol = mydb['data']
        mycol.insert_one(avg)


# coinpaprika

def get_coinpaprika_price(symbol):
    url = 'https://api.coinpaprika.com/v1/tickers'
    time1 = time.time()
    response = requests.get(url).json()
    
        
    for iteam in response:
        if iteam['symbol'] == symbol.split('-')[0]:
            price = iteam['quotes']['USD']['price']
            break
    avg = {
        'token' : symbol.split('-')[0],
        'source' : 'coinpaprika',
        'price' : float(price),
        'timestamp' : time1,
    }
    mycol = mydb['data']
    mycol.insert_one(avg)
            
               
     
# worldcoinindex

def get_worldcoinindex_price(symbol):
    url = 'https://www.worldcoinindex.com/apiservice/v2getmarkets?key=3c8AaT3hg4qL1w3RiRyysIIE0SzdErTVEfS&fiat=usd'
    time1 = time.time()
    response = requests.get(url).json()
    for iteam in response['Markets'][0]:
        if iteam['Label'] == symbol.replace('-','/'):
            price = iteam['Price']
            break
    avg = {
        'token' : symbol.split('-')[0],
        'source' : 'worldcoinindex',
        'price' : float(price),
        'timestamp' : time1,
    }
    mycol = mydb['data']
    mycol.insert_one(avg)

while True:
    threading_ = []
    for test in dict_test:
        threading_.append(threading.Thread(target=get_binance_price,args={test,}))
        threading_.append(threading.Thread(target=get_coinbase_price,args={test,}))
        threading_.append(threading.Thread(target=get_kucoin_price,args={test,}))
        threading_.append(threading.Thread(target=get_gateio_price,args={test,}))
        threading_.append(threading.Thread(target=get_chainlink_price,args={test,}))
        threading_.append(threading.Thread(target=get_coingecko_price,args={test,}))
        threading_.append(threading.Thread(target=get_coinpaprika_price,args={test,}))
        threading_.append(threading.Thread(target=get_worldcoinindex_price,args={test,}))
    threading_.append(threading.Thread(target=get_coinmarketcap_price))
    for thread in threading_:
        thread.start()
    time.sleep(180)