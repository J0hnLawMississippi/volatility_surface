#!/usr/bin/env python3

#import h5py
import asyncio
import aiohttp
import pandas as pd
from math import ceil
from datetime import datetime
from async_requests import req0,req1,req2,req2_0,req3

#######################################################################################################

jsonhead = {"Content-Type": "application/json"}

def stringin(string, v0, v1, v2):
        return string.format(v0,v1,v2)

        
async def implied_volatility(opt_string, asset, maturity, strike_itvl, strike_num):
        

        idxurl = f"https://deribit.com/api/v2/public/get_index?currency={asset}"
        index = await req1(idxurl, jsonhead, asset).reqjson1()

        strikezero = ceil(index/strike_itvl) * strike_itvl
        X0 = [strikezero - strike_num*strike_itvl + i*strike_itvl for i in range(2*strike_num)]

        #instead of constructing strings get instruments and sort them along maturities and strikes?
        pt = len(list(X0))
        expiry0  = [maturity]
        inst0 = ['C','P']
        expiry1 = expiry0 * len(X0) * len(inst0)
        inst1 = len(expiry0) * (inst0[0] * len(X0) + inst0[1] *len (X0))
        X1 = list(X0) * len(expiry0) * len(inst0)
        
        UrlList =  [stringin(opt_string, x,y,z) for x,y,z, in  zip(expiry1, X1, inst1)]
        callUrls = UrlList[0:pt] 
        putUrls = UrlList[pt:int(2*pt)] 

        #get IV from deribit
        call_IV = await asyncio.gather(*[req1(url, jsonhead, 'ask_iv').reqjson1() for url in callUrls])
        put_IV = await asyncio.gather(*[req1(url, jsonhead, 'ask_iv').reqjson1()  for url in putUrls])
        
        df = pd.DataFrame(list(zip(X0, call_IV, put_IV)),
                               columns=['strikes','call_IV','put_IV'])

        filename_str = f'{asset}-{maturity}_IV_{datetime.now().strftime("%Y-%m-%d %H:%M")}'
        df.to_hdf(f'{filename_str}.h5', key=filename_str, mode='w')


                        

async def main():

        #OPTION URLs          
        string0 = 'https://deribit.com/api/v2/public/get_order_book?depth=5&instrument_name=BTC-{0}-{1}-{2}'
        #string1 = 'https://deribit.com/api/v2/public/get_order_book?depth=5&instrument_name=ETH-{0}-{1}-{2}'


        async with aiohttp.ClientSession() as session:
                tasks = [
                        
                        asyncio.create_task(implied_volatility(string0, 'BTC', '31JAN25', 5000, 6)),
                        asyncio.create_task(implied_volatility(string0, 'BTC', '28MAR25', 5000, 6)),
                        asyncio.create_task(implied_volatility(string0, 'BTC', '27JUN25', 5000, 6)),
                        asyncio.create_task(implied_volatility(string0, 'BTC', '26SEP25', 5000, 6)),
                        
                        #asyncio.create_task(implied_volatility(string1, 'ETH', '31JAN25', 200, 6)),
                        #asyncio.create_task(implied_volatility(string1, 'ETH', '28MAR25', 200, 6)),
                        #asyncio.create_task(implied_volatility(string1, 'ETH', '27JUN25', 200, 6)),
                        #asyncio.create_task(implied_volatility(string1, 'ETH', '26SEP25', 200, 6)),
                        
                ]
                results = await asyncio.gather(*tasks)
    


        

asyncio.run(main())
