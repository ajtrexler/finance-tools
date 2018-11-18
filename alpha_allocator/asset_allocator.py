# -*- coding: utf-8 -*-
"""
Created on Tue Oct 30 06:49:36 2018

@author: adam
"""
import os
import pandas as pd
import datetime
import random


def allocator(DATA_PATH,RAW_PATH,top_level_cats,
                    top_level_ratios,bean_plugin):
    # DATA_PATH: persistent directory where data will be stored
    # RAW_PATH: directory where raw CSVs for parsing are located
    LKUP_FILE= "asset_lookup.csv"
    
    merrill_columns = ['Symbol','Desc','Quantity','Price','DayPriceChange',
                       'Value','DayValueChange','UnrealGL']
    
    possible_asset_classes = ['equity','bond','IDX_world','emerging','defensive',
                              'IDX_usa','gold','cash','realestate','euro']
                              
    asset_to_top_map = {'equity':'equity',
                        'bond':'bonds',
                        'IDX_world':'equity',
                        'emerging':'equity',
                        'defensive':'equity',
                        'IDX_usa':'equity',
                        'gold':'bonds',
                        'cash':'cash',
                        'realestate':'realestate',
                        'euro':'equity'}
    
    # TODO: get prices from this and write to beancount entries
    # TODO: read asset_classes DF, this is persistence layer of lookup DF.
    try:
        asset_lookup = pd.read_csv(os.path.join(DATA_PATH,LKUP_FILE),index_col=0)
    except:
        print 'load failed.'
        al_columns = ['class']
        asset_lookup = pd.DataFrame(columns=al_columns)
        
    def get_asset_class(symbol, desc):
        tries = 0
        while tries<5:
            print 'enter asset class for {x} (press 1 for more info): '.format(x=symbol)
            print 'press 2 for listing of possible classes'
            print 'press x to break'
            ac = raw_input('-->')
            tries += 1
            if ac == 'x':
                break
            try:
                num_entry = int(ac)
                if num_entry == 1:
                    print desc
                elif num_entry == 2:
                    print possible_asset_classes
            except:
                if ac not in possible_asset_classes:
                    print 'try again, invalid asset class'
                else:
                    return ac
     
    asset_df = pd.DataFrame()   
    total_cash = 0   
         
    for f in os.listdir(RAW_PATH):

        if 'lock' not in f and 'lookup' not in f:
            print f,'detected and parsed'
            data = pd.read_csv(os.path.join(RAW_PATH,f),header=7)
            data.columns = merrill_columns
            cash = float(data.loc[data['Symbol'] == 'Money accounts ']['Value'].values[0].replace('$','').replace(',',''))
            total_cash += cash
            idx_balances = data.loc[data['Symbol'] == 'Balances '].index.values[0]
            assets = data.iloc[:idx_balances,:]
            for asset in assets.iterrows():
                symbol = asset[1]['Symbol'].replace(' ','')
                
                value = float(asset[1]['Value'].replace(',','').replace(' ','').replace('$',''))
                price = float(asset[1]['Price'].replace(' ','').replace('$',''))
                desc = asset[1]['Desc'].replace(' ','')
                if symbol not in asset_lookup.index:
                    asset_class = get_asset_class(symbol,desc)
                    asset_lookup = pd.concat([asset_lookup,pd.DataFrame(index=[symbol],data={'class':asset_class})],0)
                else:
                    asset_class = asset_lookup.loc[symbol]['class']
                tmp = pd.DataFrame(index=[random.randint(0,10000)],data = {'class':asset_class,'value':value,'price':price,'symbol':symbol})
                asset_df = pd.concat([asset_df,tmp],0)

    

    # table view of summed asset class amounts, fraction of invested total.
    (asset_df.groupby('class')['value'].sum()/asset_df['value'].sum()*100).round(1)

    # add cash to the mix and get fractions of total portfolio
    cash_frame = pd.DataFrame(index=[random.randint(0,10000)],data = {'class':'cash','value':total_cash,'price':1,'symbol':'USD'})
    asset_df = pd.concat([asset_df,cash_frame],0)
    (asset_df.groupby('class')['value'].sum()/asset_df['value'].sum()*100).round(1)
    
    # view summed over equity, bond, realestate, cash.    
    asset_df['top_level'] = asset_df['class'].apply(lambda x: asset_to_top_map[x])
    top_view = (asset_df.groupby('top_level')['value'].sum()/asset_df['value'].sum()*100).round(1)
    top_view = pd.concat([pd.Series(top_level_ratios,index=top_level_cats,name='target')*100,top_view],1)
    top_view.columns = ['target','current']  
    top_view['delta'] = top_view['target'] - top_view['current']
    total_port_value = asset_df.groupby('top_level')['value'].sum().sum()
    top_view['money to move'] = ((top_view['delta']/100)*total_port_value).round()

    # TODO: view of values and percentages, write both of these to a report.

    # TODO: save down pie chart or bar chart

    asset_lookup.to_csv(os.path.join(DATA_PATH,LKUP_FILE))
    print top_view
    timestamp = datetime.datetime.strftime(datetime.datetime.now(),'%Y%m%d')
    top_view.to_csv(os.path.join(DATA_PATH,timestamp+'_top_view.csv'))
    asset_df.to_csv(os.path.join(DATA_PATH,timestamp+'_assets.csv'))

    # TODO: autocomplete on
    # TODO: write beancount entries to tmp file for copy over.s
    
    return asset_df

    
    # TODO: save asset_df with timestamped file.

