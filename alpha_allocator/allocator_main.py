# -*- coding: utf-8 -*-
"""
Created on Sun Nov 18 12:35:32 2018
asset allocator main
collect config file location and run the asset allocator

@author: adam
"""
import ConfigParser
import argparse
import os
from asset_allocator import allocator,beancount_integrator

def main(args):
    CONFIG_PATH = args.config_file
    config = ConfigParser.ConfigParser()
    config.readfp(open(CONFIG_PATH))
    
    ROOT_ALLOCATOR_PATH = config.get('allocator config','allocator_root')
    top_level_cats = (config.get('allocator config','top_level_categories')).split(',')
    top_level_ratios = [float(x) for x in (config.get('allocator config','top_level_ratios')).split(',')]
    BEAN_PATH = config.get('allocator config','beancount_file')
    
    # do filesystem stuff, setup directory
    RAWDATA_PATH = os.path.join(ROOT_ALLOCATOR_PATH,'rawdata/')
    DATA_PATH = os.path.join(ROOT_ALLOCATOR_PATH,'data/')
    if not os.path.isdir(RAWDATA_PATH):
        os.mkdir(RAWDATA_PATH)
        print "Warning: no raw data in correct directory has been detected."
    if not os.path.isdir(DATA_PATH):
        os.mkdir(DATA_PATH)
    
    asset_df = allocator(DATA_PATH,RAWDATA_PATH,top_level_cats,top_level_ratios)
    
    if args.bean == 'Y':
        beancount_integrator(asset_df,BEAN_PATH)

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file",help='specify the config file to be used')
    parser.add_argument("--bean",default='N',help='Y or N to indicate if beancount entry should be generated.')
    args = parser.parse_args()
    main(args)
