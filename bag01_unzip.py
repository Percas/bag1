#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
9 juni, Anton
version: 0.2
doel: uitpakken van de gezipte BAG XML bestanden
"""

# ################ import libraries ###############################
import sys
import os
import baglib
import zipfile
import time
from config import LOCATION

# ############### Define functions ################################

# Main function for this package:

def bag_unzip(current_month='testdata02',
              koppelvlak0='../data/00-zip/',
              koppelvlak1='../data/01-xml/',
              loglevel=True):
    '''Uitpakken van door Kadaster gezipte XML bestanden.''' 

    tic = time.perf_counter()

    _ll = loglevel
    baglib.aprint(_ll+40, '-------------------------------------------')
    baglib.aprint(_ll+40, '------------- Start bag_unzip -------------')
    baglib.aprint(_ll+40, '-------------------------------------------')
    
    inputdir = koppelvlak0 + current_month + '/'
    outputdir = koppelvlak1 + current_month + '/'
    
    # print('DEBUG:', inputdir)
    
    unzip_files = os.listdir(inputdir)
    bagobj_starts_with = {'vbo': '9999VBO',
                          'lig': '9999LIG',
                          'sta': '9999STA',
                          'pnd': '9999PND',
                          'num': '9999NUM',
                          'opr': '9999OPR',
                          'wpl': '9999WPL',
                          'wplgem': 'GEM-WPL-RELATIE'}

    for bagobj in bagobj_starts_with.keys():
        for unzip_file_name in unzip_files:
            if unzip_file_name.startswith(bagobj_starts_with[bagobj]):
                unzip_dir = outputdir + bagobj + '/'
                unzip_file = inputdir + unzip_file_name
                baglib.make_dir(unzip_dir, loglevel=_ll)
                baglib.aprint(_ll+10, '\n\tUitpakken van bestand', unzip_file_name,
                              '\n\tin directory', inputdir,
                              '\n\tnaar directory', unzip_dir, '...')
                _ti = time.perf_counter()
                with zipfile.ZipFile(unzip_file, 'r') as zip_ref:
                    zip_ref.extractall(unzip_dir)
                _to = time.perf_counter()
                baglib.aprint(_ll+40,'\tfile', unzip_file_name, 'unzipped in', (_to - _ti)/60, 'min')
                                  
    toc = time.perf_counter()

    baglib.aprint(_ll+40, '\n----------- Einde bag_unzip in', (toc - tic)/60, 'min\n')



# ########################################################################
# ########################################################################

if __name__ == '__main__':

    print('-------------------------------------------')
    print('-------------', LOCATION['OMGEVING'], '-----------')
    print('-------------------------------------------\n')
    
    DATADIR_IN = LOCATION['DATADIR_IN']
    DATADIR_OUT = LOCATION['DATADIR_OUT']
    DIR00 = DATADIR_IN + '00-zip/'
    DIR01 = DATADIR_OUT + '01-xml/'
    DIR02 = DATADIR_OUT + '02-csv/'
    DIR03 = DATADIR_OUT + '03-bewerktedata/'
    current_month = baglib.get_arg1(sys.argv, DIR00)
    ll = 0

    baglib.aprint(ll+40, '------------- Start unzip_bag lokaal ------------- \n')
    bag_unzip(current_month=current_month,
              koppelvlak0=DIR00,
              koppelvlak1=DIR01,
              loglevel=ll)
