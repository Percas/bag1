#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

23 aug, Anton
version: 0.1
doel: plak modules aan elkaar en genereer alle gewenste output

input:      xml bestanden in map ../data/01-xml/...
output1:    levcycl.csv  Het levenscyclus bestand

"""

# ################ import libraries ###############################

import sys
import os
import shutil
import baglib
import time
import bag01_unzip
import bag12_xml2csv
import bag12_wplgem2csv
import bag23a_fix_vk
import bag33_hoofdpnd
import bag33b_levcycl
import bag34_vbostatus

# import bag23a_vbovk_wplvk
# import bag23b_levcycl
from config import LOCATION

# ############### Define functions ################################

tic = time.perf_counter()
print('-------------------------------------------')
print('-------------', LOCATION['OMGEVING'], '-----------')
print('-------------------------------------------\n')

print('-------------------------------------------')
print('------------- Start bag_main --------------')
print('-------------------------------------------')

DATADIR_IN = LOCATION['DATADIR_IN']
DATADIR_OUT = LOCATION['DATADIR_OUT']
DIR00 = DATADIR_IN + '00-zip/'
DIR01 = DATADIR_OUT + '01-xml/'
DIR02 = DATADIR_OUT + '02-csv/'
DIR03 = DATADIR_OUT + '03-bewerktedata/'
DIR04 = DATADIR_OUT + '04-aggr/'
current_month = baglib.get_arg1(sys.argv, DIR00)
ll = 20

print('\thuidige maand (verslagmaand + 1):', current_month, '\n')
baglib.print_legenda()

# we hebben testdata van koppelvlak 0 t/m 2 en van 2 t/m 3. Deze zijn verschillend
if current_month == 'testdata':
    current_month = 'testdata02'
    
# unzip XML files van koppelvlak 0 naar koppelvlak 1
bag01_unzip.bag_unzip(current_month=current_month,
                      koppelvlak0=DIR00,
                      koppelvlak1=DIR01,
                      loglevel=ll)

# zet de duizenden xml bestanden om in 7 csv bestanden, te weten:
# vbo.csv, pnd.csv, num.csv, opr.csv, wpl.csv, sta.csv, lig.csv
bag12_xml2csv.bag_xml2csv(current_month=current_month,
                          koppelvlak1=DIR01,
                          koppelvlak2=DIR02,
                          loglevel=ll)

print('\n*** bag03_main: hernoem bestand wpl.csv naar wpl_naam.csv\n')
os.rename(DIR02+current_month+'/wpl.csv', DIR02+current_month+'/wpl_naam.csv')


bag12_wplgem2csv.bag_wplgem2csv(current_month=current_month,
                                koppelvlak1=DIR01,
                                koppelvlak2=DIR02,
                                loglevel=ll)

baglib.aprint(ll+40, '\n*** bag03_main: XML bestanden uit koppelvlak 1 verwijderen...\n')
shutil.rmtree(DIR01 + current_month)

# tussen rp0 en rp2 hebben we testdata02
# vanaf rp2 hebben we testdata23
if current_month == 'testdata02':
    current_month = 'testdata23'

bag23a_fix_vk.bag_fix_vk(current_month=current_month,
                               koppelvlak3=DIR03,
                               koppelvlak2=DIR02,
                               loglevel=ll)
# leidt voor elk vbo voorkomen (vbovk) een precies 1 pndvk af. Het hoofdpndvk

bag33_hoofdpnd.bag_hoofdpnd(current_month=current_month,
                            koppelvlak3=DIR03,
                            loglevel=ll)

bag33b_levcycl.bag_levcycl(current_month=current_month,
                           koppelvlak3=DIR03,
                           loglevel=ll)

bag34_vbostatus.bag_vbostatus(current_month=current_month,
                              koppelvlak4=DIR04,
                              koppelvlak3=DIR03,
                              loglevel=ll)

toc = time.perf_counter()
baglib.aprint(ll+40, '\n------------- Einde bag_main in', (toc - tic)/60, 'min')
