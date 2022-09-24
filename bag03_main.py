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
import baglib
import time
import bag01_unzip
import bag12_xml2csv
import bag23a_vbovk_pndvk
import bag23a_vbovk_wplvk
import bag23b_levcycl

# ############### Define functions ################################

tic = time.perf_counter()

print('-------------------------------------------')
print('------------- Start bag_main --------------')
print('-------------------------------------------')

os.chdir('..')
BASEDIR = os.getcwd() + '/'
DATADIR = BASEDIR + 'data/'
DIR00 = DATADIR + '00-zip/'
DIR01 = DATADIR + '01-xml/'
DIR02 = DATADIR + '02-csv/'
DIR03 = DATADIR + '03-bewerktedata/'
current_month = baglib.get_arg1(sys.argv, DIR00)
printit=True

baglib.print_omgeving(BASEDIR)
print('\thuidige maand (verslagmaand + 1):', current_month, '\n')
baglib.print_legenda()

# unzip XML files van koppelvlak 0 naar koppelvlak 1
bag01_unzip.bag_unzip(current_month=current_month,
                      koppelvlak0=DIR00,
                      koppelvlak1=DIR01,
                      loglevel=printit)

# zet de duizenden xml bestanden om in 7 csv bestanden, te weten:
# vbo.csv, pnd.csv, num.csv, opr.csv, wpl.csv, sta.csv, lig.csv
bag12_xml2csv.bag_xml2csv(current_month=current_month,
                          koppelvlak1=DIR01,
                          koppelvlak2=DIR02,
                          loglevel=printit)

# leidt voor elk vbo voorkomen (vbovk) een precies 1 pndvk af. Het hoofdpndvk
bag23a_vbovk_pndvk.bag_vbovk_pndvk(current_month=current_month,
                                   koppelvlak2=DIR02,
                                   koppelvlak3=DIR03,
                                   loglevel=printit)

# leidt voor een vbovk een woonplaats voorkomen (wplvk) af
bag23a_vbovk_wplvk.bag_vbovk_pndvk(current_month=current_month,
                                   koppelvlak2=DIR02,
                                   koppelvlak3=DIR03,
                                   loglevel=printit)

# maakt het (bekende) vbo levenscyclus bestand. De levenscyclus van een vbo
# bestaat uit zijn opeenvolgende vk (voorkomens)
bag23b_levcycl.bag_levcycl(current_month=current_month,
                           koppelvlak2=DIR02,
                           koppelvlak3=DIR03,
                           loglevel=printit)

toc = time.perf_counter()
baglib.print_time(toc - tic, '\n------------- Einde bag_main in',
                  printit)
