#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 22

Purpose: make the following connections:
    vbovk - numvk
    numvk - oprvk
    oprvk  - wplvk
    pndvk - wplvk
    
Notes
De eerste vk-s van een VBO hebben soms(0.00037) nog geen num vk (wel een num)
    

Purpose step 1: connect vbovk to numvk (instead of num)
step 1a: connect vbovk to all numvk of its num in num.csv
step 1b: select that numvk for which numvkbg < vbovkeg <= numvkeg 
    result: vbovk - numvk
Step 2: make numvk - oprvk (instead of numvk - opr)
Step 3: make oprvk - wplvk (instead of oprvk - wpl)
step 2b: connect numvk to all oprvk
0.1: first attempt to create microdata
0.2: make a VBO view for peilmomenten
0.3: connect active vbo to opr-wpl-gem
0.5.3 pndvk-wplvk

"""

# ################ import libraries ###############################
import pandas as pd
# import numpy as np
import baglib
import os
import sys
import time
from baglib import BAG_TYPE_DICT
from config import LOCATION


# import numpy as np


# ############### Define functions ################################

# print('-----------------------------------------------------------')
# print('----maak vbovk-numvk, numvk-oprvk, oprvk-wplvk---------------')
# print('-----------------------------------------------------------')

def bag_vbovk_wplvk(current_month='202208',
                    koppelvlak2='../data/02-csv',
                    koppelvlak3='../data/03-bewerkte-data',
                    loglevel=True):
    '''Koppel aan VBO voorkomens de woonplaats voorkomens waarbij het vkeg
    van de vbo vk tussen de vkbg en vkeg van het wpl vk ligt.'''

    tic = time.perf_counter()
    print('-------------------------------------------')
    print('------------- Start bag_vbovk_wplvk ------ ')
    print('-------------------------------------------')
   
    
    # #############################################################################
    # print('00.............Initializing variables...............................')
    # #############################################################################
    # month and dirs
    k2dir = koppelvlak2 + current_month + '/'
    k3dir = koppelvlak3 + current_month + '/'
    baglib.make_dir(k3dir)
    INPUT_FILS_DICT = {'vbo': k2dir + 'vbo.csv',
                       'pnd': k2dir + 'pnd.csv',
                       'num': k2dir + 'num.csv',
                       'opr': k2dir + 'opr.csv',
                       'wpl': k2dir + 'wpl.csv',
                       'vbovk_pndvk': k3dir + 'vbovk_pndvk.csv'}
        
    vbovk = ['vboid', 'vbovkid']
    pndvk = ['pndid', 'pndvkid']
    oprvk = ['oprid', 'oprvkid']
    numvk = ['numid', 'numvkid']
    wplvk = ['wplid', 'wplvkid']
    
    KEY_DICT = {'vbo': vbovk,
                'pnd': pndvk,
                'num': numvk,
                'opr': oprvk,
                'wpl': wplvk,
                'vbovk_pndvk': vbovk}
    
    bd = {}         # dict with df with bagobject (vbo en pnd in this case)
    nrec = {}       # aantal records
    nkey = {}       # aantal keyrecords
    pd.set_option('display.max_columns', 20)


    # ############################################################################
    print('----1. Inlezen bagobjecten -------------\n')
    # #############################################################################
        
    print('huidige maand (verslagmaand + 1):', current_month)
    
    bd = baglib.read_input_csv(INPUT_FILS_DICT, BAG_TYPE_DICT)
 
    for bobj in INPUT_FILS_DICT.keys():
        if bobj != 'vbovk_pndvk':
            bd[bobj] = baglib.fix_eendagsvlieg(bd[bobj], bobj+'vkbg',
                                               bobj+'vkeg')
        (nrec[bobj], nkey[bobj]) =\
            baglib.df_comp(bd[bobj], key_lst=KEY_DICT[bobj])
        
    
        nkey['doel'+bobj] = nkey[bobj]
        print('\t*** Doel: voor', nkey['doel' + bobj],
              bobj, 'vk een gekoppeld vk vinden ***')
        
    #neem alleen de velden mee die we nodig hebben:
    bd['vbo'] = bd['vbo'][['vboid', 'vbovkid', 'vbovkbg', 'vbovkeg', 'numid']]
    bd['num'] = bd['num'][['numid', 'numvkid', 'numvkbg', 'numvkeg', 'oprid']]
    bd['opr'] = bd['opr'][['oprid', 'oprvkid', 'oprvkbg', 'oprvkeg', 'wplid']]
    bd['wpl'] = bd['wpl'][['wplid', 'wplvkid', 'wplvkbg', 'wplvkeg']]
    bd['pnd'] = bd['pnd'][['pndid', 'pndvkid', 'pndvkbg', 'pndvkeg']]
    
    # #############################################################################
    print('\n------2. Koppelen voorkomens van: vbo-num, num-opr, opr-wpl---------')
    # #############################################################################
    '''
    for i,bobj in enumerate(BOBJ[0:3]):
    
        nextbobj = BOBJ[i+1]
        print('\n\t----- Koppelen van', bobj, 'met', nextbobj,
              'op alleen', bobj+'id')
        print('\t(waardoor het aantal toeneemt...)')
        
        print('\t*** Doel reminder: voor', nkey['doel' + bobj],
              bobj, 'vk een gekoppeld', nextbobj, 'vk vinden ***')
        
            
        bdict[bobj] = pd.merge(bdict[bobj],
                               bdict[nextbobj],
                               how='left',
                               left_on=nextbobj+'id',
                               right_on=nextbobj+'id')
        
        # controle    
        (nrec[bobj], nkey[bobj]) =\
            baglib.df_comp(bdict[bobj], key_lst=[bobj+'id', bobj+'vkid'],
                           nrec=nrec[bobj], nkey=nkey[bobj],
                           u_may_change=False)
    
        print('\n\t--- Vervolgens dit aantal weer terugbrengen door inperking\n',
              '\tvia de einddatum van het vk')
        
        msk = (bdict[bobj][nextbobj+'vkbg'] <= bdict[bobj][bobj+'vkeg']) &\
            (bdict[bobj][bobj+'vkeg'] <= bdict[bobj][nextbobj+'vkeg'])
        bdict[bobj] = bdict[bobj][msk]
    
        # controle
        (nrec[bobj], nkey[bobj]) =\
            baglib.df_comp(bdict[bobj], key_lst=[bobj+'id', bobj+'vkid'],
                           nrec=nrec[bobj], nkey=nkey[bobj],
                           u_may_change=True)
    
        key_cols = [bobj+'id', bobj+'vkid']
        max_cols = [nextbobj+'vkid']
        
        print('\n\tOntdubbel nu', bobj, 'op dubbele', key_cols, 'door het max\n',
              '\tte nemen van de waarden in kolom', max_cols[0])
        
        bdict[bobj] = bdict[bobj].set_index(key_cols)
        bdict[bobj] = baglib.ontdubbel_idx_maxcol(bdict[bobj], max_cols)
        
        # controle
        (nrec[bobj], nkey[bobj]) =\
            baglib.df_comp(bdict[bobj], nrec=nrec[bobj], nkey=nkey[bobj],
                           u_may_change=True)
    
        print('\t\tHet deel van de', bobj,'vk waarbij een', nextbobj, 
              'vk gevonden kon worden:\n\t\t\t',
              nkey[bobj] / nkey['doel' + bobj])
    
        # print(bdict[bobj].info())
    
        print('\n\t--- Bestand met', bobj, 'vk  bewaren------------------------')
    
        koppel_filenaam = bobj + '-' + nextbobj + '.csv'
        cols = [nextbobj +'id', nextbobj + 'vkid' ]
        
        print('\n\tBewaar', koppel_filenaam, 'in', OUTPUTDIR, ':',
              '\n\tAantal records:', nrec[bobj],
              '\n\tAantal uniek:  ', nkey[bobj])
    
        outputfile = OUTPUTDIR + koppel_filenaam
        bdict[bobj][cols].to_csv(outputfile, index=True)
        toc = time.perf_counter()
        
        print('\n')
    
    
    print('-----3. Koppelen voorkomens van vbo-opr en vbo-wpl -------------------')
    bdict['vbo'].reset_index(inplace=True)
    
    vbowpl_df = pd.merge(bdict['vbo'][['vboid', 'vbovkid', 'numid', 'numvkid']],
                         bdict['num'][['oprid', 'oprvkid']], how='inner',
                         left_on=['numid', 'numvkid'],
                         right_on=['numid', 'numvkid'])\
        [['vboid', 'vbovkid', 'oprid', 'oprvkid']]
    (nrec['vbo'], nkey['vbo']) =\
        baglib.df_comp(vbowpl_df, nrec=nrec['vbo'], nkey=nkey['vbo'],
                       u_may_change=True)
    print('\n\tHet deel van de vbovk waarbij een oprvk gevonden kon worden:\n\t\t',
          nkey['vbo']/nkey['doelvbo'])
    
    vbowpl_df = pd.merge(vbowpl_df,
                         bdict['opr'][['wplid', 'wplvkid']], how='inner',
                         left_on=['oprid', 'oprvkid'],
                         right_on=['oprid', 'oprvkid'])\
        [['vboid', 'vbovkid', 'wplid', 'wplvkid']]
    
    (nrec['vbo'], nkey['vbo']) =\
        baglib.df_comp(vbowpl_df, nrec=nrec['vbo'], nkey=nkey['vbo'],
                       u_may_change=True)
    print('\n\tHet deel van de vbovk waarbij een wplvk gevonden kon worden:\n\t\t',
          nkey['vbo']/nkey['doelvbo'])
    
    
    # print(vbowpl_df.info())
    
    print('\n\t--- Bestand bewaren--------------------------')
    
    koppel_filenaam = 'vbovk-wplvk.csv'
    print('\n\tBewaar', koppel_filenaam, 'in', OUTPUTDIR, ':',
          '\n\tAantal records:', nrec['vbo'],
          '\n\tAantal uniek:  ', nkey['vbo'])
    
    outputfile = OUTPUTDIR + koppel_filenaam
    vbowpl_df.to_csv(outputfile, index=False)
    
    
    # print(vbowpl_df.head(20))
    
    toc = time.perf_counter()
    baglib.print_time(toc - tic, '\n------------- Einde bag_vbovk_wplvk in',
                      loglevel)

    '''
'''             
# ########################################################################
print('------------- Start bag_vbovk_wplvk lokaal ------------- \n')
# ########################################################################
'''


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
    current_month = baglib.get_arg1(sys.argv, DIR02)

    printit=True

    bag_vbovk_wplvk(current_month=current_month,
                    koppelvlak2=DIR02,
                    koppelvlak3=DIR03,
                    loglevel=printit)

