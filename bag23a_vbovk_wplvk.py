#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 22

Purpose: make the following connections:
    vbovk - numvk
    numvk - oprvk
    oprvk  - wplvk
    
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

"""

# ################ import libraries ###############################
import pandas as pd
# import numpy as np
import baglib
import os
import sys
import time
from baglib import BAG_TYPE_DICT

# import numpy as np


# ############### Define functions ################################

# print('-----------------------------------------------------------')
# print('----maak vbovk-numvk, numvk-oprvk, oprvk-wplvk---------------')
# print('-----------------------------------------------------------')

def bag_vbovk_pndvk(current_month='202208',
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
    INPUTDIR = koppelvlak2 + current_month + '/'
    OUTPUTDIR = koppelvlak3 + current_month + '/'
    baglib.make_dir(OUTPUTDIR)
    BOBJ = ['vbo', 'num', 'opr', 'wpl']
    # BOBJ = ['vbo', 'num']
    bdict = {} # dict to store the bagobject df's
    nrec = {}
    nkey = {}
    # perc = {}
    # printit = True
    pd.set_option('display.max_columns', 20)


    # ############################################################################
    print('----1. Inlezen bagobjecten -------------\n')
    # #############################################################################
        
    print('huidige maand (verslagmaand + 1):', current_month)
    
    for bobj in BOBJ:
        print('\n', bobj, 'in', INPUTDIR, ':')
        bdict[bobj] = pd.read_csv(INPUTDIR + bobj + '.csv',
                                  dtype=BAG_TYPE_DICT)
    
        key_lst = [bobj+'id', bobj+'vkid']
        (nrec[bobj], nkey[bobj]) =\
            baglib.df_comp(bdict[bobj], key_lst=key_lst)
        print('\tAantal records:', nrec[bobj])
        print('\tAantal vk:     ', nkey[bobj])
        
        bdict[bobj] = baglib.fix_eendagsvlieg(bdict[bobj], bobj+'vkbg',
                                              bobj+'vkeg')
        (nrec[bobj], nkey[bobj]) =\
            baglib.df_comp(bdict[bobj], key_lst=key_lst,
                           nrec=nrec[bobj], nkey=nkey[bobj],
                           u_may_change=True)
        
    
        nkey['doel'+bobj] = nkey[bobj]
        print('\t*** Doel: voor', nkey['doel' + bobj],
              bobj, 'vk een gekoppeld vk vinden ***')
        
    #neem alleen de velden mee die we nodig hebben:
    bdict['vbo'] = bdict['vbo'][['vboid', 'vbovkid', 'vbovkbg', 'vbovkeg', 'numid']]
    bdict['num'] = bdict['num'][['numid', 'numvkid', 'numvkbg', 'numvkeg', 'oprid']]
    bdict['opr'] = bdict['opr'][['oprid', 'oprvkid', 'oprvkbg', 'oprvkeg', 'wplid']]
    bdict['wpl'] = bdict['wpl'][['wplid', 'wplvkid', 'wplvkbg', 'wplvkeg']]
    
    
    # #############################################################################
    print('\n------2. Koppelen voorkomens van: vbo-num, num-opr, opr-wpl---------')
    # #############################################################################
    
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
# ########################################################################
print('------------- Start bag_vbovk_wplvk lokaal ------------- \n')
# ########################################################################
'''


if __name__ == '__main__':

    os.chdir('..')
    BASEDIR = os.getcwd() + '/'
    baglib.print_omgeving(BASEDIR)
    DATADIR = BASEDIR + 'data/'
    DIR02 = DATADIR + '02-csv/'
    DIR03 = DATADIR + '03-bewerktedata/'
    current_month = baglib.get_arg1(sys.argv, DIR02)

    printit=True

    bag_vbovk_pndvk(current_month=current_month,
                    koppelvlak2=DIR02,
                    koppelvlak3=DIR03,
                    loglevel=printit)


'''
# #############################################################################
# print('00.............Initializing variables...............................')
# #############################################################################
# month and dirs
os.chdir('..')
BASEDIR = os.getcwd() + '/'
baglib.print_omgeving(BASEDIR)
DATADIR = BASEDIR + 'data/'
DIR02 = DATADIR + '02-csv/'
DIR03 = DATADIR + '03-bewerktedata/'
current_month = baglib.get_arg1(sys.argv, DIR02)
INPUTDIR = DIR02 + current_month + '/'
OUTPUTDIR = DIR03 + current_month + '/'
BOBJ = ['vbo', 'num', 'opr', 'wpl']
# BOBJ = ['vbo', 'num']
bdict = {} # dict to store the bagobject df's
nrec = {}
nkey = {}
# perc = {}
printit = True
pd.set_option('display.max_columns', 20)

# ############################################################################
print('----1. Inlezen bagobjecten ---------------------------------\n')
# #############################################################################
tic = time.perf_counter()

print('huidige maand (verslagmaand + 1):', current_month)

for bobj in BOBJ:
    print('\n', bobj, 'in', INPUTDIR, ':')
    bdict[bobj] = pd.read_csv(INPUTDIR + bobj + '.csv',
                              dtype=BAG_TYPE_DICT)

    key_lst = [bobj+'id', bobj+'vkid']
    (nrec[bobj], nkey[bobj]) =\
        baglib.df_comp(bdict[bobj], key_lst=key_lst)
    print('\tAantal records:', nrec[bobj])
    print('\tAantal vk:     ', nkey[bobj])
    
    bdict[bobj] = baglib.fix_eendagsvlieg(bdict[bobj], bobj+'vkbg',
                                          bobj+'vkeg')
    (nrec[bobj], nkey[bobj]) =\
        baglib.df_comp(bdict[bobj], key_lst=key_lst,
                       nrec=nrec[bobj], nkey=nkey[bobj],
                       u_may_change=True)
    

    nkey['doel'+bobj] = nkey[bobj]
    print('\t*** Doel: voor', nkey['doel' + bobj],
          bobj, 'vk een gekoppeld vk vinden ***')
    

    '''
'''
    print('\tBijhouden van deze key vk (zonder eendagsvliegen):')
    # bobju: (uniek) voorkomen van een bagobject
    bobju = bobj + 'u'
    bdict[bobju] = bdict[bobj][key_lst].set_index(key_lst)

    # controle
    (nrec[bobju], nkey[bobju], perc[bobju]) =\
        baglib.df_comp(bdict[bobju])

    bdict[bobju] = bdict[bobju][~bdict[bobju].index.duplicated()]
    
    # controle
    (nrec[bobju], nkey[bobju], perc[bobju]) =\
        baglib.df_comp(bdict[bobju], nrec=nrec[bobju],
                       nkey=nkey[bobju], u_may_change=False)
'''
'''

bdict['vbo'] = bdict['vbo'][['vboid', 'vbovkid', 'vbovkbg', 'vbovkeg', 'numid']]
bdict['num'] = bdict['num'][['numid', 'numvkid', 'numvkbg', 'numvkeg', 'oprid']]
bdict['opr'] = bdict['opr'][['oprid', 'oprvkid', 'oprvkbg', 'oprvkeg', 'wplid']]
bdict['wpl'] = bdict['wpl'][['wplid', 'wplvkid', 'wplvkbg', 'wplvkeg']]


# #############################################################################
print('\n------2. Koppelen voorkomens van: vbo-num, num-opr, opr-wpl---------')
# #############################################################################

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

print('\n\n')
toc = time.perf_counter()
baglib.print_time(toc - tic, 'programma duurde', printit)


'''