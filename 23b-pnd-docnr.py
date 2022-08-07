#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Zaterdag 23 april 2022

Doel: analyseer de afstand van de docnrs van panden in een woonplaats.

stappen:
    1. haal vbovk-hoofdpndvk en vbo-wpl op
    2. bepaal hiermee de wpl van een pndvk. 
    3. bepaal de afstand tussen de docnrs van de pndvk. Let met name op
    kleine afstanden. Sorteer bijvoorbeeld waarbij je de nul afstanden
    verwijdert
"""

# ################ import libraries ###############################
import pandas as pd
import numpy as np
import sys
import os
import time
import baglib

# ############### Define functions ################################

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
INPUTDIR = DIR03 + current_month + '/'
OUTPUTDIR = DIR03 + current_month + '/'
baglib.make_dir(OUTPUTDIR)

current_month = int(current_month)
current_year = int(current_month/100)


pd.set_option('display.max_columns', 20)


small_type_dict = {'vboid': 'string',
                   'vbovkid': np.short,
                   'vbovkbg': np.uintc, 
                   'vbovkeg': np.uintc,
                   'pndvkid': np.short,
                   'vbostatus': 'category',
                   'gebruiksdoel': 'category',
                   'oppervlakte': np.uintc,
                   'pndid': 'string',
                   'pndvkbg': np.uintc, 
                   'pndvkeg': np.uintc,
                   'pndstatus': 'category',
                   'bouwjaar': np.uintc,
                   'docnr': 'string',
                   'postcode': 'string',
                   'huisnr': 'string',
                   'oprid': 'string',
                   'wplid': 'string',
                   'gemid': 'string'
                   }

printit = True

# #####################################################
# ----------- Legenda ----- ---------------------------
# #####################################################
print(f'{"Legenda":~^80}')
print(f'~\t{"vbo:  verblijfsobject":<38}', f'{"pnd:  pand":<38}')
print(f'~\t{"vk:   voorkomen":<38}', f'{"pndvk:  pand voorkomen":<38}')
print(f'~\t{"vkbg: voorkomen begindatum geldigheid":<38}',
      f'{"vkeg: voorkomen einddatum geldigheid":<38}')
print(f'~\t{"n_...:  aantal records in df":<38}',
      f'{"n_..._u: aantal unieke records":<38}')
print(f'{"~":~>80}')


print('\n---------------DOELSTELLING--------------------------------')
print('----BAnalyseer de verschillen tussen docnrs van panden   ')
print('----in een woonplaats                                       )')
print('-----------------------------------------------------------')
print('\tStatistiekmaand:', current_month)

# #############################################################################
print('\n----1. Inlezen van pndvk, vbovk-hoofdpndvk en vbo-wpl--------------')
# #############################################################################

print('\n\t1.1 PND:------------------------------------------')
tic = time.perf_counter()

pndvk_df = pd.read_csv(INPUTDIR + 'pnd.csv',
                       dtype=small_type_dict)
pndvk_df.set_index('pndid', inplace=True)
(n_pndvk, n_pndvk_u, pndvk_perc) = baglib.df_comp(pndvk_df)

print('\tVerwijder pnd voorkomens met dezelfde begin en einddag')
pndvk_df = baglib.fix_eendagsvlieg(pndvk_df, 'pndvkbg', 'pndvkeg')

(n_pndvk, n_pndvk_u, pndvk_perc) = baglib.df_comp(pndvk_df,
                                                  n_rec = n_pndvk,
                                                  n_rec_u = n_pndvk_u)

toc = time.perf_counter()
baglib.print_time(toc - tic, 'reading, indexing, fixing eendagspndvk in',
                  printit)


print('\n\t1.1 vbovk_hoofdpnd.csv-----------------------------')
tic = time.perf_counter()

vbopnd_df = pd.read_csv(INPUTDIR + 'vbovk_hoofdpndvk.csv',
                        dtype=small_type_dict)
vbopnd_df.set_index(['vboid', 'vbovkid'], inplace=True)

(n_vp, n_vp_u, vp_perc) = baglib.df_comp(vbopnd_df)
print('\tGedeelte unieke vbovk:', vp_perc)

toc = time.perf_counter()
baglib.print_time(toc - tic, 'reading, indexing, fixing eendagsvbovk in',
                  printit)

# rint(vbopnd_df.info())

print('\n\t1.2 vbo_wpl----------------------------------------')
tic = time.perf_counter()

vbowpl_df = pd.read_csv(INPUTDIR + 'vbo_wpl.csv',
                       dtype=small_type_dict)
vbowpl_df.set_index(['vboid', 'vbovkid'], inplace=True)
(n_vw, n_vw_u, vw_perc) = baglib.df_comp(vbowpl_df)
print('\tntal actieve vbovk:', n_vw_u)

toc = time.perf_counter()
baglib.print_time(toc - tic, 'reading, indexing, fixing eendagspndvk in',
                  printit)
# print(vbowpl_df.info())


# #############################################################################
print('\n----2. Bepalen wpl van een pnd vk----------------------------')
# #############################################################################
tic = time.perf_counter()

vbowpl_df = vbowpl_df.join(vbopnd_df, how='left')
(n_vw, n_vw_u, vw_perc) = baglib.df_comp(vbowpl_df, n_rec=n_vw, n_rec_u=n_vw_u)
# print(vbowpl_df.info())

'''
deze hangt:
pndvk_df = vbowpl_df.groupby(['pndid', 'pndvkid'])['wplid'].min()
'''

pndvk_df = vbowpl_df.reset_index().set_index(['pndid', 'pndvkid'])



print(pndvk_df.info())

toc = time.perf_counter()
baglib.print_time(toc - tic, 'connecting pndvk with wpl in',
                  printit)

'''

# #############################################################################
print('\n----2. Voeg de informatie van de pndvk toe aan de vbovk----')
# #############################################################################
# print('\tDOEL reminder: aantal unieke vbovk:', doel_vbovk_u)
print('\tStart met de', n_vbovk, '(niet unieke) vbovk. Elk vbovk heeft 1 of',
      '\n\tmeer pndid (dubbele bij de zelf aangemaakte vbovk).\n',
      '\tDus\n',
      '\t\tvbovk1, pndid1\n',
      '\t\tvbovk1, pndid2 (dit vbovk is dan zelf aangemaakt (en dubbel))\n',
      '\t\tvbovk2, pndid3\n',
      '\tKoppel nu aan pndid1, pndid2, pndid3 alle voorkomens van het\n',
      '\tbetreffende pnd. Het aantal vbovk wordt hiermee ruim verdubbeld')
print('\tten opzichte van het eerdere aantal', n_vbovk)

tic = time.perf_counter()

vbovk_df = pd.merge(vbovk_df,
                    pndvk_df,
                    how='left',
                    on='pndid')
(n_vbovk, n_vbovk_u, vbovk_perc) = \
    baglib.df_comp(vbovk_df, key_lst=['vboid', 'vbovkid'], 
                   n_rec=n_vbovk, n_rec_u=n_vbovk_u, u_may_change=False)

vbovk_df.set_index(['vboid', 'vbovkid'], inplace=True)

vbovk_df = baglib.recast_df_floats(vbovk_df, pnd_type_dict)
(n_vbovk, n_vbovk_u, vbovk_perc) = \
    baglib.df_comp(vbovk_df, n_rec=n_vbovk, n_rec_u=n_vbovk_u,
                   u_may_change=False)

toc = time.perf_counter()
baglib.print_time(toc - tic, 'step 2 in', printit)


# #############################################################################
print('\n----3. Selecteer nu het pndvk waarin de vkeg valt van de vbovk----')
# #############################################################################
tic = time.perf_counter()

msk = (vbovk_df['pndvkbg'] <= vbovk_df['vbovkeg']) & \
    (vbovk_df['vbovkeg'] <= vbovk_df['pndvkeg'])
vbovk_df = vbovk_df[msk]

(n_vbovk, doel2_vbovk_u, vbovk_perc) = \
    baglib.df_comp(vbovk_df, n_rec=n_vbovk, n_rec_u=n_vbovk_u,
                   u_may_change=True)

verschil_u =  n_vbovk_u - doel2_vbovk_u
n_vbovk_u = doel2_vbovk_u

print('\t\tAantal vbovk zonder pndvk:', verschil_u)

print('\tVoor gegeven vbovk zou je gegeven een pnd, precies 1 pndvk moeten\n',
      '\tvinden op dag vbovkeg. Hiermee zou het aantal weer op', n_vbovk, '\n',
      '\tmoeten uitkomen, waarin', doel_vbovk_u, 'unieke vbovk zitten. In de\n',
      '\tpraktijk vinden we echter twee andere situaties:\n',
      '\t\t Situatie 3a: meer dan 1 pndvk koppelt met de vbovk\n',
      '\t\t Situatie 3b: geen enkel pndvk koppelt met de vbovk\n',
      '\tWe zullen deze apart beschouwen:')
print('\t\t3a, >1 is niet zo erg. Dat lossen we op met de prio functie\n',
      '\t\t3b  is een datafout. Bewaar deze vbovk in vbovk_geen_pndvk')

# welke unieke vbovk houden we over:
print('\n\t---3a: meer dan 1 pndvk koppelt met een vbovk')
print('\tStartpunt (reminder) aantal records vbovk:', n_vbovk)
print('\tvbovk na het koppelen met pndvk obv vbovkeg: ')
if vbovk_df.shape[0] == 0:
    sys.exit('Fout: in pnd.csv staat geen enkel pand dat koppelt met\
             vbo.csv. Verder gaan heeft geen zin. Programma stopt...')

print('\t\tDe reden dat 3a optreedt is technisch van aard::\n',
      '\t\tIn bepaald-hoofdpand zoeken we pndvk bij vbovk met formule\n',
      '\t\t\tpndvkbg <= vbovkeg <= pndvkeg\n',
      '\t\tOmdat hier twee keer <= staat hou je nog enkele dubbele\n',
      '\t\tvbovk. Als je echter links of rechts < zou doen, dan blijkt\n',
      '\t\tdat je koppelingen met pndvk gaat missen')


# verschil_u = doel_vbovk_u - result_dict['3_vbovk_pndvk_uniek']
if verschil_u != 0:  # er zijn vbovk met 0 pndvk
    print('\tbij deze vbovk is er dus op datum vkeg geen pndvk te vinden:',
          '\n\t\tAantal unieke vbovk in: ', doel_vbovk_u,
          # '\n\t\tAantal unieke vbovk uit:', result_dict['3_vbovk_pndvk_uniek'],
          '\n\t\tVerschil van de unieke: ', verschil_u),
    perc_doel = round(100 * (doel_vbovk_u - verschil_u) / doel_vbovk_u, 3)
    print('\tDoor deze', verschil_u, 'vbovk die geen pndvk hebben halen we\n',
          '\tons bovengenoemd DOEL voor', perc_doel, '%.')
    
    print('\tNieuwe doel:', doel2_vbovk_u)
    # result_dict['3b_perc_doel'] = perc_doel
    # result_dict['3b_vbovk_geen_pandvk'] = verschil_u

    # vbovk_u = vbovk_df[['vboid', 'vbovkid']].drop_duplicates()
  

else:
    print('\tSituatie 3b komt niet voor: aantal unieke vbovk is (DOEL):',
            doel_vbovk_u)

toc = time.perf_counter()
baglib.print_time(toc - tic, 'countin vbovk in', printit)


# #############################################################################
print('\n----4. Bepaal prio voor pndvk: welke is het best om te koppelen--')
# #############################################################################
tic = time.perf_counter()

print('\tWe voegen een kolom prio toe aan vbovk_pndvk...')
vbovk_df = prio_pnd(vbovk_df,
                    IN_VOORRAAD_P, IN_VOORRAAD,
                    BOUWJAAR_P, YEAR_LOW, current_year + 1,
                    BOUWJAAR_DIV, PND_DIV)
(n_vbovk, n_vbovk_u, vbovk_perc) = \
    baglib.df_comp(vbovk_df, n_rec=n_vbovk, n_rec_u=n_vbovk_u,
                   u_may_change=False)
# print(vbovk_df[['pndid', 'pndvkid','prio']])

print('\tSelecteer nu het pand met de hoogste prio. Alle pndvk krijgen een\n',
      '\tprio, maar de prio is alleen belangrijk bij de',
      # 'bij die', result_dict['3_vbovk_pndvk_verschil'],
      'extra aangemaakte vbovk\n',
      '\tdie we in stap 3a geconstateerd hebben.')
# print(vbovk_df[['pndid', 'pndvkid','prio']])

# how to remove the right non unique vbovk:
# https://stackoverflow.com/questions/13035764/remove-pandas-rows-with-duplicate-indices/13036848#13036848
vbovk_df = vbovk_df.sort_values('prio', ascending=False)
vbovk_df = vbovk_df[~vbovk_df.index.duplicated(keep='first')]
(n_vbovk, n_vbovk_u, vbovk_perc) = \
    baglib.df_comp(vbovk_df, n_rec=n_vbovk, n_rec_u=n_vbovk_u,
                   u_may_change=False)

# print(vbovk_df[['pndid', 'pndvkid','prio']])
toc = time.perf_counter()
print('\t\ttictoc - prio, sort and drop duplicates', toc - tic, 'seconds')




tic = time.perf_counter()

print('\tBewaar de pnd prios in pndvk_prio.csv')
outputfile = OUTPUTDIR + 'pndvk_prio.csv'
vbovk_df[['pndid', 'pndvkid', 'prio']]\
    .drop_duplicates().to_csv(outputfile, index=False)
# print('DEBUG-----------------\n', vbovk_prio_df.info())
toc = time.perf_counter()
print('\t\ttictoc - saving pnd-prio file in', toc - tic, 'seconds')
'''
'''
# #############################################################################
print('\n----5. Bewaren in koppelvlak3: vbovk_hoofdpndvk.csv met',
      doel2_vbovk_u, 'records...')
# #############################################################################
tic = time.perf_counter()


outputfile = OUTPUTDIR + 'vbovk_hoofdpndvk.csv'

# vbovk_hoofdpndvk_df = vbovk_prio_df[['vboid', 'vbovkid', 'pndid', 'pndvkid']]
# vbovk_hoofdpndvk_df.sort_values(['vboid', 'vbovkid']).to_csv(outputfile,
#                                                            index=False)

vbovk_df[['pndid', 'pndvkid']].sort_index().to_csv(outputfile, index=True)

toc = time.perf_counter()
print('\t\ttictoc - saving vbo-pnd file in', toc - tic, 'seconds')
'''
'''
if vbovk_geen_pndvk_df.shape[0] != 0:
    print('\tWe verrijken vbovk_geen_pndvk met "het laagste pndvk dat geen\n',
          '\teendagsvlieg is". Dat is meestal het vk dat het dichtst in de\n',
          '\tbuurt ligt van de vkeg van het vbovk...')
    vbovk_geen_pndvk_df = pd.merge(vbovk_geen_pndvk_df,
                                   pnd_laagstevk_df, how='left')
    if vbovk_geen_pndvk_df.shape[0] != n_vbovk_geen_pndvk:
        print('ERROR: aantal vk gewijzigd:', n_vbovk_geen_pndvk, '=>',
              vbovk_geen_pndvk_df.shape[0])
    else:
        print('\tBewaren van vbovk_geen_pndvk.csv met', n_vbovk_geen_pndvk,
              'records...')
        outputfile = OUTPUTDIR + 'vbovk_geen_pndvk.csv'
        vbovk_geen_pndvk_df.to_csv(outputfile)
else:
    print('\tEr is geen bestand aangemaakt met vbovk zonder pndvk')

# #############################################################################
print('\n----6. Bepalen n_vbo_in_pndvk, gerelateerd aan vbo.typeinliggend')
# #############################################################################
print('\tn_vbo_in_pndvk is een eigenschap van pndvk. Het is het aantal\n',
      '\tunieke vbo bij een pndvk, waarbij 1 van de vbovk bij die vbo wel\n',
      '\tin IN_VOORRAAD moet zijn')
print('\tStappen:\n',
      '\t\t1. Bepaal voor elke vbovk of deze in IN_VOORRAAD zit\n',
      '\t\t2. Bepaal voor elke pndvk de vbovk in IN_VOORRAAD\n',
      '\t\t\t2a. zoek bij pndvk een vbovk\n',
      '\t\t\t2b. koppel deze vbovk met de vbovk in voorraad\n',
      '\t\t\t2c. laat vkid weg en drop dubbele. tel nu het aantal keren dat\n',
      '\t\t\t\t voorraad==TRUE per pndvk\n'
      '\t\t3. Tel deze. Het aantal is 0 of meer. Er zijn pndvk zonder VBO\n',
      '\t\t4. Dit aantal is n_vbo_in_pndvk. Ken dit aantal ook toe aan de\n',
      '\t\t\tvbovk die bij het betreffende pndvk horen')
print('\tDe situaties die voorkomen zijn:\n',
      '\t\tA. pndvk heeft geen vbo. Datafout: pndvk_zonder_vbo.csv\n',
      '\t\tB. pndvk heeft 1 vbo: vbovk typeinliggend = False: woonhuis\n',
      '\t\tC. pndvk heeft >1 vbo: vbovk typeinliggend = True: flat')

print('\n\t\t5.1:  vbovk: leid voorraad af')
vbovk_df['voorraad'] = vbovk_df['vbostatus'].isin(IN_VOORRAAD)
vbovk_df2 = vbovk_df[vbovk_df['voorraad']==True]
vbovk_df2 = vbovk_df2[['vboid', 'vbovkid', 'voorraad']].drop_duplicates()
baglib.df_total_vs_key2('unieke vbovk in IN_VOORRAAD',
                        vbovk_df2, ['vboid', 'vbovkid'])

print('\n\t\t5.2a pak alle pndvk. voeg vbovk toe via vbovk-hoofdpndvk ')
# vbovk-hoofdpndvk zit in het dataframe vbovk_prio_df
baglib.df_total_vs_key2('pndvk', pndvk_df, ['pndid', 'pndvkid'])
# baglib.df_total_vs_key2('vbovk_prio', vbovk_prio_df, ['vboid', 'vbovkid'])
pndvk_vbovk_df = pd.merge(pndvk_df[['pndid', 'pndvkid']],
                          vbovk_hoofdpndvk_df, how='left')
baglib.df_total_vs_key2('pndvk_vbovk_df na merge met vbovk_hoofdpandvk_df',
                        pndvk_vbovk_df, ['pndid', 'pndvkid'])
# print(pndvk_df.info())

print('\n\t\t5.2b koppel deze pndvk met de vbovk in voorraad')
pndvk_vbovk2_df = pd.merge(pndvk_vbovk_df, vbovk_df2, how='left')
baglib.df_total_vs_key2('pndvk_vbovk2_df na merge met vbovk_df',
                        pndvk_vbovk2_df, ['pndid', 'pndvkid'])

print('\n\t\t5.2c laat vkid weg en ontdubbel: we willen unieke vbo')
pndvk_vbo_df = pndvk_vbovk2_df[['pndid', 'pndvkid','vboid', 'voorraad']]\
    .drop_duplicates()
baglib.df_total_vs_key2('pndvk_vbo', pndvk_vbo_df, ['pndid', 'pndvkid'])

print('\t\t5.3 tel aantal keren voorraad per pndvk')
pndvk_vbo_df['voorraad'] = pndvk_vbo_df['voorraad'].fillna(0)
pndvk_vbo_df['voorraad'] = pndvk_vbo_df['voorraad'].astype(int)
pndvk_nvbo_df = pndvk_vbo_df.groupby(['pndid', 'pndvkid'])['voorraad']\
    .sum()\
    .reset_index()
pndvk_nvbo_df.rename({'voorraad': 'nvbo'}, axis='columns', inplace=True)
# print('---------DEBUG1', pndvk_nvbo_df.info())

result_dict['gem_nvbo_in_pndvk'] = pndvk_nvbo_df['nvbo'].mean()
print('\t\tgemiddeld aantal vbos in pndvk:', result_dict['gem_nvbo_in_pndvk'])

print('\n\t\t5.4 dit aantal toewijzen aan vbovk')
print('\t\t\tkoppel hiervoor weer vbovk weer aan pndvk, nu vanuit vbovk')
vbovk_pndvk_nvbo_df = pd.merge(vbovk_hoofdpndvk_df, pndvk_nvbo_df, how='left')
# print('---------DEBUG2', vbovk_pndvk_nvbo_df.info())
result_dict['gem_inliggend_per_vbovk'] = vbovk_pndvk_nvbo_df['nvbo'].mean()
print('\t\tgemiddeld aantal vbos in vbovk:',
      result_dict['gem_inliggend_per_vbovk'])

print('\n\t\t5.5. bewaren in pndvk_nvbo.csv vbovk_nvbo.csv...')
outputfile = OUTPUTDIR + 'pndvk_nvbo.csv'
pndvk_nvbo_df.to_csv(outputfile, index=True)
outputfile = OUTPUTDIR + 'vbovk_nvbo.csv'
vbovk_pndvk_nvbo_df[['vboid', 'vbovkid', 'nvbo']].to_csv(outputfile,
                                                         index=True)


# #############################################################################
print('\n----7. Samenvatting: bewaren van de belangrijkste kerngetallen------')
# #############################################################################
bepaal_hoofdpand_kerngetallen_file = DIR03 +\
    'bepaal_hoofdpand_kerngetallen.csv'
current_month = str(current_month)
try:
    result_df = pd.read_csv(bepaal_hoofdpand_kerngetallen_file)
except:
    result_df = pd.DataFrame(list(result_dict.items()),
                              columns=['Maand', current_month])
else:
    print('Aanmaken nieuw bestand bepaal_hoofdpand_kerngetallen...')
    current_df = pd.DataFrame(list(result_dict.items()),
                              columns=['Maand', current_month])
    result_df[current_month] = current_df[current_month]

outputfile = DIR03 + 'bepaal_hoofdpand_kerngetallen.csv'
result_df.to_csv(outputfile, index=False)
print(result_df)
'''