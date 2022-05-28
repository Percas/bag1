#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Zaterdag 23 april 2022

Doel: selecteer bij elke VBO voorkomen (vbovk) een hoofdpand voorkomen (pndvk).
Het kadaster koppelt 1 of meer pnd aan een vbovk. Dit is om twee redenen niet
eenduidig:
    1. er kan meer dan 1 pnd aan een vbovk gekoppeld zijn (0,44%)
    2. bij het pnd kan het bouwjaar veranderen in een volgend vk

Voor elk pndvk bepalen we een prioriteit. Het pndvk met de hoogste prio wordt
het hoofdpnd vk. Hoe je de prioriteit berekent staat in de functie prio_pnd

0.2: Clean up code. Make one outputfile with all vbo vk:
    vboid, vbovkid, vbovkbg, vbovkeg, pndid, pndvkid, pndvkbg, pndvkeg, prio
    where that pnd vk is linked with pndvkbg < vbovkeg <= pndvkeg
    prio is only calculated if there is more than one pnd linked the the vbo
    vk. So if prio is empty (nan), it means that there is only one pnd vk
    linked to this vbo vk.
0.3: Koppel een hoofd-pndvk aan een vbovk: pndvk ipv een pnd dus!
0.3.2: Documentation and string formatting
0.3.3b: tel vbovk totaal en vbovk uniek (vku) in elke stap
0.4.1 koppel laagste pndvk aan vbovk_geen_pndvk
0.4.2 samenvatting toevoegen aan het eind met sleutelgetallen
0.5.1 aanpassing id naar vboid and pndid in koppelvlak 2 (waar het hoort ;-),
idem voor status naar vbostatus, vkid naar vbovkid etc.
0.6 functies verhuisd naar bag_functions.py
0.6.1 cleaning up
0.6.2 writes file bepaal_hoofdpand_kerngetallen to compare months
0.6.3 debugging error: in pndvk_prio staat in 202205 bij pnd 0003100000118116 
dubbele pndvkid met ook nog verschillende prio ????

"""

# ################ import libraries ###############################
import pandas as pd
import numpy as np
import sys
import os
import baglib

# ############### Define functions ################################
def prio_pnd(pnd1_df,
             in_voorraad_points, in_voorraad_statuslst,
             bouwjaar_points, bouwjaar_low, bouwjaar_high, bouwjaar_divider,
             pndid_divider):
    """
    Bereken een prioriteit voor elk pndid in pnd1_df.

    In detail:
        1. Als bouwjaar vh pand logisch is:     +bouwjaar_points
           logisch betekent tussen de bouwjaar_low en bouwjaar_high
        2. Als het pand in de voorraad is:      +in_voorraad_points
           in de voorraad betekend status in in_voorraad_statuslist:
               {inge, inni, verb}
        3. We trekken het vkid er nog af om het switchen naar een
           nieuw voorkomen te ontmoedigen
        4. Oudere panden krijgen (ietsje) minder aftrekpunten:
            -bouwjaar / bouwjaar_divider
        5. Om het uniek te maken trekken we er een klein getal van af obv
        pnd_idPND_DIV:       -pndid/pndid_divider

    Returns
    -------
    Hetzelfde input dataframe pnd1_df met een extra kolom prio.

    """
    print('\tDe prio van een pnd is de som van onderstaande punten:')
    print('\t\tPunten als het pand in voorraad is:', in_voorraad_points)
    _in_voorraad_check = pnd1_df['pndstatus'].isin(in_voorraad_statuslst)
    pnd1_df['prio'] = np.where(_in_voorraad_check, in_voorraad_points, 0)

    print('\t\tPunten als het bouwjaar logisch is:', bouwjaar_points)
    _logisch_bouwjaar_check = pnd1_df['bouwjaar'].\
        between(bouwjaar_low, bouwjaar_high)
    pnd1_df['prio'] = pnd1_df['prio'] + \
        np.where(_logisch_bouwjaar_check, bouwjaar_points, 0)
    print('\t\tMinpunten om switchen iets te ontmoedigen:', '-vkid')
    #bug: pnd1_df['prio'] = pnd1_df['prio'] - pnd1_df['vbovkid']
    pnd1_df['prio'] = pnd1_df['prio'] - pnd1_df['pndvkid']
    print('\t\tMinpunten voor het bouwjaar zelf (ca -1):',
            '-bouwjaar/' + str(bouwjaar_divider))
    pnd1_df['prio'] = pnd1_df['prio'] - \
        pnd1_df['bouwjaar'] / bouwjaar_divider

    print('\t\tMinpunten voor pandid (tbv herhaalbaarheid):',
            '-pndid /' + str(pndid_divider))
    pnd1_df['prio'] = pnd1_df['prio'] - \
        pnd1_df['pndid'].astype(int) / pndid_divider

    # print(active_pnd_df[['pndid', 'pndstatus', 'bouwjaar', 'prio']].head(30))
    # print('\tnumber of records in activ_pnd_df:', active_pnd_df.shape[0])
    # if pnd1_df.shape[0] != pnd1_df['pndid'].unique().shape[0]:
    #     print('Error of niet he;
    #           in functie prio_pnd: unieke panden versus actief')

    return pnd1_df

# #############################################################################
# print('00.............Initializing variables...............................')
# #############################################################################
# month and dirs
os.chdir('..')
BASEDIR = os.getcwd() + '/'
if BASEDIR[-4:-1] == 'ont':
    print('\t\t\t---------------------------------')
    print('\t\t\t--------ONTWIKKELOMGEVING--------')
    print('\t\t\t---------------------------------')
else:
    print('\t\t\t---------------------------------')
    print('\t\t\t--------PRODUCTIEOMGEVING--------')
    print('\t\t\t---------------------------------')
DATADIR = BASEDIR + 'data/'
DIR02 = DATADIR + '02-csv/'
DIR03 = DATADIR + '03-bewerktedata/'
current_month = baglib.get_arg1(sys.argv, DIR02)
INPUTDIR = DIR02 + current_month + '/'
OUTPUTDIR = DIR03 + current_month + '/'
current_month = int(current_month)
current_year = int(current_month/100)

# The tuning variables to determine pand prio
IN_VOORRAAD_P = 100         # if pndstatus in {inge, inni, verb}
BOUWJAAR_P = 50             # if YEAR_LOW < bouwjaar < current_year + 1
BOUWJAAR_DIV = 2000         # divide bouwjaar with 2000 (small around 1)
YEAR_LOW = 1000
PND_DIV = 10 ** 16
vbovk_geen_pndvk_df = pd.DataFrame()
n_vbovk_geen_pndvk = 0
result_dict = {}

pd.set_option('display.max_columns', 20)
# #####################################################
# ----------- Legenda ----- ---------------------------
# #####################################################
print(f'{"Legenda":~^80}')
print(f'~\t{"vbo:  verblijfsobject":<38}', f'{"pnd:  pand":<38}')
print(f'~\t{"vk:   voorkomen":<38}', f'{"pndvk:  pand voorkomen":<38}')
print(f'~\t{"vkbg: voorkomen begindatum geldigheid":<38}',
      f'{"vkeg: voorkomen einddatum geldigheid":<38}')
print(f'{"~":~>80}')


print('\n---------------DOELSTELLING--------------------------------')
print('----Bepaal voor elk VBO voorkomen een hoofdpand voorkomen')
print('----op basis van de vbo vkeg (voorkomen einddatum geldigheid)')
print('-----------------------------------------------------------')
print('\tStatistiekmaand:', current_month)

# #############################################################################
print('\n----1. Inlezen van vbo.csv en pnd.csv ------------------------')
# #############################################################################

print('\n\t1.1 VBO:------------------------------------------')
vbovk_df = pd.read_csv(INPUTDIR + 'vbo.csv',
                       dtype={'vboid': str, 'vbostatus': str, 'pndid': str,
                              'numid': str, 'vbovkid': np.short,
                              'vbovkbg': int, 'vbovkeg': int})
result_dict = baglib.df_total_vs_key('1a_vbovk', vbovk_df, ['vboid', 'vbovkid'],
                                    result_dict)

print('\tVerwijder vbo voorkomens met dezelfde begin en einddag:')
vbovk_df = baglib.fix_eendagsvlieg('vbo', vbovk_df, 'vbovkbg', 'vbovkeg')

print('\tAantal vbovk na verwijderen eendagsvliegen:')
result_dict = baglib.df_total_vs_key('1b_vbovk_1dagv', vbovk_df, ['vboid', 'vbovkid'],
                                    result_dict)
n_vbovk = vbovk_df.shape[0]
doel_vbovk_u = vbovk_df[['vboid', 'vbovkid']].drop_duplicates().shape[0]
print('\tDOEL:', doel_vbovk_u, 'vbovk van een pndvk voorzien.')

print('\n\t1.2 PND:------------------------------------------')
pndvk_df = pd.read_csv(INPUTDIR + 'pnd.csv',
                       dtype ={'pndid': str, 'pndstatus': str,
                               'pndvkid': 'Int64', 'pndvkbg': int,
                               'pndvkeg': int, 'bouwjaar': np.short,
                               'docnr': str})
result_dict = baglib.df_total_vs_key('1c_pndvk', pndvk_df, ['pndid', 'pndvkid'],
                                    result_dict)

print('\tVerwijder pnd voorkomens met dezelfde begin en einddag:')
pndvk_df = baglib.fix_eendagsvlieg('pnd', pndvk_df, 'pndvkbg', 'pndvkeg')

pnd_laagstevk_df = pndvk_df.sort_values('pndvkid', ascending=True).\
    drop_duplicates('pndid')
# print(pnd_laagstevk_df.info())
# print('pndvk_df:', pndvk_df.info())
print('Debug1:\n', pndvk_df[pndvk_df['pndid']=='0003100000118116'])
# #############################################################################
print('\n----2. Voeg de informatie van de pndvk toe aan de vbovk----')
# #############################################################################
print('\tDOEL reminder: aantal unieke vbovk:', doel_vbovk_u)
print('\tStart met de', n_vbovk, '(niet unieke) vbovk. Elk vbovk heeft 1 of',
      '\n\tmeer pndid (dubbele bij de zelf aangemaakte vbovk).\n',
      '\tDus\n',
      '\t\tvbovk1, pndid1\n',
      '\t\tvbovk1, pndid2 (dit vbovk is dan zelf aangemaakt (en dubbel))\n',
      '\t\tvbovk2, pndid3\n',
      '\tKoppel nu aan pndid1, pndid2, pndid3 alle voorkomens van het\n',
      '\tbetreffende pnd. Het aantal vbovk wordt hiermee ruim verdubbeld')
print('\tten opzichte van het eerdere aantal', n_vbovk)
vbovk_pndvk_df = pd.merge(vbovk_df,
                          pndvk_df,
                          how='left',
                          on='pndid')
print('vbovk_pndvk_df:', vbovk_pndvk_df.info())
print('\tAantal vbovk na koppeling met alle pndvk:')
baglib.df_total_vs_key('2_vbovk_alle_pndvk', vbovk_pndvk_df, ['vboid', 'vbovkid'],
                      result_dict)

# #############################################################################
print('\n----3. Selecteer nu het pndvk waarin de vkeg valt van de vbovk----')
# #############################################################################
msk = (vbovk_pndvk_df['pndvkbg'] <= vbovk_pndvk_df['vbovkeg']) & \
    (vbovk_pndvk_df['vbovkeg'] <= vbovk_pndvk_df['pndvkeg'])
vbovk_pndvk_df = vbovk_pndvk_df[msk]
# print('Debug2:\n', vbovk_pndvk_df[vbovk_pndvk_df['pndid']=='0003100000118116'].sort_values('vbovkid'))
print('Debug2:\n', vbovk_pndvk_df[vbovk_pndvk_df['vboid']=='0003010000125988'].sort_values('vbovkid'))

# print('vbovk_pndvk_df:', vbovk_pndvk_df.info())
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
if vbovk_pndvk_df.shape[0] == 0:
    sys.exit('Fout: in pnd.csv staat geen enkel pand dat koppelt met\
             vbo.csv. Verder gaan heeft geen zin. Programma stopt...')
result_dict = baglib.df_total_vs_key('3_vbovk_pndvk', vbovk_pndvk_df,
                                    ['vboid', 'vbovkid'],
                                    result_dict)
print('\t\tDe reden dat 3a optreedt is technisch van aard::\n',
      '\t\tIn bepaald-hoofdpand zoeken we pndvk bij vbovk met formule\n',
      '\t\t\tpndvkbg <= vbovkeg <= pndvkeg\n',
      '\t\tOmdat hier twee keer <= staat hou je nog enkele dubbele\n',
      '\t\tvbovk. Als je echter links of rechts < zou doen, dan blijkt\n',
      '\t\tdat je koppelingen met pndvk gaat missen')
'''
verschil_3a = result_dict['3_vbovk_pndvk_tot'] -\
    result_dict['3_vbovk_pndvk_uniek']

verschil_3a = result_dict['3_vbovk_pndvk_verschil']
'''
print('\t3a samengevat: aan', result_dict['3_vbovk_pndvk_tot'], '-',
      result_dict['3_vbovk_pndvk_uniek'], '=',
      result_dict['3_vbovk_pndvk_verschil'], 'hangen dus nog\n',
      '\teventjes twee (of meer) pndvk')

print('\n\t---3b. geen enkel pndvk koppelt met dit soort vbovk')
verschil_u = doel_vbovk_u - result_dict['3_vbovk_pndvk_uniek']
if verschil_u != 0:  # er zijn vbovk met 0 pndvk
    print('\tbij deze vbovk is er dus op datum vkeg geen pndvk te vinden:',
          '\n\t\tAantal unieke vbovk in: ', doel_vbovk_u,
          '\n\t\tAantal unieke vbovk uit:', result_dict['3_vbovk_pndvk_uniek'],
          '\n\t\tVerschil van de unieke: ', verschil_u),
    perc_doel = round(100 * (doel_vbovk_u - verschil_u) / doel_vbovk_u, 3)
    print('\tDoor deze', verschil_u, 'vbovk die geen pndvk hebben halen we\n',
          '\tons bovengenoemd DOEL voor', perc_doel, '%')
    result_dict['3b_perc_doel'] = perc_doel
    result_dict['3b_vbovk_geen_pandvk'] = verschil_u
    # Welke unieke vbovk missen we?
    vbovk_u = vbovk_df[['vboid', 'vbovkid']].drop_duplicates()
    vbovk_pndvk_u = vbovk_pndvk_df[['vboid',
                                    'vbovkid']].drop_duplicates()
    missing_vbovk_df = pd.concat([vbovk_pndvk_u,
                                  vbovk_u]).drop_duplicates(keep=False)
    vbovk_geen_pndvk_df = pd.merge(missing_vbovk_df, vbovk_df, how='left')
    n_vbovk_geen_pndvk = vbovk_geen_pndvk_df.shape[0]
    # print(vbovk_geen_pndvk_df.info())
else:
    print('\tSituatie 3b komt niet voor: aantal unieke vbovk is (DOEL):',
            doel_vbovk_u)


# #############################################################################
print('\n----4. Bepaal prio voor pndvk: welke is het best om te koppelen--')
# #############################################################################
print('\tWe voegen een kolom prio toe aan vbovk_pndvk...')
vbovk_pndvk2_df = prio_pnd(vbovk_pndvk_df,
                           IN_VOORRAAD_P, ['inge', 'inni', 'verb'],
                           BOUWJAAR_P, YEAR_LOW, current_year + 1,
                           BOUWJAAR_DIV, PND_DIV)
# print('vbovk_pndvk2_df:', vbovk_pndvk2_df.info())
print('\tSelecteer nu het pand met de hoogste prio. Alle pndvk krijgen een\n',
      '\tprio, maar de prio is alleen belangrijk',
      'bij die', result_dict['3_vbovk_pndvk_verschil'],
      'extra aangemaakte vbovk\n',
      '\tdie we in stap 3a geconstateerd hebben.')
print('Debug3:\n', vbovk_pndvk2_df[vbovk_pndvk_df['vboid']=='0003010000125988'].sort_values('vbovkid'))
vbovk_prio_df = vbovk_pndvk2_df.sort_values('prio', ascending=False).\
    drop_duplicates(['vboid', 'vbovkid'])
print('Debug4:\n', vbovk_prio_df[vbovk_pndvk_df['vboid']=='0003010000125988'].sort_values('vbovkid'))

print('\tBewaar de pnd prios in pndvk_prio.csv')
outputfile = OUTPUTDIR + 'pndvk_prio.csv'
vbovk_prio_df[['pndid', 'pndvkid', 'prio']]\
    .drop_duplicates().to_csv(outputfile, index=False)

print('\tHet aantal unieke vbovk mag niet wijzigen door het prioriteren:')
result_dict = baglib.df_total_vs_key('4_vbovk_prio', vbovk_prio_df,
                                    ['vboid', 'vbovkid'], result_dict)
doel2_vbovk = result_dict['4_vbovk_prio_tot']
n_vbovk_prio_u = result_dict['4_vbovk_prio_uniek']

if doel2_vbovk == result_dict['3_vbovk_pndvk_uniek'] and\
    doel2_vbovk == n_vbovk_prio_u:
    print('\tPrioritering geslaagd:')
    print('\tAantal unieke vbovk met unieke pndvk:', doel2_vbovk)
    print('\tAantal unieke vbovk zonder pndvk:', n_vbovk_geen_pndvk)
    print('\tTotaal:',
            doel2_vbovk + n_vbovk_geen_pndvk)
else:
    print('ERROR met aantal vbovk bij prioretering:')
    print('Aantal vbovk na prioritering:', doel2_vbovk)
    print('Aantal unieke vbovk na prioritering:', n_vbovk_prio_u)
if doel2_vbovk != n_vbovk_prio_u:
    print('Error 4: bij prioritering is aantal vbovk verstoord:',
            doel2_vbovk, '==>', n_vbovk_prio_u)

# #############################################################################
print('\n----4. Bewaren in koppelvlak3: vbovk_hoofdpndvk.csv met',
      doel2_vbovk, 'records...')
# #############################################################################
# print('vbovk_prio_df:', vbovk_prio_df.info())
outputfile = OUTPUTDIR + 'vbovk_hoofdpndvk.csv'
vbovk_prio_df[['vboid', 'vbovkid', 'pndid', 'pndvkid']]\
    .sort_values(['vboid', 'vbovkid'])\
    .to_csv(outputfile, index=False)
    # .rename({'vboid': 'id', 'vbovkid': 'vkid'}, axis='columns')\

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
print('\n----6. Samenvatting: bewaren van de belangrijkste kerngetallen------')
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
    current_df = pd.DataFrame(list(result_dict.items()),
                              columns=['Maand', current_month])
    result_df[current_month] = current_df[current_month]

outputfile = DIR03 + 'bepaal_hoofdpand_kerngetallen.csv'
result_df.to_csv(outputfile, index=False)
print(result_df)
