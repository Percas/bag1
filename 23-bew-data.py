#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 22

Purpose: make a count of voorraad and other VBO stuff
0.1: first attempt to create microdata
0.2: make a VBO view for peilmomenten

"""

# ################ import libraries ###############################
import pandas as pd
# import numpy as np


# ############### Define functions ################################
def select_active_vk(df, idate):
    """
    Select active voorkomens of df on idate.

    Returns
    -------
    subset of df of active voorkomens. Active on idate.

    Select id,vkid of those records of df with vkbg <= idate <= vkeg.

    Parameters
    ----------
    df : dataframe with column names vkbg and vkeg
        both these fields are 8-digit In664 of format YYYYMMDD
    idate : int64
        8-digit Integer of format YYYYMMDD
    Returns
    -------
    returns id, vkid those records with vkbg <= idate <= vkeg and the
    highest vkid. This is the active one

    """
    _mask = (df['vkbg'] <= idate) & (idate < df['vkeg'])
    _df_active = df[_mask].copy()

    _all_voorkomens = df.shape[0]
    _all_active_voorkomens = _df_active.shape[0]
    _perc_active = round(100 * _all_active_voorkomens / _all_voorkomens, 2)
    print('\tActieve voorkomens:                   ', _all_active_voorkomens)
    print('\tPercentage actieve voorkomens tov tot:', _perc_active, '%')
    # ################# LOGGING ################
    return _df_active


def prio_pnd(active_pnd_df,
             in_voorraad_points, in_voorraad_statuslst,
             bouwjaar_points, bouwjaar_low, bouwjaar_high, bouwjaar_divider,
             pndid_divider):
    """
    Calculate unique pnd priority for linkin to vbo.

    Returns
    -------
    pnd_df with extra column called prio.

    """
    print('\tDe prio van een pnd is de som van onderstaande punten:')
    print('\tPunten als het pand in voorraad is:   ', in_voorraad_points)
    _in_voorraad_check = active_pnd_df['pndstatus'].isin(in_voorraad_statuslst)
    active_pnd_df['prio'] = np.where(_in_voorraad_check, in_voorraad_points, 0)

    print('\tPunten als het bouwjaar logisch is:   ', bouwjaar_points)
    _logisch_bouwjaar_check = active_pnd_df['bouwjaar'].\
        between(bouwjaar_low, bouwjaar_high)
    active_pnd_df['prio'] = active_pnd_df['prio'] + \
        np.where(_logisch_bouwjaar_check, bouwjaar_points, 0)

    print('\tPunten voor het bouwjaar zelf:        -bouwjaar /',
          bouwjaar_divider)
    active_pnd_df['prio'] = active_pnd_df['prio'] - \
        active_pnd_df['bouwjaar'] / bouwjaar_divider

    print('\tPunten voor het pandid:               -pndid /', pndid_divider)
    active_pnd_df['prio'] = active_pnd_df['prio'] - \
        active_pnd_df['pndid'].astype(int) / pndid_divider

    # print(active_pnd_df[['pndid', 'pndstatus', 'bouwjaar', 'prio']].head(30))
    # print('\tnumber of records in activ_pnd_df:', active_pnd_df.shape[0])
    if active_pnd_df.shape[0] != active_pnd_df['pndid'].unique().shape[0]:
        print('Error in functie prio_pnd: unieke panden versus actief')

    return active_pnd_df


def update_prio(vbopnd_df, previous_df):
    """
    Increase column prio in vbopnd_df for those [vbo,pnd] in previous_df.

    Increase with the number in de sticky column of previous_df.

    Returns
    -------
    The updated vbopnd_df.

    """
    print('\tVerhoog voor een vbo-pnd koppeling de prio met', PND_STICKY)
    print('\t\tals dat pnd vorige maand ook al gekoppeld was aan die vbo')
    if previous_df.empty:
        print('\tGeen punten voor vbopnnd in loopnummer 0, check:', loop_count)
    else:
        print('\tEerder gekoppelde panden krijgen extra punten:', PND_STICKY)
        vbopnd_df = vbopnd_df.merge(previous_df, how='left',
                                    on=['vboid', 'pndid']).reindex()
        # print('vbopnd_df.info in update_prio')
        # print(vbopnd_df.info())
        vbopnd_df.fillna(0, inplace=True)
        vbopnd_df['prio'] = vbopnd_df['prio'] + vbopnd_df['sticky']
        # print(vbopnd_df[['vboid', 'pndid', 'prio', 'sticky']].head(30))
        # print(vbopnd_df.info())
    return vbopnd_df


# def read_csv(inputdir, file_with_bag_objects, cols, dtype_dict):
def read_csv(inputdir, file_with_bag_objects, dtype_dict):
    """
    Read voorkomens from file in inputdir, do some counting.

    Returns
    -------
    Dataframe with voorkomens.

    """
    # print('\tread ', file_with_bag_objects, '...')
    _df = pd.read_csv(inputdir + file_with_bag_objects,
                      # usecols=cols,
                      dtype=dtype_dict)
    _all_voorkomens = _df.shape[0]
    _all_kadaster_voorkomens = _df[['id', 'vkid']].drop_duplicates().shape[0]
    _verschil = _all_voorkomens - _all_kadaster_voorkomens
    print('\tVoorkomens totaal:                    ', _all_voorkomens)
    print('\tVoorkomens volgens kadaster:          ', _all_kadaster_voorkomens)
    print('\tZelf aangemaakte voorkomens:          ', _verschil)
    return _df


def my_leftmerge(left_df, right_df, check_nan_column=None):
    """
    Do a simple left merge including a checks on number of records.

    Returns
    -------
    The left merged dataframe.

    """
    _before = left_df.shape[0]
    _df = pd.merge(left_df, right_df, how='left')
    _after = _df.shape[0]
    if _before != _after:
        print('aantal records gewijzigd in merge. voor:', _before, 'na:',
              _after)
    if check_nan_column is not None:
        _empty_fields = _df[check_nan_column].isna().sum()
        _perc = round(100 * _empty_fields/_after, 2)
        if _empty_fields != 0:
            print('\tMerge resultaat heeft', _perc, 'procent lege velden')
    return _df


# #############################################################################
# print('00.............Initializing variables...............................')
# #############################################################################
current_month = 202204
basedir = '/home/anton/python/bag/'
inputdir = basedir + '02-csv/' + str(current_month) + '/'
outputdir = basedir + '03-bewerktedata/' + str(current_month) + '/'
# peildatum_lst = [20211101, 20211201, 20220101, 20220201]
peildatum_lst = [20220401]
# peildatum_lst = [20220101, 20220201]
current_year = int(current_month/100)
loop_count = 0
pnd_voorraad = ['inge', 'inni', 'verb']
vbo_voorraad = ['inge', 'inni', 'verb']
print('-----------------------------------------------------------')
print('----Tel de woningvoorraad in Nederland---------------------')
print('-----------------------------------------------------------')

# #############################################################################
print('----Inlezen vbo, num, opr, wpl, gem.csv in geheugen------------')
# #############################################################################
print('\tVBO:')
vbo_df = read_csv(inputdir,
                  'vbo.csv',
                  {'id': str, 'pndid': str, 'oppervlakte': str, 'numid': str})
print('\tPND:')
pnd_df = read_csv(inputdir, 'pnd.csv', {'id': str})

print('\tNUM:')
num_df = read_csv(inputdir, 'num.csv', {'id': str, 'oprid': str})

print('\tOPR:')
opr_df = read_csv(inputdir, 'opr.csv', {'id': str, 'wplid': str})

print('\tWPL:')
wpl_df = read_csv(inputdir, 'wpl.csv', {'id': str})

print('\tWPLGEM:')
gemwpl_df = pd.read_csv(inputdir + 'gemwpl.csv', dtype={'wplid': str,
                                                        'gemid': str})

print('\nLoop over maanden:', peildatum_lst)
for peildatum_i in peildatum_lst:
    print('-----------------------------------------------------------')
    print('Peildatum in loop nummer', loop_count, ':\t\t', peildatum_i)
    print('-----------------------------------------------------------')

    # #############################################################################
    print('----VBO----------------------------------------------------')
    # #############################################################################
    active_vbo_df = select_active_vk(vbo_df, peildatum_i)
    active_vbo_df.drop(['idx', 'vkeg', 'vkid', 'vkbg'], axis=1, inplace=True)
    active_vbo_df.rename(columns={'status': 'vbostatus', 'id': 'vboid'},
                         inplace=True)

    # #############################################################################
    print('----PND----------------------------------------------------')
    # #############################################################################
    active_pnd_df = select_active_vk(pnd_df, peildatum_i)
    active_pnd_df.drop(['idx', 'vkeg', 'vkid', 'vkbg'], axis=1, inplace=True)
    active_pnd_df.rename(columns={'status': 'pndstatus', 'id': 'pndid'},
                         inplace=True)

    # #############################################################################
    print('----NUM----------------------------------------------------')
    # #############################################################################
    active_num_df = select_active_vk(num_df, peildatum_i)
    active_num_df.drop(['idx', 'vkeg', 'vkbg', 'vkid'], axis=1, inplace=True)
    active_num_df.rename(columns={'id': 'numid', 'status': 'numstatus'},
                         inplace=True)
    # print('active_pnd_df:')
    # print(active_pnd_df.info())

    # #############################################################################
    print('----OPR----------------------------------------------------')
    # #############################################################################
    active_opr_df = select_active_vk(opr_df, peildatum_i)
    active_opr_df.drop(['idx', 'vkeg', 'vkbg', 'vkid'], axis=1, inplace=True)
    active_opr_df.rename(columns={'id': 'oprid', 'status': 'oprstatus'},
                         inplace=True)
    # print('active_pnd_df:')
    # print(active_pnd_df.info())

    # #############################################################################
    print('----WPL----------------------------------------------------')
    # #############################################################################
    active_wpl_df = select_active_vk(wpl_df, peildatum_i)
    active_wpl_df.drop(['idx', 'vkeg', 'vkbg', 'vkid'], axis=1, inplace=True)
    active_wpl_df.rename(columns={'id': 'wplid', 'status': 'wplstatus'},
                         inplace=True)

    # #############################################################################
    print('----GEMWPL-------------------------------------------------')
    # #############################################################################
    active_gemwpl_df = select_active_vk(gemwpl_df, peildatum_i)
    active_gemwpl_df.drop(['idx', 'vkeg', 'vkbg'], axis=1, inplace=True)
    active_gemwpl_df.rename(columns={'status': 'gemwplstatus'},
                            inplace=True)

    # #############################################################################
    print('----Verrijken van VBO met woonplaats en gemeente---------    ')
    # #############################################################################

    print('\tAdd gemid to wpl, link on wplid...')
    wg_df = my_leftmerge(active_wpl_df, active_gemwpl_df, 'gemid')

    print('\tAdd wplid, gemid to opr, link on wplid...')
    owg_df = my_leftmerge(active_opr_df, wg_df, 'gemid')

    print('\tAdd oprid, wplid, gemid to num, link on oprid')
    nowg_df = my_leftmerge(active_num_df, owg_df, 'wplid')

    print('\tAdd numid, oprid, wplid, gemid to vbo, link on numid')
    vnowg_df = my_leftmerge(active_vbo_df, nowg_df, 'oprid')
    # print(vnowg_df.info())

    outputfile = outputdir + 'vbo.csv'
    vnowg_df.dropna().to_csv(outputfile)

    '''
    # #############################################################################
    print('----Punten voor panden: welke is het best om te koppelen:---')
    # #############################################################################
    active_pnd_prio_df = prio_pnd(active_pnd_df,
                                  IN_VOORRAAD_P, ['inge', 'inni', 'verb'],
                                  BOUWJAAR_P, YEAR_LOW, current_year + 1,
                                  BOUWJAAR_DIV, PND_DIV)

    # #############################################################################
    print('----Koppel het pand met de meeste punten aan VBO---------')
    # #############################################################################

    print('\tIn vbopnd zitten nu nog dubbele. Zie zelf aangemaakte voorkomens')
    print('\tKoppel prio van een pand aan vbopnd')
    vbopnd_dbl_df = pd.merge(active_vbo_df, active_pnd_prio_df,
                             how='left', on='pndid').reset_index()
    n_not_linked = vbopnd_dbl_df['prio'].isna().sum()
    n_vbopnd_dbl_df = vbopnd_dbl_df.shape[0]
    print('\tAantal in vbopnd:', n_vbopnd_dbl_df,
          ', aantal vbo niet gekoppeld: ', n_not_linked)
    
    vbopnd_dbl_df = update_prio(vbopnd_dbl_df, sticky_vbopnd_df)
    
    # print('vbopnd_dbl_df after updated prio:')
    # print(vbopnd_dbl_df.info())

    print('\tControles op aantallen voor de groupby: ', end='')
    if vbopnd_dbl_df.shape[0] != active_vbo_df[['vboid', 'pndid']].\
            drop_duplicates().shape[0]:
        print('Error:')
        print('\tnumber of records in vbopnd_df after merge:               ',
              vbopnd_dbl_df.shape[0])
        print('\t     this should equal the number of unique vboid, pndid: ',
              active_vbo_df[['vboid', 'pndid']].drop_duplicates().shape[0])
    else:
        print('ok2', vbopnd_dbl_df.shape[0])

    print('\tSelecteer nu het pand met de hoogeste prio. Dit is alleen van\n\
          \t\tbelang bij die vbo die meer dan 1 pnd hebben')

    vbopnd_df = vbopnd_dbl_df.sort_values('prio', ascending=False).\
        drop_duplicates(['vboid'])
    vbopnd_df.set_index('vboid', inplace=True)


    count_df = vbopnd_dbl_df.groupby('vboid').size().to_frame(name='aantal_pnd')
    print('\tChecking n of records vbopnd_df...', end='')
    if vbopnd_df.shape[0] == count_df.shape[0]:
        print('ok3')
    else:
        print('ERROR: count_df records', count_df.shape[0],
              'should equal vbopnd_df records', vbopnd_df.shape[0])
    vbopnd_df = vbopnd_df.join(count_df).reset_index()
    
    # print(count_df)
    # print(count_df.info())
    
    
    ''' 
    '''
    this works, but still need to add pnd
    gb_object = vbopnd_dbl_df.groupby('vboid')
    count_df = gb_object.size().to_frame(name='aantal_pnd')
    vbopnd_df = count_df.join(gb_object.agg({'prio': 'max'}))
    '''
    '''
    gb_object = vbopnd_dbl_df.groupby('vboid')
    count_df = gb_object.size().to_frame(name='aantal_pnd')
    vbo_df = count_df.join(gb_object.agg({'prio': 'max'})).reset_index()
    vbopnd_df = pd.merge(left=vbo_df,
                         right=vbopnd_dbl_df[['vboid', 'prio', 'pndid']],
                         on=['vboid', 'prio'],
                         how='inner')
    # print(vbopnd_df)
    # print(vbopnd_df.info())
    '''
    '''
    # print('vbopnd_df na de groupby:')
    # print(vbopnd_df.info())
    print('\tControle op aantallen na de groupby:\t')
    print('\tAantal zelf gemaakte voorkomens bij de actieve:',
          active_vbo_df[['vboid', 'pndid']].drop_duplicates().shape[0] -
          vbopnd_df.shape[0])
    print('\tvboid is nu uniek in vbopnd_df')

    # #############################################################################
    print('----Bewaar info uit vorige loop en verzamel verschillen--')
    # #############################################################################

    print('\tBewaar vbopnd uit deze loop voor de volgende: +', PND_STICKY)
    sticky_vbopnd_df = vbopnd_df[['vboid', 'pndid']]
    sticky_vbopnd_df = sticky_vbopnd_df.assign(sticky=PND_STICKY)

    if previous_vbopnd_df.empty:
        print('\tIn loopnummer 0 valt er nog niets te bewaren, loop:',
              loop_count)
    else:
        print('\tBewaar het verschil met de vorige loop in vbopnd_diff_df')
        vbopnd_diff_df = vbopnd_df.merge(previous_vbopnd_df, on='vboid',
                                         how='inner')
        vbopnd_diff_df = vbopnd_diff_df[vbopnd_diff_df['pndid_x'] !=
                                        vbopnd_diff_df['pndid_y']]
        vbopnd_diff_df = vbopnd_diff_df.assign(maand=peildatum_i)
        print('\tAantal verschillen tussen', peildatum_i,
              'en de vorige maand:', vbopnd_diff_df.shape[0])
        if vbopnd_diff_all_df.empty:
            print('\tDit is loop 1, loop:', loop_count)
            vbopnd_diff_all_df = vbopnd_diff_df
        else:
            # print('\tDit is loop 2 of meer, loop: ', loop_count)
            vbopnd_diff_all_df = pd.concat([vbopnd_diff_all_df,
                                            vbopnd_diff_df])
    print('Aantal verzamelde verschillen t/m loopnummer', loop_count, ':',
          vbopnd_diff_all_df.shape[0])
    # print('\t---Saving the vbopnd that are linked...')
    previous_vbopnd_df = vbopnd_df[['vboid', 'pndid', 'prio', 'pndstatus',
                                    'bouwjaar']]
    loop_count += 1

outputfile = outputdir + 'vbopnd_changed.csv'
vbopnd_diff_all_df.to_csv(outputfile)
'''
