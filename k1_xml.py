#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Created on Sat Mar  5 12:40:26 2022
@author: anton
Purpose: Verwerk BAG XML bestanden naar parquet of CSV met multiprocessing
version 1.3

Notes
------
Een verblijfsobject kan een polygon zijn (lijkt op pand, maar is het niet)

---------------
###### Typical XML layout ##########################################

<Objecten:Verblijfsobject>
  <Objecten:heeftAlsHoofdadres>
    <Objecten-ref:NummeraanduidingRef domein="NL.IMBAG.Nummeraanduiding">0501200002014848</Objectenref:NummeraanduidingRef>
  </Objecten:heeftAlsHoofdadres>
  <Objecten:voorkomen>
    <Historie:Voorkomen>
      <Historie:voorkomenidentificatie>1</Historie:voorkomenidentificatie>
      <Historie:beginGeldigheid>2015-11-06</Historie:beginGeldigheid>
      <Historie:eindGeldigheid>2017-07-05</Historie:eindGeldigheid>
      <Historie:tijdstipRegistratie>2015-11-06T16:02:17.000</Historie:tijdstipRegistratie>
      <Historie:eindRegistratie>2017-07-05T08:58:28.000</Historie:eindRegistratie>
      <Historie:BeschikbaarLV>
        <Historie:tijdstipRegistratieLV>2015-11-06T16:30:03.891</Historie:tijdstipRegistratieLV>
        <Historie:tijdstipEindRegistratieLV>2017-07-05T09:01:12.799</Historie:tijdstipEindRegistratieLV>
      </Historie:BeschikbaarLV>
    </Historie:Voorkomen>
  </Objecten:voorkomen>
  <Objecten:identificatie domein="NL.IMBAG.Verblijfsobject">0501010002007128</Objecten:identificatie>
  <Objecten:geometrie>
    <Objecten:punt>
      <gml:Point srsName="urn:ogc:def:crs:EPSG::28992" srsDimension="3">
        <gml:pos>69920.523 435585.372 0.0</gml:pos>
      </gml:Point>
    </Objecten:punt>
  </Objecten:geometrie>
  <Objecten:gebruiksdoel>woonfunctie</Objecten:gebruiksdoel>
  <Objecten:oppervlakte>72</Objecten:oppervlakte>
  <Objecten:status>Verblijfsobject gevormd</Objecten:status>
  <Objecten:geconstateerd>N</Objecten:geconstateerd>
  <Objecten:documentdatum>2015-11-06</Objecten:documentdatum>
  <Objecten:documentnummer>BRI-CONS-259</Objecten:documentnummer>
  <Objecten:maaktDeelUitVan>
    <Objecten-ref:PandRef domein="NL.IMBAG.Pand">0501100001999616</Objecten-ref:PandRef>
  </Objecten:maaktDeelUitVan>
</Objecten:Verblijfsobject>

######### Properties of input XML that are used in this code #########

##### general part for vbo,sta,lig,pand,num,opr,wpl
Objecten:voorkomen
	Historie:Voorkomen
    Historie:voorkomenidentificatie
    Historie:beginGeldigheid
    Historie:eindGeldigheid [optional]
Objecten:identificatie
Objecten:status

##### vbo specific
Objecten:gebruiksdoel
Objecten:oppervlakte
Objecten:maaktDeelUitVan
    Objecten-ref:PandRef [1:n]

##### pnd specific
Objecten:oorspronkelijkBouwjaar

##### num specific
Objecten:huisnummer
Objecten:postcode
Objecten:typeAdresseerbaarObject
Objecten:ligtAan
    Objecten-ref:OpenbareRuimteRef [1:n]

##### opr specific
Objecten:naam
Objecten:type
Objecten:ligtIn
    Objecten-ref:WoonplaatsRef [1:n]

# #### vsl specific
Objecten:heeftAlsHoofdadres
    Objecten-ref:NummeraanduidingRef [1:n]
"""

# --------------------------------------------------------------------------
# ################ Import libraries ###############################
# --------------------------------------------------------------------------
import xml.etree.ElementTree as ET
# import lxml.etree as ET
import os
# from os.path import exists
import sys
import pandas as pd
import time
import logging

import baglib                # general functions user defined
from config import KOPPELVLAK0, KOPPELVLAK1, KOPPELVLAK2, NS, SHORT,\
    STATUS_DICT, FUTURE_DATE, GEBRUIKSDOEL_DICT, COLS_DICT,\
    BAG_OBJECTEN, LOGFILE, BATCH_SIZE, FILE_EXT, BAG_OBJECTEN
from k0_unzip import k0_unzip

# from k2_fixvk import k2_fixvk
# --------------------------------------------------------------------------
# ############### Define functions #################################
# --------------------------------------------------------------------------

# The main function:

def k1_xml(bagobject, maand, logit):
    '''Zet om: 
        de xml bestanden van bagobject van kadaster in koppelvlak k1 van 
        extractmaand maand 
    naar:
        parquet (of csv, zie FILE_EXT) formaat in koppelvlak k2'''
        
    tic = time.perf_counter()
    logit.info(f'start functie k1_xml met {bagobject} en {maand}')


    # input
    dir_k1_maand_bagobject = os.path.join(KOPPELVLAK1, maand, bagobject)
    # if not os.path.exists(dir_k1_maand_bagobject):
    baglib.make_dirs(dir_k1_maand_bagobject, logit)
    xml_files = os.listdir(dir_k1_maand_bagobject)

    if len(xml_files) == 0:
        logit.info(f'geen xml bestanden in {dir_k1_maand_bagobject}. Probeer ze te unzippen')
        k0_unzip(bagobject, maand, logit)
        xml_files = os.listdir(dir_k1_maand_bagobject)
    # logit.debug(f'zoek XML bestanden in {dir_k1_maand_bagobject}')


    # output
    dir_k2_maand = os.path.join(KOPPELVLAK2, maand)
    file_k2_maand_bagobject = os.path.join(dir_k2_maand, bagobject)
    file_k2_maand_bagobject_ext = file_k2_maand_bagobject + '.' + FILE_EXT
    baglib.make_dirs(dir_k2_maand, logit) # only make it if it doesn't exist yet
    # remove if outputfile is already present

    if os.path.isfile(file_k2_maand_bagobject_ext):
        logit.debug(f'removing {file_k2_maand_bagobject_ext}')
        os.remove(file_k2_maand_bagobject_ext)


    # de wplgem koppeling is niet van het kadaster zelf en heeft een afwijkend
    # formaat. We nemen het soms mee als bagobject en soms niet.
    if bagobject == 'wplgem':
        k1_xmlgem(bagobject, inputdir=dir_k1_maand_bagobject, xml_files=xml_files,
              outputfile=file_k2_maand_bagobject, logit=logit)
    else:
        k1_xmlbag(bagobject, inputdir=dir_k1_maand_bagobject, xml_files=xml_files,
                  outputfile=file_k2_maand_bagobject, logit=logit)
        
    toc = time.perf_counter()
    logit.info(f'einde k1_xml {bagobject}, {maand} in {(toc - tic)/60} min')

    
# def k1_xmlbag(bagobject, inputdir, outputfile, logit):
def k1_xmlbag(bagobject, inputdir, xml_files, outputfile, logit):
    '''Zet xml bestanden van alle bagobjecten (exclusief wplgem) om van
    KV1 naar KV2.'''
    
    # tellertjes
    input_bagobject_count = 0
    output_bagobject_count = 0
    file_count = 0
    batch_count = 0
    report_count = 0
    dubbel_gebruiksdoel_count = 0
    
    vsl = {'vbo', 'sta', 'lig'}

    # xml_files = os.listdir(inputdir)
    outp_lst_d = []  # list of dict containing output records
    file_k2_maand_bagobject = outputfile
    file_k2_maand_bagobject_ext = file_k2_maand_bagobject + '.' + FILE_EXT

    logit.debug(f'k1_xmlbag: aantal input xml bestanden van {bagobject}: {len(xml_files)}')
    
    for xml_file in xml_files:
        bagtree = ET.parse(os.path.join(inputdir, xml_file))
        root = bagtree.getroot()
        tag = '{' + NS['Objecten'] + '}' + SHORT[bagobject]
        input_bagobject_filecount = 0
        output_bagobject_filecount = 0
        file_count += 1
        batch_count += 1
        report_count += 1

        # ######### Loop over bagobjects in one bag file       #####

        for level0 in root.iter(tag):   # level0 is the bagobject-tree
            input_bagobject_filecount += 1
            id1 = baglib.assigniffound(level0, ['Objecten:identificatie'], NS)
            status = STATUS_DICT[
                baglib.assigniffound(level0, ['Objecten:status'], NS)]

            vkid = baglib.assigniffound(level0, ['Objecten:voorkomen',
                                                 'Historie:Voorkomen',
                                                 'Historie:voorkomenidentificatie'],
                                        NS)

            date_str = baglib.assigniffound(level0, ['Objecten:voorkomen',
                                                     'Historie:Voorkomen',
                                                     'Historie:beginGeldigheid'],
                                            NS)
            vkbg = baglib.date2int(date_str)
            date_str = baglib.assigniffound(level0, ['Objecten:voorkomen',
                                                     'Historie:Voorkomen',
                                                     'Historie:eindGeldigheid'],
                                            NS,
                                            str(FUTURE_DATE))
            vkeg = baglib.date2int(date_str)

            output_record = {bagobject + 'id':     id1,
                             bagobject + 'status':  status,
                             bagobject + 'vkid':    vkid,
                             bagobject + 'vkbg':    vkbg,
                             bagobject + 'vkeg':    vkeg}

            # ######### vsl: VBO, Staplaats, Ligplaats              ###########

            if bagobject in vsl:
                output_record['numid'] = \
                    baglib.assigniffound(level0,
                                         ['Objecten:heeftAlsHoofdadres',
                                          'Objecten-ref:NummeraanduidingRef'],
                                         NS)

            # specific for sta, lig
            
            if bagobject in {'sta', 'lig'}:
                gml_string = baglib.assigniffound(level0, ['Objecten:geometrie',
                                                           'gml:Polygon',
                                                           'gml:exterior',
                                                           'gml:LinearRing',
                                                           'gml:posList'],
                                                  NS)
                if gml_string is not None:
                    xyz = gml_string.split()
                    (output_record[bagobject + 'gmlx'],
                     output_record[bagobject + 'gmly']) = middelpunt(xyz)

                output_record['docnr'] = \
                    baglib.assigniffound(level0, ['Objecten:documentnummer'],
                                         NS)

                output_record['docdd'] = baglib.date2int(\
                    baglib.assigniffound(level0, ['Objecten:documentdatum'],
                                         NS))

                outp_lst_d.append(output_record.copy())
                output_bagobject_filecount += 1


            # specific for VBO

            if bagobject == 'vbo':

                gml_string = baglib.assigniffound(level0, ['Objecten:geometrie',
                                                           'Objecten:punt',
                                                           'gml:Point',
                                                           'gml:pos'],
                                           NS)
                if gml_string is not None:
                    xyz = gml_string.split()
                    output_record[bagobject + 'gmlx'] = float(xyz[0])
                    output_record[bagobject + 'gmly'] = float(xyz[1])
                else: # BUGFIX: ook een VBO kan een Polygon zijn
                    gml_string = baglib.assigniffound(level0, ['Objecten:geometrie',
                                                               'Objecten:vlak',
                                                               'gml:Polygon',
                                                               'gml:exterior',
                                                               'gml:LinearRing',
                                                               'gml:posList'],
                                                      NS)
                    if gml_string is not None:
                        xyz = gml_string.split()
                        (output_record[bagobject + 'gmlx'],
                         output_record[bagobject + 'gmly']) = middelpunt(xyz)
                    
                
                gebruiksdoelen = level0.findall('Objecten:gebruiksdoel', NS)
                if len(gebruiksdoelen) > 1:
                    dubbel_gebruiksdoel_count += 1


                for gebruiksdoel in gebruiksdoelen:
                    # baglib.aprint(ll+40, 'DEBUG: gebruiksdoelen', gebruiksdoelen)
                    try:
                        output_record[GEBRUIKSDOEL_DICT[gebruiksdoel.text]] = True
                    except KeyError:
                        print('gebruiksdoel', gebruiksdoel.text, 'niet gevonden')
                               
                
                output_record['oppervlakte'] = \
                    baglib.assigniffound(level0,
                                         ['Objecten:oppervlakte'],
                                         NS)
                level1 = level0.find('Objecten:maaktDeelUitVan', NS)


                if level1 is not None:
                    level2 = level1.findall('Objecten-ref:PandRef', NS)
                    if level2 is not None:
                        for j in level2:
                            output_record['pndid'] = j.text
                            outp_lst_d.append(output_record.copy())
                            output_bagobject_filecount += 1
                    else:
                        outp_lst_d.append(output_record.copy())
                        output_bagobject_filecount += 1
                else:
                    outp_lst_d.append(output_record.copy())
                    output_bagobject_filecount += 1


            # ######### pnd: Pand  #######

            if bagobject == 'pnd':

                output_record['bouwjaar'] = \
                    baglib.assigniffound(level0, ['Objecten:oorspronkelijkBouwjaar'],
                                         NS)

                output_record['docnr'] = \
                    baglib.assigniffound(level0, ['Objecten:documentnummer'],
                                         NS)

                output_record['docdd'] = baglib.date2int(\
                    baglib.assigniffound(level0, ['Objecten:documentdatum'],
                                         NS))

                gml_string = baglib.assigniffound(level0, ['Objecten:geometrie',
                                                           'gml:Polygon',
                                                           'gml:exterior',
                                                           'gml:LinearRing',
                                                           'gml:posList'],
                                                  NS)
                if gml_string is not None:
                    xyz = gml_string.split()
                    (output_record[bagobject + 'gmlx'],
                     output_record[bagobject + 'gmly']) = middelpunt(xyz)

                outp_lst_d.append(output_record.copy())
                output_bagobject_filecount += 1


            # ######### num: Nummeraanduiding                           #######
            if bagobject == 'num':
                output_record['huisnr'] = \
                    baglib.assigniffound(level0, ['Objecten:huisnummer'], NS)
                output_record['postcode'] = \
                    baglib.assigniffound(level0, ['Objecten:postcode'], NS)
                output_record['typeao'] = \
                    baglib.assigniffound(level0, ['Objecten:typeAdresseerbaarObject'],
                                         NS)
                level1 = level0.find('Objecten:ligtAan', NS)
                if level1 is not None:
                    level2 = level1.findall('Objecten-ref:OpenbareRuimteRef',
                                            NS)
                    if level2 is not None:
                        for j in level2:
                            output_record['oprid'] = j.text
                            outp_lst_d.append(output_record.copy())
                            output_bagobject_filecount += 1
                    else:
                        outp_lst_d.append(output_record.copy())
                        output_bagobject_filecount += 1
                else:
                    outp_lst_d.append(output_record.copy())
                    output_bagobject_filecount += 1

            # #################################################################
            # ######### opr: Openbare Ruimte -                          #######
            # #################################################################
            if bagobject == 'opr':
                output_record['oprnaam'] = \
                    baglib.assigniffound(level0, ['Objecten:naam'], NS)
                output_record['oprtype'] = \
                    baglib.assigniffound(level0, ['Objecten:type'], NS)
                level1 = level0.find('Objecten:ligtIn', NS)
                if level1 is not None:
                    for j in level1:
                        output_record['wplid'] = j.text
                        outp_lst_d.append(output_record.copy())
                        output_bagobject_filecount += 1
                else:
                    outp_lst_d.append(output_record.copy())
                    output_bagobject_filecount += 1
            # #################################################################
            # ######### wpl: Woonplaats -                               #######
            # #################################################################
            if bagobject == 'wpl':
                output_record['wplnaam'] = \
                    baglib.assigniffound(level0, ['Objecten:naam'], NS)
                outp_lst_d.append(output_record.copy())
                output_bagobject_filecount += 1

        input_bagobject_count += input_bagobject_filecount
        output_bagobject_count += output_bagobject_filecount

        if batch_count == BATCH_SIZE:
            logit.debug(f'tussendoor bewaren: {file_count} van {len(xml_files)}')
            bob_df = pd.DataFrame.from_dict(outp_lst_d)
            bob_df = bob_df.reindex(columns=COLS_DICT[bagobject])
            append = os.path.exists(file_k2_maand_bagobject_ext)
            baglib.save_df2file(df=bob_df[COLS_DICT[bagobject]],
                                outputfile=file_k2_maand_bagobject,
                                file_ext='parquet', includeindex=False,
                                append=append, logit=logit)

            # dict2df2file(output_dict=outp_lst_d, outputfile=outputfile,
            #              cols=cols_dict[bagobject], file_ext=file_ext,
            #              loglevel=ll)
            batch_count = 0
            outp_lst_d = []
            
    
    if outp_lst_d != []:
        bob_df = pd.DataFrame.from_dict(outp_lst_d)
        bob_df = bob_df.reindex(columns=COLS_DICT[bagobject])
        append = os.path.exists(file_k2_maand_bagobject_ext)
        baglib.save_df2file(df=bob_df[COLS_DICT[bagobject]],
                            outputfile=file_k2_maand_bagobject,
                            file_ext=FILE_EXT, includeindex=False,
                            append=append, logit=logit)
        
        
 
    logit.debug(f'outputfile: {bagobject}.{FILE_EXT}; records in: {input_bagobject_count};\
               records aangemaakt: {output_bagobject_count} {bagobject}')

    if bagobject == 'vbo':
        if output_bagobject_count != 0:
            logit.debug(f'gedeelte vbo met dubbel gebruiksdoel: {dubbel_gebruiksdoel_count / output_bagobject_count}')

    '''
    baglib.aprint(ll+40, '\n*** bag_xml: hernoem bestand wpl naar wpl_naam\n')
    wpl_naam = os.path.join(KOPPELVLAK2, maand, 'wpl_naam.parquet')
    if os.path.exists(wpl_naam):
        os.remove(wpl_naam)
    os.rename(os.path.join(KOPPELVLAK2, maand, 'wpl.parquet'),
              os.path.join(KOPPELVLAK2, maand, 'wpl_naam.parquet'))


    '''





def k1_xmlgem(bagobject, inputdir, xml_files, outputfile, logit):
    '''Zet het xml bestand voor de woonplaats-gemeente koppeling (dat in koppelvlak k1 staat)
    om in een parquet/csv bestand dat in koppelvlak k2 wordt gezet met de naam wplgem.parquet.
    Input: xml bestanden in wplgem
    Output: wplgem.parquet
    '''
 
    # namespace stuff we have to deal with
    ns = {'gwr-bestand': "www.kadaster.nl/schemas/lvbag/gem-wpl-rel/gwr-deelbestand-lvc/v20200601",
          'selecties-extract': "http://www.kadaster.nl/schemas/lvbag/extract-selecties/v20200601",
          'bagtypes': "www.kadaster.nl/schemas/lvbag/gem-wpl-rel/bag-types/v20200601",
          'gwr-product': "www.kadaster.nl/schemas/lvbag/gem-wpl-rel/gwr-producten-lvc/v20200601",
          'DatatypenNEN3610': "www.kadaster.nl/schemas/lvbag/imbag/datatypennen3610/v20200601"}

    # ######### works only for wplgem                ###########

    # xml_files = os.listdir(inputdir)
    
    logit.debug(f'gemeente-woonplaats map bevat {len(xml_files)} bestand')
    output_dict = []           # list of dict containing output records
    input_bagobject_count = 0
    output_bagobject_count = 0
    file_count = 0

    # ######### Loop over files in a directory with same bag objects #####
    for inputfile in xml_files:
        bagtree = ET.parse(os.path.join(inputdir, inputfile))
        root = bagtree.getroot()
        tag = '{' + ns['gwr-product'] + '}' + 'GemeenteWoonplaatsRelatie'
        input_bagobject_filecount = 0
        output_bagobject_filecount = 0
        file_count += 1
        logit.debug(f'inputfile: {inputfile}')

        # ######### Loop over bagobjects in one bag file       #####
        for level0 in root.iter(tag):   # level0 is the bagobject-tree
            input_bagobject_filecount += 1
            wpl = baglib.assigniffound(level0, ['gwr-product:gerelateerdeWoonplaats',
                                                'gwr-product:identificatie'], ns)
            gem = baglib.assigniffound(level0, ['gwr-product:gerelateerdeGemeente',
                                                'gwr-product:identificatie'], ns)
            status = STATUS_DICT[
                baglib.assigniffound(level0, ['gwr-product:status'], ns)]
    
            date_str = baglib.assigniffound(level0,
                                            ['gwr-product:tijdvakgeldigheid',
                                             'bagtypes:begindatumTijdvakGeldigheid'],
                                            ns)
            vkbg = baglib.date2int(date_str)
            date_str = baglib.assigniffound(level0, ['gwr-product:tijdvakgeldigheid',
                                                     'bagtypes:einddatumTijdvakGeldigheid'],
                                            ns, str(FUTURE_DATE))
            vkeg = baglib.date2int(date_str)
    
            output_record = {'wplid':   wpl,
                             'gemid':   gem,
                             'wplstatus':  status,
                             'wplvkbg':    vkbg,
                             'wplvkeg':    vkeg}
    
            output_dict.append(output_record.copy())
            output_bagobject_filecount += 1
    
        input_bagobject_count += input_bagobject_filecount
        output_bagobject_count += output_bagobject_filecount
    df = pd.DataFrame.from_dict(output_dict)
    df.index.name = 'idx'

    
    logit.debug('voeg wplvkid toe')
    logit.debug('deze actie hoort thuis tussen koppelvlak 2 en 3, maar wordt hier uitgevoerd zodat ook dit bestand een vkid heeft, net als alle andere.')
    # df = df.sort_values(['wplid', 'wplvkbg'])
    df = baglib.make_counter(df=df, dfid='wplid', dfvkbg='wplvkbg',
                             old_counter='',
                             new_counter='wplvkid', logit=logit)
    
    logit.debug(f'outputfile: wplgem.parquet; records in: {input_bagobject_count}; records aangemaakt: {output_bagobject_count}')
    
    baglib.save_df2file(df=df, outputfile=outputfile, file_ext=FILE_EXT,
                        append=False, includeindex=False, logit=logit)







def middelpunt(float_lst):
    '''Return tuple that is the middle of a polygon float_lst. float_list
    contains a list coordinates [x1, y1, z1, x2, y2, z2, ...xn, yn, zn]'''

    # baglib.aprint(ll+40, 'DEBUG1: entering middelpunt')

    _x = [float(i) for i in float_lst[0::3]]
    _y = [float(j) for j in float_lst[1::3]]
    
    # baglib.aprint(ll+40, 'DEBUG2: _x _y: ', _x, _y)
    return (sum(_x)/len(_x), sum(_y)/len(_y))
# --------------------------------------------------------------------------




# --------------------------------------------------------------------------
# ################ Main program ###########################
# --------------------------------------------------------------------------

if __name__ == '__main__':
    
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOGFILE),
            logging.StreamHandler()])    
    logit = logging.getLogger()
    logit.debug('Start k1_xml lokaal')

    maand_lst = baglib.get_args(sys.argv, KOPPELVLAK0)

    for maand in maand_lst:
        for bagobject in BAG_OBJECTEN + ['wplgem']:
            k1_xml(bagobject, maand, logit)
    
    


