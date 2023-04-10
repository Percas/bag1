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
import bag01_unzip
import multiprocessing
from multiprocessing import Process


import baglib                # general functions user defined
from config import OMGEVING, DIR00, DIR01, DIR02, FUTURE_DATE, status_dict, SHORT, GEBRUIKSDOEL_DICT, BAG_OBJECTEN, COLS_DICT


# --------------------------------------------------------------------------
# ############### Define functions #################################
# --------------------------------------------------------------------------

# The main function:

    
def bag_xml(koppelvlak1=DIR01,
            koppelvlak2=DIR02,
            current_month='testdata02',
            file_ext='parquet',
            loglevel=20):
    ''' Read xml files in koppelvlak1/current_month and writh them in parquet or
    csv format in koppelvlak 2. Use simple multiprocessing by doing this in
    parallel for the 7 bag objects.'''

    tic = time.perf_counter()
    ll = loglevel
    batch_size = 200
    aantal_cpu = multiprocessing.cpu_count()
    
    baglib.printkop(ll+40, 'Start bag_xml')
    baglib.aprint(ll+30, 'Extractmaand (verslagmaand + 1):', current_month,
                  '\nloglevel:', ll, '\tbatch_size:', batch_size,
                  '\taantal cpu:', aantal_cpu)

    baglib.make_dir(DIR02) # in case it doesn't exist yet
    
      
    # ############################################################
    
    # --------------------------------------------------------------------------
    baglib.aprint(ll+30, '\n----- Loop over de bagobjecten')
    # --------------------------------------------------------------------------
 
    procs = []

    for bagobject in BAG_OBJECTEN:
        
        
        arg_lst = [DIR01, # koppelvlak1
                   DIR02, # koppelvlak2
                   current_month,
                   file_ext,
                   loglevel,
                   bagobject,
                   batch_size]


        # code for processing them sequentially:
        # process_xml_van_bagobject(arg_lst)

        proc = Process(target=process_xml_van_bagobject, args=(arg_lst,))
        procs.append(proc)
        proc.start()
        
    for proc in procs:
        proc.join()
        
    baglib.aprint(ll+10, '\n---- Loop gereed over bagtypen\n')
        
    toc = time.perf_counter()
    baglib.aprint(ll+40, '\n*** Einde bag_xml2csv in', (toc - tic)/60, 'min ***\n')
        

def process_xml_van_bagobject(arg_lst):
    
    # ########## general part for all 7 bagobjects
    koppelvlak1 = arg_lst[0]
    koppelvlak2 = arg_lst[1]
    current_month = arg_lst[2]
    file_ext = arg_lst[3]
    loglevel = arg_lst[4]
    bagobject = arg_lst[5]
    batch_size = arg_lst[6]
    ll=loglevel
    
    baglib.aprint(ll+30, '\n\t------ Bagobject', bagobject,
                  '('+SHORT[bagobject]+') ------')


    # mappen en bestanden klaarzetten
    # baseheader = ['id', 'status', 'vkid', 'vkbg', 'vkeg']
    # namespace stuff we have to deal with
    NS = {'Objecten': "www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601",
          'gml': "http://www.opengis.net/gml/3.2",
          'Historie': "www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601",
          'Objecten-ref':
              "www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601",
          }   
    inputdir = os.path.join(koppelvlak1, current_month, bagobject)
    if not os.path.exists(inputdir):
        baglib.aprint(ll, '\t', inputdir, 'koppelvlak 1 niet gevonden. Genereer vanuit zip:')                        
        bag01_unzip.bag_unzip(current_month=current_month,
                              koppelvlak0=DIR00,
                              koppelvlak1=DIR01,
                              loglevel=ll)

    baglib.aprint(ll, '\tZoek XML bestanden in', inputdir)
    bag_files = os.listdir(inputdir)
    
    outp_lst_d = []  # list of dict containing output records
    outputfile = os.path.join(DIR02, bagobject)
    outputfile_ext = outputfile + '.' + file_ext
    
    if os.path.isfile(outputfile_ext):
        baglib.aprint(ll+10, '\tRemoving', outputfile_ext)
        os.remove(outputfile_ext)

    input_bagobject_count = 0
    output_bagobject_count = 0
    file_count = 0
    batch_count = 0
    report_count = 0
    dubbel_gebruiksdoel_count = 0
    vsl = {'vbo', 'sta', 'lig'}

    baglib.aprint(ll+20, '\tAantal input xml bestanden:', str(len(bag_files)))
    if ll+10 >= 40:
        print('\t', end='')
    

    for inputfile in bag_files:
        bagtree = ET.parse(os.path.join(inputdir, inputfile))
        root = bagtree.getroot()
        tag = '{' + NS['Objecten'] + '}' + SHORT[bagobject]
        input_bagobject_filecount = 0
        output_bagobject_filecount = 0
        file_count += 1
        batch_count += 1
        report_count += 1
        baglib.aprint(ll-10, '\tInputfile:', inputfile)

        # ######### Loop over bagobjects in one bag file       #####

        for level0 in root.iter(tag):   # level0 is the bagobject-tree
            input_bagobject_filecount += 1
            id1 = baglib.assigniffound(level0, ['Objecten:identificatie'], NS)
            status = status_dict[
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
                    baglib.aprint(ll-20, 'Meerdere gebruiksdoelen. vboid:', id1, 'met',
                                  gebruiksdoelen)
                    dubbel_gebruiksdoel_count += 1


                for gebruiksdoel in gebruiksdoelen:
                    # baglib.aprint(ll+40, 'DEBUG: gebruiksdoelen', gebruiksdoelen)
                    try:
                        output_record[GEBRUIKSDOEL_DICT[gebruiksdoel.text]] = True
                    except KeyError:
                        baglib.aprint(ll+40, 'gebruiksdoel', gebruiksdoel.text, 'niet gevonden')
                               
                
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
        if ll+20 >= 40:
            print(".", end='')


        if report_count == 100:
            baglib.aprint(ll, file_count, 'of', len(bag_files))
            report_count = 0
        
        if batch_count == batch_size:
            baglib.aprint(ll+20, 'tussendoor bewaren:', file_count, 'van', len(bag_files))


            bob_df = pd.DataFrame.from_dict(outp_lst_d)
            bob_df = bob_df.reindex(columns=COLS_DICT[bagobject])
            append = os.path.exists(outputfile_ext)
            baglib.aprint(ll, '\tappend mode:', append)

            baglib.save_df2file(df=bob_df[COLS_DICT[bagobject]],
                                outputfile=outputfile,
                                file_ext=file_ext, includeindex=False,
                                append=append, loglevel=ll)

            # dict2df2file(output_dict=outp_lst_d, outputfile=outputfile,
            #              cols=cols_dict[bagobject], file_ext=file_ext,
            #              loglevel=ll)
            batch_count = 0
            outp_lst_d = []
            if ll+20 >= 40:
                print('\t', end='')

            
    
    if outp_lst_d != []:
        baglib.aprint(ll+20, 'laatste stukje bewaren:', file_count, 'van', len(bag_files))
        bob_df = pd.DataFrame.from_dict(outp_lst_d)
        bob_df = bob_df.reindex(columns=COLS_DICT[bagobject])
        append = os.path.exists(outputfile_ext)
        baglib.aprint(ll, '\tappend mode:', append)
        baglib.save_df2file(df=bob_df[COLS_DICT[bagobject]],
                            outputfile=outputfile,
                            file_ext=file_ext, includeindex=False,
                            append=append, loglevel=ll)
        
        
        # dict2df2file(output_dict=outp_lst_d, outputfile=outputfile, 
        #              cols=cols_dict[bagobject], file_ext=file_ext, loglevel=ll)
 
    baglib.aprint(ll+10, '\n\tOutputfile:', bagobject + '.' + file_ext,
                  '\n\t\trecords in:\t' +
             str(input_bagobject_count) + '\n\t\trecords aangemaakt: ' +
             str(output_bagobject_count) + ' ' + bagobject)

    if bagobject == 'vbo':
        if output_bagobject_count != 0:
            baglib.aprint(ll+10, '\tGedeelte vbo met dubbel gebruiksdoel:',
                  dubbel_gebruiksdoel_count / output_bagobject_count)

    baglib.aprint(ll+10, '\t---- XML bestanden verwerkt voor bagobject',
                  bagobject, '\n')










def middelpunt(float_lst):
    '''Return tuple that is the middle of a polygon float_lst. float_list
    contains a list coordinates [x1, y1, z1, x2, y2, z2, ...xn, yn, zn]'''

    # baglib.aprint(ll+40, 'DEBUG1: entering middelpunt')

    _x = [float(i) for i in float_lst[0::3]]
    _y = [float(j) for j in float_lst[1::3]]
    
    # baglib.aprint(ll+40, 'DEBUG2: _x _y: ', _x, _y)
    return (sum(_x)/len(_x), sum(_y)/len(_y))

'''    
def dict2df2file(output_dict={}, outputfile='', cols=[], file_ext='parquet', loglevel=20):
    Convert dict1 to df. Write df to outputfile. Append if outputfile exists.
    Use cols1 to define the order of the columns

    # selecting integer valued columns
    # df.select_dtypes(include=['int64'])

    
    # baglib.aprint(ll+40, 'Writing batch to file')
    # baglib.aprint(ll+40, '\tConverting dict to dataframe:')
    _ll = loglevel
    _df = pd.DataFrame.from_dict(output_dict)
    # baglib.aprint(loglevel-10, _df.info())
    # baglib.aprint(ll+40, '\tChanging column order:')
    # _cols = list(_df.columns.values)
    _df = _df.reindex(columns=cols)
    # this gives some issues with parquet
    # _df.fillna(False, inplace=True)
    
    # if not os.path.isfile(outputfile):
    if not exists(outputfile + '.' + file_ext):
        _append = False
        baglib.aprint(_ll+40, '\tOutputfile', outputfile, 'bestaat nog niet. Aanmaken')
    else:
        _append = True
        baglib.aprint(_ll+40, '\tAppending to', outputfile)

    baglib.save_df2file(df=_df, outputfile=outputfile, file_ext=file_ext,
                        includeindex=False, append=_append, loglevel=_ll)    
'''
# --------------------------------------------------------------------------
# ################ Main program ###########################
# --------------------------------------------------------------------------

if __name__ == '__main__':
    
    ll = 10
    file_ext = 'parquet'
    # file_ext = 'csv'    

    baglib.printkop(ll+40, OMGEVING)  

    current_month = baglib.get_arg1(sys.argv, DIR00)

    bag_xml(koppelvlak1=DIR01,
            koppelvlak2=DIR02,
            current_month=current_month,
            file_ext=file_ext,
            loglevel=ll)


