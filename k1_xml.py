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


import baglib                # general functions user defined
from config import OMGEVING, KOPPELVLAK0, KOPPELVLAK1, KOPPELVLAK2, NS, SHORT, status_dict, FUTURE_DATE, GEBRUIKSDOEL_DICT, COLS_DICT, BASISDIR, BAG_OBJECTEN

# --------------------------------------------------------------------------
# ############### Define functions #################################
# --------------------------------------------------------------------------

# The main function:

def k1_xml(maand, bagobject):
    '''Zet xml bestanden van bagobject om in parquet'''
    print('Zet xml bestanden van', bagobject, 'van maand', maand, 'om in parquet')    

    # input
    ll=40
    dir_k1_maand_bagobject = os.path.join(KOPPELVLAK1, maand, bagobject)
    #output
    dir_k2_maand = os.path.join(KOPPELVLAK2, maand)
    file_k2_maand_bagobject = os.path.join(dir_k2_maand, bagobject)
    file_k2_maand_bagobject_ext = file_k2_maand_bagobject + '.parquet'
    
    #params
    batch_size = 400

    baglib.make_dir(dir_k2_maand) # only make it if it doesn't exist yet
     
    baglib.aprint(ll, '\tZoek XML bestanden in', dir_k1_maand_bagobject)
    xml_files = os.listdir(dir_k1_maand_bagobject)
    
    outp_lst_d = []  # list of dict containing output records
    
    # remove if outputfile is already present
    if os.path.isfile(file_k2_maand_bagobject_ext):
        baglib.aprint(ll+10, '\tRemoving', file_k2_maand_bagobject_ext)
        os.remove(file_k2_maand_bagobject_ext)

    input_bagobject_count = 0
    output_bagobject_count = 0
    file_count = 0
    batch_count = 0
    report_count = 0
    dubbel_gebruiksdoel_count = 0
    vsl = {'vbo', 'sta', 'lig'}

    baglib.aprint(ll+20, '\tAantal input xml bestanden:', str(len(xml_files)))

    for xml_file in xml_files:
        bagtree = ET.parse(os.path.join(dir_k1_maand_bagobject, xml_file))
        root = bagtree.getroot()
        tag = '{' + NS['Objecten'] + '}' + SHORT[bagobject]
        input_bagobject_filecount = 0
        output_bagobject_filecount = 0
        file_count += 1
        batch_count += 1
        report_count += 1
        baglib.aprint(ll-10, '\txml_file:', xml_file)

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
            baglib.aprint(ll, file_count, 'of', len(xml_files))
            report_count = 0
        
        if batch_count == batch_size:
            baglib.aprint(ll+20, 'tussendoor bewaren:', file_count, 'van', len(xml_files))


            bob_df = pd.DataFrame.from_dict(outp_lst_d)
            bob_df = bob_df.reindex(columns=COLS_DICT[bagobject])
            append = os.path.exists(file_k2_maand_bagobject_ext)
            baglib.aprint(ll, '\tappend mode:', append)

            baglib.save_df2file(df=bob_df[COLS_DICT[bagobject]],
                                outputfile=file_k2_maand_bagobject,
                                file_ext='parquet', includeindex=False,
                                append=append, loglevel=ll)

            # dict2df2file(output_dict=outp_lst_d, outputfile=outputfile,
            #              cols=cols_dict[bagobject], file_ext=file_ext,
            #              loglevel=ll)
            batch_count = 0
            outp_lst_d = []
            if ll+20 >= 40:
                print('\t', end='')

            
    
    if outp_lst_d != []:
        baglib.aprint(ll+20, 'laatste stukje bewaren:', file_count, 'van', len(xml_files))
        bob_df = pd.DataFrame.from_dict(outp_lst_d)
        bob_df = bob_df.reindex(columns=COLS_DICT[bagobject])
        append = os.path.exists(file_k2_maand_bagobject_ext)
        baglib.aprint(ll, '\tappend mode:', append)
        baglib.save_df2file(df=bob_df[COLS_DICT[bagobject]],
                            outputfile=file_k2_maand_bagobject,
                            file_ext='parquet', includeindex=False,
                            append=append, loglevel=ll)
        
        
        # dict2df2file(output_dict=outp_lst_d, outputfile=outputfile, 
        #              cols=cols_dict[bagobject], file_ext=file_ext, loglevel=ll)
 
    baglib.aprint(ll+10, '\n\tOutputfile:', bagobject + '.parquet',
                  '\n\t\trecords in:\t' +
             str(input_bagobject_count) + '\n\t\trecords aangemaakt: ' +
             str(output_bagobject_count) + ' ' + bagobject)

    if bagobject == 'vbo':
        if output_bagobject_count != 0:
            baglib.aprint(ll+10, '\tGedeelte vbo met dubbel gebruiksdoel:',
                  dubbel_gebruiksdoel_count / output_bagobject_count)

    '''
    baglib.aprint(ll+40, '\n*** bag_xml: hernoem bestand wpl naar wpl_naam\n')
    wpl_naam = os.path.join(KOPPELVLAK2, maand, 'wpl_naam.parquet')
    if os.path.exists(wpl_naam):
        os.remove(wpl_naam)
    os.rename(os.path.join(KOPPELVLAK2, maand, 'wpl.parquet'),
              os.path.join(KOPPELVLAK2, maand, 'wpl_naam.parquet'))


    '''
    
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

# --------------------------------------------------------------------------
# ################ Main program ###########################
# --------------------------------------------------------------------------

if __name__ == '__main__':
    
    ll = 40
    file_ext = 'parquet'
    # file_ext = 'csv'    

    baglib.printkop(ll+40, OMGEVING)  

    maand = baglib.get_arg1(sys.argv, KOPPELVLAK0)

    for bagobject in BAG_OBJECTEN:
        k1_xml(maand=maand,
               bagobject=bagobject)
    
    
    


