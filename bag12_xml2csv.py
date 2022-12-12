#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Created on Sat Mar  5 12:40:26 2022
@author: anton
Purpose: convert BAG XML to CSV file
version 1.2.1

Notes
------
Een verblijfsobject kan een polygon zijn (lijkt op pand, maar is het niet)

Version history
---------------
0.8.1: renamed adres in vbo.csv to numid
0.8.2: debugging below bug, read_csv('file', dtype=str)
    Fix bug: this output in vbo.csv:
    2874,0503010000038262,1,7,2020-01-21,EMPTYFIELD,0503200000023195,woonfunctie,205,0503100000028807
    2875,0503010000038262,1,7,2020-01-21,EMPTYFIELD,0503200000023195,woonfunctie,205,0503100000028807

    It should be:
    2874,0503010000038262,1,7,2020-01-21,EMPTYFIELD,0503200000023195,woonfunctie,205,0503100000025247
    2875,0503010000038262,1,7,2020-01-21,EMPTYFIELD,0503200000023195,woonfunctie,205,0503100000028807

    Solution: it took me a few hours to figure this out: if you append a dict to a list
    and change the dict after that (in a loop), then the appended dict in the list
    changes as well. To prevent this you need to make a copy of the dict: list.append(dict.copy())

0.9: add geo info using geopandas and shapeley - on hold
0.9.1: make proper date fields, properly handle empty or not found fields in assigniffound function
0.9.2: fixed a small bug
0.9.3: dates in nanoseconds don't fit because of range: make it int...
0.9.4: fixed some PEP warnings
0.9.5: gebruiksdoel added
0.9.7: adding counting
0.9.8: adding num-opr-wpl-gem dimension; 
0.9.9: rp1 kolomnamen uniek maken, dus vboid, vbostatus etc. Hierdoor moet ook
read_csv een dict meekrijgen om deze kolomnamen mee te kunnen geven
1.0 niets gewijzigd
1.1 fix gebruiksdoel: een vbo kan er meer dan eentje hebben. concateneer ze maar...
1.2 remove logging; clean up
1.2.1 added timing
1.3 added geometry for vbo; platslaan van gebruiksdoelen
1.4 bugfix: verblijfsobject kan een polygon zijn
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
import os
import sys
import pandas as pd
import time
import baglib                # general functions user defined
from config import LOCATION


# --------------------------------------------------------------------------
# ############### Define functions #################################
# --------------------------------------------------------------------------

# The main function:

    
def bag_xml2csv(current_month='202208',
                koppelvlak1='../data/01-xml/',
                koppelvlak2='../data/02-csv/',
                loglevel=True):

    tic = time.perf_counter()
    print('-------------------------------------------')
    print('------------- Start bag_xml2csv ---------- ')
    print('-------------------------------------------')

    inputdir = koppelvlak1 + current_month + '/'
    outputdir = koppelvlak2 + current_month + '/'
    baglib.make_dir(outputdir)
    
    print('Huidige maand (verslagmaand + 1):', current_month)
    '''    
    status_dict = {
        'Plaats ingetrokken':                           'splai',
        'Plaats aangewezen':                            'splaa',
        'Naamgeving ingetrokken':                       'snami',
        'Naamgeving aangewezen':                        'snama',
        'Naamgeving uitgegeven':                        'wnamu',
        'Verblijfsobject gevormd':                      'vgevo',
        'Verblijfsobject in gebruik':                   'vinge',
        'Verblijfsobject in gebruik (niet ingemeten)':  'vinni',
        'Verblijfsobject ingetrokken':                  'vintr',
        'Verbouwing verblijfsobject':                   'vverb',
        'Verblijfsobject ten onrechte opgevoerd':       'vonre',
        'Niet gerealiseerd verblijfsobject':            'vnrea',
        'Verblijfsobject buiten gebruik':               'vbuig',
        'Bouwvergunning verleend':                      'pbver',
        'Bouw gestart':                                 'pbstr',
        'Pand in gebruik':                              'pinge',
        'Pand in gebruik (niet ingemeten)':             'pinni',
        'Verbouwing pand':                              'pverb',
        'Pand gesloopt':                                'pslop',
        'Niet gerealiseerd pand':                       'pnrea',
        'Pand ten onrechte opgevoerd':                  'ponre',
        'Pand buiten gebruik':                          'pbuig',
        'Sloopvergunning verleend':                     'pslov',
        'Woonplaats aangewezen':                        'wwoan',
        'Woonplaats ingetrokken':                       'wwoin'}
    '''

    status_dict = {
        'Plaats ingetrokken':                           's2',
        'Plaats aangewezen':                            's1',
        'Naamgeving ingetrokken':                       's3',
        'Naamgeving aangewezen':                        's4',
        'Naamgeving uitgegeven':                        'w3',
        'Verblijfsobject gevormd':                      'v1',
        'Verblijfsobject in gebruik':                   'v4',
        'Verblijfsobject in gebruik (niet ingemeten)':  'v3',
        'Verblijfsobject ingetrokken':                  'v5',
        'Verbouwing verblijfsobject':                   'v8',
        'Verblijfsobject ten onrechte opgevoerd':       'v7',
        'Niet gerealiseerd verblijfsobject':            'v2',
        'Verblijfsobject buiten gebruik':               'v6',
        'Bouwvergunning verleend':                      'p1',
        'Bouw gestart':                                 'p2',
        'Pand in gebruik (niet ingemeten)':             'p3',
        'Pand in gebruik':                              'p4',
        'Verbouwing pand':                              'p5',
        'Pand gesloopt':                                'p6',
        'Niet gerealiseerd pand':                       'p7',
        'Pand ten onrechte opgevoerd':                  'p8',
        'Pand buiten gebruik':                          'p9',
        'Sloopvergunning verleend':                     'p0',
        'Woonplaats aangewezen':                        'w1',
        'Woonplaats ingetrokken':                       'w2'}

    ligtype_dict = {
            'Verblijfsobject':       0,
            'Standplaats':           1,
            'Ligplaats':             2,
            'Pand':                  3,
            'Nummeraanduiding':      4,
            'Openbareruimte':        5,
            'Woonplaats':            6
    }
    gebruiksdoel_dict = {
        'woonfunctie':              'woon',
        'overige gebruiksfunctie':  'over',
        'kantoorfunctie':           'kant',
        'gezondheidszorgfunctie':   'gezo',
        'bijeenkomstfunctie':       'bij1',
        'onderwijsfunctie':         'ondr',
        'winkelfunctie':            'wink',
        'sportfunctie':             'sprt',
        'logiesfunctie':            'logi',
        'industriefunctie':         'indu',
        'celfunctie':               'celf'
    }
    dubbel_gebruiksdoel_count = 0
    vsl = {'vbo', 'sta', 'lig'}
    # baseheader = ['id', 'status', 'vkid', 'vkbg', 'vkeg']
    # namespace stuff we have to deal with
    ns = {'Objecten': "www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601",
          'gml': "http://www.opengis.net/gml/3.2",
          'Historie': "www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601",
          'Objecten-ref':
              "www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601",
          }
    # shorthands to translate directory names to tags and so
    short = {'vbo': 'Verblijfsobject',
             'lig': 'Ligplaats',
             'sta': 'Standplaats',
             'pnd': 'Pand',
             'num': 'Nummeraanduiding',
             'opr': 'OpenbareRuimte',
             'wpl': 'Woonplaats'
             }
    futureday_str = '20321231'
    
    cols_dict = {
        'vbo': ['vboid','vbovkid', 'vbovkbg', 'vbovkeg', 'vbostatus', 'numid',
                'oppervlakte', 'pndid', 
                'woon', 'gezo', 'indu', 'over', 'ondr' ,'logi', 'kant', 'wink',
                'bij1', 'celf', 'sprt',
                'vbogmlx', 'vbogmly'],
        'pnd': ['pndid', 'pndvkid', 'pndvkbg', 'pndvkeg',
                'pndstatus', 'bouwjaar', 'docnr', 'docdd', 'pndgmlx', 'pndgmly'],
        'lig': ['ligid', 'ligvkid', 'ligvkbg', 'ligvkeg', 'ligstatus', 
                'numid', 'docnr', 'docdd', 'liggmlx', 'liggmly'],
        'num': ['numid', 'numvkid', 'numvkbg', 'numvkeg', 'numstatus',
                'huisnr', 'postcode', 'typeao', 'oprid'],
        'opr': ['oprid', 'oprvkid', 'oprvkbg', 'oprvkeg', 'oprstatus', 
                'oprnaam', 'oprtype', 'wplid'],
        'sta': ['staid', 'stavkid', 'stavkbg', 'stavkeg', 'stastatus',
                'numid', 'docnr', 'docdd', 'stagmlx', 'stagmly'],
        'wpl': ['wplid', 'wplvkid', 'wplvkbg', 'wplvkeg', 'wplstatus', 'wplnaam']
        }
    
    batch_size = 100
    printit = True
    
    # --------------------------------------------------------------------------
    print('\n----- Loop over de bag typen')
    # --------------------------------------------------------------------------
    # xml_dirs = ['sta', 'lig']
    # xml_dirs = ['pnd']
    # xml_dirs = ['num']
    # xml_dirs = ['vbo', 'pnd', 'num']
    xml_dirs = ['lig', 'sta', 'opr', 'wpl', 'vbo', 'pnd', 'num']
    # xml_dirs = ['lig', 'sta', 'vbo', 'pnd']
    # xml_dirs = ['pnd', 'num']
    # xml_dirs = ['opr', 'wpl']
    # xml_dirs = ['vbo', 'pnd']
    
    for bagobject in xml_dirs:
        # ########## general part for all 7 bagobjects
        print('\n\t------ Bagobject', bagobject)
        subdir = bagobject + '/'
        ddir = inputdir + subdir
        bag_files = os.listdir(ddir)
        outp_lst_d = []  # list of dict containing output records
        outputfile = outputdir + bagobject + '.csv'
        if os.path.isfile(outputfile):
            print('\tRemoving', outputfile)
            os.remove(outputfile)
        input_bagobject_count = 0
        output_bagobject_count = 0
        file_count = 0
        batch_count = 0
        report_count = 0
        print('\tAantal input xml bestanden:' + str(len(bag_files)))
        print('\t', end='')
        
        for inputfile in bag_files:
            bagtree = ET.parse(ddir + inputfile)
            root = bagtree.getroot()
            tag = '{' + ns['Objecten'] + '}' + short[bagobject]
            input_bagobject_filecount = 0
            output_bagobject_filecount = 0
            file_count += 1
            batch_count += 1
            report_count += 1
            # print(inputfile)
    
            # ######### Loop over bagobjects in one bag file       #####
    
            for level0 in root.iter(tag):   # level0 is the bagobject-tree
                input_bagobject_filecount += 1
                id1 = assigniffound(level0, ['Objecten:identificatie'], ns)
                status = status_dict[
                    assigniffound(level0, ['Objecten:status'], ns)]
    
                vkid = assigniffound(level0, ['Objecten:voorkomen',
                                              'Historie:Voorkomen',
                                              'Historie:voorkomenidentificatie'],
                                     ns)
    
                date_str = assigniffound(level0, ['Objecten:voorkomen',
                                                  'Historie:Voorkomen',
                                                  'Historie:beginGeldigheid'],
                                         ns)
                vkbg = date2int(date_str)
                date_str = assigniffound(level0, ['Objecten:voorkomen',
                                                  'Historie:Voorkomen',
                                                  'Historie:eindGeldigheid'],
                                         ns,
                                         futureday_str)
                vkeg = date2int(date_str)
    
                output_record = {bagobject + 'id':     id1,
                                 bagobject + 'status':  status,
                                 bagobject + 'vkid':    vkid,
                                 bagobject + 'vkbg':    vkbg,
                                 bagobject + 'vkeg':    vkeg}
    
                # ######### vsl: VBO, Staplaats, Ligplaats              ###########
    
                if bagobject in vsl:
                    output_record['numid'] = \
                        assigniffound(level0,
                                      ['Objecten:heeftAlsHoofdadres',
                                       'Objecten-ref:NummeraanduidingRef'],
                                      ns)
    
                # specific for sta, lig
                
                if bagobject in {'sta', 'lig'}:
                    gml_string = assigniffound(level0, ['Objecten:geometrie',
                                                        'gml:Polygon',
                                                        'gml:exterior',
                                                        'gml:LinearRing',
                                                        'gml:posList'],
                                                 ns)
                    if gml_string is not None:
                        xyz = gml_string.split()
                        (output_record[bagobject + 'gmlx'],
                         output_record[bagobject + 'gmly']) = middelpunt(xyz)
    
                    output_record['docnr'] = \
                        assigniffound(level0, ['Objecten:documentnummer'],
                                      ns)
    
                    output_record['docdd'] = date2int(\
                        assigniffound(level0, ['Objecten:documentdatum'],
                                      ns))
    
                    outp_lst_d.append(output_record.copy())
                    output_bagobject_filecount += 1
    
    
                # specific for VBO
    
                if bagobject == 'vbo':
    
                    gml_string = assigniffound(level0, ['Objecten:geometrie',
                                                        'Objecten:punt',
                                                        'gml:Point',
                                                        'gml:pos'],
                                               ns)
                    if gml_string is not None:
                        xyz = gml_string.split()
                        output_record[bagobject + 'gmlx'] = float(xyz[0])
                        output_record[bagobject + 'gmly'] = float(xyz[1])
                    else: # BUGFIX: ook een VBO kan een Polygon zijn
                        gml_string = assigniffound(level0, ['Objecten:geometrie',
                                                            'Objecten:vlak',
                                                            'gml:Polygon',
                                                            'gml:exterior',
                                                            'gml:LinearRing',
                                                            'gml:posList'],
                                                     ns)
                        if gml_string is not None:
                            xyz = gml_string.split()
                            (output_record[bagobject + 'gmlx'],
                             output_record[bagobject + 'gmly']) = middelpunt(xyz)
                        
                    
                    gebruiksdoelen = level0.findall('Objecten:gebruiksdoel', ns)
                    if len(gebruiksdoelen) > 1:
                        # print('Meerdere gebruiksdoelen. vboid:', id1, 'met',
                        #       gebruiksdoelen)
                        dubbel_gebruiksdoel_count += 1
    
    
                    for gebruiksdoel in gebruiksdoelen:
                        # print('DEBUG: gebruiksdoelen', gebruiksdoelen)
                        try:
                            output_record[gebruiksdoel_dict[gebruiksdoel.text]] = True
                        except KeyError:
                            print('gebruiksdoel', gebruiksdoel.text, 'niet gevonden')
                                   
                    
                    output_record['oppervlakte'] = \
                        assigniffound(level0,
                                      ['Objecten:oppervlakte'],
                                      ns)
                    level1 = level0.find('Objecten:maaktDeelUitVan', ns)
    
    
                    if level1 is not None:
                        level2 = level1.findall('Objecten-ref:PandRef', ns)
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
                        assigniffound(level0, ['Objecten:oorspronkelijkBouwjaar'],
                                      ns)
    
                    output_record['docnr'] = \
                        assigniffound(level0, ['Objecten:documentnummer'],
                                      ns)
    
                    output_record['docdd'] = date2int(\
                        assigniffound(level0, ['Objecten:documentdatum'],
                                      ns))
    
                    gml_string = assigniffound(level0, ['Objecten:geometrie',
                                                        'gml:Polygon',
                                                        'gml:exterior',
                                                        'gml:LinearRing',
                                                        'gml:posList'],
                                               ns)
                    if gml_string is not None:
                        xyz = gml_string.split()
                        (output_record[bagobject + 'gmlx'],
                         output_record[bagobject + 'gmly']) = middelpunt(xyz)
    
                    outp_lst_d.append(output_record.copy())
                    output_bagobject_filecount += 1
    
    
                # ######### num: Nummeraanduiding                           #######
                if bagobject == 'num':
                    output_record['huisnr'] = \
                        assigniffound(level0, ['Objecten:huisnummer'], ns)
                    output_record['postcode'] = \
                        assigniffound(level0, ['Objecten:postcode'], ns)
                    output_record['typeao'] = \
                        assigniffound(level0, ['Objecten:typeAdresseerbaarObject'],
                                      ns)
                    level1 = level0.find('Objecten:ligtAan', ns)
                    if level1 is not None:
                        level2 = level1.findall('Objecten-ref:OpenbareRuimteRef',
                                                ns)
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
                        assigniffound(level0, ['Objecten:naam'], ns)
                    output_record['oprtype'] = \
                        assigniffound(level0, ['Objecten:type'], ns)
                    level1 = level0.find('Objecten:ligtIn', ns)
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
                        assigniffound(level0, ['Objecten:naam'], ns)
                    outp_lst_d.append(output_record.copy())
                    output_bagobject_filecount += 1
    
            input_bagobject_count += input_bagobject_filecount
            output_bagobject_count += output_bagobject_filecount
            print(".", end='')
    
            if report_count == 100:
                # print(file_count, 'of', len(bag_files))
                report_count = 0
            
            if batch_count == batch_size:
                print('tussendoor bewaren:', file_count, 'van', len(bag_files))
                dict2df2file(outp_lst_d, outputfile, cols_dict[bagobject])
                batch_count = 0
                outp_lst_d = []
                print('\t', end='')
    
                
        print('\tLoop ended for bagobject', bagobject)
        
        if outp_lst_d != []:
            dict2df2file(outp_lst_d, outputfile, cols_dict[bagobject])
     
        print('\tOutputfile:', bagobject + '.csv ', '\n\t\trecords in:\t' +
                 str(input_bagobject_count) + '\n\t\trecords aangemaakt: ' +
                 str(output_bagobject_count) + ' ' + bagobject)
    
        if bagobject == 'vbo':
            if output_bagobject_count != 0:
                print('\tGedeelte vbo met dubbel gebruiksdoel:',
                      dubbel_gebruiksdoel_count / output_bagobject_count)
    
    toc = time.perf_counter()
    baglib.print_time(toc - tic, '\n------------- Einde bag_xml2csv in', printit)

def assigniffound(node,
                  taglist,
                  namespace,
                  value_if_not_found=None):
    '''
        Parameters
    ----------
    node: elem element
       the node of the subtree to be searched
    taglist : list of strings
       list of tags containing the tages of the nodes to be processed
       while going one level deeper in the xml tree
    namespace : string
        the dictionary ns with the namespace stuff
    value_if_not_found: if the tag in the taglist is not found, assigniffound
        returns this value

    Returns
    -------
    assignifffound digs through the xml tree until it reaches the node with
    the last tagname. If this node is found, its .text property is returned,
    if not the parameter value_if_not_found is returned


           level1 = level0.find('Objecten:voorkomen', ns)
           if level1 is not None:
               level2 = level1.find('Historie:Voorkomen', ns)
               if level2 is not None:
                   vkid = assignifexist(level2, 'Historie:voorkomenidentificatie', ns)
                   vkbg = assignifexist(level2, 'Historie:beginGeldigheid', ns)
                   vkegexist = level2.find('Historie:eindGeldigheid', ns)
                   if vkegexist is not None:
                       vkeg = vkegexist.text
                   else:
                       vkeg = '2999-01-01'
    Example taglist:
    _taglist = ['Objecten:voorkomen',
                'Historie:Voorkomen',
                'Historie:voorkomenidentificatie']
    '''

    i = 0
    current_level = node
    while i < len(taglist):
        level_down = current_level.find(taglist[i], namespace)
        if level_down is not None:
            i += 1
            current_level = level_down
        else:
            return value_if_not_found
    return current_level.text

def date2int(_date_str):
    '''
    Parameters
    ----------
    datestring : string
        string of format 2019-03-24
    Returns
    -------
    the integer 20190314
    '''
    _str = _date_str.replace('-', '')
    return int(_str)

def middelpunt(float_lst):
    '''Return tuple that is the middle of a polygon float_lst. float_list
    contains a list coordinates [x1, y1, z1, x2, y2, z2, ...xn, yn, zn]'''

    # print('DEBUG1: entering middelpunt')

    _x = [float(i) for i in float_lst[0::3]]
    _y = [float(j) for j in float_lst[1::3]]
    
    # print('DEBUG2: _x _y: ', _x, _y)
    return (sum(_x)/len(_x), sum(_y)/len(_y))
    
def dict2df2file(dict1, file1, cols1):
    '''Convert dict1 to df. Write df to file1. Append if file1 exists.
    Use cols1 to define the order of the columns'''
    
    # print('Writing batch to file')
    # print('\tConverting dict to dataframe:')
    _df = pd.DataFrame.from_dict(dict1)

    # print('\tChanging column order:')
    # _cols = list(_df.columns.values)
    _df = _df.reindex(columns=cols1)
    _df.fillna(False, inplace=True)
    
    if not os.path.isfile(file1):
        # print('\tWriting to file...')
        _df.to_csv(file1, index=False)
    else:
        # print('\tAppending to file...')
        _df.to_csv(file1, mode='a', header=False, index=False)


# --------------------------------------------------------------------------
# ################ Main program ###########################
# --------------------------------------------------------------------------

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
    current_month = baglib.get_arg1(sys.argv, DIR01)

    printit=True

    bag_xml2csv(current_month=current_month,
                koppelvlak1=DIR01,
                koppelvlak2=DIR02,
                loglevel=printit)


