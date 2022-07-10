#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""


Created on Sat Mar  5 12:40:26 2022
@author: anton
Purpose: convert BAG XML to CSV file
version 1.2.2

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
1.2.2 added makedir OUTPUTDIR (if not exist)

    
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

# --------------------------------------------------------------------------
# ############### Define functions #################################
# --------------------------------------------------------------------------
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

tic = time.perf_counter()
print('-----------------------------------------------------------------')
print('----- DOEL: bag xml bestanden omzetten naar csv -----------------')
print('-----------------------------------------------------------------')

# --------------------------------------------------------------------------
# ################ Initialize variables ###########################
# --------------------------------------------------------------------------
# month and dirs
os.chdir('..')
BASEDIR = os.getcwd() + '/'
baglib.print_omgeving(BASEDIR)
DATADIR = BASEDIR + 'data/'
DIR01 = DATADIR + '01-xml/'
DIR02 = DATADIR + '02-csv/'
DIR03 = DATADIR + '03-bewerktedata/'
current_month = baglib.get_arg1(sys.argv, DIR01)
INPUTDIR = DIR01 + current_month + '/'
OUTPUTDIR = DIR02 + current_month + '/'
baglib.make_dir(OUTPUTDIR)

status_dict = {
    'Plaats ingetrokken':                           'plai',
    'Plaats aangewezen':                            'plaa',
    'Naamgeving ingetrokken':                       'nami',
    'Naamgeving aangewezen':                        'nama',
    'Naamgeving uitgegeven':                        'namu',
    'Verblijfsobject gevormd':                      'gevo',
    'Verblijfsobject in gebruik':                   'inge',
    'Verblijfsobject in gebruik (niet ingemeten)':  'inni',
    'Verblijfsobject ingetrokken':                  'intr',
    'Verbouwing verblijfsobject':                   'verb',
    'Verblijfsobject ten onrechte opgevoerd':       'onre',
    'Niet gerealiseerd verblijfsobject':            'nrea',
    'Verblijfsobject buiten gebruik':               'buig',
    'Bouwvergunning verleend':                      'bver',
    'Bouw gestart':                                 'bstr',
    'Pand in gebruik':                              'inge',
    'Pand in gebruik (niet ingemeten)':             'inni',
    'Verbouwing pand':                              'verb',
    'Pand gesloopt':                                'slop',
    'Niet gerealiseerd pand':                       'nrea',
    'Pand ten onrechte opgevoerd':                  'onre',
    'Pand buiten gebruik':                          'buig',
    'Sloopvergunning verleend':                     'slov',
    'Woonplaats aangewezen':                        'woan',
    'Woonplaats ingetrokken':                       'woin'}
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
baseheader = ['id', 'status', 'vkid', 'vkbg', 'vkeg']
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
print('\n\tVerslagmaand is', current_month)

# --------------------------------------------------------------------------
print('\n----- Loop over de bag typen')
# --------------------------------------------------------------------------
# xml_dirs = ['num']
# xml_dirs = ['vbo', 'pnd', 'num']
xml_dirs = ['lig', 'sta', 'opr', 'wpl', 'vbo', 'pnd', 'num']
# xml_dirs = ['pnd', 'num']
# xml_dirs = ['opr', 'wpl']
for bagobject in xml_dirs:
    # ########## general part for all 7 bagobjects
    subdir = bagobject + '/'
    ddir = INPUTDIR + subdir
    bag_files = os.listdir(ddir)
    output_dict = []           # list of dict containing output records
    input_bagobject_count = 0
    output_bagobject_count = 0
    file_count = 0
    report_count = 0
    print('\n\tBagobject  ' + bagobject)
    print('\tDirectory: ' + ddir)
    print('\tAantal bestanden: ' + str(len(bag_files)))
    for inputfile in bag_files:
        bagtree = ET.parse(ddir + inputfile)
        root = bagtree.getroot()
        tag = '{' + ns['Objecten'] + '}' + short[bagobject]
        input_bagobject_filecount = 0
        output_bagobject_filecount = 0
        file_count += 1
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
            if bagobject in {'sta', 'lig'}:
                output_record['docnr'] = \
                    assigniffound(level0, ['Objecten:documentnummer'],
                                  ns)
                output_dict.append(output_record.copy())
                output_bagobject_filecount += 1

            # specific for VBO
            if bagobject == 'vbo':
                gebruiksdoelen = level0.findall('Objecten:gebruiksdoel', ns)
                if len(gebruiksdoelen) > 1:
                    # print('Meerdere gebruiksdoelen. vboid:', id1, 'met',
                    #       gebruiksdoelen)
                    dubbel_gebruiksdoel_count += 1
                output_record['gebruiksdoel'] = ''
                for gebruiksdoel in gebruiksdoelen:
                    output_record['gebruiksdoel'] += \
                        gebruiksdoel_dict[gebruiksdoel.text]
                output_record['oppervlakte'] = \
                    assigniffound(level0,
                                  ['Objecten:oppervlakte'],
                                  ns)
                level1 = level0.find('Objecten:maaktDeelUitVan', ns)
                '''
                gml_string = assigniffound(level0, ['Objecten:geometrie',
                                                    'Objecten:punt',
                                                    'gml:Point',
                                                    'gml:pos'],
                                           ns)
                if gml_string is not None:
                    xyz = gml_string.split()
                    output_record['gml'] = Point(float(xyz[0]), float(xyz[1]))
                '''
                if level1 is not None:
                    level2 = level1.findall('Objecten-ref:PandRef', ns)
                    if level2 is not None:
                        for j in level2:
                            output_record['pndid'] = j.text
                            output_dict.append(output_record.copy())
                            output_bagobject_filecount += 1
                    else:
                        output_dict.append(output_record.copy())
                        output_bagobject_filecount += 1
                else:
                    output_dict.append(output_record.copy())
                    output_bagobject_filecount += 1
            # ######### pnd: Pand  #######
            if bagobject == 'pnd':
                output_record['bouwjaar'] = \
                    assigniffound(level0, ['Objecten:oorspronkelijkBouwjaar'],
                                  ns)
                output_record['docnr'] = \
                    assigniffound(level0, ['Objecten:documentnummer'],
                                  ns)
                output_dict.append(output_record.copy())
                output_bagobject_filecount += 1

            # ######### num: Nummeraanduiding                           #######
            if bagobject == 'num':
                output_record['huisnr'] = \
                    assigniffound(level0, ['Objecten:huisnummer'], ns)
                output_record['postcode'] = \
                    assigniffound(level0, ['Objecten:postcode'], ns)
                output_record['huisnr'] = \
                    assigniffound(level0, ['Objecten:huisnummer'], ns)
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
                            output_dict.append(output_record.copy())
                            output_bagobject_filecount += 1
                    else:
                        output_dict.append(output_record.copy())
                        output_bagobject_filecount += 1
                else:
                    output_dict.append(output_record.copy())
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
                        output_dict.append(output_record.copy())
                        output_bagobject_filecount += 1
                else:
                    output_dict.append(output_record.copy())
                    output_bagobject_filecount += 1
            # #################################################################
            # ######### wpl: Woonplaats -                               #######
            # #################################################################
            if bagobject == 'wpl':
                output_record['wplnaam'] = \
                    assigniffound(level0, ['Objecten:naam'], ns)
                output_dict.append(output_record.copy())
                output_bagobject_filecount += 1

        input_bagobject_count += input_bagobject_filecount
        output_bagobject_count += output_bagobject_filecount
        print(".", end='')
        if report_count == 100:
            print(file_count, 'of', len(bag_files))
            report_count = 0
    print('\n')
    df = pd.DataFrame.from_dict(output_dict)
    df.index.name = 'idx'
    outputfile = OUTPUTDIR + bagobject + '.csv'
    print('\tOutputfile: ' + bagobject + '.csv ' + '\n\t\trecords in:\t' +
             str(input_bagobject_count) + '\n\t\trecords aangemaakt: ' +
             str(output_bagobject_count) + ' ' + bagobject)
    if bagobject == 'vbo':
        print('Perc dubbel gebruiksdoel: ' +\
                 str(round(100 * dubbel_gebruiksdoel_count /\
                           output_bagobject_count, 3)))
    df.to_csv(outputfile, index=False)
toc = time.perf_counter()
print('\nProgram time in seconds:', toc - tic)