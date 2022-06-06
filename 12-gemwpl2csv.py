#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""


Created on Sat Mar  5 12:40:26 2022
@author: anton
Purpose: convert BAG XML to CSV file
version 0.2


# #### Typical XML layout ##########################################

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

# ################ Import libraries ###############################
import xml.etree.ElementTree as ET
import os
import pandas as pd
import sys
import baglib

# ############### Define functions #################################
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


# ################ Initialize variables ###########################
# month and dirs
os.chdir('..')
BASEDIR = os.getcwd() + '/'
baglib.print_omgeving(BASEDIR)
DATADIR = BASEDIR + 'data/'
DIR01 = DATADIR + '01-xml/'
DIR02 = DATADIR + '02-csv/'
DIR03 = DATADIR + '03-bewerktedata/'
current_month = baglib.get_arg1(sys.argv, DIR02)
INPUTDIR = DIR01 + current_month + '/'
OUTPUTDIR = DIR02 + current_month + '/'


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
    'Woonplaats ingetrokken':                       'woin',
    'definitief':                                   'defi',
    'voorlopig':                                    'vrlg'}
'''
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

baseheader = ['id', 'status', 'vkid', 'vkbg', 'vkeg']
'''
# namespace stuff we have to deal with
'''
ns = {'Objecten': "www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601",
      'gml': "http://www.opengis.net/gml/3.2",
      'Historie': "www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601",
      'Objecten-ref':
          "www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601"
      }
'''  
    
ns = {'gwr-bestand': "www.kadaster.nl/schemas/lvbag/gem-wpl-rel/gwr-deelbestand-lvc/v20200601",
      'selecties-extract': "http://www.kadaster.nl/schemas/lvbag/extract-selecties/v20200601",
      'bagtypes': "www.kadaster.nl/schemas/lvbag/gem-wpl-rel/bag-types/v20200601",
      'gwr-product': "www.kadaster.nl/schemas/lvbag/gem-wpl-rel/gwr-producten-lvc/v20200601",
      'DatatypenNEN3610': "www.kadaster.nl/schemas/lvbag/imbag/datatypennen3610/v20200601"}


# shorthands to translate directory names to tags and so
short = {'vbo': 'Verblijfsobject',
         'lig': 'Ligplaats',
         'sta': 'Standplaats',
         'pnd': 'Pand',
         'num': 'Nummeraanduiding',
         'opr': 'OpenbareRuimte',
         'wpl': 'Woonplaats',
         'gemwpl': 'GemeenteWoonplaatsRelatie'
         }

futureday_str = '20321231'

# ######### works just for gemwpl                ###########

bagobject = 'gemwpl'

subdir = bagobject + '/'
ddir = INPUTDIR + subdir
bag_files = os.listdir(ddir)
print(short[bagobject], 'directory bevat', len(bag_files), 'bestanden')
output_dict = []           # list of dict containing output records
input_bagobject_count = 0
output_bagobject_count = 0
file_count = 0
report_count = 0

# ######### Loop over files in a directory with same bag objects #####
for inputfile in bag_files:
    bagtree = ET.parse(ddir + inputfile)
    root = bagtree.getroot()
    tag = '{' + ns['gwr-product'] + '}' + short[bagobject]
    input_bagobject_filecount = 0
    output_bagobject_filecount = 0
    file_count += 1
    report_count += 1
    # print(inputfile)
    # ######### Loop over bagobjects in one bag file       #####
    for level0 in root.iter(tag):   # level0 is the bagobject-tree
        input_bagobject_filecount += 1
        wpl = assigniffound(level0, ['gwr-product:gerelateerdeWoonplaats',
                                     'gwr-product:identificatie'], ns)
        gem = assigniffound(level0, ['gwr-product:gerelateerdeGemeente',
                                     'gwr-product:identificatie'], ns)
        status = status_dict[
            assigniffound(level0, ['gwr-product:status'], ns)]

        date_str = assigniffound(level0,
                                 ['gwr-product:tijdvakgeldigheid',
                                  'bagtypes:begindatumTijdvakGeldigheid'],
                                 ns)
        vkbg = date2int(date_str)
        date_str = assigniffound(level0, ['gwr-product:tijdvakgeldigheid',
                                          'bagtypes:einddatumTijdvakGeldigheid'],
                                ns, futureday_str)
        vkeg = date2int(date_str)

        output_record = {'wplid':   wpl,
                         'gemid':   gem,
                         'wplstatus':  status,
                         'wplvkbg':    vkbg,
                         'wplvkeg':    vkeg}

        output_dict.append(output_record.copy())
        output_bagobject_filecount += 1

    input_bagobject_count += input_bagobject_filecount
    output_bagobject_count += output_bagobject_filecount
    print(".", end='')
    if report_count == 100:
        print(file_count, 'of', len(bag_files))
        report_count = 0
df = pd.DataFrame.from_dict(output_dict)
df.index.name = 'idx'
# gdf[['id', 'gml']].plot()
# print(gdf[['id', 'gml']])

# print(gdf[['id', 'gml']].dtype())

outputfile = OUTPUTDIR + bagobject + '.csv'
print('\nOutputfile:', bagobject + '.csv',
      ',                      records in:', input_bagobject_count,
      ', records aangemaakt:', output_bagobject_count,
      bagobject, '\n')

df.to_csv(outputfile, index=False)
