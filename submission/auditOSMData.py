# -*- coding: utf-8 -*-
"""
Created on Sat Apr  4 16:11:16 2015

@author: Mark Wright

Clean and Audit OSM data, for conversion into JSON to be loaded into mongo db.
"""

import xml.etree.ElementTree as ET 
from collections import defaultdict
import re
import pprint
import operator
import codecs
import json

#Files
INPUT_FILE = "data/birmingham_england.osm"
OUTPUT_FILE = 'data/birmingham_england_osm.json'

#regular expressions
lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
postcode = re.compile(r'[A-Z]{1,2}[\dR][\dA-Z]? \d[A-Z]{2}')
postcode_without_space = re.compile(r'[A-Z]{1,2}[\dR][\dA-Z]?\d[A-Z]{2}')
valid_partial_postcode = re.compile(r'[A-Z]{1,2}[\dR][\dA-Z]?')

address = re.compile(r'^addr:.*')
address_ignore = re.compile(r'^addr:.*:.*')
attribKey = re.compile(r'^.*:.*')



#Audit data structures
count_dict = { "way" : 0, "node" : 0, "relation" : 0, "other" : 0}
keys_dict = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
expected_streets = ["Street", "Avenue", "Close", "Drive", "Court", "Place", "Square", "Lane", "Road", "Trail", "Way", "Walk", "View", "Rise", "Grove", "Croft", "Crescent", "Hill", "Mews", "Row", "Gardens", "East", "West", "North"]
expected_amenity = ["pub", "parking", "restaurant", "cafe", "fast_food", "toilets", "telephone", "school", "college", "university", "post_box", "bench", "pharmacy", "fuel", "place_of_worship", "grit_bin", "post_office", "fire_station", "grave_yard"]
expected_landuse = ["residential","grass","meadow","farmland","industrial","forest","farmyard","retail","commercial","recreation_ground","greenhouse_horticulture","brownfield","paddock","orchard","graveyard","farm","cemetery","allotments","construction","pasture","reservoir"]
expected_natural = ["wood", "tree", "water"]
expected_leisure = ["garden", "park", "pitch", 'playground', "golf_course", "stadium"]
street_types = defaultdict(set)
keys_data = {}
amenities = defaultdict(set)
landuse = defaultdict(set)
natural = defaultdict(set)
leisure = defaultdict(set)
erroneous_postcodes = []

#Cleaning conversion mappings
street_mapping = { "lane": "Lane", "road": "Road", "Aveune": "Avenue"}
amenity_mapping = { "police" : "police_station", "biergarten" : "pub"}
landuse_mapping = {'greenhouse_horticulture': 'agricultural', 'orchard': 'agricultural', 'industrial': 'industrial', 
                   'meadow': 'agricultural', 'graveyard': 'green_space', 'retail': 'commercial', 'farm': 'agricultural', 'residential': 'residential', 
                   'brownfield': 'industrial', 'grass': 'green_space', 'cemetery': 'green_space', 'allotments': 'agricultural', 'construction': 'industrial', 
                   'commercial': 'commercial', 'forest': 'green_space', 'pasture': 'agricultural', 'farmland': 'agricultural', 'paddock': 'agricultural', 
                   'recreation_ground': 'green_space', 'farmyard': 'agricultural', 'reservoir': 'green_space', 'quarry' : 'industrial', 'park' : 'green_space',
                   'basin' : 'green_space', 'village_green' : 'green_space', 'garages' : 'residential', 'landfill' : 'industrial', 'railway' : 'industrial', 'greenfield' : 'green_space',
                   'vineyard' : 'agricultural', 'plant_nursery' : 'agricultural', 'nature_reserve' : 'green_space', 'field' : 'agricultural'}

#Fields to add to the created element in the JSON output.
CREATED = [ "version", "changeset", "timestamp", "user", "uid", "visible"]



#Audit the OSM data across various facets.            
def audit():
    for i, element in enumerate(get_element(INPUT_FILE)):
        #countInputTags(element, countDict)
        for tag in element.iter("tag"):
            if is_street_name(tag):
                audit_street_type(street_types, tag.attrib['v'])    
            #if is_landuse(tag):
            #    audit_landuse(landuse, tag.attrib['v'])
            #if is_postcode(tag):
            #    audit_postcodes(erroneous_postcodes, tag.attrib['v'])
    #print erroneous_postcodes         
    print print_audit(street_types)

#Clean the OSM data and produce JSON output for every node. Will need to stream the results to ensure the output doensnt get too big.
def clean():
    data = []    
    with codecs.open(OUTPUT_FILE, "w") as fo:    
        for i, element in enumerate(get_element(INPUT_FILE)):    
            node = shape_element(element)
            if node:
                fo.write(json.dumps(node) + "\n")         

def shape_element(element):
    node = {}
    node['created'] = {}
    lat, lon = 0.0, 0.0
    
    if element.tag == "node" or element.tag == "way" :
        #handle top level element attribs
        node['type'] = element.tag
        for attrib in element.attrib.items():
            if attrib[0] in CREATED:
                node['created'][attrib[0]] = attrib[1]
            elif attrib[0] == "lat":
                lat = float(attrib[1])
            elif attrib[0] == "lon":
                lon = float(attrib[1])
            else:
                node[attrib[0]] = attrib[1]
        
        #handle second level tags
        for subEle in element:
            if is_node_ref(subEle):
                handle_node_refs(subEle, node)                     
            elif subEle.tag == 'tag':
                key, value = process_tag(subEle)
                #if we have any dodgy characters or its a field we are not intrested in skip this,
                if problemchars.search(key) or address_ignore.search(key):
                    continue
                
                #additional processing for certain fields
                if is_landuse(subEle):
                    summarise_landuse(value, node)
                
                #clean address and amenity fields
                if is_amenity(subEle):
                    clean_generic(value, key, amenity_mapping, node, False)
                elif is_address(subEle):
                    if 'address' not in node:
                        node['address'] = {}
                    if is_street_name(subEle):
                        clean_street_name(value, street_mapping, node)
                    elif is_postcode(subEle):
                        clean_postcode(value, node)
                    else:
                        key_arr = key.split(':')
                        if len(key_arr) == 2:
                            node['address'][key_arr[1]] = value
                
                #process remaining elements, nest if we have to
                elif attribKey.search(key):
                    key_arr = key.split(':')
                    if len(key_arr) > 2:
                        continue
                    if key_arr[0] not in node:
                        node[key_arr[0]] = {}
                    if key_arr[0] in node and type(node[key_arr[0]]) is not dict:
                        v = node[key_arr[0]]
                        node[key_arr[0]] = {}
                        node[key_arr[0]]['v'] = v
                    node[key_arr[0]][key_arr[1]] = value
                else:
                    if key in node and type(node[key]) is dict:
                        node[key]['v'] = value
                    else:
                        node[key] = value
                    
        if lat != 0.0 and lon != 0.0:
            pos = [lat, lon]
            node['pos'] = pos
        return node
    else:
        return None

#get key and value
def process_tag(subEle):
    return subEle.attrib['k'], subEle.attrib['v']

#add the node reference to an array
def handle_node_refs(subEle, node):
    if "node_refs" in node:
        node['node_refs'].append(subEle.attrib['ref'])
    else:
        node['node_refs'] = [subEle.attrib['ref']]
        
#clean generic data given a dictionary of mappings, 
#if not in mappings use the value it already has if skip is not true      
def clean_generic(value, tag_type, mapping, node, skip):
    if value in mapping:
        node[tag_type] = mapping[value]
    elif not skip:
        node[tag_type] = value

#clean street mapping    
def clean_street_name(value, mapping, node):
    m = street_type_re.search(value)
    if m:
        if m.group() in mapping:
            new = mapping[m.group()]
            value = value[:m.start()] + new
    node['address']['street'] = value
    
#summarise the     
def summarise_landuse(value, node):
    clean_generic(value, 'land_sum', landuse_mapping, node, True)
    
    
#clean postcode
def clean_postcode(value, node):
     m = postcode.search(value)
     if not m:
         m = postcode_without_space.search(value)
         if m:
             temp = value[:len(value)-3] + " " + value[len(value)-3:]
             value = temp
         else:         
             m = valid_partial_postcode.search(value)
             if not m:
                 return     
     node['address']['postcode'] = value
         
        
#Helper to get elements of intrest from the input stream
def get_element(input_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag
    Reference:
    http://stackoverflow.com/questions/3095434/inserting-newlines-in-xml-file-generated-via-xml-etree-elementtree-in-python
    """
    context = ET.iterparse(input_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


####
# Auditing functions
####

#count the type of top level tags
def audit_input_tags(element, countDict):
    if element.tag == "way":
        countDict['way'] += 1
    elif element.tag == "node":
        countDict['node'] += 1
    elif element.tag == "relation":
        countDict['relation'] += 1
    else:
        countDict['other'] += 1
    return countDict

#Audit keys in tag elements
def audit_keys(element, keys):
    if element.tag == "tag":
        attrib = element.attrib["k"]
        if lower.search(attrib):
            keys["lower"] = keys["lower"] + 1
        elif lower_colon.search(attrib):
            keys["lower_colon"] = keys["lower_colon"] + 1
        elif problemchars.search(attrib):
            keys["problemchars"] = keys["problemchars"] + 1
        else:
            keys["other"] = keys["other"] + 1 
    return keys
    
#Audit the keys in tag elements to understand the different types    
def describe_keys(element, keys):
    if element.tag == "tag":
        attrib = element.attrib["k"]
        if attrib not in keys:
            keys[attrib] = 1
        else:
            keys[attrib] += 1

#Print audit result
def print_audit(keysData):
    for x in sorted(keysData.items(), key=operator.itemgetter(1), reverse=True):
        print "%s: %s" % (x[0], x[1])     
        
#Tests for the type of tag
def is_node_ref(elem):
    return (elem.tag == 'nd')   

def is_address(elem):
    return (address.search(elem.attrib['k']))    
    
def is_amenity(elem):
    return (elem.attrib['k'] == "amenity")

def is_leisure(elem):
    return (elem.attrib['k'] == "leisure")

def is_natural(elem):
    return (elem.attrib['k'] == "natural")
    
def is_landuse(elem):
    return (elem.attrib['k'] == "landuse")
    
def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")
    
def is_postcode(elem):
    return (elem.attrib['k'] == "addr:postcode")
    
#Audit amenity    
def audit_amenity(amenity, a):
    if a not in expected_amenity:
        if a not in amenity:
            amenity[a] = 1
        else:
            amenity[a] += 1

#Audit leisure    
def audit_leisure(leisure, l):
    if l not in expected_leisure:
        if l not in leisure:
            leisure[l] = 1
        else:
            leisure[l] += 1

#Audit natural    
def audit_natural(natural, n):
    if n not in expected_natural:
        if n not in natural:
            natural[n] = 1
        else:
            natural[n] += 1

#Audit landuse    
def audit_landuse(landuse, l):
    if l not in expected_landuse:
        if l not in landuse:
            landuse[l] = 1
        else:
            landuse[l] += 1
            
def audit_postcodes(erroneousPostcodes, p):
     m = postcode.search(p)
     if not m:
         m = postcode_without_space.search(p)
         if m:
             erroneousPostcodes.append(p)
         m = valid_partial_postcode.search(p)
         if not m:
             erroneousPostcodes.append(p)
                                
            
#Audit steet types    
def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected_streets:
            street_types[street_type].add(street_name)
            

if __name__ == '__main__':
    #audit()    
    clean()

