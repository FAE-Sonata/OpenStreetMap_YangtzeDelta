# -*- coding: utf-8 -*-
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
import pprint
import re
import codecs
import json
from collections import defaultdict
from pymongo import MongoClient

""" constants """
OSM_FILE = 'shanghai_cn.osm'
OUTFILE_JSON = OSM_FILE[:OSM_FILE.rfind('.')] + ".json"

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons"]

mapping = { "St": "Street",
            "St.": "Street",
            "Ave": "Avenue",
            "Ave.": "Avenue",
            "Rd": "Road",
            "Rd.": "Road",
            "Lu": "Road",   """pinyin of 路"""
            "Raod": "Road",
            "Road)": "Road",
            "Roaf": "Road",
            "Rode": "Road",
            "road(west)": "Road West",
            "Dong": "East"  """pinyin of 东"""
            }

CREATED = ["version", "changeset", "timestamp", "user", "uid"]

def count_tags(filename):
    tree = ET.parse(filename)
    doc_root = tree.getroot()
    tags = {}
    def acquire_tags(root):
        this_tag = root.tag
        if tags.has_key(this_tag):
            tags[this_tag] += 1
        else:
            tags[this_tag] = 1
        for elem in root.getchildren():
            acquire_tags(elem)
    acquire_tags(doc_root)
    return tags

def key_type(element, keys):
    if element.tag == "tag":
        key = element.attrib['k']
        if len(re.findall(lower, key)):
            keys["lower"] += 1
        elif len(re.findall(lower_colon, key)):
            keys["lower_colon"] += 1
        elif len(re.findall(problemchars, key)):
            print(key)
            keys["problemchars"] += 1
        else:
            keys["other"] += 1
    return keys

def process_key_types(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)
    return keys

def pre_cleaning_diagnostics():
    tags = count_tags(OSM_FILE)
    pprint.pprint(tags)
    keys = process_key_types(OSM_FILE)
    pprint.pprint(keys)

"""cleaning street names"""

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)

def audit_postal_code(code_set, code):
    if not(len(code) == 6):
        code_set.add(code)
    else:
        try:
            code_int = int(code)
        except:
            code_set.add(code)

def audit_streets(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    non_compliant_codes = set()
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if tag.attrib['k'] == "addr:street":
                    audit_street_type(street_types, tag.attrib['v'])
                if tag.attrib['k'] == "postal_code" or tag.attrib['k'] == "addr:postcode":
                    audit_postal_code(non_compliant_codes, tag.attrib['v'])
    result = {"streets": street_types, "codes": non_compliant_codes}
    return result

def update_name(name, mapping):
    search = street_type_re.search(name)
    if search is None:
        return name
    else:
        previous_street_type = street_type_re.search(name).group()
        if mapping.has_key(previous_street_type):
            correct_street_type = mapping[previous_street_type]
            return name[:name.rfind(previous_street_type)] + correct_street_type
        else:
            return name
        
def street_types():
    audit_result = audit_streets(OSM_FILE)
    st_types = audit_result["streets"]
    print(st_types.keys())
    for st_type in ["Bund", "District", "M)", "Wukang", "Xiangyang", "West)", "garden", "Dong", "Rd"]:
        print(st_type + ": [")
        for entry in st_types[st_type]:
            print(entry + ", ")
        print("]")
        # ^^ the above street types are not actionable
    codes = audit_result["codes"]
    # set([u'201315 \u4e0a\u6d77', '21351', '20032'])
    print(codes)

""" building to JSON to prepare for MongoDB upload """
def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way":
        node["type"] = element.tag
        """ basic metadata """
        created_attributes = {}
        coordinates = {}
        for attribute in element.attrib:
            val = element.attrib[attribute]
            if attribute in CREATED:
                created_attributes[attribute] = val
            elif attribute == "lat" or attribute == "lon":
                coordinates[attribute] = val
            else:
                node[attribute] = val
        if len(coordinates) == 2:
            node["pos"] = [float(coordinates["lat"]), float(coordinates["lon"])]
        node["created"] = created_attributes
        """ node_refs """
        nds = element.findall("nd")
        node_refs = []
        for node_ref in nds:
            node_refs.append(node_ref.attrib["ref"])
        if len(node_refs) > 0:
            node["node_refs"] = node_refs
        
        """ extract from tags """
        tags = element.findall("tag")    
        name_attributes = {}
        address_attributes = {}
        street_attributes = {}
        for tag in tags:
            key = tag.attrib["k"]
            val = tag.attrib["v"]
            if not(re.match(lower_colon, key) is None):
                if key.find("name") > -1:
                    name_attributes[key[5:]] = val
                if key.find("addr") > -1:
                    address_attributes[key[5:]] = val
            elif key == "name":
                name_attributes["name"] = val
            elif key == "postal_code" or key == "addr:postcode":
                # postal codes in mainland China are six digits
                ord_val = [ord(ch) for ch in val]
                if len(val) >= 6 and all(ord_val[:6] in range(48, 58)):
                    if len(val) == 6:
                        node[key] = val
                    elif ord_val[6] == 32:
                        node[key] = val[:6]
            elif len(re.findall(":", key)) <= 1:
                node[key] = val
        """ clean name """
        if len(name_attributes) > 0:
            # prefer name_en over name_zh or the longer of name_en and English part of name
            if name_attributes.has_key("en"):
                if not(name_attributes.has_key("name")):
                    name_attributes["name"] = name_attributes["en"]
                else:
                    name_ascii = [ord(ch) for ch in name_attributes["name"]]
                    # 32 is ASCII for space
                    if not(32 in name_ascii):
                        name_attributes["name"] = name_attributes["en"]
                    else:
                        first_space = name_ascii.index(32)
                        alt_eng_name = name_attributes["name"][(first_space+1):]
                        if len(alt_eng_name) > len(name_attributes["en"]):
                            name_attributes["name"] = alt_eng_name
            node["name"] = name_attributes
        def is_street_attrib(key):
            return key.find("street") > -1
        """ clean address and street """
        if len(address_attributes) > 0:
            address_keys = address_attributes.keys()
            street_keys = filter(is_street_attrib, address_keys)
            if len(street_keys) > 0:
                for key in street_keys:
                    val = address_attributes[key]
                    if key == "street":
                        street_attributes["name"] = val
                    else: # form of "street:"
                        street_attributes[key[7:]] = val
                if street_attributes.has_key("en"):
                    raw_name = street_attributes["name"]
                    en_name = street_attributes["en"]
                    en_in_raw_idx = raw_name.find(en_name)
                    if en_in_raw_idx > -1:
                        street_attributes["zh"] = raw_name[:(en_in_raw_idx-1)]
                    else:
                        street_attributes["zh"] = raw_name
                    street_attributes["name"] = en_name
                """ update street suffixes with mapping;
                if transliteration / English translation is not available,
                the street name is left as is
                """
                street_attributes["name"] = update_name(street_attributes["name"], mapping)
                for key in street_keys:
                    address_attributes.pop(key)
                address_attributes["street"] = street_attributes
            node["address"] = address_attributes
        return node
    else:
        return None

def build_data(file_in):
    data = []
    for _, element in ET.iterparse(file_in):
        el = shape_element(element)
        if el:
            data.append(el)
    return data

def build_to_json():
    data = build_data(OSM_FILE)
    with open(OUTFILE_JSON, "w") as outfile:
        json.dump(data, outfile, indent=4)

def insert_data(data, db):
    for entry in data:
        db.cities.insert(entry)

def load_to_mongodb():
    client = MongoClient("mongodb://localhost:27017")
    db = client.examples
    db.cities.drop()
    with open(OUTFILE_JSON) as f:
        data = json.loads(f.read())
        insert_data(data, db)
        print(db.cities.find_one())

def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def aggregate(db, pipeline):
    return [doc for doc in db.cities.aggregate(pipeline)]

def run_queries():
    db = get_db('examples')
    # data overview
    print(db.cities.find().count())
    print(db.cities.find({"type": "node"}).count())
    print(db.cities.find({"type": "way"}).count())
    # contributors
    query_users_desc_contrib = [{"$group": {"_id": "$created.user",
                                            "contribs": {"$sum": 1}}},
                                {"$sort": {"contribs": -1}}]
    result_users_desc_contrib = aggregate(db, query_users_desc_contrib)
    def retrieve_contribs(d):
        return d['contribs']
    def is_one(x):
        return x==1
    contribs = map(retrieve_contribs, result_users_desc_contrib)
    total_contribs = sum(contribs)
    for user in result_users_desc_contrib[:5]:
        pprint.pprint(user)
    print("Number of unique users: %d" % len(contribs))
    print("Total contributions from all users: %d" % total_contribs)
    print("%contribs from top 10 users: %f" % (100.0 * sum(
        contribs[:10]) / total_contribs))
    pct_90 = len(contribs)*1/10 + 1
    print("%contribs from bottom 90%: %f" % (100.0 * sum(contribs[pct_90:]) / total_contribs))
    print("#Users with 1 contrib: %d" % len(filter(is_one, contribs)))
    # amenities
    query_amenities_desc = [{"$match": {"amenity": {"$exists": 1}}},
                            {"$group": {"_id": "$amenity",
                                        "count": {"$sum": 1}}},
                            {"$sort": {"count": -1}}]
    result_amenities_desc = aggregate(db, query_amenities_desc)
    print(len(result_amenities_desc))
    for amenity_type in result_amenities_desc[:10]:
        pprint.pprint(amenity_type)
    
    query_cuisine_desc = [{"$match": {"cuisine": {"$exists": 1}}},
                          {"$group": {"_id": "$cuisine",
                                      "count": {"$sum": 1}}},
                          {"$sort": {"count": -1}}]
    result_cuisine_desc = aggregate(db, query_cuisine_desc)
    pprint.pprint(result_cuisine_desc)
    
    # roads
    query_hwy_desc = [{"$match": {"highway": {"$exists": 1}}},
                      {"$group": {"_id": "$highway",
                                  "count": {"$sum": 1}}},
                      {"$sort": {"count": -1}}]
    result_hwy_desc = aggregate(db, query_hwy_desc)
    pprint.pprint(result_hwy_desc)
    
    # cities
    query_city_desc = [{"$match": {"address.city": {"$exists": 1}}},
                       {"$group": {"_id": "$address.city",
                                   "count": {"$sum": 1}}},
                       {"$sort": {"count": -1}}]
    result_city_desc = aggregate(db, query_city_desc)
    pprint.pprint(result_city_desc)