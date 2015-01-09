# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This will generate a movie data set of 1100 records.
These are the first 1100 movies which appear when querying the Freebase of type '/film/film'.
Here is the link to the freebase page - https://www.freebase.com/film/film?schema=

Usage - python3 film_data_generator.py
"""

import csv
import copy
import json
import codecs
import datetime
import urllib.parse
import urllib.request
import xml.etree.cElementTree as ET
from xml.dom import minidom

MAX_ITERATIONS=10  #10 limits it to 1100 docs

# You need an API Key by Google to run this
API_KEY = '<insert your Google developer API key>'
service_url = 'https://www.googleapis.com/freebase/v1/mqlread'
query = [{
  "id": None,
  "name": None,
  "initial_release_date": None,
  "directed_by": [],
  "genre": [],
  "type": "/film/film",
  "initial_release_date>" : "2000"
}]

def gen_csv(filmlist):
  filmlistDup = copy.deepcopy(filmlist)
  #Convert multi-valued to % delimited string
  for film in filmlistDup:
      for key in film:
        if isinstance(film[key], list):
          film[key] = '|'.join(film[key])
  keys = ['name', 'directed_by', 'genre', 'type', 'id', 'initial_release_date']
  with open('films.csv', 'w', newline='', encoding='utf8') as csvfile:
    dict_writer = csv.DictWriter(csvfile, keys)
    dict_writer.writeheader()
    dict_writer.writerows(filmlistDup)

def gen_json(filmlist):
  filmlistDup = copy.deepcopy(filmlist)
  with open('films.json', 'w') as jsonfile:
    jsonfile.write(json.dumps(filmlist, indent=2))

def gen_xml(filmlist):
  root = ET.Element("add")
  for film in filmlist:
    doc = ET.SubElement(root, "doc")
    for key in film:
      if isinstance(film[key], list):
        for value in film[key]:
          field = ET.SubElement(doc, "field")
          field.set("name", key)
          field.text=value
      else:
        field = ET.SubElement(doc, "field")
        field.set("name", key)
        field.text=film[key]
  tree = ET.ElementTree(root)
  with open('films.xml', 'w') as f:
    f.write( minidom.parseString(ET.tostring(tree.getroot(),'utf-8')).toprettyxml(indent="  ") )

def do_query(filmlist, cursor=""):
  params = {
          'query': json.dumps(query),
          'key': API_KEY,
          'cursor': cursor
  }
  url = service_url + '?' + urllib.parse.urlencode(params)
  data = urllib.request.urlopen(url).read().decode('utf-8')
  response = json.loads(data)
  for item in response['result']:
    del item['type'] # It's always /film/film. No point of adding this.
    try:
      datetime.datetime.strptime(item['initial_release_date'], "%Y-%m-%d")
    except ValueError:
      #Date time not formatted properly. Keeping it simple by removing the date field from that doc
      del item['initial_release_date']
    filmlist.append(item)
  return response.get("cursor")


if __name__ == "__main__":
  filmlist = []
  cursor = do_query(filmlist)
  i=0
  while(cursor):
      cursor = do_query(filmlist, cursor)
      i = i+1
      if i==MAX_ITERATIONS:
          break

  gen_json(filmlist)
  gen_csv(filmlist)
  gen_xml(filmlist)