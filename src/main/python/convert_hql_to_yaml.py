#!/usr/bin/python

import sys
import re
import yaml

if (len(sys.argv) < 3):
    print("Please provide 2 arguments: 1) The location of your hive schema and 2) The name of your db.")
    exit(1)

lines = []
with open(sys.argv[1], 'r') as myfile:
    lines = myfile.read().splitlines()

fields = []
num_fields = 1
for line in lines:
    parsed_line = line.split(" ")
    if (len(parsed_line) > 2 and parsed_line[0] != '#'):
        fields.append(tuple([parsed_line[0], parsed_line[len(parsed_line) - 1]]))

yamlFields = []
for field in fields:
    fieldObj = dict(name = field[0], type = field[1], id = num_fields)
    num_fields+=1
    yamlFields.append(fieldObj)

schemaName = sys.argv[2]
schema = {}
schema["schemas"] = []
schema["schemas"].append(dict(name = schemaName, fields = yamlFields))

with open('result.yaml', 'w') as f:
  yaml.dump(schema, f, default_flow_style=False)