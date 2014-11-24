from collections import defaultdict
import inspect
import json
import psycopg2
import os
import copy
import decimal
import argparse
import statsrunner
import datetime
from statsrunner import common

# FIXME this does not belong here
conn = psycopg2.connect("dbname=dashboard")
cur = conn.cursor()

def decimal_default(obj):
    if hasattr(obj, 'value'):
        if type(obj.value) == datetime.datetime:
            return obj.value.strftime('%Y-%m-%d %H:%M:%S %z')
        else:
            return obj.value
    else:
        return common.decimal_default(obj)

def dict_sum_inplace(d1, d2):
    if d1 is None: return
    for k,v in d2.items():
        if type(v) == dict or type(v) == defaultdict:
            if k in d1:
                dict_sum_inplace(d1[k], v)
            else:
                d1[k] = copy.deepcopy(v)
        elif (type(d1) != defaultdict and not k in d1):
            d1[k] = copy.deepcopy(v)
        elif d1[k] is None:
            continue
        else:
            d1[k] += v

def make_blank(stats_module):
    blank = {}
    for stats_object in [ stats_module.ActivityStats(), stats_module.ActivityFileStats(), stats_module.OrganisationStats(), stats_module.OrganisationFileStats(), stats_module.PublisherStats(), stats_module.AllDataStats() ]:
        stats_object.blank = True
        for name, function in inspect.getmembers(stats_object, predicate=inspect.ismethod):
            if not statsrunner.shared.use_stat(stats_object, name): continue
            blank[name] = function()
    return blank

def aggregate_file(stats_module, stats_json, folder, xml_file):
    subtotal = make_blank(stats_module) # FIXME This may be inefficient
    for activity_json in stats_json['elements']:
        dict_sum_inplace(subtotal, activity_json)
    dict_sum_inplace(subtotal, stats_json['file'])

    for aggregate_name,aggregate in subtotal.items():
        json_string = json.dumps(aggregate, sort_keys=True, indent=2, default=decimal_default)
        cur.execute("INSERT INTO aggregated_file (data, publisher, dataset, statname) VALUES (%s, %s, %s, %s)", (json_string, folder, xml_file, aggregate_name))
        conn.commit()

    return subtotal

def aggregate(args):
    import importlib
    stats_module = importlib.import_module(args.stats_module)
    
    for newdir in ['aggregated-publisher', 'aggregated-file', 'aggregated']:
        try:
            os.mkdir(os.path.join(args.output, newdir))
        except OSError: pass

    blank = make_blank(stats_module)

    if args.verbose_loop:
        raise NotImplementedError
    total = copy.deepcopy(blank)

    cur.execute("SELECT DISTINCT publisher FROM aggregated_file")
    for (publisher,) in list(cur):
        publisher_total = copy.deepcopy(blank)
        cur.execute("SELECT DISTINCT dataset FROM aggregated_file WHERE publisher=%s", (publisher,))
        for (dataset,) in list(cur):
            subtotal = copy.deepcopy(blank)
            cur.execute("SELECT statname, data FROM aggregated_file WHERE publisher=%s AND dataset=%s", (publisher,dataset,))
            for statname, stats_json in cur:
                subtotal[statname] = stats_json

            dict_sum_inplace(publisher_total, subtotal)

        publisher_stats = stats_module.PublisherStats()
        publisher_stats.aggregated = publisher_total
        publisher_stats.today = args.today
        for name, function in inspect.getmembers(publisher_stats, predicate=inspect.ismethod):
            if not statsrunner.shared.use_stat(publisher_stats, name): continue
            publisher_total[name] = function()

        dict_sum_inplace(total, publisher_total)
        for aggregate_name,aggregate in publisher_total.items():
            json_string = json.dumps(aggregate, sort_keys=True, indent=2, default=decimal_default)
            cur.execute("INSERT INTO aggregated_publisher (data, publisher, statname) VALUES (%s, %s, %s)", (json_string, publisher, aggregate_name))
            conn.commit()

    all_stats = stats_module.AllDataStats()
    all_stats.aggregated = total
    for name, function in inspect.getmembers(all_stats, predicate=inspect.ismethod):
        if not statsrunner.shared.use_stat(all_stats, name): continue
        total[name] = function()

    for aggregate_name,aggregate in total.items():
        json_string = json.dump(aggregate, sort_keys=True, indent=2, default=decimal_default)
        cur.execute("INSERT INTO aggregated (data, statname) VALUES (%s, %s)", (json_string, publisher, aggregate_name))
        conn.commit()

