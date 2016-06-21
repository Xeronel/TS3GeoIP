#!/usr/bin/python3.4
from tornado import ioloop, httpclient
import pandas as pd
import progressbar
import json
import re


clients = set()
ip_addresses = None
geoloc = []
bar = None


def get_clients():
    global clients
    global ip_addresses
    global bar
    r = re.compile(r"client\sconnected\s'(.+)'\(id:[0-9]+\)\sfrom\s(\d+\.\d+\.\d+\.\d+)+")

    with open('tslog.txt') as f:
        tslog = f.read()
    tslog = tslog.split('\n')

    for line in tslog:
        client = r.search(line)
        if client is not None:
            clients.add(client.groups())
    clients = pd.DataFrame(list(clients), columns=['name', 'ip'])
    ip_addresses = clients['ip'].unique()
    bar = progressbar.ProgressBar(
            maxval=len(ip_addresses),
            widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    bar = bar.start()


def handle_request(request):
    global geoloc
    global bar
    geoloc.append(json.loads(request.body.decode('utf-8', 'ignore')))
    bar.update(len(geoloc))

    # Stop IO loop once all ip's have been read
    if len(geoloc) == len(ip_addresses):
        ioloop.IOLoop.current().stop()


def load_ips():
    global ip_addresses
    global geoloc
    global bar
    httpclient.AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
    s = httpclient.AsyncHTTPClient()
    for ip in ip_addresses:
        s.fetch('http://freegeoip.net/json/%s' % ip, handle_request)
    ioloop.IOLoop.current().start()
    bar.finish()
    geoloc = pd.DataFrame(geoloc)


# Load client list
get_clients()

# Fetch all geolocation data
load_ips()

# Merge clients and geolocation data
df = pd.merge(clients, geoloc, on='ip')
df.to_csv('geoloc.csv')
