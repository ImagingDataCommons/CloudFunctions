import pymysql
import pathlib
import functions_framework
import requests
import sys
import os
import tempfile
from requests.auth import HTTPBasicAuth
from os import getenv

# SOLR connection params

# Not a secret:
SOLR_QUERY = getenv('SOLR_QUERY')

# Secrets. Secrets uploaded from a text file might have \n appended if not careful. PEM HAS TO have \n characters!

SOLR_IP = getenv('SOLR_IP').replace("\n", "")
SOLR_PORT = getenv('SOLR_PORT').replace("\n", "")
SOLR_USER = getenv('SOLR_USER').replace("\n", "")
SOLR_PWD = getenv('SOLR_PWD').replace("\n", "")
SOLR_PEM = getenv('SOLR_PEM')

# MySQL connection params (secrets):

CONNECTION_NAME = getenv("CONNECTION_NAME").replace("\n", "")
DB_USER = getenv("DB_USER").replace("\n", "")
DB_PASSWORD = getenv("DB_PASSWORD").replace("\n", "")
DB_NAME = getenv("DB_NAME").replace("\n", "")

#
# live response:
#

CHECK_LIVE='"status":0,'

#
# MySQL connection setup:
#

mysql_config_for_cloud_functions = {
    'unix_socket': f'/cloudsql/{CONNECTION_NAME}',
    'user': DB_USER,
    'password': DB_PASSWORD,
    'db': DB_NAME,
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

#
# Get connection to database:
#

def get_db_connection():
    connection = pymysql.connect(**mysql_config_for_cloud_functions)
    return connection

#
# Get the latest version of solr cores from the DB:
#

def get_latest_version():
    try:
        connection = get_db_connection()
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = 'CALL get_current_solr_cores();'
            cursor.execute(sql)
            version_list = cursor.fetchall()
        for ver in version_list:
            ver_name = ver["name"]
            if "dicom_derived_study_v" in ver_name:
                return ver_name
        print("SOLR PING DATABASE ERROR: no current version")
        return None
    except Exception as e:
        print("SOLR PING DATABASE ERROR:", str(e))
        return None

def call_solr(latest_core):
    #
    # We *have* to provide a filesystem path to do verification. Do this to a tmp file (available
    # in cloud functions):
    #
    response_head = None
    with tempfile.NamedTemporaryFile(mode='w+t', delete_on_close=False) as fp:
        fp.write(SOLR_PEM)
        fp.close()

        request_url = f'https://{SOLR_IP}:{SOLR_PORT}/solr/{latest_core}/query?{SOLR_QUERY}'
        auth_tuple = HTTPBasicAuth(SOLR_USER, SOLR_PWD)
        try:
            response = requests.get(request_url, auth=auth_tuple, verify=fp.name, timeout=5)
            response_head = response.text[:140].replace("\n", "").replace(" ", "")
            print(response_head)
            if not CHECK_LIVE in response_head:
                print("SOLR PING ERROR: no live response")
                return None
        except Exception as e:
            print("SOLR PING ERROR:", str(e))
            return None

    return response_head

@functions_framework.http
def solr_check(request):
    latest_core = get_latest_version()
    if latest_core is None:
        print("CANNOT check SOLR PING")
        return {"code": 200, "message": "SolrPing could not check."}
    print(latest_core)
    response = call_solr(latest_core)
    if response is not None:
        print("SOLR PING Server is alive")
    else:
        print("SOLR PING Server is not responding")
    return {"code": 200, "message": "SolrPing ran successfully."}
