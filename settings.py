import os
from configparser import RawConfigParser, ConfigParser
import oracledb

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ----------------------------------------
# Sensitive settings into another location /etc/
# ----------------------------------------
config = RawConfigParser()
mypath = "/gis" # path to .ini files
# OS
config.read([os.path.join(BASE_DIR , r'JPPH/%s/config.conf' % mypath)])
# windows
# config.read(r"C:\Users\Administrator\Documents\Aiman\process_data_gis\gis\config.conf")

# ----------------------------------------------------------
# Databases settings
# ----------------------------------------------------------

NVIS_SOURCE_USER = config.get('NVIS', 'USERNAME')
NVIS_SOURCE_PWD = config.get('NVIS', 'PWD')
NVIS_SOURCE_HOST = config.get('NVIS', 'HOST')
NVIS_SOURCE_SERVICE_NAME = config.get('NVIS', 'SERVICE_NAME')
NVIS_SOURCE_PORT = config.get('NVIS', 'PORT')
NVIS_DICT = {
    "USER": NVIS_SOURCE_USER,
    "PASSWORD": NVIS_SOURCE_PWD,
    "HOST": NVIS_SOURCE_HOST,
    "SERVICE_NAME": NVIS_SOURCE_SERVICE_NAME,
    "PORT": NVIS_SOURCE_PORT
}

GOMPB_SOURCE_USER = config.get('GOMPB', 'USERNAME')
GOMPB_SOURCE_PWD = config.get('GOMPB', 'PWD')
GOMPB_SOURCE_HOST = config.get('GOMPB', 'HOST')
GOMPB_SOURCE_SERVICE_NAME = config.get('GOMPB', 'SERVICE_NAME')
GOMPB_SOURCE_PORT = config.get('GOMPB', 'PORT')
GOMPB_SOURCE_DSN = config.get('GOMPB', 'DSN')
GOMPB_DICT = {
    "USER": GOMPB_SOURCE_USER,
    "PASSWORD": GOMPB_SOURCE_PWD,
    "HOST": NVIS_SOURCE_HOST,
    "SERVICE_NAME": NVIS_SOURCE_SERVICE_NAME,
    "PORT": NVIS_SOURCE_PORT,
    "DSN": GOMPB_SOURCE_DSN
}

GOMPB_DEV_SOURCE_USER = config.get('GOMPB_DEV', 'USERNAME')
GOMPB_DEV_SOURCE_PWD = config.get('GOMPB_DEV', 'PWD')
GOMPB_DEV_SOURCE_HOST = config.get('GOMPB_DEV', 'HOST')
GOMPB_DEV_SOURCE_SERVICE_NAME = config.get('GOMPB_DEV', 'SERVICE_NAME')
GOMPB_DEV_SOURCE_PORT = config.get('GOMPB_DEV', 'PORT')
GOMPB_DEV_SOURCE_DSN = config.get('GOMPB_DEV', 'DSN')
GOMPB_DEV_DICT = {
    "USER": GOMPB_DEV_SOURCE_USER,
    "PASSWORD": GOMPB_DEV_SOURCE_PWD,
    "HOST": GOMPB_DEV_SOURCE_HOST,
    "SERVICE_NAME": GOMPB_DEV_SOURCE_SERVICE_NAME,
    "PORT": GOMPB_DEV_SOURCE_PORT,
    "DSN": GOMPB_DEV_SOURCE_DSN
}

processes = [
    'TV_TRX_LOT',
    'TV_TRX_LOT_NEWT',
    'TV_LABEL_LOT_A',
    'TV_TEMATIK_A',
    'TV_TX_STRATA'
]

# SELECT column_name(s)
# FROM table_name
# ORDER BY column_name(s)
# FETCH FIRST number ROWS ONLY;
