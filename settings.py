import os
import platform
from configparser import RawConfigParser, ConfigParser
import oracledb

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# set this according to current deployment environment
ENVIRONMENT = "development" # Accepted development, staging or production

# ----------------------------------------
# Sensitive settings into another location /etc/
# ----------------------------------------
config = RawConfigParser()
mypath = "/gis" # path to .ini files
# OpenSUSE Leap
# config.read(r"/home/jpph-dev/GIS/process_data_gis/gis/config.conf")
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

GOMPB_SST_QAS_SOURCE_USER = config.get('GOMPB_SST_QAS', 'USERNAME')
GOMPB_SST_QAS_SOURCE_PWD = config.get('GOMPB_SST_QAS', 'PWD')
GOMPB_SST_QAS_SOURCE_HOST = config.get('GOMPB_SST_QAS', 'HOST')
GOMPB_SST_QAS_SOURCE_SERVICE_NAME = config.get('GOMPB_SST_QAS', 'SERVICE_NAME')
GOMPB_SST_QAS_SOURCE_PORT = config.get('GOMPB_SST_QAS', 'PORT')
GOMPB_SST_QAS_SOURCE_DSN = config.get('GOMPB_SST_QAS', 'DSN')
GOMPB_SST_QAS_DICT = {
    "USER": GOMPB_SST_QAS_SOURCE_USER,
    "PASSWORD": GOMPB_SST_QAS_SOURCE_PWD,
    "HOST": GOMPB_SST_QAS_SOURCE_HOST,
    "SERVICE_NAME": GOMPB_SST_QAS_SOURCE_SERVICE_NAME,
    "PORT": GOMPB_SST_QAS_SOURCE_PORT,
    "DSN": GOMPB_SST_QAS_SOURCE_DSN
}

GOMPB_SST_DEV_SOURCE_USER = config.get('GOMPB_SST_DEV', 'USERNAME')
GOMPB_SST_DEV_SOURCE_PWD = config.get('GOMPB_SST_DEV', 'PWD')
GOMPB_SST_DEV_SOURCE_HOST = config.get('GOMPB_SST_DEV', 'HOST')
GOMPB_SST_DEV_SOURCE_SERVICE_NAME = config.get('GOMPB_SST_DEV', 'SERVICE_NAME')
GOMPB_SST_DEV_SOURCE_PORT = config.get('GOMPB_SST_DEV', 'PORT')
GOMPB_SST_DEV_SOURCE_DSN = config.get('GOMPB_SST_DEV', 'DSN')
GOMPB_SST_DEV_DICT = {
    "USER": GOMPB_SST_DEV_SOURCE_USER,
    "PASSWORD": GOMPB_SST_DEV_SOURCE_PWD,
    "HOST": GOMPB_SST_DEV_SOURCE_HOST,
    "SERVICE_NAME": GOMPB_SST_DEV_SOURCE_SERVICE_NAME,
    "PORT": GOMPB_SST_DEV_SOURCE_PORT,
    "DSN": GOMPB_SST_DEV_SOURCE_DSN
}

CONNECTION_STRING = None
CONNECTION_DICT = None

if ENVIRONMENT == "development":
    CONNECTION_DICT = GOMPB_SST_DEV_DICT
    CONNECTION_STRING = f'oracle+oracledb://{GOMPB_SST_DEV_DICT["USER"]}:{GOMPB_SST_DEV_DICT["PASSWORD"]}@{GOMPB_SST_DEV_DICT["HOST"]}:{GOMPB_SST_DEV_DICT["PORT"]}/?service_name={GOMPB_SST_DEV_DICT["SERVICE_NAME"]}'
elif ENVIRONMENT == "staging":
    CONNECTION_DICT = GOMPB_SST_QAS_DICT
    CONNECTION_STRING = f'oracle+oracledb://{GOMPB_SST_QAS_DICT["USER"]}:{GOMPB_SST_QAS_DICT["PASSWORD"]}@{GOMPB_SST_QAS_DICT["HOST"]}:{GOMPB_SST_QAS_DICT["PORT"]}/?service_name={GOMPB_SST_QAS_DICT["SERVICE_NAME"]}'
elif ENVIRONMENT == "production":
    CONNECTION_DICT = GOMPB_DICT
    CONNECTION_STRING = f'oracle+oracledb://{GOMPB_DICT["USER"]}:{GOMPB_DICT["PASSWORD"]}@{GOMPB_DICT["HOST"]}:{GOMPB_DICT["PORT"]}/?service_name={GOMPB_DICT["SERVICE_NAME"]}'

processes = [
    'TV_TRX_LOT',
    'TV_TRX_LOT_NEWT',
    'TV_LABEL_LOT_A',
    'TV_TEMATIK_A',
    'TV_TX_STRATA'
]

d = None  # default suitable for Linux
if platform.system() == "Darwin" and platform.machine() == "arm64":   # macOS arm64 @ x86_64
  d = os.environ.get("HOME")+("/Downloads/instantclient_23_3")
elif platform.system() == "Windows":
  d = r"C:\oracle\instantclient_19_18"
oracledb.init_oracle_client(lib_dir=d)

# SELECT column_name(s)
# FROM table_name
# ORDER BY column_name(s)
# FETCH FIRST number ROWS ONLY;
