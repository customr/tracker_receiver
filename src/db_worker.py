import pymysql

from json import loads, load
from contextlib import closing
from time import time

from src.db_connect import CONN, RECORDS_TABLE

GEO_COLUMNS = '`imei`,`lat`,`lon`,`datetime`,`type`,`speed`,`direction`,`bat`,`fuel`,'
GEO_COLUMNS += '`ignition`,`sensor`,`reserve`, `ts`'

PROTOCOLS = (
	'teltonika',
	'wialon'
	)

PROTOCOLS_IDS = {k:v for k, v in zip(PROTOCOLS, range(1,len(PROTOCOLS)+1))}

conf_cache = {}
ign_cache = {}
upd_time_min = 20

def get_ignition_v(connection, imei):
	ign = ign_cache.get(str(imei))

	update = False
	if ign:
		if (time()-ign[1])/60>upd_time_min:
			update = True

	if not ign or update:
		query = f'SELECT `ignition_v` from `devices` WHERE `imei`={int(imei)}'
		with connection.cursor() as cursor:
			cursor.execute(query)
			try:
				ign_v = cursor.fetchone()['ignition_v']
			except Exception:
				return None

		ign_cache[str(imei)] = [ign_v, time()]
	else:
		ign_v, _ = ign_cache[str(imei)]

	return ign_v


def get_configuration(connection, protocol_name, imei, d_model=None):
	key = f'{protocol_name}{imei}'
	conf = conf_cache.get(key)

	update = False
	if conf:
		if (time()-conf[1])/60>upd_time_min:
			update = True

	if not conf or update:
		pid = PROTOCOLS_IDS[protocol_name.lower()]
		query = f'SELECT `settings`, `model` from `receiver_settings` WHERE `protocol`={pid} AND `imei`={int(imei)}'
		with connection.cursor() as cursor:
			cursor.execute(query)
			x = cursor.fetchone()
			if not isinstance(x, dict):
				if d_model:
					with open(f'tracker_receiver/src/protocols/{protocol_name}/configurations/{d_model}.json', 'r') as fd:
						settings = load(fd)
						model = d_model
				else:
					with open(f'tracker_receiver/src/protocols/{protocol_name}/default_configuration.json', 'r') as fd:
						settings = load(fd)
						model = None

				ins_settings = str(settings).replace("'", '"')
				if model:
					query = f"INSERT INTO `receiver_settings` VALUES ({pid}, {int(imei)}, '{model}', '{ins_settings}')"
				else:
					query = f"INSERT INTO `receiver_settings` VALUES ({pid}, {int(imei)}, NULL, '{ins_settings}')"
				
				cursor.execute(query)
				connection.commit()

			else:
				settings = loads(x['settings'])
				model = x['model']

		conf_cache[key] = [settings, time()]
	else:
		settings, _ = conf_cache[key]

	return settings


def insert_geo(connection, data, debug=False):
	count = 0
	for rec in data:
		if not debug:
			query = f'INSERT INTO `{RECORDS_TABLE}` ({GEO_COLUMNS}) VALUES ('
		else:
			query = f'INSERT INTO `geo_test` ({GEO_COLUMNS}) VALUES ('

		for name, value in rec.items():
			if name in ('datetime', 'reserve', 'ts'):
				query += f"'{value}',"
			else:
				query += f"{value},"

		query = query[:-1]
		query += ')'
		with connection.cursor() as cursor:
			try:
				cursor.execute(query)
				count += 1
			except Exception as e:
				with open('tracker_receiver/src/logs/errors.log', 'a') as fd:
					fd.write(f'Ошибка в mysql insert запросе {e}\n')

		connection.commit()

	return count