import pymysql

from json import loads, load
from contextlib import closing

from src.db_connect import CONN, RECORDS_TABLE

GEO_COLUMNS = '`imei`,`lat`,`lon`,`datetime`,`type`,`speed`,`direction`,`bat`,`fuel`,'
GEO_COLUMNS += '`ignition`,`sensor`,`reserve`, `ts`'

PROTOCOLS = (
	'teltonika',
	'wialon'
	)

PROTOCOLS_IDS = {k:v for k, v in zip(PROTOCOLS, range(1,len(PROTOCOLS)+1))}

def get_ignition_v(imei):
	with closing(pymysql.connect(**CONN)) as connection:
		query = f'SELECT `ignition_v` from `devices` WHERE `imei`={int(imei)}'
		with connection.cursor() as cursor:
			cursor.execute(query)
			try:
				ign_v = cursor.fetchone()['ignition_v']
			except Exception:
				return None

		return ign_v


def get_configuration(protocol_name, imei, d_model=None):
	with closing(pymysql.connect(**CONN)) as connection:
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
					query = f"INSERT INTO `receiver_settings` VALUES ({int(imei)}, '{model}', '{ins_settings}')"
				else:
					query = f"INSERT INTO `receiver_settings` VALUES ({int(imei)}, NULL, '{ins_settings}')"
				
				cursor.execute(query)
				connection.commit()

			else:
				params = loads(x['settings'])
				model = x['model']

		return params


def insert_geo(data, debug=False):
	with closing(pymysql.connect(**CONN)) as connection:
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