import pymysql

from json import loads, load
from contextlib import closing

from src.db_connect import CONN, RECORDS_TABLE

GEO_COLUMNS = '`imei`,`lat`,`lon`,`datetime`,`type`,`speed`,`direction`,`bat`,`fuel`,'
GEO_COLUMNS += '`ignition`,`sensor`,`reserve`, `ts`'


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


def get_configuration(imei, d_model):
	with closing(pymysql.connect(**CONN)) as connection:
		query = f'SELECT `params`, `model` from `teltonika_config` WHERE `imei`={int(imei)}'
		with connection.cursor() as cursor:
			cursor.execute(query)
			x = cursor.fetchone()
			if not isinstance(x, dict):
				with open(f'tracker_receiver/src/protocols/Teltonika/configurations/{d_model}.json', 'r') as fd:
					params = load(fd)
					model = d_model

				ins_params = str(params).replace("'", '"')
				query = f"INSERT INTO `teltonika_config` VALUES ({int(imei)}, '{model}', '{ins_params}')"
				cursor.execute(query)
				connection.commit()

			else:
				params = loads(x['params'])
				model = x['model']

		return params


def insert_geo(data):
	with closing(pymysql.connect(**CONN)) as connection:
		count = 0
		for rec in data:
			query = f'INSERT INTO `{RECORDS_TABLE}` ({GEO_COLUMNS}) VALUES ('
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
