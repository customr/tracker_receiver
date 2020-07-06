import pymysql

from json import loads

from src.db_connect import CONN, RECORDS_TABLE

GEO_COLUMNS = '`imei`,`lat`,`lon`,`datetime`,`type`,`speed`,`direction`,`bat`,`fuel`,'
GEO_COLUMNS += '`ignition`,`sensor`,`reserve`, `ts`'


def get_ignition_v(imei):
	with closing(pymysql.connect(**CONN)) as connection:
		query = f'SELECT `ignition_v` from `devices` WHERE `imei`={int(imei)}'
		with connection.cursor() as cursor:
			cursor.execute(query)
			ign_v = cursor.fetchone()['ignition_v']

		return ign_v


def get_configuration_and_model(imei):
	with closing(pymysql.connect(**CONN)) as connection:
		query = f'SELECT `params`, `model` from `trackers_config` WHERE `imei`={int(imei)}'
		with connection.cursor() as cursor:
			cursor.execute(query)
			x = cursor.fetchone()
			params = loads(x['params'])
			model = x['model']

		return params, model


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
					logger.info(f'Ошибка в mysql insert запросе {e}')

			connection.commit()

	return count