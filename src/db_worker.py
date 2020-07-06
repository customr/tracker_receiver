import pymysql

from src.db_connect import CONN, RECORDS_TABLE


connection = pymysql.connect(**CONN)

def get_ignition_v(imei):
	query = f'SELECT `ignition_v` from `devices` WHERE `imei`={int(imei)}'
	with connection.cursor() as cursor:
		cursor.execute(query)
		ign_v = cursor.fetchone()['ignition_v']

	return ign_v


def get_configuration_and_model(imei):
	query = f'SELECT `params`, `model` from `trackers_config` WHERE `imei`={int(imei)}'
	with connection.cursor() as cursor:
		cursor.execute(query)
		x = cursor.fetchone()
		params = x['params']
		model = x['model']

	return params, model


def insert_geo(data):
	for rec in data:
		query = f'INSERT INTO `{RECORDS_TABLE}` VALUES ('
		for value in rec.values():
			query += f'{value},'

		query = query[:-1]
		query += ')'
		with connection.cursor() as cursor:
			cursor.execute(query)

		connection.commit()