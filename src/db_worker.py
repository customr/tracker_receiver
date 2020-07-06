import pymysql

from copy import deepcopy
from contextlib import closing

from src.db_connect import CONN


def get_ignition_v(connection, imei):
	query = f'SELECT `ignition_v` from `devices` WHERE `imei`={int(imei)}'
	with connection.cursor() as cursor:
		cursor.execute(query)
		ign_v = cursor.fetchone()['ignition_v']

	return ign_v


if __name__=="__main__":
	with closing(pymysql.connect(**CONN)) as connection:
		print(get_ignition_v(connection, ))