import pymysql

from src.db_connect import CONN


connection = pymysql.connect(**CONN)
imei = input('Введите imei нового трекера: ')
same_as = input('Введите imei трекера, с которого следует скопировать настройки: ')

query = f'SELECT `model`, `params` FROM `trackers_config` WHERE `imei`={int(same_as)}'
with connection.cursor() as cursor:
	cursor.execute(query)
	x = cursor.fetchone()
	params = x['params']
	model = x['model']

query = f'INSERT INTO `trackers_config` VALUES ({int(imei)}, {model}, {params})'
with connection.cursor() as cursor:
	cursor.execute(query)

connection.commit()
connection.close()
input('Новый трекер скопирован успешно.\nНажмите любую кнопку чтобы выйти...')