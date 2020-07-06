import pymysql

from src.db_connect import CONN


connection = pymysql.connect(**CONN)
try:
	while True:
		imei = input('Введите imei нового трекера (оставьте пустым чтобы выйти): ')
		if not imei: break
		same_as = input('Введите imei трекера, с которого следует скопировать настройки: ')
		
			query = f'SELECT `model`, `params` FROM `trackers_config` WHERE `imei`={int(same_as)}'
			with connection.cursor() as cursor:
				cursor.execute(query)
				x = cursor.fetchone()
				
				if not isinstance(x, dict):
					print('Трекер с таким imei не найден\n')
					continue

				params = x['params']
				model = x['model']

			query = f"INSERT INTO `trackers_config` VALUES ({int(imei)}, '{model}', '{params}')"
			with connection.cursor() as cursor:
				cursor.execute(query)

			connection.commit()
			connection.close()
			print('Новый трекер скопирован успешно\n')

except Exception as e:
	print(e)
	input()