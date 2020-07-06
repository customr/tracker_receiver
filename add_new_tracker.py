import pymysql

from src.db_connect import CONN


model = 'fmb910'
conf = {
	"Digital Input 1": "ignition",
	"Digital Input 2": "sensor"
}
conf = str(conf).replace("'", '"')
connection = pymysql.connect(**CONN)

try:
	while True:
		imei = input('Введите imei нового трекера (оставьте пустым чтобы выйти): ')
		if not imei: break
		query = f"INSERT INTO `trackers_config` VALUES ({int(imei)}, '{model}', '{conf}')"
		with connection.cursor() as cursor:
			cursor.execute(query)

		connection.commit()
		print('Успешно добавлен\n')

	connection.close()

except Exception as e:
	print(e)
	connection.close()
	input()