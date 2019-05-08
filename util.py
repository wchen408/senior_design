from datetime import datetime, timedelta


'''
convert datetime object to date in strftime('%Y_%m_%d')
'''
def toDate(date_time_obj, date_increment = 0):
	return (date_time_obj + timedelta(date_increment)).strftime('%Y_%m_%d')



'''
given a string of 'XX_YYYY_MM_DD'
return strftime('%Y_%m_%d')
'''
def get_date_from_node(node):
	return node[3:]