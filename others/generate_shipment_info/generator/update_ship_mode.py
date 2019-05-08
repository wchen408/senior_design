import json
import csv
import string
import re
from datetime import datetime, timedelta
import dateutil.parser


'''
Given a start date and end date, find all weekdays between
weekdays should be a list.
For example, find_weekday_between(2018.10.28, 2019.1.25, [0,2,4])
finds all the Monday, Wednesday, Friday in between
'''
def find_weekday_between(start_date, end_date, weekdays):
	all_weekdays = []

	for wd in weekdays:
		dt = start_date

		# First weekday of the given period       
		dt += timedelta(wd - dt.weekday() if wd - dt.weekday() >= 0 else 7 - (dt.weekday() - wd))
		while dt <= end_date:
			all_weekdays.append(dt.isoformat())
			dt += timedelta(days = 7)

	all_weekdays = sorted(all_weekdays)

	return all_weekdays

'''
convert raw shipment information csv into 
a json file readable by the MCNF object
'''
def generate_lane_shipmodes(start_date, end_date,
	raw_file_path='Interplant_Rates_April_2019_raw.csv'):

	lane_rates = {}
	with open(raw_file_path) as csv_file:
		reader = csv.DictReader(csv_file)

		for row in reader:

			# retrieve info from row
			# stripe any white space
			origin = row['Ship_From_Org'].replace(' ', '')
			dest = row['Ship_To_Org'].replace(' ', '')
			carrier = row['Partner'].replace(' ', '')
			mode = row['Mode'].replace(' ', '')
			Service_Level = row['Service_Level'].replace(' ', '')
			rate = float(re.compile(r'[^\d\.]+').sub('', row['Rate']))
			leadtime = int(row['Transit'])

			if row['Service_Level'] == '40FT':
				pallets_per_container = 20
			elif row['Service_Level'] == '20FT':
				pallets_per_container = 10
			else:
				pallets_per_container = 0



			lane_key = ''.join([origin, '_', dest])

			shipment_full_name = ''.join([origin, '_', dest,
				'_', carrier, '_', mode, '_', Service_Level])


			if lane_key in lane_rates.keys():
				lane_rates[lane_key][shipment_full_name] = {
							'palletized': False if mode == 'Air' else True,
							'shipment_schedule': find_weekday_between(start_date, end_date, range(7)) 
								if mode == 'Air' else find_weekday_between(start_date, end_date, [0,2,4]),
							'cost': rate,
							'pallets_per_container': pallets_per_container,
							'leadtime': leadtime,
							'id': ''.join([carrier, mode[:3], Service_Level[:3]]).upper()
						}
			else:
				lane_rates[lane_key] = {}
				lane_rates[lane_key][shipment_full_name] = {
							'palletized': False if mode == 'Air' else True,
							'shipment_schedule': find_weekday_between(start_date, end_date, range(7)) 
								if mode == 'Air' else find_weekday_between(start_date, end_date, [0,2,4]),
							'cost': rate,
							'pallets_per_container': pallets_per_container,
							'leadtime': leadtime,
							'id': ''.join([carrier, mode[:3], Service_Level[:3]]).upper()
						}

	for orig_dest_pair in lane_rates.keys():
		with open('shipment_info_' + orig_dest_pair + '.json', 'w') as outfile:
			json.dump(lane_rates[orig_dest_pair], outfile, indent=4)



start_date = datetime(year=2019, month=1, day=26)
playback_duration = 90
end_date = start_date + timedelta(playback_duration)

generate_lane_shipmodes(start_date, end_date)
