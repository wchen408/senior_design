#!/usr/bin/env python
import sys

if sys.version_info[0] != 2 or sys.version_info[1] < 6:
    print("This script requires Python version 2.6 or 2.7")
    sys.exit(1)

import pymongo
import dateutil.parser
import json
import csv
import re
import math
import logging
from datetime import datetime, timedelta
from pprint import pprint


class info_retriver():

	def __init__(self, db_name, coll_name, db_ip='localhost', db_port=27017, log_lvl=logging.DEBUG):


		self.RETRIVAL_FAILED = -1
		self.INIT_MODE_ASSIGN_FILE = 'inputs/init_mode_FDO_FGU_FY19Q2.csv'
		self.TAN_PACKOUT_FILE = 'inputs/FY19Q2_PACKOUT_TOPLANES.csv'
		self.SHIPMENT_INFO_FILE = 'inputs/shipment_info_FDO_FGU.json'

		# logger config
		logging.getLogger('Info Retriever')
		logging.basicConfig(stream=sys.stdout, level=logging.INFO,\
			format='%(asctime)s %(levelname)-5s %(message)s')

		# Database connection init
		self.client = pymongo.MongoClient('localhost', 27017)
		self.db = self.client[db_name]
		self.coll = self.db[coll_name]

		# retrieve origin and destination for this mcnf 
		# and store as class variable for pulling
		# inventory and packout information
		self.get_orig_dest()

		# TEMP Func: read packout csv file
		self.read_packout_csv()

		# trigger lane_info_retrival ht 
		self.lane_info_retrival()

	'''
	retrieve origin and destination for this mcnf 
	and store as class variable for pulling
	inventory and packout information
	'''
	def get_orig_dest(self):

		with open(self.INIT_MODE_ASSIGN_FILE) as csv_file:
			reader = csv.DictReader(csv_file)
			for row in reader:
				self.orig = row['Origin'].upper()
				self.dest = row['Destination'].upper()
				break

	'''
	TEMP FUNCTION
	read from packout csv file and construct dictionary
	'''
	def read_packout_csv(self):

		self.packout = {}

		with open(self.TAN_PACKOUT_FILE) as csv_file:
			csv_reader = csv.reader(csv_file)
			for row in csv_reader:
				org, cpn, date, quantity = row
				date = self.toDate(datetime.strptime(date, "%m/%d/%Y"))

				# ignore record if packout doesn't belong to the target
				# inventoryOrg

				if org.upper() != self.orig or org.upper() != self.dest:
					continue

				# some quantities are ill formatted
				# strip illegal char and convert to integer
				quantity = int(re.compile(r'[^\d]+').sub('', quantity))

				if date in self.packout.keys():
					if cpn in self.packout[date].keys():
						self.packout[date][cpn] += quantity
					else:
						self.packout[date][cpn] = quantity
				else:
					self.packout[date] = {}
					self.packout[date][cpn] = quantity

	'''
	helper function
	convert datetime object to date in strftime('%Y_%m_%d')
	'''
	def toDate(self, date_time_obj, date_increment = 0):
		return (date_time_obj + timedelta(date_increment)).strftime('%Y_%m_%d')


	'''
	Pull lane specific information
	'''
	def lane_info_retrival(self):
		# Parse Json to retrive shipment information
		with open(self.SHIPMENT_INFO_FILE) as shipment_f:
			data = json.load(shipment_f)

		self.air_modes = []
		self.ocean_modes = [] 
		for mode in data.keys():
			data[mode]['shipment_schedule'] = [dateutil.parser.parse(d) for d in data[mode]['shipment_schedule']]
			if data[mode]['palletized'] == False:
				self.air_modes.append(mode)
			else:
				self.ocean_modes.append(mode)

		self.transportation = data

		return data


	'''
	Given a CPN
	get its inventory on hand on a specific date
	'''
	def inv_retrival(self, cpn, date):

		# retrieve initial supply on hand as the sum of siteOH and hubOH
		# from the initial inventory snapshot
		init_date = int(date.strftime('%Y%m%d'))
		init_inv_ss = self.coll\
			.find({'itemNumber': cpn, 'version': init_date, 'inventoryOrg': self.dest}, {'siteOH': 1, 'hubOH': 1, 'supply': 1})\
			.sort('version', pymongo.ASCENDING)

		# if snapshot on the day doesn't exist, retreive the closet snapshot before that
		if init_inv_ss.count() == 0:

			logging.debug("Initial inventory snapshot on %d missing for %s,\
				resorting to the latest previous record", init_date, cpn)

			init_inv_ss = self.coll\
				.find({'itemNumber': cpn, 'inventoryOrg': self.dest, 'version': {"$lt": init_date}}, {'siteOH': 1, 'hubOH': 1})\
				.sort('version', pymongo.DESCENDING)

			if init_inv_ss.count() == 0:
				logging.warning("Could not find initial inventory snapshot for %s", cpn)
				return -1


		# sort based on version number, discard non-latest snapshots
		latest_init_ss = sorted(init_inv_ss, key = lambda ss:ss['_id'])[-1]
		return latest_init_ss['siteOH'] + latest_init_ss['hubOH']


	'''
	Given a CPN and date
	Retrieve
		1. packout quantity
		2. demand forecast within the planning horizon
		3. open orders within the planning horizon
	'''
	def daily_info_retrival(self, cpn, date, planning_horizon):

		date_to_int = int(date.strftime('%Y%m%d'))
		inv_ss = self.coll\
			.find({'itemNumber': cpn, 'inventoryOrg': self.dest, 'version': date_to_int}, {'supply': 1, 'currentWeekPackOuts': 1, 'demand': 1})\
			.sort('version', pymongo.ASCENDING)


		# if snapshot on the day doesn't exist, retreive the closet snapshot before that
		if inv_ss.count() == 0:

			logging.debug("Inventory snapshot on %d missing for %s,\
				resorting to the latest previous record", date_to_int, cpn)

			inv_ss = self.coll\
				.find({'itemNumber': cpn, 'inventoryOrg': self.dest, 'version': {"$lt": date_to_int}}, {'supply': 1, 'currentWeekPackOuts': 1, 'demand': 1})\
				.sort('version', pymongo.DESCENDING)

		if inv_ss.count() == 0:
			logging.warning("Could not find inventory snapshot for %s", cpn)
			return -1

		# sort based on version number, discard non-latest snapshots
		latest_init_ss = sorted(inv_ss, key = lambda ss:ss['_id'])[-1]
		
		## get actual packout quantity
		packout_quantity = 0 
		# for each_packout in latest_init_ss['currentWeekPackOuts']:
		# 	if each_packout['date'] == date:
		# 		packout_quantity = each_packout['date']
		# logging.warning("Retrieved packout quantity for %s on %s: %d unit(s).", cpn, self.toDate(date), packout_quantity)

		## get demand forecast
		demand_info = []
		for demand_point in latest_init_ss['demand']:
			if demand_point['date'] >= date and\
				demand_point['date'] <= (date + timedelta(planning_horizon)):
				if demand_point['quantity'] > 0:
					demand_info.append({'date': demand_point['date'], 'quantity': max(int(demand_point['quantity']),0)})
		logging.debug("Retrieved %d demand point for %s from %s to %s", len(demand_info),\
			cpn, self.toDate(date), self.toDate(date, planning_horizon))

		
		# open order informatin (inventory in transit)
		open_order = []
		try:
			furtherest_supply_projection = max([self.transportation[mode]['leadtime'] for mode in self.transportation])
		except:
			furtherest_supply_projection = 30

		for supply_point in latest_init_ss['supply']:
			if supply_point['date'] >= date and\
				supply_point['date'] <= (date + timedelta(furtherest_supply_projection)):
				open_order.append({'date': supply_point['date'], 'quantity': int(supply_point['quantity'])})
		logging.debug("Retrieved %d open orders for %s from %s to %s", len(open_order),\
			cpn, self.toDate(date), self.toDate(date, furtherest_supply_projection))

		return packout_quantity, demand_info, open_order

	def packout_qty_retrival(self, cpn, date):
		try:
			return self.packout[self.toDate(date)][cpn]
		except:
			return 0

	def packout_qty_update(self, cpn, date, addition):

		if self.toDate(date) in self.packout.keys():
			if cpn in self.packout[self.toDate(date)].keys():
				self.packout[self.toDate(date)][cpn] += addition
			else:
				self.packout[self.toDate(date)][cpn] = addition
		else:
			self.packout[self.toDate(date)] = {}
			self.packout[self.toDate(date)][cpn] = addition



	'''
	retrieve static product information
	including weight, standard cost, upp, cpn_modes
	'''
	def prod_propty_retrival(self, cpns):

		product_info = {}

		with open(self.INIT_MODE_ASSIGN_FILE) as csv_file:
			reader = csv.DictReader(csv_file)
			for row in reader:
				cpn = row['TAN']
				weight = round(float(row['Weight_per_Unit(kg)']),2)

				try:
					value = round(float(row['Standard_Cost']),2)
				except Exception as e:
					logging.warning("Missing property(s) for %s: %s, skipping...", cpn, str(e))
					continue

				try:
					upp = math.floor(float(row['Units_per_Carton']) \
						* float(row['Cartons_per_Pallet']))
				except:
					upp = 100000

				if row['Preferred_Mode'] == 'Air':
					ss = math.floor(float(row['Air_Standard_Safety_Stock']))
					cpn_modes = self.air_modes
				else:
					ss = math.floor(float(row['Ocean_Standard_Safety_Stock']))
					cpn_modes = self.air_modes + self.ocean_modes

				product_info[cpn] = {}
				product_info[cpn]['cpn_modes'] = cpn_modes
				product_info[cpn]['units_per_pallet'] = upp
				product_info[cpn]['product_value'] = value
				product_info[cpn]['weight'] = weight
				product_info[cpn]['ss'] = ss

		return product_info


	def __del__(self):
		self.client.close()

