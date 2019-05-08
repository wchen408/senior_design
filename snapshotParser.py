#!/usr/bin/env python
import sys

if sys.version_info[0] != 2 or sys.version_info[1] < 6:
    print("This script requires Python version 2.6 or 2.7")
    sys.exit(1)

import json
import re
import math
import os
import csv
import copy
import logging
from info_retriver import info_retriver
from pprint import pprint
from os import listdir
from datetime import datetime, timedelta
from matplotlib import pyplot as plt
from os.path import isfile, join
from bson import json_util


class snapshotParser(object):

	def __init__(self, path, filename='SIM_RESULT_SUMMARY'):

		# logger config
		logging.getLogger('snapshot Parser')
		logging.basicConfig(stream=sys.stdout, level=logging.INFO,\
			format='%(asctime)s %(levelname)-5s %(message)s')

		self.path = path
		self.outfile = filename
		db_name = 'Q2_Interplant_Sample'
		db_collection = 'Q2_Interplant_total'
		self.ir = info_retriver(db_name, db_collection)
		self.prod_propty = self.ir.prod_propty_retrival(None)
		self.transportation = self.ir.lane_info_retrival()

		self.__find_snapshots_files(self.path)
		self.__get_sim_inv_action()


	'''
	Given a modeid, output the shipment mode fullname
	'''
	def __what_mode(self, modeid):
		for mode in self.transportation.keys():
			if modeid == self.transportation[mode]['id']:
				return mode
	'''
	convert datetime object to date in strftime('%Y_%m_%d')
	'''
	def __toDate(self, date_time_obj, date_increment = 0):
		return (date_time_obj + timedelta(date_increment)).strftime('%Y_%m_%d')



	'''
	Given a file path, returns the list of inventory snapshot names
	'''
	def __find_snapshots_files(self, path):

		self.inv_snapshot_json_files = []

		# identify all snapshot files in the path
		for file in listdir(path):
			if isfile(join(path, file)) and file[:8] == 'snapshot'\
				and file[-4:] == 'json':
				self.inv_snapshot_json_files.append(''.join([path, file]))

		#retrieve the cpn list from one of the snapshot json
		with open(self.inv_snapshot_json_files[0]) as inv_file:
			self.cpns = json.load(inv_file).keys()

		if len(self.inv_snapshot_json_files) == 0:
			logging.warning("Empty directory, existing...")
			sys.exit(-1)


		self.inv_snapshot_json_files.sort()

		# get start date from filename
		file = self.inv_snapshot_json_files[0]
		self.start_date = datetime.strptime(file[len(path)+len('snapshot_'):-5], '%Y_%m_%d')
		# get simulated period from the number of files
		self.playback_period = len(self.inv_snapshot_json_files)

		logging.info("Retrieved %d inventory snapshots since %s, with %d unique CPNs.",\
			self.playback_period, self.start_date.strftime('%Y-%m-%d'), len(self.cpns))

	'''
	Parse inventory snapshots and record simulated inventory and CM actions
	Query MongoDB for actual inventory info
	'''
	def __get_sim_inv_action(self):

		# An array of (date, inv_level) pair for each CPN
		self.sim_inv_lvl = {cpn:[] for cpn in self.cpns}
		self.act_inv_lvl = copy.deepcopy(self.sim_inv_lvl)

		# An array of ((date, sim_inv_lvl), cm_actions) for each CPN
		# if no cm action is taken, there won't be an entry corresponding
		# to that date
		self.sim_cm_actions = copy.deepcopy(self.sim_inv_lvl)
		self.cm_actions_by_date = {self.__toDate(self.start_date, i):[] for i in range(self.playback_period)}

		# service level for each tan is
		# (num days where actual_packout == planned_packout) / (num days != 0)
		# self.service_lvl_tan = {cpn:float() for cpn in self.cpns}


		for file in self.inv_snapshot_json_files:

			with open(file) as inv_file:
				cpn_snapshots = json.load(inv_file)

			date = datetime.strptime(file[len(self.path)+len('snapshot_'):-5], '%Y_%m_%d')

			logging.info("Start processing snapshot on %s.", date.strftime('%Y-%m-%d'))

			for cpn in self.cpns:
				self.sim_inv_lvl[cpn].append(cpn_snapshots[cpn]['new_OH'])
				self.act_inv_lvl[cpn].append(self.ir.inv_retrival(cpn, date))

				if 'cm_actions' in cpn_snapshots[cpn].keys():
					actions = []
					for action in cpn_snapshots[cpn]['cm_actions']:
						src,dest,modeid,quantity = action
						actions.append((modeid,quantity))
					
					# each item in the list contain (date, inventory_lvl, actions)
					# inventory level is stored for plot annotation
					self.sim_cm_actions[cpn].append(((date, cpn_snapshots[cpn]['new_OH']), actions))
					self.cm_actions_by_date[self.__toDate(date)].append((cpn, (modeid,quantity)))

	'''
	Given the simulated and actual inventory level,
	calculate the additional worth of inventory carried at DF site
	'''
	def cal_addi_inv_at_DF(self, by_tan_filename='ADDI_INV_TAN.csv'):

		addi_inv_by_tan = {cpn:0 for cpn in self.cpns}

		for cpn in self.cpns:
			avg_sim_inv_lvl = sum(self.sim_inv_lvl[cpn]) / len(self.sim_inv_lvl[cpn])
			avg_act_inv_lvl = sum(self.act_inv_lvl[cpn]) / len(self.act_inv_lvl[cpn])
			addi_inv_by_tan[cpn] = round((avg_sim_inv_lvl - avg_act_inv_lvl) * self.prod_propty[cpn]['product_value'], 2)

		tlt_addi_inv_carried = round(sum([addi_inv_by_tan[cpn] for cpn in self.cpns]), 2)


		# write additional inventory by tan to file
		with open(self.path + by_tan_filename, 'w') as csv_file:
			writer = csv.writer(csv_file, delimiter=',', lineterminator = '\n')
			writer.writerow(['CPN', 'ADDI_INV_VAL'])
			for cpn in self.cpns:
				writer.writerow((cpn, addi_inv_by_tan[cpn]))

		logging.info("%s created at:\n	%s", by_tan_filename, self.path)

		with open(self.path + self.outfile, 'a') as out_file:
			out_file.write("\n\n####### Inventory impact #######\n\n")
			out_file.write("Additional Worth of Inventory Carried: %.2f\n" % tlt_addi_inv_carried)

		logging.info("Gross Inventory Impact appended to file:\n	%s", self.path + self.outfile)

	'''
	Create inventory plots with actual and simulated inventory level,
	safety stock level and simulated CM actions taken.
	'''
	def plot_inv(self, width=25, height=10, folder='inv_plots'):

		plot_folder = self.path + folder + '/'

		try:
			os.mkdir(plot_folder)
		except:
			None

		#x-axis for time 
		x = [self.start_date + timedelta(i) for i in range(self.playback_period)]

		for cpn in self.cpns:

			# define canvas
			plt.figure(figsize=(width,height), dpi=100)

			# safety stock
			ss = [self.prod_propty[cpn]['ss']] * len(x)
			plt.plot(x, ss, label="Safety Stock Level")

			plt.plot(x, self.act_inv_lvl[cpn], label="Cisco Inv Lvl")
			plt.plot(x, self.sim_inv_lvl[cpn], label="Simulated Inv Lvl")

			## mark simulated CM actions in the graph
			action_dates = [a[0][0] for a in self.sim_cm_actions[cpn]]
			action_dates_inv_lvl = [a[0][1] for a in self.sim_cm_actions[cpn]]
			plt.scatter(action_dates, action_dates_inv_lvl, color='red', marker='o')
			for action_day in self.sim_cm_actions[cpn]:
				date = action_day[0][0]
				date_inv_lvl = action_day[0][1]
				action = action_day[1]
				# convert a list of tuple to string, strip square bracket
				annoatate_text = str(action).strip('[]')
				plt.annotate(annoatate_text, (date, date_inv_lvl))
			
			plt.tight_layout()
			plt.legend()
			plt.savefig(''.join([plot_folder, cpn, '.png']))
			plt.cla()	# clear axis
			plt.clf()   # Clear figure
			plt.close() # Close a figure window

		logging.info("%d inventory plots created at:\n	%s", len(self.cpns), plot_folder)

	'''
	Calculate transportation cost and weight, gross and by tan
	'''
	def cal_trans_cost_weight(self, by_tan_filename='TRANS_BY_TAN.csv'):

		trans_cost_by_mode = {mode:0 for mode in self.transportation.keys()}
		trans_weight_by_mode = {mode:0 for mode in self.transportation.keys()}

		trans_cost_by_tan = {cpn: copy.deepcopy(trans_cost_by_mode) for cpn in self.cpns}
		trans_weight_by_tan = copy.deepcopy(trans_cost_by_tan)
		trans_unit_by_tan = copy.deepcopy(trans_cost_by_tan)
		trans_value_by_tan = copy.deepcopy(trans_cost_by_tan)


		for sim_date in self.cm_actions_by_date.keys():

			## total pallets being shipped out on day t
			pallets = {mode:0 for mode in self.transportation.keys() if self.transportation[mode]['palletized']}

			## the number of pallets required by a cpn on day t
			pallets_by_tan = {mode: {cpn: 0 for cpn in self.cpns} for mode in self.transportation.keys() if self.transportation[mode]['palletized']}

			for cpn in self.cpns:

				# select all actions correspond to CPN
				# action is a tuple (cpn, (modeid, quantity))
				cpn_action_on_date = [action[1] for action in self.cm_actions_by_date[sim_date] if action[0] == cpn]

				if cpn_action_on_date:

					for modeid,quantity in cpn_action_on_date:
						mode = self.__what_mode(modeid)
						if not self.transportation[mode]['palletized']:
							# non palletized weight
							trans_cost_by_mode[mode] += self.transportation[mode]['cost'] * \
								self.prod_propty[cpn]['weight'] * quantity
							trans_cost_by_tan[cpn][mode] += self.transportation[mode]['cost'] * \
								self.prod_propty[cpn]['weight'] * quantity
						else:
							# palletize shipment pallet count
							pallets_by_tan[mode][cpn] = math.ceil(float(quantity)/self.prod_propty[cpn]['units_per_pallet'])
							pallets[mode] += pallets_by_tan[mode][cpn]

						trans_weight_by_mode[mode] += self.prod_propty[cpn]['weight'] * quantity

						trans_weight_by_tan[cpn][mode] += self.prod_propty[cpn]['weight'] * quantity
						trans_unit_by_tan[cpn][mode] += quantity
						trans_value_by_tan[cpn][mode] += quantity * self.prod_propty[cpn]['product_value']

			# sum the total number of pallets shipped out on day t, and find the portion
			# ascribe to one tan
			for mode in pallets.keys():
				# if any pallet get shipped out on day t
				if pallets[mode]:
					total_container_cost = math.ceil(pallets[mode]/self.transportation[mode]['pallets_per_container'])\
						* self.transportation[mode]['cost']
					trans_cost_by_mode[mode] += total_container_cost
					for cpn in self.cpns:
						trans_cost_by_tan[cpn][mode] += total_container_cost * (pallets_by_tan[mode][cpn]/pallets[mode])


		with open(self.path + by_tan_filename, 'w') as csv_file:
			writer = csv.writer(csv_file, delimiter=',', lineterminator = '\n')
			writer.writerow(['CPN', 'MODE', 'VALUE', 'SPEND', 'KG', 'UNIT'])
			for cpn in self.cpns:
				for mode in self.transportation.keys():
					if trans_cost_by_tan[cpn][mode]:
						writer.writerow((cpn, mode, round(trans_value_by_tan[cpn][mode],2),\
							round(trans_cost_by_tan[cpn][mode],2), trans_weight_by_tan[cpn][mode],\
							trans_unit_by_tan[cpn][mode]))

		logging.info("%s created at:\n	%s", by_tan_filename, self.path)


		with open(self.path + self.outfile, 'a') as out_file:
			out_file.write("\n\n####### Total Transportation Cost #######\n\n")
			for mode in self.transportation.keys():
				out_file.write("		$%s: %.2f\n" % (mode, trans_cost_by_mode[mode]))

			ocean_modes = pallets.keys()
			air_modes = [mode for mode in self.transportation.keys() if mode not in ocean_modes]

			air_total_cost = sum([trans_cost_by_mode[mode] for mode in air_modes])
			ocean_total_cost = sum([trans_cost_by_mode[mode] for mode in ocean_modes])

			out_file.write("Total Air: $%.2f\n" % air_total_cost)
			out_file.write("Total Ocean: $%.2f\n" % ocean_total_cost)
			out_file.write("Air:Ocean = %.2f:%.2f\n" % (float(air_total_cost)/(air_total_cost + ocean_total_cost),\
				float(ocean_total_cost)/(air_total_cost + ocean_total_cost)))

		logging.info("Gross transportation cost appended to file:\n	%s", self.path + self.outfile)


		with open(self.path + self.outfile, 'a') as out_file:
			out_file.write("\n\n####### Total Weight Info #######\n\n")
			for mode in self.transportation.keys():
				out_file.write("		%s: %.2fKG\n" % (mode, trans_weight_by_mode[mode]))

			air_total_weight = sum([trans_weight_by_mode[mode] for mode in air_modes])
			ocean_total_weight = sum([trans_weight_by_mode[mode] for mode in ocean_modes])

			out_file.write("Total Air: %.2fKG\n" % air_total_weight)
			out_file.write("Total Ocean: %.2fKG\n" % ocean_total_weight)
			out_file.write("Air:Ocean = %.2f:%.2f\n" % (float(air_total_weight)/(air_total_weight + ocean_total_weight),\
				float(ocean_total_weight)/(air_total_weight + ocean_total_weight)))

		logging.info("Gross Weight cost appended to file:\n	%s", self.path + self.outfile)


# !!! Must have backward slash after the folder name
file_path = '/Users/Tony/Desktop/mcnf/outputs/2019-04-25_17_30/'

sp = snapshotParser(file_path)
#sp.plot_inv(folder='inv_plots')
sp.cal_addi_inv_at_DF()
sp.cal_trans_cost_weight()