#!/usr/bin/env python
import sys

if sys.version_info[0] != 2 or sys.version_info[1] < 6:
	print("This script requires Python version 2.6 or 2.7")
	sys.exit(1)


import logging
import math
import numpy as np
import json
import csv
import os
from info_retriver import info_retriver
from flask import Flask, Response
from gurobipy import *
from datetime import datetime, timedelta

################# Helper Functions #################

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

'''
Retrieve the bv (inflow) information of a specific cpn on a specific node
	SS: huge positive inflow, assuming contract manufacturers have unlimited supply of parts 
	CM: as intermediate parts of the supply chain, has 0 inflow
	DD: inflow equal to -1 * quantity due on that day
	II: initial inventory on hand, must be positive
	OO: the sum of all open order quantity
	RI: inflow equal total net supply/demand of the model
'''
def bv_retrival(cpn, node, prod_propty, agg_demand_info):
	date = node[3:]
	if node[0:2] == 'CM':
		return 0
	elif node[0:2] == 'DF':
		return 0
	elif node[0:2] == 'SS':
		return 100000
	elif node[0:2] == 'OO':
		oo_sum = 0
		for arr_date in agg_oo_info.keys():
			if cpn in agg_oo_info[arr_date]:
				oo_sum += agg_oo_info[arr_date][cpn]
		return oo_sum
	elif node[0:2] == 'II':
		return prod_inv_info[cpn]['supply_OH']
	elif node[0:2] == 'BD':
		return -1 * prod_inv_info[cpn]['Backlog_Demand']
	elif node[0:2] == 'DD':
		try:
			return (-1 * agg_demand_info[date][cpn])
		except:
			return 0
	elif node[0:2] == 'RI':
		# quantity at the dummy 'RI' site would be the total supply - demand
		# supply: SS, II, OO
		# demand: prod_propty[cpn][total_demand] + prod_inv_info[cpn][Backlog_Demand]
		return -1 * (bv_retrival(cpn, 'SS_0000_00_00', prod_propty, agg_demand_info) +\
		bv_retrival(cpn, 'II_0000_00_00', prod_propty, agg_demand_info) +\
		bv_retrival(cpn, 'OO_0000_00_00', prod_propty, agg_demand_info) -\
		prod_inv_info[cpn]['total_demand_quantity'] -\
		prod_inv_info[cpn]['Backlog_Demand'])


def populate_init_inv(cpns, prod_inv_info, start_date, ir):

	retrival_failed_cpns = []

	for cpn in cpns:

		supply_OH = ir.inv_retrival(cpn, start_date)

		if supply_OH == ir.RETRIVAL_FAILED:
			logging.warning("skipping %s", cpn)
			retrival_failed_cpns.append(cpn)
			prod_inv_info.pop(cpn, None)
			prod_propty.pop(cpn, None)
			continue

		prod_inv_info[cpn]['initial_inventory'] = supply_OH
		prod_inv_info[cpn]['supply_OH'] = 0
		prod_inv_info[cpn]['Backlog_Demand'] = 0
		logging.debug("Initial Supply on hand: %d unit for %s", prod_inv_info[cpn]['supply_OH'], cpn)

	cpns = list(set(cpns).difference(retrival_failed_cpns))

	return (cpns, prod_inv_info)

def open_order_retrival(cpns, prod_inv_info, start_date, planning_horizon, ir):

	# see description of agg_demand_info
	agg_oo_info = {}
	for day_delta in range(planning_horizon + 1):
		arr_date = toDate(start_date, day_delta)
		agg_oo_info[arr_date] = {}

	for cpn in cpns:

		_, demand_info, open_order = ir.daily_info_retrival(cpn, start_date , planning_horizon)

		## populate open order information
		total_oo_quantity = 0
		for oo_point in open_order:
			arr_date = toDate(oo_point['date'])
			arr_quantity = oo_point['quantity']
			try:
				agg_oo_info[arr_date][cpn] += arr_quantity
				logging.debug("More than 1 opened order arriving on %s for %s", arr_date, cpn)
			except:
				agg_oo_info[arr_date][cpn] = arr_quantity
			total_oo_quantity += arr_quantity
		prod_inv_info[cpn]['total_oo_quantity'] = total_oo_quantity


	with open(output_folder_path + 'open_orders_init' + '.json', 'w') as outfile:
		json.dump(agg_oo_info, outfile, indent=4)

	return agg_oo_info


def demand_retrival(cpns, prod_inv_info, start_date, planning_horizon, ir):

	# agg_demand_info
	# a dict of dict, each dictionary correspond to the demand
	# of each product on a specific date
	
	'''
	agg_demand_info will be in the following format
	['2019_03_29': {'A': 20, 'B':100},
	 '2019_03_30': {}, 
	 '2019_03_31': {'B': 30}]
	to denote:
		20 units of A and 100 units of B due on March 29 2019
		no demand due on day March 30 2019
		30 units of B due on day March 31 2019
	'''

	agg_demand_info = {}
	for day_delta in range(planning_horizon + 1):
		due_date = toDate(start_date, day_delta)
		agg_demand_info[due_date] = {}


	for cpn in cpns:

		_, demand_info, open_order = ir.daily_info_retrival(cpn, start_date , planning_horizon)

		## populate demand information
		total_cpn_demand = 0
		for demand_point in demand_info:
			due_date = toDate(demand_point['date'])
			due_quantity = demand_point['quantity']
			try:
				agg_demand_info[due_date][cpn] += due_quantity
				logging.debug("More than 1 demand order found on %s for %s", due_date, cpn)
			except:
				agg_demand_info[due_date][cpn] = due_quantity
			total_cpn_demand += due_quantity
		prod_inv_info[cpn]['total_demand_quantity'] = total_cpn_demand


	return agg_demand_info


def create_model_schema(start_date, planning_horizon, prod_propty, cpns, transportation):

	## Create nodes
	# terminals contains 'CM', 'DF', 'DD' in each day
	supply_chain_terminals = ['CM', 'DF', 'DD']
	nodes = {terminal: [] for terminal in supply_chain_terminals}
	for day_delta in range(planning_horizon + 1):
		for terminal in supply_chain_terminals:
			nodes[terminal].append(''.join([terminal, '_', toDate(start_date, day_delta)]))

	# dummy terminals include 'Supply', 'OpenOrder_Supply', 'Initial_Inv', 'Rollover_Inv', 'Backlog_Demand'
	dummy_terminals = ['SS', 'OO', 'II', 'RI', 'BD']
	for terminal in dummy_terminals:
		nodes[terminal] = [''.join([terminal, '_0000_00_00'])]


	## Create links
	ship_modes = transportation.keys()
	all_links = tuplelist()
	links_by_type = {mode: tuplelist() for mode in ['SS_CM', 'DF_DD', 'CM_DF', 'II_DF', 'OO_DF', 'DF_DF', 'DD_DD', 'DF_RI', 'DD_RI', 'SS_RI', 'BD_DD']}


	# first day to last day
	for day_delta in range(planning_horizon + 1):
		for cpn in cpns:

			# 'Supply' to 'CM'
			source = 'SS_0000_00_00'
			dest = ''.join(['CM', '_', toDate(start_date, day_delta)])
			modeid = 'NONE'
			links_by_type['SS_CM'].append((cpn,source,dest,modeid))
			all_links.append((cpn,source,dest,modeid))

			# 'OO' to 'DF'
			# open orders: previous shipments that will arrive in planning horizon
			source = 'OO_0000_00_00'
			dest = ''.join(['DF', '_', toDate(start_date, day_delta)])
			modeid = 'NONE'
			links_by_type['OO_DF'].append((cpn,source,dest,modeid))
			all_links.append((cpn,source,dest,modeid))

			# 'DF' to 'DD'
			source = ''.join(['DF', '_', toDate(start_date, day_delta)])
			dest = ''.join(['DD', '_', toDate(start_date, day_delta)])
			modeid = 'NONE'
			links_by_type['DF_DD'].append((cpn,source,dest,modeid))
			all_links.append((cpn,source,dest,modeid))


	# first day to the day before the last day
	for day_delta in range(planning_horizon):
		for cpn in cpns:

			# 'DF' to 'DF'
			source = ''.join(['DF', '_', toDate(start_date, day_delta)])
			dest = ''.join(['DF', '_', toDate(start_date, day_delta + 1)])
			modeid = 'NONE'
			links_by_type['DF_DF'].append((cpn,source,dest,modeid))
			all_links.append((cpn,source,dest,modeid))

			# 'DD' to 'DD'
			source = ''.join(['DD', '_', toDate(start_date, day_delta)])
			dest = ''.join(['DD', '_', toDate(start_date, day_delta + 1)])
			modeid = 'NONE'
			links_by_type['DD_DD'].append((cpn,source,dest,modeid))
			all_links.append((cpn,source,dest,modeid))


	for cpn in cpns:

		# 'CM' to 'DF'
		for day_delta in range(planning_horizon):
			for mode in prod_propty[cpn]['cpn_modes']:
				outbound_date = start_date + timedelta(day_delta)
				arr_date = start_date + timedelta(day_delta + transportation[mode]['leadtime'])
				if outbound_date in transportation[mode]['shipment_schedule'] and \
					arr_date <= start_date + timedelta(planning_horizon):

					source = ''.join(['CM', '_', toDate(outbound_date)])
					dest = ''.join(['DF', '_', toDate(arr_date)])
					modeid = transportation[mode]['id']
					links_by_type['CM_DF'].append((cpn,source,dest,modeid))
					all_links.append((cpn,source,dest,modeid))

			# if mode in prod_propty[cpn]['cpn_modes']:
				# for day_delta in range(planning_horizon - transportation[mode]['leadtime'] + 1):
				# 	# add conditional statement to account for transportation schedule
				# 	source = ''.join(['CM', '_', toDate(start_date, day_delta)])
				# 	dest = ''.join(['DF', '_', toDate(start_date, day_delta + transportation[mode]['leadtime'])])
				# 	modeid = transportation[mode]['id']
				# 	links_by_type['CM_DF'].append((cpn,source,dest,modeid))
				# 	all_links.append((cpn,source,dest,modeid))


		# initial inventory
		# 'II' to 'DF'
		source = 'II_0000_00_00'
		dest = ''.join(['DF', '_', toDate(start_date)])
		modeid = 'NONE'
		links_by_type['II_DF'].append((cpn,source,dest,modeid))
		all_links.append((cpn,source,dest,modeid))

		# backlog demand
		# 'BD' to 'DD'
		source = 'BD_0000_00_00'
		dest = ''.join(['DD', '_', toDate(start_date)])
		modeid = 'NONE'
		links_by_type['BD_DD'].append((cpn,source,dest,modeid))
		all_links.append((cpn,source,dest,modeid))

		# excess inventory rollover to the next planning horizon
		# 'DF' to 'RI'
		source = ''.join(['DF', '_', toDate(start_date, planning_horizon)])
		dest = 'RI_0000_00_00'
		modeid = 'NONE'
		links_by_type['DF_RI'].append((cpn,source,dest,modeid))
		all_links.append((cpn,source,dest,modeid))

		# unmet demand rollver to the next planning horizon
		# 'DD' to 'RI'
		source = ''.join(['DD', '_', toDate(start_date, planning_horizon)])
		dest = 'RI_0000_00_00'
		modeid = 'NONE'
		links_by_type['DD_RI'].append((cpn,source,dest,modeid))
		all_links.append((cpn,source,dest,modeid))

		# collection excess dummy supply 
		# 'SS' to 'RI'
		source = 'SS_0000_00_00'
		dest = 'RI_0000_00_00'
		modeid = 'NONE'
		links_by_type['SS_RI'].append((cpn,source,dest,modeid))
		all_links.append((cpn,source,dest,modeid))


	return (nodes, all_links, links_by_type)



def shipment_planner(start_date, planning_horizon, model_schema,\
	agg_demand_info, agg_oo_info, prod_propty, cpns, transportation, \
	prod_inv_info, trans_weight = 0.1, ud_weight = 0.1, ss_weight = 0.7,\
	ei_weight = 0.1, runtime_time_limit=10):

	nodes, links, links_by_type = model_schema
	days = [toDate(start_date, day_delta) for day_delta in range(planning_horizon + 1)]



	## Gurobi Parameters Initialization

	m = Model()

	# decision variable flows will be indexed by flow[cpn, i, j]
	flow = m.addVars(links, vtype=GRB.INTEGER, name="flow")
	# for each palletize shipment method, create pallet number and container number variables
	palletized_modes = [mode for mode in transportation.keys() if transportation[mode]['palletized']]
	pallet = m.addVars(palletized_modes, cpns, days, vtype=GRB.INTEGER, name="pallet")
	container = m.addVars(palletized_modes, days, vtype=GRB.INTEGER, name="container")
	# varibles to ensure safety stock is not dipped into too much
	ss_gap = m.addVars(cpns, days, vtype=GRB.INTEGER, name="ss_gap")
	# variables to minimize excess inventory carry at the DF site
	exs_invt_val = m.addVars(cpns, days, vtype=GRB.INTEGER, name="excess_inventory_value")

	## constraints

	### flow conservation
	for terminal_type in ['CM', 'DF', 'SS', 'OO', 'II']:
		for node in nodes[terminal_type]:
			for cpn in cpns:
				total_outflow = sum(flow[cpn,node,j,modeid] for cpn,node,j,modeid in links.select(cpn, node, '*', '*'))
				total_inflow = sum(flow[cpn,j,node,modeid] for cpn,j,node,modeid in links.select(cpn, '*', node, '*'))
				node_bv = bv_retrival(cpn, node, prod_propty, agg_demand_info)
				m.addConstr(total_outflow - total_inflow == node_bv)

	for cpn in cpns:

		# flow conservation at 'BD' 
		node = 'BD_0000_00_00'
		out_unmet_demand = sum(flow[cpn,node,j,modeid] for cpn,node,j,modeid in links_by_type['BD_DD'].select(cpn, node, '*', '*'))
		node_bv = bv_retrival(cpn, node, prod_propty, agg_demand_info)
		m.addConstr(- out_unmet_demand == node_bv)

		# flow conservation at 'DD' nodes
		for node in nodes['DD']:
			in_unmet_demand = sum(flow[cpn,j,node,modeid] for cpn,j,node,modeid in links_by_type['DD_DD'].select(cpn,'*', node,'*')) +\
				sum(flow[cpn,j,node,modeid] for cpn,j,node,modeid in links_by_type['BD_DD'].select(cpn,'*', node, '*'))
			df_inflow = sum(flow[cpn,j,node,modeid] for cpn,j,node,modeid in links_by_type['DF_DD'].select(cpn,'*', node,'*'))
			out_unmet_demand = sum(flow[cpn,node,j,modeid] for cpn,node,j,modeid in links_by_type['DD_DD'].select(cpn, node, '*','*')) +\
				sum(flow[cpn,node,j,modeid] for cpn,node,j,modeid in links_by_type['DD_RI'].select(cpn, node,'*','*'))
			node_bv = bv_retrival(cpn, node, prod_propty, agg_demand_info)
			m.addConstr(- out_unmet_demand - df_inflow == node_bv - in_unmet_demand)

		# flow conservation at 'RI' node
		for node in nodes['RI']:
			in_unmet_demand = sum(flow[cpn,j,node,modeid] for cpn,j,node,modeid in links_by_type['DD_RI'].select(cpn,'*', node,'*'))
			in_df = sum(flow[cpn,j,node,modeid] for cpn,j,node,modeid in links_by_type['DF_RI'].select(cpn,'*', node,'*'))
			in_cm_excess_inv = sum(flow[cpn,j,node,modeid] for cpn,j,node,modeid in links_by_type['SS_RI'].select(cpn,'*', node,'*'))
			node_bv = bv_retrival(cpn, node, prod_propty, agg_demand_info)
			m.addConstr(in_cm_excess_inv + in_df == - (node_bv - in_unmet_demand))

	
	# container consolidation constrait
	for mode in palletized_modes:

		# required cpn pallet number = ceiling(unit/upp)
		modeid = transportation[mode]['id']
		for cpn in [cpn for cpn in cpns if mode in prod_propty[cpn]['cpn_modes']]:
			for day_delta in range(planning_horizon - transportation[mode]['leadtime'] + 1):
				shipment_release_date = toDate(start_date, day_delta)
				shipment_arr_date = toDate(start_date, day_delta + transportation[mode]['leadtime'])
				source = ''.join(['CM', '_', shipment_release_date])
				dest = ''.join(['DF', '_', shipment_arr_date])
				if links.select(cpn,source,dest,modeid):
					m.addConstr(pallet[mode,cpn,shipment_release_date] >= flow[cpn,source,dest,modeid]/prod_propty[cpn]['units_per_pallet'])
		
		# required container = ceiling(sum(required cpn pallet number))
		for day_delta in range(planning_horizon - transportation[mode]['leadtime'] + 1):
			shipment_release_date = toDate(start_date, day_delta)
			m.addConstr(container[mode,shipment_release_date] >= sum(pallet[mode,cpn,shipment_release_date] for cpn in cpns)\
					/transportation[mode]['pallets_per_container'])


	# open order constraint - ensure inventory in-transit match db records
	for day_delta in range(planning_horizon + 1):
		oo_arr_date = toDate(start_date, day_delta)
		src = 'OO_0000_00_00'
		dest = ''.join(['DF', '_', oo_arr_date])
		modeid = 'NONE'
		for cpn in cpns:
			if cpn in agg_oo_info[oo_arr_date].keys():
				m.addConstr(flow[cpn,src,dest,modeid] == agg_oo_info[oo_arr_date][cpn])
			else:
				m.addConstr(flow[cpn,src,dest,modeid] == 0)

	# set ss_gap be the difference between inventory level and pre-defined safety stock
	# set exs_invt_val to be the inventory level above ss multiplied by product value
	for day_delta in range(planning_horizon):
		inv_monitor_date = toDate(start_date, day_delta)
		src = ''.join(['DF', '_', inv_monitor_date])
		dest = ''.join(['DF', '_', toDate(start_date, day_delta+1)])
		modeid = 'NONE'
		for cpn in cpns:
			# ss_gap is guranteed non-negative by Gurobi
			m.addConstr(ss_gap[cpn, inv_monitor_date] >= prod_propty[cpn]['ss'] - flow[cpn,src,dest,modeid])
			# m.addConstr(exs_invt_val[cpn, inv_monitor_date] >= prod_propty[cpn]['product_value'] * (flow[cpn,src,dest,modeid] - prod_propty[cpn]['ss']))
			m.addConstr(exs_invt_val[cpn, inv_monitor_date] >= flow[cpn,src,dest,modeid] - prod_inv_info[cpn]['initial_inventory'])

	# Set and configure objective
	# Objectvie1: Minimize Overall Transportation Cost
	by_weight_modes = set(transportation.keys()).difference(palletized_modes)
	trans_obj = None;
	for mode in by_weight_modes:
		modeid = transportation[mode]['id']
		for cpn,i,j,modeid in links.select('*','*','*',modeid):
			trans_obj += flow[cpn,i,j,modeid] * prod_propty[cpn]['weight'] * transportation[mode]['cost']
	
	for mode in palletized_modes:
		trans_obj += container.sum(mode,'*') * transportation[mode]['cost']
	m.setObjectiveN(trans_obj, 0, weight=trans_weight)

	# Objectvie2: Minimize Total Delay Quantity
	ud_obj = None;
	for cpn,i,j,modeid in links_by_type['DD_DD'] + links_by_type['DD_RI']:
		ud_obj += 30.0 * flow[cpn,i,j,modeid]
	m.setObjectiveN(ud_obj, 1, weight=ud_weight)

	# Objectvie3: Prevent inventory levels fall below safety stock
	ss_obj = ss_gap.sum() * 5.0
	m.setObjectiveN(ss_obj, 2, weight=ss_weight)


	# Objectvie4: Prevent inventory levels from going through the roof
	exs_invt_obj = exs_invt_val.sum() * 1.0
	m.setObjectiveN(exs_invt_obj, 3, weight=ei_weight)

	m.ModelSense = GRB.MINIMIZE
	m.setParam('LogFile', "")
	# m.setParam('outputFlag', 0)
	m.setParam('TimeLimit', runtime_time_limit)
	m.optimize()

	if m.status != GRB.INFEASIBLE:
		flow_sol = m.getAttr('X', flow)
		pallet_sol = m.getAttr('X', pallet)
		container_sol = m.getAttr('X', container)

		return dict(flow_sol)

	return GRB.INFEASIBLE
				

def update_invt_info(date, cpns, planning_horizon, \
			flows, model_schema, agg_oo_info, prod_inv_info, output_folder_path):

	_,all_links,links_by_type = model_schema

	# collect today's action
	# update agg_oo_info

	# delete order already arrived
	agg_oo_info.pop(toDate(date), None)

	daily_snapshots = {cpn:{} for cpn in cpns}
	
	# for each action item of today, append to the open order
	today_CM = ''.join(['CM', '_', toDate(date)])
	for cpn in cpns:
		for cpn,src,dest,modeid in all_links.select(cpn,today_CM,'*','*'):
			if flows[(cpn,src,dest,modeid)] > 0:
				arr_date = get_date_from_node(dest)
				if arr_date not in agg_oo_info.keys():
					agg_oo_info[arr_date] = {}
					agg_oo_info[arr_date][cpn] = flows[(cpn,src,dest,modeid)]
				else:
					if cpn not in agg_oo_info[arr_date].keys():
						agg_oo_info[arr_date][cpn] = flows[(cpn,src,dest,modeid)]
					else:
						agg_oo_info[arr_date][cpn] += flows[(cpn,src,dest,modeid)]

				prod_inv_info[cpn]['total_oo_quantity'] += flows[(cpn,src,dest,modeid)]

				daily_snapshots[cpn].setdefault('cm_actions', []).append((src,dest,modeid,flows[(cpn,src,dest,modeid)]))



	# update supply on-hand quantity (all incoming flow to DF)
	#  by subtracting packout
	today_DF = ''.join(['DF', '_', toDate(date)])
	for cpn in cpns:

		# supply_to_DF = sum([flows[(cpn,src,dest,modeid)] \
		# 	for cpn,src,dest,modeid in all_links.select(cpn,'*',today_DF,'*')])
		previous_rollover = sum([flows[(cpn,src,dest,modeid)] for\
			cpn,src,dest,modeid in links_by_type['II_DF'].select(cpn,'*',today_DF, '*')])

		oo_arrival = sum([flows[(cpn,src,dest,modeid)] for\
			cpn,src,dest,modeid in links_by_type['OO_DF'].select(cpn,'*',today_DF, '*')])

		supply_to_DF = previous_rollover + oo_arrival

		plan_packout_qty = ir.packout_qty_retrival(cpn, date)
		

		if supply_to_DF < plan_packout_qty:
			prod_inv_info[cpn]['Backlog_Demand'] = plan_packout_qty - supply_to_DF
			prod_inv_info[cpn]['supply_OH'] = 0
			ir.packout_qty_update(cpn, date + timedelta(1), prod_inv_info[cpn]['Backlog_Demand'])
		else:
			prod_inv_info[cpn]['Backlog_Demand'] = 0
			prod_inv_info[cpn]['supply_OH'] = supply_to_DF - plan_packout_qty


		daily_snapshots[cpn]['previous_rollover'] = previous_rollover
		daily_snapshots[cpn]['oo_arrival'] = oo_arrival
		daily_snapshots[cpn]['supply_before_packout'] = previous_rollover + oo_arrival
		daily_snapshots[cpn]['planned_packout_qty'] = plan_packout_qty
		daily_snapshots[cpn]['actual_packout_qty'] = min(plan_packout_qty, supply_to_DF)
		daily_snapshots[cpn]['new_OH'] = prod_inv_info[cpn]['supply_OH']
		daily_snapshots[cpn]['Backlog_Demand'] = prod_inv_info[cpn]['Backlog_Demand']

		# record what the model is expecting to come, in the open order info
		oo_arrival_info = []
		for day_delta in range(1, planning_horizon):
			arr_date = toDate(date, day_delta)
			if cpn in agg_oo_info[arr_date].keys():
				oo_arrival_info.append((arr_date, agg_oo_info[arr_date][cpn]))
		daily_snapshots[cpn]['all_oo_info'] = oo_arrival_info

	# add new entries in agg_oo_info
	# as the scheduling horizon slides forward

	agg_oo_info[toDate(date, planning_horizon + 1)] = {}

	with open(output_folder_path + 'snapshot_' +\
		toDate(date) + '.json', 'w') as outfile:
		json.dump(daily_snapshots, outfile, indent=4)


	# with open(output_folder_path + 'open_orders_' +\
	# 	toDate(date) + '.json', 'w') as outfile:
	# 	json.dump(agg_oo_info, outfile, indent=4)

	return (agg_oo_info, prod_inv_info)




if __name__ == '__main__':


	output_folder_path = './outputs/' + datetime.now().strftime("%Y-%m-%d_%H_%M") + '/'
	os.mkdir(output_folder_path)

	# # set logger basic config
	# logging.getLogger('Shipment Planner')
	# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s')

	eval_start_date = datetime(year=2018, month=10, day=28) # start date of evaluation
	planning_horizon = 90 # furthest demand point considered
	playback_duration = 90 # number of days to evaluate
	db_name = 'Q2_Interplant_Sample'
	#db_collection = 'Q2_Interplant_total'
	db_collection = 'Q2_Top_Lanes'
	ir = info_retriver(db_name, db_collection)


	transportation = ir.lane_info_retrival()

	# static product properties including
	# standard cost, weight, unit per pallet, shipment modes
	prod_propty = ir.prod_propty_retrival(None)
	cpns = prod_propty.keys()	


	# dynamic product inventory information that changes daily
	prod_inv_info = {cpn:{} for cpn in cpns}

	### Initialization

	# define objective functions weight 
	obj_weights = {'trans_weight': 0.03, 'ud_weight': 0.17, 'ss_weight': 0.6, 'ei_weight': 0.2}

	with open(output_folder_path + 'model_metadata.txt', 'w') as outfile:
		outfile.write("####### Model Objective Weights #######\n")
		for weight in obj_weights.keys():
			outfile.write("		%s: %f\n" % (weight, obj_weights[weight]))


	# populate initial inventory level
	#prod_inv_info = populate_init_inv(cpns, prod_inv_info, eval_start_date, ir)
	cpns, prod_inv_info = populate_init_inv(cpns, prod_inv_info, eval_start_date, ir)

	# populate initial open orders
	agg_oo_info = open_order_retrival(cpns, prod_inv_info, eval_start_date, planning_horizon, ir)


	for day_delta in range(playback_duration):

		date = eval_start_date + timedelta(day_delta)
			
		# pull demand from database
		agg_demand_info = demand_retrival(cpns, prod_inv_info, date, planning_horizon, ir)

		# with open(output_folder_path + 'prod_inv.json', 'w') as outfile:
		# 	json.dump(prod_inv_info, outfile, indent=4)
		for cpn in prod_inv_info.keys():
			if 'supply_OH' not in prod_inv_info[cpn].keys():
				logging.warning("%s doesn't have initial supply_OH", cpn)

		# create model schema of the day
		model_schema = create_model_schema(date, planning_horizon,\
			prod_propty, cpns, transportation)
	
		# MCF Optimizer
		opt_result = shipment_planner(date, planning_horizon, model_schema,\
			agg_demand_info, agg_oo_info, prod_propty, cpns, transportation, \
			prod_inv_info, trans_weight = obj_weights['trans_weight'], ud_weight = obj_weights['ud_weight'],\
			ss_weight = obj_weights['ss_weight'], ei_weight = obj_weights['ei_weight'], runtime_time_limit=150)

		# update latest inventory on hand based on packout quantity
		# update open order records
		agg_oo_info, prod_inv_info = update_invt_info(date, cpns, planning_horizon, \
			opt_result, model_schema, agg_oo_info, prod_inv_info, output_folder_path)
