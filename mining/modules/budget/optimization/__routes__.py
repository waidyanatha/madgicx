#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from datetime import datetime, date, timedelta
import pandas
import findspark
findspark.init()   # make sure to set SPARK_HOME environment    
from pyspark.sql import DataFrame

__desc__ = "rebalancing the weighted portfolio"
from flask import Blueprint, jsonify

etp = Blueprint("etp", __name__)

from mining.modules.crypto.etp import rwRDB as rdbm
clsSDB = rdbm.dataWorkLoads(desc=__desc__)

# @getTopN.route('/')
@etp.route('/select/assets', methods=['GET', 'POST'])
def select_assets():
    return 'select top N assets!'

@etp.route('/select/wap', methods=['GET', 'POST'])
def select_wap():
    return 'build weighted asset portfolio!'

@etp.route('/move/wap', methods=['POST'])
def move_wap():
    return 'Move WAP from nosl to sql!'

# @etp.route('/rebalance/wap', methods=['POST'])
# def rebalance():
#     return 'rebalance history!'

@etp.route('/rebalance/read', methods=['GET'])
def rebalance():
    
    sdf = clsSDB.read_realm(
        realm = 'rebalance',
        from_date=date(2022,5,1),
        to_date = date(2022,5,10),
        tbl_cols=['reb_date', # rebalanced date
                  'summary',
                  'reb_wap_price', # weighted price
                  'prev_wap_new_price', # past rebalance weighted price
                  'prev_wap_new_roi',
                 ]
    )
    pdf = sdf.toPandas()
    pdf.rename(columns={
        'reb_date': 'rebalanced date',
        'summary' : 'summary',
        'reb_wap_price' :'weighted price',
        'prev_wap_new_roi':'past weighted price'},
               inplace=True)
    _msg = f"rebalance data from {str(date(2022,5,1))} to {str(date(2022,5,10))}"
    ret_dict = {"result":pdf.to_json(orient="records"),
                "status":200,
                "message":_msg}
    return jsonify(ret_dict)
    # return 'read json object of rebalance history from to dates!'
