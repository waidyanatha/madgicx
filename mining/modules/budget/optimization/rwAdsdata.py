#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "rwRDB"
__package__= "etp"
__module__ = "crypto"
__app__ = "mining"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import os
    import sys
    import configparser    
    import logging
    import traceback
    import functools
    import findspark
    findspark.init()
    from pyspark.sql import functions as F
    from pyspark.sql import DataFrame
    from pyspark.sql.types import *
    from pyspark.sql.window import Window
    from datetime import datetime, date, timedelta

    from mining.modules.crypto.etp import __propAttr__ as attr

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))

'''
    CLASS configure the master property details, groups, reference, and geom entities

    Contributors:
        * nuwan.waidyanatha@rezgateway.com

    Resources:

'''

class dataWorkLoads(attr.properties):

    ''' Function --- INIT ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def __init__(
        self, 
        desc,
#         f_store_mode:str= None,   # set the file storage mode
#         f_store_root:str= None,   # set the root folder (bucket)
#         f_jar_dir : str = None,   # set the spark jar file dir path
        **kwargs):
        """
        Decription:
            Initializes the features: class property attributes, app configurations, 
                logger function, data store directory paths, and global classes 
        Attributes:
            desc (str) identify the specific instantiation and purpose
        Returns:
            None
        """

        ''' instantiate property attributes '''
        super().__init__(desc="read write RDB property class init")

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        if desc is None or "".join(desc.split())=="":
            self.__desc__ = " ".join([self.__app__,self.__module__,
                                      self.__package__,self.__name__])
        else:
            self.__desc__ = desc


        global pkgConf  # this package configparser class instance
        global appConf  # configparser class instance
        global logger   # rezaware logger class instance
#         global clsSDB   # etl loader sparkRDB class instance

        __s_fn_id__ = f"{self.__name__} function <__init__>"

        try:
#             ''' instantiate property attributes '''
#             super().__init__(self.__desc__)

            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,__ini_fname__))

            self.rezHome = pkgConf.get("CWDS","PROJECT")
            sys.path.insert(1,self.rezHome)

            ''' initialize the logger '''
            from rezaware.utils import Logger as logs
            logger = logs.get_logger(
                cwd=self.rezHome,
                app=self.__app__, 
                module=self.__module__,
                package=self.__package__,
                ini_file=self.__ini_fname__)

            ''' set a new logger section '''
            logger.info('########################################################')
            logger.info("%s Class",self.__name__)

#             ''' import spark RDBM work load utils to read and write data '''
#             from rezaware.modules.etl.loader import sparkRDBM as db
#             clsSDB = db.dataWorkLoads(
#                 desc=self.__desc__,
#                 db_type = 'PostgreSQL',
#                 db_driver=None,
#                 db_hostIP=None,
#                 db_port = None,
#                 db_name = 'property',
#                 db_schema='curated',
#                 db_user = 'rezaware',
#                 db_pswd = 'rezHERO',
#                 spark_partitions=None,
#                 spark_format = 'jdbc',
#                 spark_save_mode=None,
#                 spark_jar_dir = None,
#             )

            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))

            print("%s Class initialization complete" % self.__name__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return None


    ''' Function --- READ REALM ---

            author: <samana.thetha@gmail.com
    '''
    def read_realm(
        self,
        realm : str = None, # mandatory either portfolio, indicator, rebalance, or marketcap
        from_date:date = None, # datetime.date value of the start date
        to_date : date = None, # datetime.date value of the end date
        tbl_cols:list = [], # optional list of table columns 
        assets : list = [], # optional list of assets to filter by
        ids : list = [], # optional list of unique ids
        **kwargs
    ) -> DataFrame:
        """
        Description:
            Read the realm specific data for a given date range from rdbm
        Attributes :
            realm (str) mandatory either portfolio, indicator, rebalance, or marketcap
            from_date (date) datetime.date value of the start date
            to_date (date) datetime.date value of the end date
            assets (list) optional list of assets to filter by
            tbl_cols (list) optional list of table columns 
            **kwargs (dict) with optional key value pairs to change the defaults
        """
        __s_fn_id__ = f"{self.__name__} function <read_realm>"

        ''' defaulat database attributes '''
#         __def_asset_grp_id__="asset_grp_id"
#         __def_db_name__ = "tip"
        __def_db_schema__="warehouse"
        ''' default timedelta, if from_date is None '''
        __def_timedelta__ = 1   # days
        ''' deafulat [p]ortfolio attributes '''
        __def_p_tbl__ ="weighted_portfolio"
        __def_p_date__ = "wap_date"
        __def_p_asset__="asset_name"
        __def_p_id__ = "wap_id"
        ''' defaulat [i]ndicator attributes '''
        __def_i_tbl__= "tech_analysis"
        __def_i_date__ ="ta_date"
        ''' defaulat [r]ebalance attributes '''
        __def_r_tbl__= "rebalance_history"
        __def_r_date__ ="reb_date"
        ''' default [m]cap attributes '''
        __def_m_tbl__ = "mcap_past"
        __def_m_date__= "mcap_date"
        __def_m_asset__="asset_name"

        try:
            self.realm= realm

            ''' DATES validation, function inputs '''
            if not isinstance(to_date, date):
                to_date = date.today()
                logger.warning("%s unspecified to_date, setting to current date %s", 
                               __s_fn_id__, to_date)
#             if not isinstance(from_date, date):
#                 from_date = to_date - timedelta(days=__def_timedelta__)
#                 logger.warning("%s unspecified from_date, setting as %s days=%d from to_date %s", 
#                                __s_fn_id__, from_date, __def_timedelta__, to_date)
            if isinstance(from_date,date) and from_date > to_date:
                raise AssertionError("from_date: %s must be <= to_date: %s" 
                                     % (from_date, to_date))
            ''' DATABASE attribute validate & set kwargs '''
#             if "DBNAME" not in kwargs.keys() or "".join(kwargs['DBNAME'].split())=="":
#                 kwargs['DBNAME'] = __def_db_name__
            if "DBSCHEMA" not in kwargs.keys() or "".join(kwargs['DBSCHEMA'].split())=="":
                kwargs['DBSCHEMA'] = __def_db_schema__

            ''' PORTFOLIO attribute validate & set kwargs '''
            if self.realm == 'portfolio':
                ''' --- TABLE --- '''
                if "TBLNAME" not in kwargs.keys() or "".join(kwargs['TBLNAME'].split())=="":
                    kwargs["TBLNAME"] = __def_p_tbl__
                if "DATEATTR" not in kwargs.keys() or "".join(kwargs['DATEATTR'].split())=="":
                    kwargs["DATEATTR"] = __def_p_date__
                if "ASSETATTR" not in kwargs.keys() or "".join(kwargs['ASSETATTR'].split())=="":
                    kwargs["ASSETATTR"] = __def_p_asset__
                if "IDATTR" not in kwargs.keys() or "".join(kwargs['IDATTR'].split())=="":
                    kwargs["IDATTR"] = __def_p_id__

            ''' INDICATOR attribute validate & set kwargs '''
            if self.realm == 'indicator':
                ''' --- TABLE --- '''
                if "TBLNAME" not in kwargs.keys() or "".join(kwargs['TBLNAME'].split())=="":
                    kwargs["TBLNAME"] = __def_i_tbl__
                if "DATEATTR" not in kwargs.keys() or "".join(kwargs['DATEATTR'].split())=="":
                    kwargs["DATEATTR"] = __def_i_date__

            ''' REBALANCE attribute validate & set kwargs '''
            if self.realm == 'rebalance':
                ''' --- TABLE --- '''
                if "TBLNAME" not in kwargs.keys() or "".join(kwargs['TBLNAME'].split())=="":
                    kwargs["TBLNAME"] = __def_r_tbl__
                if "DATEATTR" not in kwargs.keys() or "".join(kwargs['DATEATTR'].split())=="":
                    kwargs["DATEATTR"] = __def_r_date__

            ''' MARKETCAP attribute validate & set kwargs '''
            if self.realm == 'marketcap':
                ''' --- TABLE --- '''
                if "TBLNAME" not in kwargs.keys() or "".join(kwargs['TBLNAME'].split())=="":
                    kwargs["TBLNAME"] = __def_m_tbl__
                if "DATEATTR" not in kwargs.keys() or "".join(kwargs['DATEATTR'].split())=="":
                    kwargs["DATEATTR"] = __def_m_date__
                if "ASSETATTR" not in kwargs.keys() or "".join(kwargs['ASSETATTR'].split())=="":
                    kwargs["ASSETATTR"] = __def_m_asset__

            ''' validate and set query asset string '''
            _s_ids_qry = ""
            if isinstance(ids,list) and len(ids)>0:
                _s_ids_qry = "', '".join(ids)
                _s_ids_qry = f"AND {kwargs['IDATTR']} IN ('{_s_ids_qry}') "
            ''' validate and set query asset string '''
            _s_asset_qry = ""
            if isinstance(assets,list) and len(assets)>0:
                _s_asset_qry = "', '".join(assets)
                _s_asset_qry = f"AND {kwargs['ASSETATTR']} IN ('{_s_asset_qry}') "
            ''' validate and apply date query constraint '''
            _s_date_qry =""
            if isinstance(from_date,date):
                _s_date_qry =f"AND {kwargs['DATEATTR']} BETWEEN '{from_date}' AND '{to_date}' "
            else:
                _s_date_qry =f"AND {kwargs['DATEATTR']} <= '{to_date}' "
            ''' validate and define columns to select from table '''
            if not isinstance(tbl_cols,list) or len(tbl_cols)<=0:
                tbl_cols = ['*']

            _query = ""
            _query+=f"SELECT {', '.join(tbl_cols)} FROM {kwargs['DBSCHEMA']}.{kwargs['TBLNAME']} "
#             _query+=f"WHERE {kwargs['DATEATTR']} BETWEEN '{from_date}' AND '{to_date}' "
            _query+= "WHERE deactivate_dt IS NULL "
            _query+=_s_asset_qry
            _query+=_s_date_qry
            _query+=_s_ids_qry

            ''' execute query to select data for date range '''
            self._data = None
            self.data = self._clsSDB.read_data_from_table(select=_query)
            if not isinstance(self._data,DataFrame):
                raise RuntimeError("read_data_from_table for \n\tquery=%s \n\treturned an empty %s" 
                                   % (_query, type(self._data)))
            logger.debug("%s query retrieve %d rows, between %s and %s, from table %s in %s SQLDB",
                         __s_fn_id__, self._data.count(), from_date, to_date,
                         ".".join([kwargs['DBSCHEMA'],kwargs['TBLNAME']]).upper(),
                         self._clsSDB.dbName.upper())

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data


    ''' Function --- WRITE DATA TO TABLE ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def upsert(func):

        @functools.wraps(func)
        def upsert_wrapper(self, data, realm, **kwargs,):
        
            __s_fn_id__ = f"{self.__name__} function <upsert_wrapper>"

            __def_pk_increment__ =1
            _cols_not_for_update = ['created_dt','created_by','created_proc']
            _options={
                "BATCHSIZE":1000,   # batch size to partition the dtaframe
                "PARTITIONS":1,    # number of parallel clusters to run
                "OMITCOLS":_cols_not_for_update,    # columns to be excluded from update

            }
            _def_audit_proc__ = "_".join([self.__app__, self.__module__,
                                          self.__package__, __s_fn_id__])

            try:
                tbl_name_, pk_attr_, tbl_schema_, self.data = func(self, data, realm, **kwargs,)

                if self._data is None or self._data.count()<=0:
                    raise AttributeError("Cannot upsert %s property attribute; abort processing!" 
                                         % type(self._data))
                logger.debug("%s processing %s with %d rows",
                            __s_fn_id__, type(self._data), self._data.count())

#                 if not isinstance(self._data,DataFrame) or self._data.count()<=0:
#                     raise AttributeError("Cannot clean empty %s data" % type(self._data))
                if "AUDITPROC" not in kwargs.keys() or "".join(kwargs['AUDITPROC'].split()==""):
                    kwargs['AUDITPROC']=_def_audit_proc__
                    
                ''' split data into insert (no pk) and updates (has pk) '''
                _ins_sdf = self._data.filter(F.col(pk_attr_).isNull() |
                                                F.col(pk_attr_).isin('','NaN','None','none'))\
                                        .select('*')
                _upd_sdf = self._data.filter(F.col(pk_attr_).isNotNull() |
                                                ~F.col(pk_attr_).isin('','NaN','None','none'))\
                                        .select('*')

                ''' initialize temp sdf to hold insert and update data '''
                saved_sdf_ = self._clsSDB.session.createDataFrame(data=[], schema = self._data.schema)
                ''' initialize counter '''
                ins_count_, upd_count_ = 0, 0
                ''' INSERT data '''
                if _ins_sdf.count()>0:
                    try:
                        if "created_proc" not in _ins_sdf.columns:
                            _ins_sdf = _ins_sdf.withColumn('created_proc', F.lit(kwargs['AUDITPROC']))
                            _ins_sdf=_ins_sdf.drop(pk_attr_)
                        ''' fetch the postgres table next pk value '''
                        next_pk_sdf = self._clsSDB.get_table_pk_nextval(
                            tbl_name=tbl_name_,
                            pk_attr =pk_attr_,
                        )
                        ''' create a sequence of pk values for each row '''
                        next_pk_val = next_pk_sdf.select(F.col('nextval')).collect()[0][0]
                        last_pk_val = next_pk_val+_ins_sdf.count()

                        ''' join the pk column to the dataframe '''
                        _ins_sdf = _ins_sdf.withColumn(pk_attr_,
                                                       next_pk_val-1+F.row_number()\
                                                       .over(Window.orderBy("created_proc")))

                        if _ins_sdf is None or _ins_sdf.count()<=0:
                            raise RuntimeError("Failed to augment primary key column %s from %d to %d" 
                                               % (pk_attr_, next_pk_val, last_pk_val))

                        logger.debug("%s augmented %s with %d primary key rows from %d to %d", 
                                     __s_fn_id__, pk_attr_, _ins_sdf.count(), 
                                     next_pk_val, last_pk_val)
                        ins_count_=self._clsSDB.insert_sdf_into_table(
                            save_sdf=_ins_sdf,
                            db_name =self._clsSDB.dbName,
                            db_table=tbl_name_,
                        )
                        if ins_count_ <= 0:
                            raise RuntimeError("Failed to insert %d rows in table %s of %s DB in %s" 
                                               %(_ins_sdf.count(), tbl_name_.upper(),
                                                 self._clsSDB.dbName.upper(), 
                                                 self._clsSDB.dbType.upper()))
                        logger.debug("%s Successfully INSERTED %d of %d rows in table %s of %s DB in %s", 
                                     __s_fn_id__, ins_count_, _ins_sdf.count(), tbl_name_.upper(), 
                                     self._clsSDB.dbName.upper(), self._clsSDB.dbType.upper())
#                         ''' reset the pk value in table in case something went wrong '''
#                         set_pk_val_ = self._clsSDB.set_table_pk_lastval(
#                             tbl_name= tbl_name_,
#                             pk_attr = pk_attr_,
#                             set_val = None
#                         )
#                         if not isinstance(set_pk_val_,int):
#                             raise RuntimeError("Failed to set_table_pk_lastval for %s" 
#                                                %(tbl_name_.upper()))
#                         logger.debug("%s set %s next %s value to %d",
#                                      __s_fn_id__, tbl_name_.upper(), pk_attr_.upper(), set_pk_val_)
#                         ''' augment return dataframe with inserted rows '''
                        for col in saved_sdf_.columns:
                            if col not in _ins_sdf.columns:
                                _ins_sdf = _ins_sdf.withColumn(col,F.lit(None))
                        for col in _ins_sdf.columns:
                            if col not in saved_sdf_.columns:
                                saved_sdf_ = saved_sdf_.withColumn(col,F.lit(None))
                        saved_sdf_ = saved_sdf_.unionByName(_ins_sdf)
                    except Exception as err:
                        logger.warning("%s %s \n",__s_fn_id__, err)
                        logger.debug(traceback.format_exc())
                        print("[Error]"+__s_fn_id__, err)

                    ''' reset the pk value in table in case something went wrong '''
                    set_pk_val_ = self._clsSDB.set_table_pk_lastval(
                        tbl_name= tbl_name_,
                        pk_attr = pk_attr_,
                        set_val = None
                    )
                    if not isinstance(set_pk_val_,int):
                        raise RuntimeError("Failed to set_table_pk_lastval for %s" 
                                           %(tbl_name_.upper()))
                    logger.debug("%s set %s next %s value to %d",
                                 __s_fn_id__, tbl_name_.upper(), pk_attr_.upper(), set_pk_val_)
                else:
                    logger.debug("%s No data to INSERT in %s of %s database", 
                                 __s_fn_id__, tbl_name_.upper(), self._clsSDB.dbName.upper())

                ''' UPDATE data '''
                if _upd_sdf.count()>0:
                    try:
                        if "modified_proc" not in _ins_sdf.columns:
                            _ins_sdf = _ins_sdf.withColumn('modified_proc', F.lit(kwargs['AUDITPROC']))
                        if "BATCHSIZE" in kwargs.keys() and isinstance(kwargs['BATCHSIZE'],int):
                            _options['BATCHSIZE']=kwargs['BATCHSIZE']
                        if "PARTITIONS" in kwargs.keys() and isinstance(kwargs['PARTITIONS'],int):
                            _options['PARTITIONS']=kwargs['PARTITIONS']
                        if "OMITCOLS" in kwargs.keys() and isinstance(kwargs['OMITCOLS'],int):
                            _options['OMITCOLS']=kwargs['OMITCOLS']

                        upd_count_=self._clsSDB.upsert_sdf_to_table(
                            save_sdf=_upd_sdf,
                            db_name =self._clsSDB.dbName,
                            db_table=tbl_name_,
                            unique_keys=[pk_attr_],
                            **_options,
                        )
                        if upd_count_ <= 0:
                            raise RuntimeError("Failed to insert %d rows in table %s of %s DB in %s" 
                                               %(_upd_sdf.count(), tbl_name_.upper(),
                                                 self._clsSDB.dbName.upper(), 
                                                 self._clsSDB.dbType.upper()))
                        for col in saved_sdf_.columns:
                            if col not in _upd_sdf.columns:
                                _upd_sdf = _upd_sdf.withColumn(col,F.lit(None))
                        for col in _upd_sdf.columns:
                            if col not in saved_sdf_.columns:
                                saved_sdf_ = saved_sdf_.withColumn(col,F.lit(None))
                        saved_sdf_ = saved_sdf_.unionByName(_upd_sdf)
                        logger.debug("%s Successfully UPDATED %d of %d rows in table %s of %s DB in %s", 
                                     __s_fn_id__, upd_count_, _upd_sdf.count(), tbl_name_.upper(), 
                                     self._clsSDB.dbName.upper(), self._clsSDB.dbType.upper())
                    except Exception as err:
                        logger.warning("%s %s \n",__s_fn_id__, err)
                        logger.debug(traceback.format_exc())
                        print("[Error]"+__s_fn_id__, err)

                else:
                    logger.debug("%s No data to UPSERT in %s of %s database", 
                                 __s_fn_id__, tbl_name_.upper(), self._clsSDB.dbName.upper())

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return ins_count_, upd_count_, saved_sdf_

        return upsert_wrapper


    def join_pk(func):

        @functools.wraps(func)
        def pk_wrapper(self, data, realm, **kwargs,):
        
            __s_fn_id__ = f"{self.__name__} function <pk_wrapper>"

            try:
                tbl_name_, tbl_pk_, tbl_schema_, self.data = func(self, data, realm, **kwargs,)

                if self._data is None or self._data.count()<=0:
                    raise AttributeError("Cannot join pk for %s property attribute; abort processing!"                                         % type(self._data))
                logger.debug("%s processing %s with %d rows",
                            __s_fn_id__, type(self._data), self._data.count())

#                 if not isinstance(self._data,DataFrame) or self._data.count()<=0:
#                     raise AttributeError("Cannot join pk for empty %s data" % type(self._data))

                if self._realm == "rebalance":
                    ''' read row from table for date and wap_id '''
                    _date_lst = [str(x[0]) for x in self._data.select(F.col('reb_date')).collect()]
                    _date_lst = "'"+"', '".join(_date_lst)+"'"
                    _wap_id_lst = [x[0] for x in self._data.select(F.col('reb_wap_id')).collect()]
                    _wap_id_lst = "'"+"', '".join(_wap_id_lst)+"'"
                    
                    _query = ""
                    _query+= "SELECT reb_pk, reb_date, reb_wap_id "
                    _query+=f"FROM {kwargs['DBSCHEMA']}.{kwargs['TBLNAME']} "
                    _query+= "WHERE deactivate_dt IS NULL "
                    _query+=f"AND reb_date IN ({_date_lst}) AND reb_wap_id IN ({_wap_id_lst})"
                    
                    _tbl_sdf = self._clsSDB.read_data_from_table(select=_query)
                    if not isinstance(_tbl_sdf,DataFrame):
                        raise ChildProcessError("read for \n\tquery=%s \n\treturned an empty %s" 
                                           % (_query, type(_tbl_sdf)))
                    logger.debug("%s retrieve %d rows from table %s in %s SQLDB",
                                 __s_fn_id__, _tbl_sdf.count(),
                                 kwargs['TBLNAME'].upper(), self._clsSDB.dbName.upper())
                    if _tbl_sdf.count()>0:
                        ''' join reb_pk on reb_date and reb_wap_id '''
                        _sdf_cols = [f"sdf.{x}" for x in self._data.columns if x not in [tbl_pk_]]
                        self._data = self._data.alias('sdf').join(_tbl_sdf.alias('tbl'),\
                                        (F.col('sdf.reb_date') == F.col('tbl.reb_date')) &\
                                        (F.col('sdf.reb_wap_id') == F.col('tbl.reb_wap_id')), 
                                                                  'leftouter')\
                                        .select(*_sdf_cols,f"tbl.{tbl_pk_}")

                        logger.debug("%s joined %d rows with pk values", __s_fn_id__,
                                     self._data.select(tbl_pk_).isNotNull().count())

                ''' MAYBE REQUIED FOR WEIGHTED PORTFOLIO AND TECH ANALYSYS '''
#                 _owners = self._data.select(F.col('owner_name')).distinct().collect()
#                 _owners = [x[0] for x in _owners]
#                 _rec_ids = self._data.select(F.col('owner_rec_id')).distinct().collect()
#                 _rec_ids = [str(x[0]) for x in _rec_ids]

#                 cond_kwargs = {
#                     "CONDITIONS" : {
#                         "owner_name" : _owners,   # list of owner names
#                         "owner_rec_id":_rec_ids
#                     }
#                 }

#                 tbl_sdf = self.read_from_table(
#                     tbl_name = tbl_name_,
#                     **cond_kwargs
#                 )
                
#                 self._data = self._data.alias('sdf').join(tbl_sdf.alias('tbl'),\
#                                 (F.col('sdf.owner_rec_id') == F.col('tbl.owner_rec_id')) &\
#                                 (F.col('sdf.owner_name') == F.col('tbl.owner_name')), 'leftouter')\
#                                 .select('sdf.*',f"tbl.{tbl_pk_}")
                if self._data is None or self._data.count()<=0:
                    raise RuntimeError("Failed joining %s table primary key %s values" 
                                       % (tbl_name_, tbl_pk_))

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            finally:
                logger.debug("%s completed join %d PKs of %d rows in column %s from table %s", 
                             __s_fn_id__, self._data.filter(F.col(tbl_pk_).isNotNull()).count(),
                             self._data.count(), tbl_pk_.upper(), tbl_name_.upper())
                return tbl_name_, tbl_pk_, tbl_schema_, self._data

        return pk_wrapper


    def schema(func):

        @functools.wraps(func)
        def schema_wrapper(self, data, realm, **kwargs,):
        
            __s_fn_id__ = f"{self.__name__} function <schema_wrapper>"

            try:
                tbl_name_, tbl_pk_, tbl_schema_, self.data = func(self, data, realm, **kwargs,)

                if self._data is None or self._data.count()<=0:
                    raise AttributeError("Cannot transform %s property attribute; abort processing!"                                         % type(self._data))
                logger.debug("%s processing schema transform on %s with %d rows",
                            __s_fn_id__, type(self._data), self._data.count())

#                 if not isinstance(self._data,DataFrame) or self._data.count()<=0:
#                     raise AttributeError("Cannot transform empty %s data" % type(self._data))

                ''' drop columns not in table '''
                _tbl_fields_lst=None
                _tbl_fields_lst = [field.name for field in tbl_schema_.fields]
#                 _filter_sdf = self._data.drop(*[x for x in self._data.columns 
#                                          if x not in _tbl_fields_lst])
                self._data = self._data.drop(*[x for x in self._data.columns 
                                         if x not in _tbl_fields_lst])

                ''' cast the pk with table defined data type '''
#                 if  tbl_pk_ not in _filter_sdf.columns:
                if  tbl_pk_ not in self._data.columns:
                    pk_dtype=[str(field.dataType) 
                              for field in tbl_schema_.fields 
                              if field.name==tbl_pk_][0]
                    if pk_dtype == 'IntegerType()':
#                         _filter_sdf=_filter_sdf.withColumn(tbl_pk_,F.lit(None).cast(IntegerType()))
                        self._data=self._data.withColumn(tbl_pk_,F.lit(None).cast(IntegerType()))
                    logger.warning("%s missing primary key column %s added with dtype %s",
                                   __s_fn_id__, tbl_pk_, pk_dtype)

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return tbl_name_, tbl_pk_, tbl_schema_, self._data

        return schema_wrapper

    @upsert
    @join_pk
    @schema
    def write_to_table(
        self,
        data : any = None,
        realm: str = None,
#         tbl_name:str=None,
#         tbl_pk : str=None,
        **kwargs,
    ) -> int:
        """
        Description:
            Structure the dataframe to comply with the table structure. Either insert (no pk)
            or update (with pk) the data.
            The function will use the instantiated default database, schema, user credentials
            session, and connection properties.
        Attribues :
            data (any) data structure that can be transformed to a pyspark dataframe
            tbl_name (str) the table to perform the data insert or update
        Returns :
            ins_count + upd_count (int) of the records
        Exceptions:
            data must be an non empty not null dataset
            tbl_name must be valid and already defined in the database and schema
        """
        
        __s_fn_id__ = f"{self.__name__} function <write_to_table>"

        __def_schema_name = "warehouse"
        __def_wap_tbl_name="weighted_portfolio"
        __def_ta_tbl_name="technical_analysis"
        __def_reb_tbl_name="rebalance_history"

#         _cols_not_for_update = ['created_dt','created_by','created_proc']
#         _options={
#             "BATCHSIZE":1000,   # batch size to partition the dtaframe
#             "PARTITIONS":1,    # number of parallel clusters to run
#             "OMITCOLS":_cols_not_for_update,    # columns to be excluded from update

#         }
#         _def_audit_proc__ = "_".join([self.__app__, self.__module__,
#                                       self.__package__, __s_fn_id__])
#         print(__s_fn_id__, data.show())

        try:
            ''' validate input parameters '''
            self.data = data
            if self._data.count()<=0:
                raise AttributeError("Cannot write empty %s to database" % type(self._data))
            self.realm = realm
            logger.debug("%s set %s data with %d rows and %s realm property attributes", 
                         __s_fn_id__, type(self._data), self._data.count(), self._realm.upper())

            ''' TODO - check if essential kwargs are defined, if not raise exception '''
            if "DBSCHEMA" not in kwargs.keys() or "".join(kwargs['DBSCHEMA'].split())=="":
                kwargs['DBSCHEMA'] = __def_schema_name
            ''' validate table is in database schema '''
            if "TBLNAME" not in kwargs.keys() or "".join(kwargs['TBLNAME'].split())=="":
                if self._realm == "portfolio":
                    kwargs['TBLNAME'] = __def_wap_tbl_name
                elif self._realm == "indicator":
                    kwargs['TBLNAME'] = __def_ta_tbl_name
                elif self._realm == "rebalance":
                    kwargs['TBLNAME'] = __def_reb_tbl_name
                else:
                    raise AttributeError("Something went wrong with table name assignment")
            logger.debug("%s set dbSchema %s and table %s realm %s", 
                         __s_fn_id__, kwargs['DBSCHEMA'].upper(), 
                         kwargs['TBLNAME'].upper(), self._realm.upper())
            tbl_sdf_ = self._clsSDB.get_db_table_info()
            if kwargs['TBLNAME'] not in [x[0] for x in tbl_sdf_.select(F.col('table_name')).collect()]:
                raise ValueError("Invalid table %s; did you mean one of %s " 
                                     % (kwargs['TBLNAME'].upper(), 
                                        str([x[0] for x in tbl_sdf_.select(F.col('table_name'))\
                                             .collect()]).upper()))
            ''' get the table structure to cast the data '''
            _tbl_schema = self._clsSDB.get_table_schema(
                tbl_name=kwargs['TBLNAME'],
                db_schema=kwargs['DBSCHEMA'],
            )
            _tbl_fields_lst=None
            _tbl_fields_lst = [field.name for field in _tbl_schema.fields]
            if len(_tbl_fields_lst)<=0:
                raise ValueError("Unable to recover field names from table %s, returned empty %s" 
                                 % (kwargs['TBLNAME'].upper(), type(_tbl_fields_lst)))
            logger.debug("%s %s fields list: %s", __s_fn_id__, kwargs['TBLNAME'].upper(), 
                         str(_tbl_fields_lst).upper())

            ''' if no tbl_pk specified try with first column '''
            if "TBLPK" not in kwargs.keys() or "".join(kwargs['TBLPK'])=="":
                kwargs["TBLPK"] = _tbl_fields_lst[0]
                logger.warning("%s Empty tbl_pk replaced with table first column: %s", 
                               __s_fn_id__, kwargs["TBLPK"].upper())
#             if tbl_pk is None or "".join(tbl_pk.split())=="":
#                 tbl_pk = _tbl_fields_lst[0]
#                 logger.warning("%s Empty tbl_pk replaced with table first column: %s", 
#                                __s_fn_id__, tbl_pk)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return kwargs['TBLNAME'], kwargs["TBLPK"], _tbl_schema, self._data