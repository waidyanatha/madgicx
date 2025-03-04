#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "rwAdsData"
__package__= "optimization"
__module__ = "budget"
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

    from mining.modules.budget.optimization import __propAttr__ as attr

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

            author: <samana.thetha@gmail.com>
    '''
    def __init__(
        self, 
        desc,
        f_store_mode:str= None,   # set the file storage mode
        f_store_root:str= None,   # set the root folder (bucket)
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
        ''' set file store mode '''
        if f_store_mode is not None and "".join(f_store_mode.split())!="":
            self._storeMode = f_store_mode
        else:
            self._storeMode = 'local-fs'
        ''' set file store root '''
        if f_store_root is not None and "".join(f_store_root.split())!="":
            self._storeRoot = f_store_root
        else:
            self._storeRoot = 'local-fs'

        global pkgConf  # this package configparser class instance
        global appConf  # configparser class instance
        global logger   # rezaware logger class instance
        global clsFile  # etl loader sparkFile class instance

        __s_fn_id__ = f"{self.__name__} function <__init__>"

        try:
#             ''' instantiate property attributes '''
#             super().__init__(self.__desc__)

            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,__ini_fname__))

            self.projHome = pkgConf.get("CWDS","PROJECT")
            sys.path.insert(1,self.projHome)

            ''' initialize the logger '''
            from rezaware.utils import Logger as logs
            logger = logs.get_logger(
                cwd=self.projHome,
                app=self.__app__, 
                module=self.__module__,
                package=self.__package__,
                ini_file=self.__ini_fname__)

            ''' set a new logger section '''
            logger.info('########################################################')
            logger.info("%s Class",self.__name__)

            ''' import spark File work load utils to read and write data '''
            from rezaware.modules.etl.loader import sparkFile as file
            clsFile = file.dataWorkLoads(
                desc = self.__desc__,
                store_mode=self._storeMode,
                store_root=self._storeRoot,
                jar_dir=None,
                )

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
        fname : str = None, # mandatory file name
        fpath : str = None, # mandatory file path
        from_date:date = None, # datetime.date value of the start date
        to_date : date = None, # datetime.date value of the end date
        attr_cols:list = [], # optional list of table columns 
        # objectives:list= [], # optional list of objectives to filter by
        # goals : list = [],   # optional list of optimization goals to filter by
        ids : list = [], # optional list of unique ids
        **kwargs
    ) -> DataFrame:
        """
        Description:
            Read the realm specific data for a given date range from file
        Attributes :

        """
        __s_fn_id__ = f"{self.__name__} function <read_realm>"

        __def_date_attr__ = "updated_time"

        __def_options_dict__ = {"inferSchema":True,
                                "header":True,
                                "delimiter":",",
                                "pathGlobFilter":'*.csv',
                                "recursiveFileLookup":True,
                                }


        try:
            self.realm= realm

            ''' validate file name and path '''
            if fname is None or "".join(fname.split())=="":
                raise AttributeError("Valid file name string is required; %s type was given" % type(fname))
            if fpath is None or "".join(fpath.split())=="":
                raise AttributeError("Valid file path string is required; %s type was given" % type(fpath))
            if "OPTIONS" not in kwargs.keys() or not isinstance(kwargs['OPTIONS'], dict) \
                or len(kwargs['OPTIONS'])<=0:
                kwargs['OPTIONS'] = __def_options_dict__

            ''' read file '''
            self.data=clsFile.read_files_to_dtype(
                as_type = "SPARK",      # optional - define the data type to return
                folder_path=fpath,  # optional - relative path, w.r.t. self.storeRoot
                file_name=fname,  # optional - name of the file to read (complete-60-accounts.csv)
                file_type=None,  # optional - read all the files of same type
                **kwargs['OPTIONS'],
                )
            logger.debug("%s Loaded %d rows from %s", __s_fn_id__, self._data.count(), fname.upper())
            ''' convert date to unix timestamp '''
            if "DATEATTR" not in kwargs.keys() or "".join(kwargs['DATEATTR'].split())=="":
                kwargs['DATEATTR'] = __def_date_attr__
            self._data = self._data.withColumn(f"unix_{kwargs['DATEATTR']}", 
                                               F.unix_timestamp(kwargs['DATEATTR']))
            logger.debug("timestamp converted unix_timestamp for %d rows" % self._data.count())
            ''' DATES validation, function inputs '''
            if not isinstance(to_date, date):
                to_date = date.today()
                logger.warning("%s unspecified to_date, setting to current date %s", 
                               __s_fn_id__, to_date)
            self._data = self._data.filter(F.col(kwargs['DATEATTR'])<=to_date)
            logger.debug("%s filtered data for %s <= %s", 
                         __s_fn_id__, kwargs['DATEATTR'], to_date)
            if isinstance(from_date,date):
                if from_date > to_date:
                    raise AssertionError("from_date: %s must be <= to_date: %s" % (from_date, to_date))
                else:
                    self._data = self._data.filter(F.col(kwargs['DATEATTR'])>=from_date)
                    logger.debug("%s filtered data for %s >= %s", 
                                 __s_fn_id__, kwargs['DATEATTR'], from_date)
            ''' TODO: move to wrapper, filter by realm '''
            if "REALMFILTATTR" in kwargs.keys() and "".join(kwargs['REALMFILTATTR'].split())!="":
                if "REALMFILTLIST" in kwargs.keys() and isinstance(kwargs['REALMFILTLIST'],list)\
                    and len(kwargs['REALMFILTLIST'])>=0:
                    self._data = self._data.filter(
                        F.col(kwargs['REALMFILTATTR']).isin(*kwargs['REALMFILTLIST']))
                    logger.debug("%s filtered %s with %s, reduced rows %d", 
                                 __s_fn_id__, kwargs['REALMFILTATTR'], 
                                 ", ".join(kwargs['REALMFILTLIST']), self._data.count())
                else:
                    self._data = self._data.filter(F.col(kwargs['REALMFILTATTR']).isNotNull())
                    logger.debug("%s filtered out NULL %s, reduced rows %d", 
                                 __s_fn_id__, kwargs['REALMFILTATTR'], self._data.count())
            ''' drop null columns '''
            null_counts = self._data.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in 
                                          self._data.columns]).collect()[0].asDict()
            to_drop = [k for k, v in null_counts.items() if v >= self._data.count()]
            self._data = self._data.drop(*to_drop)
            logger.debug("%s Filtered down to %d rows and reduced to %d columns", 
                         __s_fn_id__, self._data.count(), len(self._data.columns))

            ''' final check on the dataframe '''
            if not isinstance(self._data, DataFrame) or self._data.count()<=0:
                raise AttributeError("Invalid %s %s data set from %s" 
                                     % (type(self._data), self._realm, fname.upper()))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        finally:
            logger.debug("%s read %d rows with %d columns from %s", 
                         __s_fn_id__, self._data.count(), len(self._data.columns), fname.upper())
            return self._data


    ''' Function --- WRITE REALM ---

            author: <samana.thetha@gmail.com
    '''
    def write_realm(
        self,
        realm: str = None, # mandatory either portfolio, indicator, rebalance, or marketcap
        data : DataFrame=None,
        fname: str = None, # mandatory file name
        fpath: str = None, # mandatory file path
        **kwargs
    ) -> DataFrame:
        """
        Description:
            Read the realm specific data for a given date range from file
        Attributes :

        """
        __s_fn_id__ = f"{self.__name__} function <read_realm>"

        # __def_date_attr__ = "updated_time"

        # __def_options_dict__ = {"inferSchema":True,
        #                         "header":True,
        #                         "delimiter":",",
        #                         "pathGlobFilter":'*.csv',
        #                         "recursiveFileLookup":True,
        #                         }


        try:
            self.realm= realm
            self.data = data

            ''' validate file name and path '''
            if fname is None or "".join(fname.split())=="":
                raise AttributeError("Valid file name string is required; %s type was given" % type(fname))
            if fpath is None or "".join(fpath.split())=="":
                fpath = pkgConf.get("CWDS","DATA")
                logger.warning("%s invalid file path replaced with default path: %s", 
                               __s_fn_id__, fpath )
            # if "OPTIONS" not in kwargs.keys() or not isinstance(kwargs['OPTIONS'], dict) \
            #     or len(kwargs['OPTIONS'])<=0:
            #     kwargs['OPTIONS'] = __def_options_dict__

            write_to=clsFile.write_data(
                file_name=fname,
                folder_path=fpath,
                data=self._data
            )
            if not isinstance(write_to,str) or "".join(write_to.split())=="":
                raise RuntimeError("Failed to save data to %s" % fname)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        finally:
            logger.debug("%s wrote %d rows with %d columns to %s", 
                         __s_fn_id__, self._data.count(), len(self._data.columns), 
                         write_to.upper())
            return write_to

