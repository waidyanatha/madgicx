#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
''' Initialize with default environment variables '''
__name__ = "predGoalSpend"
__package__ = "optimization"
__module__ = "budget"
__app__ = "mining"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    ''' essential python packages '''
    import os
    import sys
    import configparser    
    import logging
    import traceback
    import functools
    ''' function specific python packages '''
    import pandas as pd
    import numpy as np
    from datetime import datetime, date, timedelta
    from decimal import Decimal

    import findspark
    findspark.init()   # make sure to set SPARK_HOME environment    
    from pyspark.sql import functions as F
    # from pyspark.sql.window import Window
    from pyspark.sql import DataFrame

    
    
    ''' crypto etp class property attributes; i.e. self.'''
    from mining.modules.budget.optimization import __propAttr__ as attr

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))


'''
    CLASS the pipeline for predicting goal specific spend and performance
    
'''

class adBudgetMLWorkLoads(attr.properties):

    def __init__(self, desc : str="market cap data prep", **kwargs):
        """
        Desciption:
            Initialize the class
        Attributes:
        Returns:
            None
        """

        ''' instantiate property attributes '''
        super().__init__(desc="goal spend optimization")

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

#         ''' instantiate property attributes '''
#         super().__init__(desc=self.__desc__)
        
        ''' functions '''
        global pkgConf
        global appConf
        global logger
        ''' class objs '''

        __s_fn_id__ = f"{self.__name__} function <__init__>"

        try:
            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,__ini_fname__))

            self.projHome = pkgConf.get("CWDS","PROJECT")
            sys.path.insert(1,self.projHome)

            ''' innitialize the logger '''
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

            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))

            print("%s Class initialization complete" % self.__name__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            print("[Error]"+__s_fn_id__, err)
            print(traceback.format_exc())

        return None

    ''' Function: PREDICT SPEND
    '''
    def predict_spend(
        self,
        goal: str = None,   # predictions are specific to a goal
        geom: list= None,   # list of goegraphic location dicts
        data: DataFrame=None, # array of the inputs: impression, click
        **kwargs,
    ) -> object:
        """
        Description:
            Refers to the goal and geom specific model to predict the spend
            with click, impression, data
        """
        
        __s_fn_id__ = f"{self.__name__} function <@property import_to_rdbm>"

        try:
            pass
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        finally:
            return self._prediction