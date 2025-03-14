#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "adSetMovement"
__package__= "evaluation"
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

    import numpy as np
    import pandas as pd
    import matplotlib
    import matplotlib
    import matplotlib.pyplot as plt
    import scipy.stats as stats
    # from scipy.stats import chi2_contingency, fisher_exact
    from statsmodels.stats.proportion import proportion_confint

    from mining.modules.budget.evaluation import __propAttr__ as attr

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

class evalWorkLoads(attr.properties):

    ''' Function --- INIT ---

            author: <samana.thetha@gmail.com>
    '''
    def __init__(
        self, 
        desc : str = None,
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
        super().__init__(desc="evaluate ad momentum")

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


    ''' Function --- GENERATE BASELINE DATA ---

            author: <samana.thetha@gmail.com
    '''
    @staticmethod
    def generate_baseline(
        historic_data:pd.DataFrame = None, 
        method:str = 'moving_average',
        data_window:int = 28, # Number of days to use as the baseline data,
        roll_window:int = 7,  # Number of days to use for moving average
        date_attr:str=None,
    ):
        """
        Generate baseline from historic data using different methods
        
        Parameters:
        historic_data: DataFrame with date and metrics columns
        method: One of 'moving_average', 'same_period_last_year', 'seasonal_avg'
        data_window: Number of days to use as the baseline data, typically 28 days
        roll_window: Number of days to use for moving average
        
        Returns:
        baseline: Series or DataFrame with baseline values
        """

        __s_fn_id__ = f"{evalWorkLoads.__name__} function <generate_baseline>"

        try:
            if data_window < roll_window:
                raise AttributeError("data_window %d must be > roll_window: %d" 
                                     % (data_window, roll_window))
            logger.debug("%s data_window: %d >= roll_window: %d", 
                         __s_fn_id__, data_window, roll_window)
            # _max_date = historic_data.select(F.max(date_attr)).first()[0] 
            _max_date = max(historic_data.index) 
            if method == 'moving_average':
                ''' filter data for required baseline window '''
                # _data_df = historic_data.filter(
                #     (F.col(date_attr) > _max_date-timedelta(days=window)))\
                #                     .orderBy(F.desc(date_attr)).toPandas()
                _data_df = historic_data.iloc[-data_window:] # .set_index(date_attr)
                logger.debug("%s Filtered %d rows of original %d rows from %s to %s %d data window", 
                             __s_fn_id__, _data_df.shape[0], historic_data.shape[0], 
                             str(min(_data_df.index)), str(max(_data_df.index)), data_window)
                             # str(min(data_df[date_attr])), str(max(data_df[date_attr])), data_window)
                # Use moving average of previous n days
                _data_df = _data_df.rolling(window=roll_window, min_periods=1).mean() #.shift(1)
                if _data_df is None or _data_df.shape[0]<=0:
                    raise RuntimeError("Rolling mean returned %s dataframe" % type(_data_df))
                logger.debug("%s Created dataframe with %d rows rolling means", 
                             __s_fn_id__, _data_df.shape[0])
            
            elif method == 'same_period_last_year':
                # Use same period from last year
                _data_df = historic_data.filter(
                    (F.col(date_attr) <= _max_date-timedelta(days=365)) & 
                    (F.col(date_attr) > _max_date-timedelta(days=365+window)))\
                                    .orderBy(F.desc(date_attr)).toPandas()
                _data_df = _data_df.rolling(roll_window=window).mean().shift(1)
                # return _data_df.shift(365)
            
            elif method == 'seasonal_avg':
                # Use average of same day of week over past n weeks
                _data_df = historic_data.filter(
                    (F.col(date_attr) > _max_date-timedelta(days=data_window)))\
                                    .orderBy(F.desc(date_attr)).toPandas()
                day_of_week = _data_df.index.dayofweek
                _data_df = pd.DataFrame(index=_data_df.index, columns=_data_df.columns)
                
                for day in range(7):  # 0-6 for Monday-Sunday
                    mask = (day_of_week == day)
                    for col in _data_df.columns:
                        _data_df.loc[mask, col] = _data_df.loc[mask, col].shift(1).rolling(window=12).mean()
            else:
                raise AttributeError("Invalid method %s, must be:moving_average, "+\
                                     "same_period_last_year, or seasonal_avg" % method)
            
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        finally:
            logger.debug("%s returned %d rows and %d columns for %s method", 
                         __s_fn_id__, _data_df.shape[0], _data_df.shape[1], method)
            return _data_df
            # return baseline
    
    ''' Function --- CALC CTR ---

            author: <samana.thetha@gmail.com>
    '''
    @staticmethod
    def calc_ctr(
        data,
        impr_attr :str="impressions",
        click_attr:str="clicks",
    ):
        """
        Safely calculate CTR with handling for zero impressions
        """

        __s_fn_id__ = f"{evalWorkLoads.__name__} function <calc_ctr>"

        try:
            if impr_attr not in data.columns or click_attr not in data.columns:
                raise AttributeError("Either %s or %s is not a data column, did you mean: %s"
                                     % (impr_attr.upper(), click_attr.upper(), 
                                        ", ".join(data.columns).upper()))
            
            # Create a copy to avoid modifying the original
            result = data.copy()
            # Calculate CTR safely (handling division by zero)
            result['ctr'] = np.where(
                result['impressions'] > 0,
                result['clicks'] / result['impressions'],
                np.nan
            )
            logger.debug("%s computed CTR for %d rows", __s_fn_id__, result['ctr'].shape[0])
        
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        finally:
            logger.debug("%s returned results columns %s.", 
                         __s_fn_id__, ", ".join([k for k in result.columns]).upper())
            return result

    ''' Function --- MONOTONIC TREND ---

            author: <samana.thetha@gmail.com>
    '''
    @staticmethod
    def monotonic_trend(time_series_data):
        """
        Check if a time series is monotonically increasing
        Returns: is_monotonic (bool), trend_strength (float between -1 and 1)
        """

        __s_fn_id__ = f"{evalWorkLoads.__name__} function <monotonic_trend>"

        _results = {} # return results dict

        try:
            # Handle missing data
            valid_data = [x for x in time_series_data if not pd.isna(x)]

            # Check if we have enough data points
            if len(valid_data) < 2:
                _results = {
                    'strictly_increasing': False,
                    'non_strict_increasing': False,
                    'trend_strength': np.nan,
                    'trend_p_value': np.nan,
                    'significant_trend': False,
                    'error': 'Insufficient valid data points'
                    }
                logger.warning("%s Insufficient valid data points %d", 
                               __s_fn_id__, len(valid_data))
            # Check for constant values
            elif len(set(valid_data)) == 1:
                _results = {
                    'strictly_increasing': False,
                    'non_strict_increasing': True,  # All values are equal
                    'trend_strength': 0.0,          # No correlation for constant values
                    'trend_p_value': 1.0,           # No significant trend
                    'significant_trend': False,
                    'error': 'Constant values - no trend'
                    }
                logger.warning("%s Constant values - no trend for %d", 
                               __s_fn_id__, len(set(valid_data)))
            else:
                ''' Check strict monotonic increase '''
                is_strictly_increasing = all(x < y for x, y in zip(valid_data, valid_data[1:]))
                ''' Check non-strict monotonic increase (allowing equality) '''
                is_non_strict_increasing = all(x <= y for x, y in zip(valid_data, valid_data[1:]))
                ''' Calculate Spearman rank correlation for trend strength '''
                days = list(range(1, len(valid_data) + 1))
                correlation, p_value = stats.spearmanr(days, valid_data)
                if not isinstance(is_strictly_increasing,bool) \
                    or not isinstance(is_non_strict_increasing,bool):
                    raise RuntimeError("Failed to assess monotic increase, decrease, or spearman ranking")
                _results =  {
                    'strictly_increasing': is_strictly_increasing,
                    'non_strict_increasing': is_non_strict_increasing,
                    'trend_strength': correlation,
                    'trend_p_value': p_value,
                    'significant_trend': p_value < 0.05,
                    'valid_data_points': len(valid_data)
                    }
        
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        finally:
            logger.debug("%s completed returning results %s"
                         , __s_fn_id__, ", ".join([k for k in _results.keys()]))
            return _results
    
    ''' Function --- TEST SIGNIFICANCE ---

            author: <samana.thetha@gmail.com
    '''
    @staticmethod
    def test_significance(
        test_data, 
        baseline_data, 
        metric_type='count'):
        """
        Description:
            Run appropriate significance tests comparing test data to baseline
    
            To interpret the significance test values for trend_strength and trend_p_value:
    
            **Trend_strength**:
            - This value typically ranges from 0 to 1 (or sometimes -1 to 1)
            - Higher absolute values indicate a stronger trend
            - Values closer to 0 indicate little or no trend
            - Positive values indicate an upward trend, negative values indicate a downward trend
            - For example, a value of 0.8 would indicate a strong positive trend, while -0.7 would 
              indicate a strong negative trend
            
            **Trend_p_value**:
            - This is a probability value that indicates the statistical significance of the observed trend
            - Typically, a p-value less than 0.05 is considered statistically significant
            - Lower p-values provide stronger evidence against the null hypothesis (that there is no trend)
            - A p-value of 0.01 means there is only a 1% chance that the observed trend occurred by random chance
            - If p-value is greater than 0.05, the trend may not be statistically significant
            
            For example, if you have trend_strength = 0.75 and trend_p_value = 0.003, this would indicate a 
            strong positive trend that is highly statistically significant.
                
        Parameters:
            test_data: DataFrame with recent 7 days of metrics
            baseline_data: DataFrame with baseline metrics
            metric_type: 'count' for impressions/clicks or 'rate' for CTR/conversion rates
        
        Returns:
            results: Dictionary with test results
        """

        __s_fn_id__ = f"{evalWorkLoads.__name__} function <test_significance>"

        results = {}

        try:
            
            ''' Aggregate total values for overall tests '''
            test_totals = test_data.sum(skipna=True)
            baseline_totals = baseline_data.sum(skipna=True)
            
            ''' For each metric '''
            for metric in test_data.columns:
                # Extract valid data
                test_values = test_data[metric].dropna().values
                baseline_values = baseline_data[metric].dropna().values
                ''' Add data validity check '''
                results[f"{metric}_data_check"] = {
                    'test_data_points': len(test_values),
                    'baseline_data_points': len(baseline_values),
                    'test_has_nan': test_data[metric].isna().any(),
                    'baseline_has_nan': baseline_data[metric].isna().any(),
                    'data_sufficient': bool(len(test_values) >= 2 and len(baseline_values) >= 2),
                    }
                logger.debug("%s data validity check returned results for %s", __s_fn_id__, 
                              ", ".join([k for k in results[f"{metric}_data_check"].keys()]))
                ''' Test for monotonic trend '''
                if len(test_values) >= 2:
                    results[f"{metric}_trend"] = evalWorkLoads.monotonic_trend(test_values)
                    if results[f"{metric}_trend"] is None or len(results[f"{metric}_trend"])<=0:
                        raise ChildProcessError("Failed to check monotonic trends returned %s" 
                                                % type(results[f"{metric}_trend"]))
                    logger.debug("%s monotonic trend returned results for %s", __s_fn_id__,
                                 ", ".join([k for k in results[f"{metric}_trend"].keys()]))
                else:
                    results[f"{metric}_trend"] = {
                        'error': 'Insufficient data for trend analysis',
                        'data_points': len(test_values)
                        }
                    logger.error("%s Insufficient data for trend analysis, %d data points", 
                                 __s_fn_id__, len(test_values))
                
                ''' Daily t-test comparison '''
                if len(test_values) >= 2 and len(baseline_values) >= 2:
                    try:
                        t_stat, p_value = stats.ttest_ind(
                            test_values, baseline_values, nan_policy='omit')
                        results[f"{metric}_daily_ttest"] = {
                            't_statistic': t_stat,
                            'p_value': p_value,
                            'significant': p_value < 0.05
                        }
                    except Exception as t_err:
                        results[f"{metric}_daily_ttest"] = {
                            'error': str(t_err),
                            'test_values': test_values.tolist() \
                                                if len(test_values) < 10 else len(test_values),
                            'baseline_values': baseline_values.tolist() \
                                                if len(baseline_values) < 10 else len(baseline_values)
                            }
                        logger.error("%s %s",__s_fn_id__, str(t_err))
                else:
                    results[f"{metric}_daily_ttest"] = {
                        'error': 'Insufficient data for t-test',
                        'test_data_points': len(test_values),
                        'baseline_data_points': len(baseline_values)
                    }
                
            ''' For rates (CTR, conversion rate) '''
            if metric_type == 'rate' and metric == 'ctr':
                if not pd.isna(test_totals.get('clicks', np.nan)) and \
                   not pd.isna(test_totals.get('impressions', np.nan)) and \
                   test_totals.get('impressions', 0) > 0 and \
                   baseline_totals.get('impressions', 0) > 0:
                    ''' set the numbers for contingency table '''
                    test_successes = test_totals['clicks']
                    test_trials = test_totals['impressions']
                    baseline_successes = baseline_totals['clicks']
                    baseline_trials = baseline_totals['impressions']
                    
                    # Contingency table
                    contingency = [
                        [test_successes, test_trials - test_successes],
                        [baseline_successes, baseline_trials - baseline_successes]
                    ]
                    
                    try:
                        # Chi-square test
                        chi2, chi2_p, dof, expected = stats.chi2_contingency(contingency)
                        # Fisher exact test (more accurate for small samples)
                        fisher_odds_ratio, fisher_p = stats.fisher_exact(contingency)
                        
                        test_rate = test_successes / test_trials
                        baseline_rate = baseline_successes / baseline_trials
                        
                        results[f"{metric}_rate_tests"] = {
                            'chi2_statistic': chi2,
                            'chi2_p_value': chi2_p,
                            'chi2_significant': chi2_p < 0.05,
                            'fisher_odds_ratio': fisher_odds_ratio,
                            'fisher_p_value': fisher_p,
                            'fisher_significant': fisher_p < 0.05,
                            'test_rate': test_rate,
                            'baseline_rate': baseline_rate,
                            'relative_difference': ((test_rate - baseline_rate) / baseline_rate) * 100 \
                                                                        if baseline_rate > 0 else np.nan
                            }
                        
                        # Calculate confidence intervals
                        try:
                            test_ci_lower, test_ci_upper = proportion_confint(
                                test_successes, test_trials, alpha=0.05
                                )
                            baseline_ci_lower, baseline_ci_upper = proportion_confint(
                                baseline_successes, baseline_trials, alpha=0.05
                                )
                            
                            results[f"{metric}_rate_ci"] = {
                                'test_rate': test_rate,
                                'test_ci': (test_ci_lower, test_ci_upper),
                                'baseline_rate': baseline_rate,
                                'baseline_ci': (baseline_ci_lower, baseline_ci_upper),
                                'non_overlapping_ci': (test_ci_lower > baseline_ci_upper) or 
                                                    (test_ci_upper < baseline_ci_lower)
                                }
                            logger.debug("%s completed proportion_confint, return results for %s", __s_fn_id__, 
                                         ", ".join([x for x in results[f"{metric}_rate_ci"].keys()]))
                        except Exception as e_ci:
                            logger.error("%s %s",__s_fn_id__, str(e_ci))
                            results[f"{metric}_rate_ci"] = {'error': str(e_ci)}
                            
                    except Exception as e_cont:
                        logger.error("%s %s",__s_fn_id__, str(e_cont))
                        results[f"{metric}_rate_tests"] = {'error': str(e_cont)}
                else:
                    results[f"{metric}_rate_tests"] = {
                        'error': 'Insufficient or invalid data for rate tests',
                        'test_clicks': test_totals.get('clicks', np.nan),
                        'test_impressions': test_totals.get('impressions', np.nan),
                        'baseline_clicks': baseline_totals.get('clicks', np.nan),
                        'baseline_impressions': baseline_totals.get('impressions', np.nan)
                        }
                    logger.error("%s Insufficient or invalid data for rate tests",__s_fn_id__)
        
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        finally:
            logger.debug("%s test returned results for: %s", __s_fn_id__, 
                         ", ".join([k for k in results.keys()]))
            return results
    
    ''' Function --- ANALYZE MOMENTUM ---

            author: <samana.thetha@gmail.com
    '''
    def analyze_momentum(
        self,
        data : DataFrame = None, 
        recent_days: int = 7, 
        baseline_method:str='moving_average', 
        baseline_window:int=28,
        **kwargs
    ):
        """
        Complete analysis of ad metrics with statistical tests and trend detection
        
        Parameters:
        ad_data: DataFrame with date index and columns for impressions, clicks, reach
        recent_days: Number of days to consider as test period
        baseline_method: Method to generate baseline
        baseline_window: Window parameter for baseline method
        
        Returns:
        analysis_results: Dictionary with comprehensive analysis results
        """

        __s_fn_id__ = f"{self.__name__} function <analyze_momentum>"
        ''' defaults '''
        __def_dt_attr__ = "updated_time"
        __def_ad_metr_lst__ = ['impressions','clicks','reach']
        __def_conv_metr_lst__=[]
        __def_recents_days__= 7
        __def_baseline_win__= 14

        results = {}

        try:
            self.data=data
            logger.debug("%s loaded %s with %d rows and %d columns", __s_fn_id__, 
                         type(self._data), self._data.count(), len(self._data.columns))
            if "DATEATTR" not in kwargs.keys() or "".join(kwargs["DATEATTR"].split())=="":
                kwargs["DATEATTR"]=__def_dt_attr__
            if not isinstance(recent_days,int) or recent_days<=1:
                recent_days = __def_recents_days__
                logger.warning("%s invalid input, set recent_days = %d default value", 
                               __s_fn_id__, recent_days)
            if not isinstance(baseline_window,int) or baseline_window<=recent_days:
                baseline_window = __def_baseline_win__
                logger.warning("%s invalid input, set baseline_window = %d default value", 
                               __s_fn_id__, baseline_window)
            ''' check data sufficiency '''
            if self._data.select(kwargs["DATEATTR"]).distinct().count() < (baseline_window+recent_days):
                raise AttributeError("Insufficient data: need >= %d days, but got %d" 
                                     % ((baseline_window+recent_days),
                                     self._data.select(kwargs["DATEATTR"]).distinct().count()))
            ''' check metric attributes '''
            if "ADMETRICLIST" not in kwargs.keys() or len(kwargs['ADMETRICLIST'])<=0:
                kwargs['ADMETRICLIST'] = __def_ad_metr_lst__
                missing_columns = [col for col in kwargs['ADMETRICLIST'] 
                                   if col not in self._data.columns]
                if len(missing_columns)>0:
                    raise AssertionError("Missing required columns:" % str(missing_columns).upper())
            if "CONVMETRICLIST" not in kwargs.keys():
                kwargs['ADMETRICLIST'] = __def_conv_metr_lst__

            ''' Split data into test and historic periods '''
            _max_date = self._data.select(F.max(kwargs['DATEATTR'])).first()[0]
            _test_data = self._data.filter((F.col(kwargs['DATEATTR']) <= _max_date) &
                                    (F.col(kwargs['DATEATTR']) > _max_date-timedelta(days=recent_days)))\
                            .orderBy(F.asc(kwargs['DATEATTR']))\
                            .toPandas().set_index(kwargs['DATEATTR'])
            logger.debug("%s test data, with %d rows, set between %s and %s.",
                         __s_fn_id__, _test_data.shape[0], 
                         str(min(_test_data.index)), 
                         str(max(_test_data.index)))
            _pre_recent__data = self._data.filter(
                F.col(kwargs['DATEATTR']) <= _max_date-timedelta(days=recent_days))\
                            .orderBy(F.asc(kwargs['DATEATTR']))\
                            .toPandas().set_index(kwargs['DATEATTR'])
            logger.debug("%s Filtered %d of %d original rows from %s to %s with %d recent_days window", 
                         __s_fn_id__, _pre_recent__data.shape[0], self._data.count(),
                         min(_pre_recent__data.index), 
                         max(_pre_recent__data.index), recent_days)

            ''' Generate appropriate baseline '''
            # baseline_data = evalWorkLoads.generate_baseline(
            baseline_for_test = evalWorkLoads.generate_baseline(
                historic_data = _pre_recent__data, 
                method=baseline_method,
                data_window=baseline_window,
                roll_window=recent_days,
                date_attr=kwargs['DATEATTR'])

            # baseline_for_test = baseline_data.iloc[-recent_days:]
            # baseline_for_test = baseline_data.iloc[-baseline_window:]
            logger.debug("%s baseling data, with %d rows, set between %s and %s.",
                         __s_fn_id__, baseline_for_test.shape[0], 
                         str(min(baseline_for_test.index)), 
                         str(max(baseline_for_test.index)))
            ''' Run significance tests for count metrics '''
            count_results = evalWorkLoads.test_significance(
                _test_data[kwargs['ADMETRICLIST']], #[['impressions', 'clicks', 'reach']], 
                baseline_for_test[kwargs['ADMETRICLIST']], #[['impressions', 'clicks', 'reach']], 
                metric_type='count'
            )
            ''' Calculate and test derived metrics '''
            if 'impressions' in self._data.columns and 'clicks' in self._data.columns:
                # Calculate CTR safely (both for test and baseline)
                test_data_with_ctr = evalWorkLoads.calc_ctr(_test_data)
                baseline_for_test_with_ctr = evalWorkLoads.calc_ctr(baseline_for_test)
                # Add diagnostic information about CTR calculation
                ctr_diagnostic = {
                    'test_ctr_calculated': 'ctr' in test_data_with_ctr.columns,
                    'test_has_zero_impressions': (_test_data['impressions'] == 0).any(),
                    'test_ctr_has_nan': test_data_with_ctr['ctr'].isna().any() \
                                                if 'ctr' in _test_data else True,
                    'test_ctr_values': test_data_with_ctr['ctr'].dropna().tolist() \
                                                if 'ctr' in test_data_with_ctr else [],
                    'baseline_ctr_calculated': 'ctr' in baseline_for_test_with_ctr.columns,
                    'baseline_has_zero_impressions': (baseline_for_test['impressions'] == 0).any(),
                    'baseline_ctr_has_nan': baseline_for_test_with_ctr['ctr'].isna().any() \
                                                if 'ctr' in baseline_for_test_with_ctr else True,
                    'baseline_ctr_values': baseline_for_test_with_ctr['ctr'].dropna().tolist() \
                                                if 'ctr' in baseline_for_test_with_ctr else [],
                    }
                
                rate_results = evalWorkLoads.test_significance(
                    test_data_with_ctr[['ctr']], 
                    baseline_for_test_with_ctr[['ctr']],
                    metric_type='rate'
                )
                
                results = {**ctr_diagnostic, **count_results, **rate_results}
            else:
                results = count_results

            ''' Add CTR diagnostic information to results '''
            results['ctr_diagnostic'] = ctr_diagnostic
            
            # # Visualize trends
            # Agg backend doesn't display figures
            current_backend = matplotlib.get_backend()
            matplotlib.use('Agg') 
            # Create figure
            fig, ax = plt.subplots(len(kwargs['ADMETRICLIST']),figsize=(15, 10))

            _all_df = self._data.toPandas().set_index(kwargs['DATEATTR'])
            _test_data_ma = evalWorkLoads.generate_baseline(
                historic_data = _test_data, 
                method=baseline_method,
                data_window=recent_days,
                roll_window=recent_days,
                date_attr=kwargs['DATEATTR'])

            for i, metric in enumerate(kwargs['ADMETRICLIST']):
                # ax[i] = fig.add_subplot(len(metrics), 1, i+1)
                # ax[i].subplot(len(metrics), 1, i+1)
                ax[i].plot(_all_df.index[-30:], _all_df[metric][-30:], 'b-', label=f'Actual {metric}')
                ax[i].plot(_test_data_ma.index, _test_data_ma[metric], 'g--', label=f'Test {metric}')
                ax[i].plot(baseline_for_test.index, baseline_for_test[metric], 'r--', label=f'Baseline {metric}')
                ax[i].axvline(_test_data.index[0], color='g', linestyle='--', label='Test period start')
                ax[i].set_title(f"{metric.capitalize()} Trend Analysis")
                ax[i].legend()
            plt.tight_layout()
            ''' add plot figure '''
            # results['trend_plot']=fig


        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        finally:
            logger.debug("%s returned results", __s_fn_id__,)
            return {
                'statistical_tests': results,
                'test_data': _test_data,
                'baseline_data': baseline_for_test,
                'trend_plots' : fig,
                }
