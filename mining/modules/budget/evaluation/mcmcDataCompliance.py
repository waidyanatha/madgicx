#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "mcmcDataCompliance"
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
    import matplotlib.pyplot as plt
    import scipy.stats as stats
    from statsmodels.tsa.stattools import adfuller, acf
    from statsmodels.graphics.tsaplots import plot_acf
    from sklearn.neighbors import KernelDensity
    from scipy.signal import find_peaks
    import seaborn as sns
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler
    from sklearn.impute import SimpleImputer
    
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

class dataWorkLoads(attr.properties):

    ''' Function --- INIT ---

            author: <samana.thetha@gmail.com>
    '''
    def __init__(
        self, 
        desc : str = None,
        # f_store_mode:str= None,   # set the file storage mode
        # f_store_root:str= None,   # set the root folder (bucket)
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

            # ''' set file store mode '''
            # if f_store_mode is not None and "".join(f_store_mode.split())!="":
            #     self._storeMode = f_store_mode
            # else:
            #     self._storeMode = 'local-fs'
            # ''' set file store root '''
            # if f_store_root is not None and "".join(f_store_root.split())!="":
            #     self._storeRoot = f_store_root
            # else:
            #     self._storeRoot = pkgConf.get("CWDS","DATA")
            # ''' import spark File work load utils to read and write data '''
            # from mining.modules.budget.optimization import rwAdsDataFile as file
            # clsFile = file.dataWorkLoads(
            #     desc = self.__desc__,
            #     f_store_mode=self._storeMode,
            #     f_store_root=self._storeRoot,
            #     jar_dir=None,
            # )

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

    ''' Function --- STATIONARITY ---

            author: <samana.thetha@gmail.com
    '''
    @staticmethod
    def stationarity(data, significance=0.05) -> dict:
        """
        Test for stationarity using Augmented Dickey-Fuller test.
        
        Parameters:
        -----------
        data : array-like
            Time series data to test
        significance : float
            Significance level for the test
            
        Returns:
        --------
        p_value_, comply_, explain_
            True if stationary, False otherwise
        """

        __s_fn_id__ = f"{dataWorkLoads.__name__} function <stationarity>"

        return_dict_ = {}

        try:
            result_ = adfuller(data)
            if result_ is None or len(result_)<=0:
                raise ChildProcessError("Failed ADF Test process for %s" % type(data))
            # p_value = result_[1]
            return_dict_["check"] = "stationary"
            return_dict_["p_value"] = result_[1]
            # print(f"ADF Test p-value: {p_value}")
            
            if return_dict_["p_value"] < significance:
                return_dict_["explained"] ="✓ Data appears to be stationary"
                return_dict_["comply"] = True
            else:
                return_dict_["explained"] ="✗ Data does not appear to be stationary"
                return_dict_["comply"] = False

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        finally:
            logger.debug("%s ADF Test completed with results: %s", __s_fn_id__, str(return_dict_))
            return return_dict_
    
    
    def data_volume(data, min_samples=1000):
        """
        Check if there is sufficient data volume.
        
        Parameters:
        -----------
        data : array-like
            Data to test
        min_samples : int
            Minimum recommended sample size
            
        Returns:
        --------
        bool
            True if sufficient, False otherwise
        """

        __s_fn_id__ = f"{dataWorkLoads.__name__} function <data_volume>"

        return_dict_ = {}

        try:    
            return_dict_["check"] = "data volume"
            return_dict_['samples'] = len(data)
            # print(f"Number of samples: {n_samples}")
            
            if return_dict_['samples'] >= min_samples:
                return_dict_["explained"] ="✓ Data volume is likely sufficient (>= "+\
                                                f"{min_samples} samples)"
                return_dict_["comply"] = True
                # print(f"✓ Data volume is likely sufficient (>= {min_samples} samples)")
                # return True
            else:
                return_dict_["explained"] =f"✗ Data volume may be insufficient < "+\
                                                f"{min_samples} samples)"
                return_dict_["comply"] = False
                # print(f"✗ Data volume may be insufficient (< {min_samples} samples)")
                # return False
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        finally:
            logger.debug("%s Data volume Test completed with results: %s", __s_fn_id__, str(return_dict_))
            return return_dict_
    
    
    def representativeness(data, multivariate=False, n_bins=20):
        """
        Visualize the distribution to check for representativeness.
        This is somewhat subjective and depends on domain knowledge.
        
        Parameters:
        -----------
        data : array-like
            Data to test
        n_bins : int
            Number of bins for histogram
        """

        __s_fn_id__ = f"{dataWorkLoads.__name__} function <representativeness>"

        return_dict_ = {}

        try:           
            return_dict_["check"] = "representativeness"
            return_dict_["comply"] = None
            _explain_str = ""

            data_clean = data[~np.isnan(data)].reshape(-1, 1)
            print(data.shape, data_clean.shape)
            if len(data_clean) == 0:
                _explain_str+="✗ All data values are NaN. Cannot perform representativeness check."

                # elif len(data_clean) < len(data):
            else:
                logger.warning("%s Removed %d NaN values for visualization", 
                               __s_fn_id__, (len(data) - len(data_clean)))
                imputer = SimpleImputer(strategy='mean')
                data_imputed = imputer.fit_transform(data_clean)
                print("data_imputed", len(data_imputed))
                if multivariate:
                    # For multivariate data, we'll use PCA for visualization
                    scaler = StandardScaler()
                    data_scaled = scaler.fit_transform(data_imputed)
                    pca = PCA(n_components=1)
                    data_pca = pca.fit_transform(data_scaled).flatten()
                    return_dict_["data_pca"]= data_pca
                    _explain_str += "Note: Using PCA first component for representativeness visualization"
                # else:
                #     results.append(check_representativeness(data))
                # plt.figure(figsize=(10, 6))
                
                # # Plot histogram
                # plt.hist(data, bins=n_bins, alpha=0.7, density=True)
                
                # # Plot kernel density estimate
                # x_grid = np.linspace(min(data), max(data), 1000)
                # kde = KernelDensity(bandwidth=0.5).fit(data.reshape(-1, 1))
                # log_dens = kde.score_samples(x_grid.reshape(-1, 1))
                # plt.plot(x_grid, np.exp(log_dens), 'r-', label='KDE')
                
                # plt.title('Data Distribution')
                # plt.xlabel('Value')
                # plt.ylabel('Density')
                # plt.legend()
                # plt.show()
                
                # # Check for zeros in the histogram bins
                # hist, bin_edges = np.histogram(data, bins=n_bins)
                # empty_bins = np.sum(hist == 0)
                
                # if empty_bins > 0:
                #     _explain_str += f"✗ Found {empty_bins} empty regions in the distribution out of {n_bins} bins. "
                #     _explain_str +="This might indicate gaps in your data's coverage"
                # else:
                #     _explain_str += "✓ No empty regions found in the distribution "
                
                _explain_str += "Note: Visual inspection is recommended to ensure representativeness. "

            return_dict_["explain"] = _explain_str

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        finally:
            logger.debug("%s Representativeness Test completed with results: %s", __s_fn_id__, str(return_dict_))
            return return_dict_
    
    
    def check_dimensionality(data, max_recommended_dim=20):
        """
        Check the dimensionality of the data and provide guidance.
        
        Parameters:
        -----------
        data : array-like
            Data to test, should be 2D (samples x features)
        max_recommended_dim : int
            Maximum recommended dimensionality for standard MCMC
            
        Returns:
        --------
        bool
            True if dimensionality is manageable, False otherwise
        """
        if len(data.shape) == 1:
            n_dim = 1
        else:
            n_dim = data.shape[1]
        
        print(f"Data dimensionality: {n_dim}")
        
        if n_dim <= max_recommended_dim:
            print(f"✓ Dimensionality is manageable for standard MCMC")
            return True
        else:
            print(f"✗ High dimensionality detected. Consider:")
            print("  - Dimensionality reduction techniques")
            print("  - Specialized MCMC methods for high dimensions")
            print("  - Hamiltonian Monte Carlo")
            return False
    
    
    def check_correlation_structure(data):
        """
        Analyze the correlation structure of the data.
        
        Parameters:
        -----------
        data : array-like
            Data to test, should be 2D (samples x features)
        """
        if len(data.shape) == 1:
            # For 1D data, show autocorrelation
            fig, ax = plt.subplots(figsize=(10, 6))
            plot_acf(data, ax=ax, lags=30)
            plt.title('Autocorrelation Function')
            plt.show()
            
            # Check for significant autocorrelation
            acf_values = acf(data, nlags=30)
            significant_lags = np.sum(np.abs(acf_values[1:]) > 1.96/np.sqrt(len(data)))
            
            if significant_lags > 5:
                print(f"✗ Significant autocorrelation detected at {significant_lags} lags")
                print("  High autocorrelation may slow MCMC convergence")
            else:
                print("✓ Autocorrelation appears manageable")
        else:
            # For multivariate data, show correlation matrix
            df = pd.DataFrame(data)
            corr_matrix = df.corr()
            
            plt.figure(figsize=(10, 8))
            sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1)
            plt.title('Correlation Matrix')
            plt.show()
            
            # Count high correlations
            high_corr_count = np.sum(np.abs(np.triu(corr_matrix.values, k=1)) > 0.7)
            
            if high_corr_count > 0:
                print(f"✗ Found {high_corr_count} pairs of highly correlated variables")
                print("  High correlation may slow MCMC convergence")
            else:
                print("✓ Correlation structure appears manageable")
    
    
    def check_multimodality(data, bandwidth=0.5, prominence=0.05):
        """
        Check if the distribution appears to be multimodal.
        
        Parameters:
        -----------
        data : array-like
            Data to test
        bandwidth : float
            Bandwidth for kernel density estimation
        prominence : float
            Minimum prominence of peaks to be considered
            
        Returns:
        --------
        bool
            True if multimodal, False otherwise
        """
        # Create a KDE of the data
        x_grid = np.linspace(min(data), max(data), 1000)
        kde = KernelDensity(bandwidth=bandwidth).fit(data.reshape(-1, 1))
        log_dens = kde.score_samples(x_grid.reshape(-1, 1))
        density = np.exp(log_dens)
        
        # Find peaks in the density
        peaks, properties = find_peaks(density, prominence=prominence)
        
        plt.figure(figsize=(10, 6))
        plt.plot(x_grid, density)
        plt.plot(x_grid[peaks], density[peaks], 'ro')
        plt.title(f'Density Estimate with {len(peaks)} Detected Modes')
        plt.xlabel('Value')
        plt.ylabel('Density')
        plt.show()
        
        if len(peaks) > 1:
            print(f"✗ Distribution appears to be multimodal with {len(peaks)} modes")
            print("  Multimodal distributions may require:")
            print("  - Longer MCMC chains")
            print("  - Multiple chains with different starting points")
            print("  - Tempering methods")
            return True
        else:
            print("✓ Distribution appears to be unimodal")
            return False
    
    
    def check_boundary_conditions(data):
        """
        Check for boundary conditions or constraints in the data.
        
        Parameters:
        -----------
        data : array-like
            Data to test
        """
        # Check for common boundary types
        min_val = np.min(data)
        max_val = np.max(data)
        
        # Calculate distance from min/max to next closest points
        sorted_data = np.sort(data)
        min_gap = np.min(np.diff(sorted_data))
        
        # Check if data is close to 0, 1, or other common boundaries
        boundaries = {
            '0': 0,
            '1': 1,
            'Integer values': np.all(data == np.round(data))
        }
        
        print("Potential boundary conditions:")
        print(f"Data range: [{min_val}, {max_val}]")
        
        for name, value in boundaries.items():
            if name == 'Integer values':
                if value:
                    print(f"✗ Data appears to be discrete (integer-valued)")
                    print("  Discrete parameters require special MCMC approaches")
            else:
                if np.any(np.isclose(data, value, atol=min_gap)):
                    print(f"✗ Data contains values very close to {name}")
                    print("  Boundary constraints may require specialized MCMC")
        
        # Check for potential truncation
        if np.isclose(min_val, max_val - (max_val - min_val) * 0.05, atol=min_gap) or \
           np.isclose(max_val, min_val + (max_val - min_val) * 0.05, atol=min_gap):
            print("✗ Data may be truncated (high concentration at extremes)")
            print("  Truncated distributions require special handling")
    
    
    def check_mixing_properties(data, lag=1):
        """
        For time series data, check properties that might affect mixing.
        
        Parameters:
        -----------
        data : array-like
            Time series data to test
        lag : int
            Lag for checking autocorrelation
        """
        # Calculate autocorrelation
        acf_1 = acf(data, nlags=lag)[-1]
        
        plt.figure(figsize=(10, 6))
        plt.scatter(data[:-lag], data[lag:], alpha=0.5)
        plt.title(f'Lag-{lag} Plot (r = {acf_1:.3f})')
        plt.xlabel(f'X(t)')
        plt.ylabel(f'X(t+{lag})')
        plt.grid(True)
        plt.show()
        
        if abs(acf_1) > 0.7:
            print(f"✗ High lag-{lag} autocorrelation detected: {acf_1:.3f}")
            print("  High autocorrelation may lead to poor mixing")
            print("  Consider:")
            print("  - Thinning your MCMC chain")
            print("  - Using longer burn-in periods")
            print("  - More efficient MCMC samplers (HMC, NUTS)")
        else:
            print(f"✓ Lag-{lag} autocorrelation is moderate: {acf_1:.3f}")
            print("  Mixing properties look reasonable")
    
    
    def check_prior_knowledge(data):
        """
        Evaluate if the data aligns with common distributions
        to help with prior selection.
        
        Parameters:
        -----------
        data : array-like
            Data to test
        """
        # Normalize data for distribution testing
        data_norm = (data - np.mean(data)) / np.std(data)
        
        # List of distributions to test
        distributions = [
            ('Normal', stats.norm),
            ('Student-t', stats.t),
            ('Cauchy', stats.cauchy),
            ('Laplace', stats.laplace),
            ('Logistic', stats.logistic)
        ]
        
        # Perform Kolmogorov-Smirnov tests
        results = []
        for name, dist in distributions:
            statistic, p_value = stats.kstest(data_norm, dist.cdf)
            results.append((name, p_value, statistic))
        
        # Sort by p-value (higher is better fit)
        results.sort(key=lambda x: x[1], reverse=True)
        
        print("Distribution fitting results (higher p-value = better fit):")
        for name, p_value, statistic in results:
            print(f"  {name}: p-value = {p_value:.4f}, KS statistic = {statistic:.4f}")
        
        # Suggest priors based on best fit
        best_dist = results[0][0]
        print(f"\nBased on your data, consider priors related to the {best_dist} distribution")
        
        if results[0][1] < 0.05:
            print("✗ No common distribution fits well (all p-values < 0.05)")
            print("  Consider non-parametric or mixture priors")
        else:
            print(f"✓ {best_dist} distribution is a reasonable fit (p-value > 0.05)")
    
    
    def check_noise_characteristics(data):
        """
        Analyze noise characteristics in the data.
        
        Parameters:
        -----------
        data : array-like
            Data to test
        """
        # Calculate differences to examine noise
        diffs = np.diff(data)
        
        # Test for heteroskedasticity (changing variance)
        window_size = min(100, len(diffs) // 5)
        rolling_std = pd.Series(diffs).rolling(window=window_size).std().dropna()
        
        plt.figure(figsize=(10, 8))
        
        # Plot 1: Original data
        plt.subplot(3, 1, 1)
        plt.plot(data)
        plt.title('Original Data')
        plt.grid(True)
        
        # Plot 2: Differences
        plt.subplot(3, 1, 2)
        plt.plot(diffs)
        plt.title('First Differences (Noise)')
        plt.grid(True)
        
        # Plot 3: Rolling standard deviation
        plt.subplot(3, 1, 3)
        plt.plot(rolling_std)
        plt.title('Rolling Standard Deviation of Noise')
        plt.grid(True)
        
        plt.tight_layout()
        plt.show()
        
        # QQ plot to check noise normality
        plt.figure(figsize=(8, 8))
        stats.probplot(diffs, dist="norm", plot=plt)
        plt.title('Q-Q Plot of Noise')
        plt.grid(True)
        plt.show()
        
        # Test for heteroskedasticity
        std_ratio = np.max(rolling_std) / np.min(rolling_std)
        
        if std_ratio > 2:
            print(f"✗ Heteroskedasticity detected (std ratio: {std_ratio:.2f})")
            print("  Consider:")
            print("  - Transformation to stabilize variance")
            print("  - MCMC methods that account for varying noise")
        else:
            print(f"✓ Noise appears homoskedastic (std ratio: {std_ratio:.2f})")
        
        # Test for normality of noise
        _, p_value = stats.normaltest(diffs)
        
        if p_value < 0.05:
            print(f"✗ Noise is not normally distributed (p-value: {p_value:.4f})")
            print("  Consider MCMC likelihood functions that match your noise distribution")
        else:
            print(f"✓ Noise appears normally distributed (p-value: {p_value:.4f})")
    
    
    def run_checks(
        self,
        data : DataFrame = None, 
        checks:list = None,
        multivariate:bool=False,
        time_series :bool=True
    ) -> list:
        """
        Run all MCMC suitability checks on the data.
        
        Parameters:
        -----------
        data : array-like
            Data to test
        multivariate : bool
            Whether the data is multivariate (2D array)
        time_series : bool
            Whether the data should be treated as time series
        """

        __s_fn_id__ = f"{self.__name__} function <run_checks>"

        try:
            ''' validate checks '''
            self.checks = checks
            ''' validate and convert dataframe to array '''
            self.data = data
            ''' convert datetime to string '''
            
            data = np.asarray(self.data.toPandas())
            # Check for NaN values
            nan_mask = np.isnan(data)
            has_nans = np.any(nan_mask)
            if has_nans:
                print(f"Warning: Data contains NaN values. Handling them appropriately in each test.")
    
            # Ensure data is in the right format
            if multivariate and len(data.shape) == 1:
                data = data.reshape(-1, 1)
                logger.warning("%s Data was reshaped to 2D for multivariate analysis",__s_fn_id__)

            ''' Run checks '''
            results = []
            ''' STATIONARITY '''
            if "stationarity" in checks and time_series:
                print("running STATIONARITY CHECK ...")
                results.append(dataWorkLoads.stationarity(
                    data if len(data.shape) == 1 else data[:, 0],
                    multivariate,))
            elif "stationarity" in checks and not time_series:
                print("Skipped stationarity (not time series data)")
                logger.warning("Skipped stationarity (not time series data)")
            else:
                pass
            ''' DATA VOLUME '''
            if "data volume" in checks:
                print("running DATA VOLUME CHECK ...")
                results.append(dataWorkLoads.data_volume(data=data))
            ''' REPRESENTATIVENESS '''
            if "representativeness" in checks:
                print("running REPRESENTATIVENESS CHECK ...")
                # schema = {col: col_type for col, col_type in self._data.dtypes}
                # cast_cols = [ col for col, col_type in schema.items() if col_type in ['date', 'datetime']]
                # _sdf = functools.reduce(
                #     lambda df, x: df.withColumn(x, F.unix_timestamp(x)),
                #     cast_cols, self._data,
                #     )
                # rep_data = np.asarray(_sdf.toPandas())
                results.append(
                    dataWorkLoads.representativeness(data=data, 
                    multivariate=multivariate))
            
            # if multivariate:
            #     # For multivariate data, we'll use PCA for visualization
            #     scaler = StandardScaler()
            #     data_scaled = scaler.fit_transform(data)
            #     pca = PCA(n_components=1)
            #     data_pca = pca.fit_transform(data_scaled).flatten()
            #     print("Note: Using PCA first component for representativeness visualization")
            #     results.append(check_representativeness(data_pca))
            # else:
            #     results.append(check_representativeness(data))
            
            # print("\n==== 4. DIMENSIONALITY CHECK ====")
            # check_dimensionality(data)
            
            # print("\n==== 5. CORRELATION STRUCTURE CHECK ====")
            # check_correlation_structure(data)
            
            # print("\n==== 6. MULTIMODALITY CHECK ====")
            # if multivariate:
            #     print("For multivariate data, checking first PCA component")
            #     check_multimodality(data_pca)
            # else:
            #     check_multimodality(data)
            
            # print("\n==== 7. BOUNDARY CONDITIONS CHECK ====")
            # if multivariate:
            #     print("Checking each dimension separately:")
            #     for i in range(data.shape[1]):
            #         print(f"\nDimension {i+1}:")
            #         check_boundary_conditions(data[:, i])
            # else:
            #     check_boundary_conditions(data)
            
            # print("\n==== 8. MIXING PROPERTIES CHECK ====")
            # if time_series:
            #     if multivariate:
            #         print("Checking first dimension for mixing properties:")
            #         check_mixing_properties(data[:, 0])
            #     else:
            #         check_mixing_properties(data)
            # else:
            #     print("Skipped (not time series data)")
            
            # print("\n==== 9. PRIOR KNOWLEDGE CHECK ====")
            # if multivariate:
            #     print("Checking each dimension separately:")
            #     for i in range(min(3, data.shape[1])):  # Limit to first 3 dimensions
            #         print(f"\nDimension {i+1}:")
            #         check_prior_knowledge(data[:, i])
            # else:
            #     check_prior_knowledge(data)
            
            # print("\n==== 10. NOISE CHARACTERISTICS CHECK ====")
            # if time_series:
            #     if multivariate:
            #         print("Checking first dimension for noise characteristics:")
            #         check_noise_characteristics(data[:, 0])
            #     else:
            #         check_noise_characteristics(data)
            # else:
            #     print("Skipped (not time series data)")
            print("\nFinished running checks!")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        finally:
            logger.debug("%s completed %d checks to return results", 
                         __s_fn_id__, len(results))
            return results
