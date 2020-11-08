import os, sys
import pandas as pd
import numpy as np
import time
import talib as ta
from patsy import dmatrices
from statsmodels.stats.outliers_influence import variance_inflation_factor
from statsmodels.tools.tools import add_constant



#################################################################################    
##################### Get Variance Influence Factors ############################
#################################################################################
def get_variance_influence_factors(df_prices):
    
    vif = pd.DataFrame()    
    X1 = df_prices.copy()
    features = "+".join(X1.columns[~X1.columns.isin(["Close"])])
    y, X = dmatrices('Close ~' + features,X1, return_type='dataframe')    
    vif["VIF Factor"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
    vif["features"] = X.columns
    return vif.round(1)



#################################################################################    
########################### Momentum Indicators #################################
#################################################################################
def get_momentum_indicators(df_price):

    df_local = df_price.copy()
    df_nonna_idxs = df_local[~df_local.Close.isna()].Close.index        
       
    np_adj_close = df_local.Adj_Close.values
    np_close = df_local.Close.values
    np_open = df_local.Open.values
    np_high = df_local.High.values
    np_low = df_local.Low.values
    np_volume = df_local.Volume.values
    
    np_nan_indices = np.isnan(np_close)
    
    
    #ADX-Average Directional Movement Index
    ADX = pd.Series(ta.ADX(np_high[~np_nan_indices], 
                           np_low[~np_nan_indices], 
                           np_adj_close[~np_nan_indices], timeperiod=20
                          ), index=df_nonna_idxs)
    df_local['ADX'] = ADX
    
    
    #ADXR-Average Directional Movement Index Rating
    ADXR = pd.Series(ta.ADXR(np_high[~np_nan_indices], 
                             np_low[~np_nan_indices], 
                             np_adj_close[~np_nan_indices]
                            ), index=df_nonna_idxs)
    df_local['ADXR'] = ADXR
    
    
    #APO-Absolute Price Oscillator
    APO = pd.Series(ta.APO(np_adj_close[~np_nan_indices]), index=df_nonna_idxs)
    df_local['APO'] = APO
    
    
    #AROON-Aroon
    AROON = ta.AROON(np_high[~np_nan_indices], 
                     np_low[~np_nan_indices])
                              
    
    df_local['AROON_DOWN'] = pd.Series(AROON[0], index=df_nonna_idxs)
    df_local['AROON_UP'] = pd.Series(AROON[1], index=df_nonna_idxs)
    
    
    
    #AROONOSC-Aroon Oscillator
    AROONOSC = pd.Series(ta.AROONOSC(np_high[~np_nan_indices], 
                                     np_low[~np_nan_indices]
                                    ), index=df_nonna_idxs)
    df_local['AROONOSC'] = AROONOSC
    
    
    #BOP-Balance Of Power
    BOP = pd.Series(ta.BOP(np_open[~np_nan_indices], 
                           np_high[~np_nan_indices], 
                           np_low[~np_nan_indices],
                           np_adj_close[~np_nan_indices]
                          ), index=df_nonna_idxs)
    df_local['BOP'] = BOP
    
    
    #AROONOSC-Aroon Oscillator
    AROONOSC = pd.Series(ta.AROONOSC(np_high[~np_nan_indices], 
                                     np_low[~np_nan_indices]
                                    ),index=df_nonna_idxs)
    df_local['AROONOSC'] = AROONOSC
    
    
    #CCI-Commodity Channel Index
    CCI = pd.Series(ta.CCI(np_high[~np_nan_indices], 
                            np_low[~np_nan_indices],
                            np_adj_close[~np_nan_indices]
                          ), index=df_nonna_idxs)
    df_local['CCI'] = CCI
    
    
    #CMO-Chande Momentum Oscillator
    CMO = pd.Series(ta.CMO(np_adj_close[~np_nan_indices]
                          ), index=df_nonna_idxs)
    df_local['CMO'] = CMO
    
    
    #DX-Directional Movement Index
    DX = pd.Series(ta.DX(np_high[~np_nan_indices], 
                            np_low[~np_nan_indices],
                            np_adj_close[~np_nan_indices]
                          ), index=df_nonna_idxs)
    df_local['DX'] = DX
    
    
    #MACD-Moving Average Convergence/Divergence
    MACD = pd.Series(ta.MACD(np_adj_close[~np_nan_indices]
                            )[0], index=df_nonna_idxs)
    df_local['MACD'] = MACD
    
    
    #MACDEXT-MACD with controllable MA type
    MACDEXT = pd.Series(ta.MACDEXT(np_adj_close[~np_nan_indices]
                                    )[0], index=df_nonna_idxs)
    df_local['MACDEXT'] = MACDEXT
    
    
    #MACDFIX-Moving Average Convergence/Divergence Fix 12/26
    MACDFIX = pd.Series(ta.MACDFIX(np_adj_close[~np_nan_indices]
                                  )[0], index=df_nonna_idxs)
    df_local['MACDFIX'] = MACDFIX
    
    #MFI-Money Flow Index
    MFI = pd.Series(ta.MFI(np_high[~np_nan_indices], 
                           np_low[~np_nan_indices],
                           np_adj_close[~np_nan_indices],
                           ##np_volume[~np_nan_indices]
                           np.asarray(np_volume[~np_nan_indices], dtype='float')
                          ), index=df_nonna_idxs)
    df_local['MFI'] = MFI
    
    
    #MINUS_DI-Minus Directional Indicator
    MINUS_DI = pd.Series(ta.MINUS_DI(np_high[~np_nan_indices], 
                                     np_low[~np_nan_indices], 
                                     np_adj_close[~np_nan_indices]
                                    ), index=df_nonna_idxs)
    df_local['MINUS_DI'] = MINUS_DI

    
    #MINUS_DM-Minus Directional Movement
    MINUS_DM = pd.Series(ta.MINUS_DM(np_high[~np_nan_indices], 
                                     np_low[~np_nan_indices]
                                    ), index=df_nonna_idxs)
    df_local['MINUS_DM'] = MINUS_DM

    
    #MOM-Momentum
    MOM = pd.Series(ta.MOM(np_adj_close[~np_nan_indices]
                          ), index=df_nonna_idxs)
    df_local['MOM'] = MOM

    #PLUS_DI-Plus Directional Indicator    
    PLUS_DI = pd.Series(ta.PLUS_DI(np_high[~np_nan_indices], 
                                   np_low[~np_nan_indices],
                                   np_adj_close[~np_nan_indices]
                                  ), index=df_nonna_idxs)
    df_local['PLUS_DI'] = PLUS_DI
    
    
    #PLUS_DM-Plus Directional Movement
    PLUS_DM = pd.Series(ta.PLUS_DM(np_high[~np_nan_indices], 
                                   np_low[~np_nan_indices]
                                  ), index=df_nonna_idxs)
    df_local['PLUS_DM'] = PLUS_DM
    
    
    
    #PPO-Percentage Price Oscillator
    PPO = pd.Series(ta.PPO(np_adj_close[~np_nan_indices]
                          ), index=df_nonna_idxs)
    df_local['PPO'] = PPO

    
    #ROC-Rate of change : ((price/prevPrice)-1)*100
    ROC = pd.Series(ta.ROC(np_adj_close[~np_nan_indices]), index=df_nonna_idxs)
    df_local['ROC'] = ROC
                                        
                                      
    #ROCP-Rate of change Percentage: (price-prevPrice)/prevPrice                                        
    ROCP = pd.Series(ta.ROCP(np_adj_close[~np_nan_indices]
                            ), index=df_nonna_idxs)
    df_local['ROCP'] = ROCP
                                        

    #RSI-Relative Strength Index
    RSI = pd.Series(ta.RSI(np_adj_close[~np_nan_indices]
                          ), index=df_nonna_idxs)
    df_local['RSI'] = RSI
    
                                        
    #STOCH-Stochastic
    STOCH = ta.STOCH(np_high[~np_nan_indices], 
                     np_low[~np_nan_indices],
                     np_adj_close[~np_nan_indices])                                  
    df_local['STOCH_SLOWK'] = pd.Series(STOCH[0], index=df_nonna_idxs)
    df_local['STOCH_SLOWD'] = pd.Series(STOCH[1], index=df_nonna_idxs)

                                        
    #STOCHF-Stochastic Fast
    STOCHF = ta.STOCHF(np_high[~np_nan_indices], 
                     np_low[~np_nan_indices],
                     np_adj_close[~np_nan_indices])                                  
    df_local['STOCHF_FASTK'] = pd.Series(STOCHF[0], index=df_nonna_idxs)
    df_local['STOCHF_FASTD'] = pd.Series(STOCHF[1], index=df_nonna_idxs)                                        
                                        
                                        
    #STOCHRSI-Stochastic Relative Strength Index
    STOCHRSI = ta.STOCHF(np_high[~np_nan_indices], 
                         np_low[~np_nan_indices],
                         np_adj_close[~np_nan_indices])                                  
    df_local['STOCHRSI_FASTK'] = pd.Series(STOCHRSI[0], index=df_nonna_idxs)
    df_local['STOCHRSI_FASTD'] = pd.Series(STOCHRSI[1], index=df_nonna_idxs)                                                                                
                                        

    #TRIX-1-day Rate-Of-Change (ROC) of a Triple Smooth EMA                                        
    TRIX = pd.Series(ta.TRIX(np_adj_close[~np_nan_indices]
                            ), index=df_nonna_idxs)
    df_local['TRIX'] = TRIX

    #ULTOSC-Ultimate Oscillator 
    ULTOSC = pd.Series(ta.ULTOSC(np_high[~np_nan_indices], 
                                   np_low[~np_nan_indices],
                                   np_adj_close[~np_nan_indices]
                                  ), index=df_nonna_idxs)
    df_local['ULTOSC'] = ULTOSC
                                
    #WILLR-Williams' %R                                        
    WILLR = pd.Series(ta.WILLR(np_high[~np_nan_indices], 
                                  np_low[~np_nan_indices], 
                                  np_adj_close[~np_nan_indices],
                                  timeperiod=14), index=df_nonna_idxs)
    df_local['WILLR'] = WILLR
    
    return df_local
    

    
#################################################################################    
############################ Volumne Indicators #################################
#################################################################################
def get_volumne_indicators(df_price):
    
    df_local = df_price.copy()    
    df_nonna_idxs = df_local[~df_local.Close.isna()].Close.index        

    np_adj_close = df_local.Adj_Close.values
    np_close = df_local.Close.values
    np_open = df_local.Open.values
    np_high = df_local.High.values
    np_low = df_local.Low.values
    np_volume = df_local.Volume.values

    np_nan_indices = np.isnan(np_close)

    #AD-Chaikin A/D Line        
    AD = pd.Series(ta.AD(np_high[~np_nan_indices], 
                         np_low[~np_nan_indices], 
                         np_close[~np_nan_indices],
                         # np_volume[~np_nan_indices],
                         np.asarray(np_volume[~np_nan_indices], dtype='float')
                        ), index=df_nonna_idxs)
    df_local['AD'] = AD
    
    #ADOSC-Chaikin A/D Oscillator
    ADOSC = pd.Series(ta.ADOSC(np_high[~np_nan_indices], 
                               np_low[~np_nan_indices], 
                               np_close[~np_nan_indices],
                               #np_volume[~np_nan_indices],
                               np.asarray(np_volume[~np_nan_indices], dtype='float')                          
                              ), index=df_nonna_idxs)
    df_local['ADOSC'] = ADOSC
    
    #OBV-On Balance Volume
    OBV = pd.Series(ta.OBV(np_close[~np_nan_indices],
                           #np_volume[~np_nan_indices],
                           np.asarray(np_volume[~np_nan_indices], dtype='float')
                          ), index=df_nonna_idxs)
    df_local['OBV'] = OBV
    
    return df_local
        
    
    
    
#################################################################################    
########################## Volatility Indicators ################################
#################################################################################
def get_volatility_indicators(df_price):
    
    df_local = df_price.copy()
    df_nonna_idxs = df_local[~df_local.Close.isna()].Close.index        

    np_adj_close = df_local.Adj_Close.values
    np_close = df_local.Close.values
    np_open = df_local.Open.values
    np_high = df_local.High.values
    np_low = df_local.Low.values
    np_volume = df_local.Volume.values

    np_nan_indices = np.isnan(np_close)
    
        
    #ATR-Average True Range
    ATR = pd.Series(ta.ATR(np_high[~np_nan_indices], 
                           np_low[~np_nan_indices], 
                           np_adj_close[~np_nan_indices]
                          ), index=df_nonna_idxs)
    df_local['ATR'] = ATR
    
    #NATR-Normalized Average True Range
    NATR = pd.Series(ta.NATR(np_high[~np_nan_indices], 
                             np_low[~np_nan_indices], 
                             np_adj_close[~np_nan_indices]
                          ), index=df_nonna_idxs)
    df_local['NATR'] = NATR
    
    #TRANGE-True Range
    TRANGE = pd.Series(ta.TRANGE(np_high[~np_nan_indices], 
                                 np_low[~np_nan_indices], 
                                 np_adj_close[~np_nan_indices]
                                ), index=df_nonna_idxs)
    df_local['TRANGE'] = TRANGE
   
    
    return df_local




#################################################################################    
########################## Cycle Indicators ################################
#################################################################################
def get_cycle_indicators(df_price):
    
    df_local = df_price.copy()
    df_nonna_idxs = df_local[~df_local.Close.isna()].Close.index        

    np_adj_close = df_local.Adj_Close.values
    np_close = df_local.Close.values
    np_open = df_local.Open.values
    np_high = df_local.High.values
    np_low = df_local.Low.values
    np_volume = df_local.Volume.values

    np_nan_indices = np.isnan(np_close)
    
            
    #HT_DCPERIOD-Hilbert Transform - Dominant Cycle Period
    HT_DCPERIOD = pd.Series(ta.HT_DCPERIOD(np_adj_close[~np_nan_indices]
                                            ), index=df_nonna_idxs)    
    df_local['HT_DCPERIOD'] = HT_DCPERIOD
    
    
    #HT_DCPHASE-Hilbert Transform - Dominant Cycle Phase
    HT_DCPHASE = pd.Series(ta.HT_DCPHASE(np_adj_close[~np_nan_indices]
                                            ), index=df_nonna_idxs)    
    df_local['HT_DCPHASE'] = HT_DCPHASE
    
    
    #HT_PHASOR-Hilbert Transform - Phasor Components
    HT_PHASOR = ta.HT_PHASOR(np_adj_close[~np_nan_indices])
    
    df_local['HT_PHASOR_INPHASE'] = pd.Series(HT_PHASOR[0], index=df_nonna_idxs)    
    df_local['HT_PHASOR_QUADRATURE'] = pd.Series(HT_PHASOR[1], index=df_nonna_idxs)    
    
    
    #HT_SINE - Hilbert Transform - SineWave
    HT_SINE  = ta.HT_SINE (np_adj_close[~np_nan_indices])
    
    df_local['HT_SINE_SINE'] = pd.Series(HT_SINE[0], index=df_nonna_idxs)    
    df_local['HT_SINE_LEADSINE'] = pd.Series(HT_SINE [1], index=df_nonna_idxs)    
    
    
    #HT_TRENDMODE-Hilbert Transform - Trend vs Cycle Mode
    HT_TRENDMODE = pd.Series(ta.HT_TRENDMODE(np_adj_close[~np_nan_indices]
                                            ), index=df_nonna_idxs)    
    df_local['HT_TRENDMODE'] = HT_TRENDMODE
    
    return df_local

    


    
    