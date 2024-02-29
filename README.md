# CryptoQuantSignals
This R package contains the code for Crypto quantitative analysis on OHLCV data.

Note that use of such data for making investment decisions is up to you - the quality of the data is very good, so it can
be used for research purposes and for backtesting with very high degree of certainty.

# Installation

The package is not yet on CRAN; the easiest way to install it is to open RStudio (or any other R IDE) and type:

```
devtools::install_github("simonskok/crypto")
```

Note, installation takes a long time as the folder contains one slightly larger csv file with all the historical quantiles data.

# Hot to use

To get the crypto coins with volume of at least 10000000 in the last 24h, type:

```
Crypto_Symbols_Selection(Volume24h = 10000000, Only_Symbols = F)
```

To get the largest movers in the last 7d, type:

```
Crypto_Largest_Movers(Period = "7d")
```

To get the OHLCV data for symbols from the lists above, type:

```
Crypto_OHLC_Data(Symbols = "BTC", Interval = 15, Count = 100)

Crypto_Largest_Movers(Period = "24h") %>% .$symbol %>% head(10) %>% Crypto_OHLC_Data(Interval = c(15,60, "D"), Count = 100)
```
Note that for some alt coins the OHLC data is not available.

Combining all main functions in the package gives: 

```
Crypto_Largest_Movers(Period = "24h") %>% .$symbol %>% head(20)  %>% c(., "BTC") %>% unique() %>% Crypto_OHLC_Data(Interval = 15, Count = 100) %>% Imap_and_rbind(New_Column_Name = "Candle") %>% Apply_Calculate_Trading_Signal() %>% Normalize_By_BTC_Value() %>% Add_Historical_Quantiles()

```
The code sequence above takes the 20 largest crypto movers in the absolute sense in the last 24h, adds BTC, fetches 15 Min OHLCV data for these symbols, calculates quantitave indicators, normalizes them with BTC values and adds historical quantiles to check how extreme are current values relative to the past. Nrow in the final dataset represents the number of data points in the calculation of the historical quantiles.

To get the most out of this package, you can open the terminal and type:

```
Update_Crypto_OHLC_Multiple_Granularity(Granularity = "15M", N_symbols = 100, Duration = 60, Wait_after_round_time = 15, Write_CSV  = T)
```
This will write a CSV file every 15 Min with the quantitative signals obtained in the step above.










