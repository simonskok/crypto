# CryptoQuantSignals
This R package contains the code for Crypto quantitative analysis. 

# Installation

The package is not yet on CRAN; the easiest way to install it is to open RStudio (or any other R IDE) and type:

```
devtools::install_github("simonskok/crypto")
```

Note, installation takes a long time as the folder contains one slightly larger csv file with all the historical quantiles data.

# Hot to use

To get the crypto coins by with volume of at least 10000000, type:

```
Crypto_Symbols_Selection(Volume24h = 10000000, Only_Symbols = F)
```

To get the largest movers in the last 7d, type:

```
Crypto_Largest_Movers(Period = "7d")
```

To get the OHLCV data for symbols from the lists above, type

```
Crypto_OHLC_Data(Symbols = "BTC", Interval = 15, Count = 100)
```
Note that for some alt coins the OHLC data is not available.






