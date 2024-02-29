#' Update Crypto quant signals at regular times
#'
#' @param Granularity 5M/15M/30M/60M - how often to calculate the signals?
#' @param N_symbols Top N symbols from Binance by dollar volume
#' @param Columns_to_normalize_by_BTC Which colums to compare with BTC values?
#' @param Duration How long to update the signals
#' @param Start_time WHwn to start
#' @param Wait_after_round_time Wait after round time (in seconds) for data provider to update real time data
#' @param Write_CSV Write Signals to a csv file
#' @param N_Cores How many cores for fetching the data and calculations
#'
#' @return
#' @export

Update_Crypto_OHLC_Multiple_Granularity <- function(Granularity                 = "5M",
                                                    N_symbols                   = 100,
                                                    Columns_to_normalize_by_BTC = c("Returns", "atr", "BB_Width", "ADX", "pctB", "RSI"),
                                                    Duration                    = "Regular",
                                                    Start_time                  = "08:00:00",
                                                    Wait_after_round_time       = 10,
                                                    Write_CSV                   = T,
                                                    N_Cores                     = 2){

  if (Sys.time() < as.POSIXct(paste(Sys.Date(), Start_time))){

    Sys.sleep(as.numeric(difftime(as.POSIXct(paste(Sys.Date(), Start_time)), Sys.time(), units = "secs")))

  }

  Log_File       <- "./helper/Crypto_OHLC_Error.txt"

  if (file.exists(Log_File)) file.remove(Log_File)

  Crypto_Folder  <- "./data/Signals/"

  ###

  Duration            <- if (Duration == "Regular") ceiling(Time_Since_US_Market_Open_Close(TOD = "Close")) + 2 else Duration

  Last_Run            <- lubridate::floor_date(Sys.time() + Duration*60, unit = "1M")

  Next_Round_Time     <- lubridate::ceiling_date(x = Sys.time(), unit = Granularity)

  Now                 <- lubridate::floor_date(x = Sys.time(), unit = Granularity)

  Minute              <- c(5, 15, 30, 60)[which(lubridate::minute(Now) %% c(5, 15, 30, 60) == 0) %>% last()]

  Last_File           <- paste0("./data/Signals/Signals_", Correct_Colnames(Now),"_", Minute, "M", ".csv")

  if (file.exists(Last_File)){

    Sleep_For           <- as.numeric(difftime(Next_Round_Time, Sys.time(), units = "secs"))

    print(paste("Sleep for:", round(Sleep_For, digits = 2)))

    Sys.sleep(Sleep_For)

  }

  Initiate_Parallel(N_Cores = N_Cores)

  Data_Symbols         <- Crypto_Symbols_Selection()

  while (Sys.time() < Last_Run){

    Data_Parameters      <- Multiple_Granularity_Parameters(Round_time_unit = Granularity)

    if (identical(Data_Parameters$Aggregate_Size, 5) & identical(Data_Parameters$Granularity, "1M")){

      Data_Parameters$Aggregate_Size <- NULL

      Data_Parameters$Granularity    <- "5M"

    }

    tryCatch(expr = {

      if (length(Data_Symbols) > 0){

        Sys.sleep(Wait_after_round_time)

        Round_Time       <-  lubridate::floor_date(x = Sys.time(), unit = Data_Parameters$Granularity)

        Symbols          <- if (Data_Parameters$Granularity == Granularity) Data_Symbols %>% head(N_symbols) else Data_Symbols

        print(Sys.time())

        Ticker_Groups <- split(Symbols, ceiling(seq_along(Symbols)/(length(Symbols)/N_Cores)))

        Data          <- (foreach::foreach(i = 1:length(Ticker_Groups), .export = "Crypto_OHLC_Data") %dopar% {

                          Interval <- if (is.null(Data_Parameters$Aggregate_Size)){

                                        Data_Parameters$Granularity

                                      } else {

                                        Basic_Granularity     <- Data_Parameters$Granularity

                                        Sorted_Granularities  <- c("1M", "5M", "15M", "30M", "60M")

                                        Sorted_Granularities[match(Basic_Granularity, Sorted_Granularities):(match(Basic_Granularity, Sorted_Granularities) + length(Data_Parameters$Aggregate_Size))]


                                      }

                          Interval <- as.numeric(gsub("M", "", Interval))

                          Crypto_OHLC_Data(Symbols  = Ticker_Groups[[i]],
                                           Interval = Interval,
                                           Count    = 150)

                        }) %>% unlist(recursive = F)

        if (length(Data) > 0){

          Data             <-  Data %>% Imap_and_rbind(New_Column_Name = "Candle")

          Data             <-  Data %>% dplyr::group_by(across(intersect(c("Symbol", "Candle"), colnames(.)))) %>%

                               dplyr::filter(dplyr::last(Time) == Round_Time) %>% dplyr::ungroup()

          Missing_Symbols  <-  setdiff(Symbols, unique(Data$Symbol))

          print(paste("Proportion missing data:", length(Missing_Symbols)/length(Symbols)))

          if (length(Missing_Symbols)/length(Symbols) > 0.2 | "BTC" %in% Missing_Symbols){

            # Allow at least 15 s extra sleep at round times

            Additional_Sleep <- if (lubridate::minute(Sys.time()) %% 5 == 0) max(15, tail(Wait_after_round_time, 1)) else tail(Wait_after_round_time, 1)

            Sys.sleep(Additional_Sleep)

            missing_tickers       <- split(Missing_Symbols, ceiling(seq_along(Missing_Symbols)/(length(Missing_Symbols)/(N_Cores))))

            Missing_Data          <- (foreach(i = 1:length(missing_tickers), .export = "Crypto_OHLC_Data") %dopar% {

                                      Interval <- if (is.null(Data_Parameters$Aggregate_Size)){

                                        Data_Parameters$Granularity

                                      } else {

                                        Basic_Granularity    <- Data_Parameters$Granularity

                                        Sorted_Granularities <- c("1M", "5M", "15M", "30M", "60M")

                                        Sorted_Granularities[match(Basic_Granularity, Sorted_Granularities):(match(Basic_Granularity, Sorted_Granularities) + length(Data_Parameters$Aggregate_Size))]


                                      }

                                      Interval <- as.numeric(gsub("M", "", Interval))

                                      Crypto_OHLC_Data(Symbols  = missing_tickers[[i]],
                                                       Interval = Interval,
                                                       Count    = 150)

                                    }) %>% unlist(recursive = F) %>% purrr::keep(~.x %>% nrow > 0)

            if (length(Missing_Data) > 0){

              Missing_Data      <-  Missing_Data %>% Imap_and_rbind(New_Column_Name = "Candle")

              Missing_Data      <-  Missing_Data %>% dplyr::group_by(across(intersect(c("Symbol", "Candle"), colnames(.)))) %>%

                                    dplyr::filter(dplyr::last(Time) == Round_Time) %>% dplyr::ungroup()

              Data              <-  dplyr::bind_rows(Data, Missing_Data)

            }

            Data

          }

          print(paste("Data obtained."))

          if (nrow(Data) > 0){

            Symbol_Groups     <- Ticker_Groups %>% purrr::map(~.x %>% data.frame()) %>%

                                 Imap_and_rbind(New_Column_Name = "Group") %>% set_colnames(c("Symbol", "Group"))

            Data              <- dplyr::left_join(Data, Symbol_Groups, by = "Symbol")

            Data              <- Data %>% split(., .$Group)

            Signal            <- (foreach(i = 1:length(Data), .export = "Apply_Calculate_Trading_Signal") %dopar% {

                                     Apply_Calculate_Trading_Signal(Dataset          = Data[[i]],
                                                                    Relative_value_N = Data_Parameters$Relative_value_N,
                                                                    N                = Data_Parameters$Pct_rank_N)

                                  }) %>% dplyr::bind_rows() %>% dplyr::select(-intersect(colnames(.), "Group"))

            print(paste("Signal calculated."))

            Signal             <- Normalize_By_BTC_Value(Dataset = Signal, Columns_to_normalize = Columns_to_normalize_by_BTC)

            print(paste("Signal normalized by SPY."))

            Signal             <- Add_Historical_Quantiles(Dataset               = Signal,
                                                           Hist_Quantile_Columns = Columns_to_normalize_by_BTC,
                                                           Filter_Nas            = F)

            print(paste("Hist Quantiles added."))

            Largest_Granularity <- ifelse(is.null(Data_Parameters$Aggregate_Size),

                                          Data_Parameters$Granularity,

                                          paste0(as.numeric(gsub("M", "", Data_Parameters$Granularity))*max(Data_Parameters$Aggregate_Size), "M"))


            if (Write_CSV) data.table::fwrite(x          = Signal,
                                              file       = paste0("./data/Signals/Signals_", Correct_Colnames(Round_Time),"_", Largest_Granularity, ".csv"),
                                              quote      = F,
                                              row.names  = F,
                                              dateTimeAs = "write.csv")

            Signal

          }

        }

      }

    }, error = function(e) write(paste0("Crypto OHLC failed at ", lubridate::floor_date(x = Sys.time(), unit = Granularity), "because ", e), file = Log_File, append = TRUE))

    Next_Check_Time  <- lubridate::ceiling_date(x = Sys.time(), unit = Granularity)

    Wait_For         <- as.numeric(difftime(Next_Check_Time, Sys.time(), units = "secs")) + 1

    gc()

    write(paste0("Wait for ", Wait_For, " at ", Sys.time()) , file = Log_File, append = TRUE)

    print(paste("Sleep for:", Wait_For))

    Sys.sleep(Wait_For)

  }

}

#' Determine parameters for fetching data at round times
#'
#' @param Round_time_unit
#' @param Allow_greater
#' @param Multiple_granularities
#' @param Pct_rank_N
#' @param Relative_value_N
#' @param Weight_model_col
#' @param Sort_colname
#' @param Aggregate_dataset
#' @param Allow_smaller_than_granularity
#'
#' @return

Multiple_Granularity_Parameters  <- function(Round_time_unit        = "1M",
                                             Allow_greater          = T,
                                             Multiple_granularities = T,
                                             Pct_rank_N             = 100,
                                             Relative_value_N       = 20,
                                             Weight_model_col       = "Relative_Value_BB_Width",
                                             Sort_colname           = "Relative_Value_BB_Width",
                                             Aggregate_dataset   = T,
                                             Allow_smaller_than_granularity = T){

  Granularity      <- Determine_Granularity(Round_time_unit = Round_time_unit, Allow_greater = Allow_greater) %>% as.character()

  Parameters       <- switch (Granularity,

                            "15"  = list(Last_N = 105,  Aggregate_Size = NULL,    Granularity = 15,     Bid_Ask_Spread = 0.015,   Remove_Earnings_N_Days = 2, Add_Daily_Stats = F),
                            "30"  = list(Last_N = 105,  Aggregate_Size = 2,       Granularity = 15,     Bid_Ask_Spread = 0.02,  Remove_Earnings_N_Days = 2, Add_Daily_Stats = F),
                            "1M"  = list(N_Days = 4,    Aggregate_Size = NULL,    Granularity = "1M",   Bid_Ask_Spread = 0.05,  Sub_Min_Granular = FALSE, Remove_Earnings_N_Days = 2, Add_Daily_Stats = F),
                            "5M"  = list(N_Days = 5,    Aggregate_Size = 5,       Granularity = "1M",   Bid_Ask_Spread = 0.075,  Combo = TRUE, Remove_Earnings_N_Days = 3, Add_Daily_Stats = F),
                            "15M" = list(N_Days = 10,   Aggregate_Size = 3,       Granularity = "5M",   Bid_Ask_Spread = 0.1,    Combo = TRUE, Remove_Earnings_N_Days = 3, Add_Daily_Stats = F),
                            "30M" = list(N_Days = 15,   Aggregate_Size = c(3,6),  Granularity = "5M",   Bid_Ask_Spread = 0.15,   Combo = TRUE, Remove_Earnings_N_Days = 4, Add_Daily_Stats = F),
                            "60M" = list(N_Days = 30,   Aggregate_Size = c(2,4),  Granularity = "15M",  Bid_Ask_Spread = 0.20,    Combo = TRUE, Remove_Earnings_N_Days = 5, Add_Daily_Stats = T),

                     )

  Parameters$Columns_to_normalize   <-  c("BB_Width", "atr")
  Parameters$End_Candle             <-  TRUE
  Parameters$Pct_rank_N             <-  Pct_rank_N
  Parameters$Relative_value_N       <-  Relative_value_N
  Parameters$Aggregate_dataset   <-  Aggregate_dataset
  Parameters$Weight_model_col       <-  Weight_model_col
  Parameters$Sort_colname           <-  Sort_colname

  if (!Multiple_granularities | !Allow_smaller_than_granularity){

    if (Determine_Granularity(Round_time_unit = Granularity, Allow_greater = F) == Determine_Granularity(Round_time_unit = Granularity, Allow_greater = T) | !is.null(Parameters$Aggregate_Size)){

      Parameters$Aggregate_Size <- NULL
      Parameters$Granularity    <- Granularity

      Parameters

    }

  }

  Parameters

}

#' Determine granularity for fetching data
#'
#' @param Round_time_unit
#' @param Allow_greater
#'
#' @return
#' @export

Determine_Granularity <- function(Round_time_unit = "15S",
                                  Allow_greater   = T){

  if (Allow_greater){

    Round_Time   <- lubridate::floor_date(x = Sys.time(), unit = Round_time_unit)

    Granularity  <- ifelse(Round_Time == lubridate::floor_date(x = Sys.time(), unit = "1H"), "60M",
                           ifelse(Round_Time == lubridate::floor_date(x = Sys.time(), unit = "30M"), "30M",
                                  ifelse(Round_Time == lubridate::floor_date(x = Sys.time(), unit = "15M"), "15M",
                                         ifelse(Round_Time == lubridate::floor_date(x = Sys.time(), unit = "5M"), "5M",
                                                ifelse(Round_Time == lubridate::floor_date(x = Sys.time(), unit = "1M"), "1M",
                                                       ifelse(Round_Time == lubridate::floor_date(x = Sys.time(), unit = "30S"), 30, 15))))))

  } else {

    Granularity <- Round_time_unit

  }


  Granularity

}


#' Helper function for fast curl json reading
#'
#' @param URL_Prefix
#' @param Variable
#' @param URL_Suffix
#' @param FUN
#' @param Names
#' @param Match_URLs
#' @param N
#' @param Verbose
#' @param ...
#'
#' @return

Curl_Multi_Json_Data <- function(URL_Prefix,
                                 Variable,
                                 URL_Suffix,
                                 FUN  = NA,
                                 Names = TRUE,
                                 Match_URLs = T,
                                 N = 100,
                                 Verbose = F,  ...){

  Start <- Sys.time()

  pool <- curl::new_pool()

  uris <- paste0(URL_Prefix,Variable,URL_Suffix)

  if (!Match_URLs)  uris <- rep(uris, N)

  data <- vector("list", length = length(uris))

  if (Match_URLs){

    cb <- function(req, Match_URLs = Match_URLs){

      Match <- match(req$url, uris)

      if (!is.na(Match)) data[[Match]] <<- rawToChar(req$content)

    }

  } else {

    cb <- function(req, Match_URLs = Match_URLs){

      data[[which(sapply(data, is.null))[1]]] <<- rawToChar(req$content)

    }

  }

  sapply(uris, curl::curl_fetch_multi, done=cb, pool=pool, ...)

  out   <- curl::multi_run(pool = pool)

  if (Names) names(data) <- Variable

  data  <- data %>% purrr::compact()

  Index <- which(sapply(data, jsonlite::validate))

  data  <- data[Index] %>% purrr::map(., ~jsonlite::fromJSON(.x))

  if (!is.na(FUN)){

    Fun_To_Apply <- match.fun(FUN)

    data         <- purrr::map(data, function(JSON) Fun_To_Apply(JSON, ...))
  }

  data <- data %>% purrr::compact()

  if (Verbose) print(Sys.time() - Start)

  data

}



#' Apply Trading Signal for multiple symbols
#'
#' @param Dataset
#' @param Relative_value_N
#' @param N
#' @param BB_stats_smooth
#' @param Last_row
#' @param Pct_rank_and_rel_value_stat
#' @param Remove_first_N_zero_values
#' @param Roll_LM_columns
#' @param ...
#'
#' @return
#' @export

Apply_Calculate_Trading_Signal  <- function(Dataset,
                                            Relative_value_N            = 20,
                                            N                           = 100,
                                            BB_stats_smooth             = "EMA",
                                            Last_row                    = T,
                                            Pct_rank_and_rel_value_stat = T,
                                            Remove_first_N_zero_values  = T,
                                            Roll_LM_columns             = c(), ...){


  if (class(Dataset)[1] == "list"){

    Stats      <- purrr::map(Dataset, function(x) tryCatch(expr  =   Calculate_Trading_Signal(Dataset                     = x,
                                                                                              N                           = N,
                                                                                              Relative_value_N            = Relative_value_N,
                                                                                              Last_row                    = Last_row,
                                                                                              BB_stats_smooth             = BB_stats_smooth,
                                                                                              Pct_rank_and_rel_value_stat = Pct_rank_and_rel_value_stat,
                                                                                              Roll_LM_columns             = Roll_LM_columns) %>%

                                                             {if (Remove_first_N_zero_values) .[(which(.$MACD_Hist != 0)[1] %>% replace(., is.na(.), 1)):nrow(.),] else . },

                                                           error = function(e) NULL)) %>%

                  purrr::compact() %>% {if (length(.) > 0) Imap_and_rbind(Named_List = . ) else NULL }

  } else if (any(c("tbl_data.frame()", "data.frame") %in% class(Dataset))){

    if ("Time" %in% attributes(Dataset)$names){

      Dataset <-  Dataset %>% split(., if ("Candle" %in% colnames(.)) list(.$Symbol, .$Candle) else .$Symbol)

      Dataset <-  Dataset[which(Dataset %>% lapply(nrow) %>% unlist(recursive = T, use.names = F) > 40)]

      Stats   <-  Dataset  %>% purrr::map(function(x)   tryCatch(expr  = Calculate_Trading_Signal(Dataset                     = x,
                                                                                                  N                           = N,
                                                                                                  Relative_value_N            = Relative_value_N,
                                                                                                  Last_row                    = Last_row,
                                                                                                  BB_stats_smooth             = BB_stats_smooth,
                                                                                                  Pct_rank_and_rel_value_stat = Pct_rank_and_rel_value_stat,
                                                                                                  Roll_LM_columns             = Roll_LM_columns) %>%

                                                                   {if (Remove_first_N_zero_values) .[(which(.$MACD_Hist != 0)[1] %>% replace(., is.na(.), 1)):nrow(.),] else . },

                                                                 error = function(e) NULL )) %>%

                  Imap_and_rbind() %>% {if ("Candle" %in% colnames(.)) Add_Minute_Factor_Column(Dataset = . ) %>%

                  dplyr::arrange(Symbol, Minute_Factor) %>% dplyr::select(-Minute_Factor) else . }

    } else {

      Stats   <-  purrr::map(Dataset %>% .[-1],

                             function(Column) purrr::map(Column, function(x) Calculate_Trading_Signal(Dataset                     = x,
                                                                                                      N                           = N,
                                                                                                      Relative_value_N            = Relative_value_N,
                                                                                                      Last_row                    = Last_row,
                                                                                                      BB_stats_smooth             = BB_stats_smooth,
                                                                                                      Pct_rank_and_rel_value_stat = Pct_rank_and_rel_value_stat,
                                                                                                      Roll_LM_columns             = Roll_LM_columns) %>%

                                                           {if (Remove_first_N_zero_values) .[(which(.$MACD_Hist != 0)[1] %>% replace(., is.na(.), 1)):nrow(.),] else .}) %>%

                               set_names(Dataset$Symbol) %>% Imap_and_rbind()) %>%

        {if (!"Candle" %in% colnames(.[[1]])) purrr::imap(., ~cbind(.x, Candle = .y)) else .} %>% dplyr::bind_rows()

    }

  }

  Stats <- Stats %>% dplyr::ungroup()

  Stats

}

#' Calculate Trading Signal for a OHLCV data.frame or data.table
#'
#' @param Dataset
#' @param Pct_rank_and_rel_value_stat
#' @param Last_row
#' @param N
#' @param Relative_value_N
#' @param Roll_LM_columns
#' @param BB_stats_N
#' @param BB_stats_smooth
#' @param Wilder
#' @param Rel_value_cols
#' @param Local_extremes_columns
#' @param ADX_threshold
#' @param Smooth
#' @param Fibonnaci
#' @param Remove_leading_NAs
#' @param ...
#'
#' @return
#' @export

Calculate_Trading_Signal  <- function(Dataset,
                                      Pct_rank_and_rel_value_stat = TRUE,
                                      Last_row                    = T,
                                      N                           = 100,
                                      Relative_value_N            = 20,
                                      Roll_LM_columns             = c(),
                                      BB_stats_N                  = 20,
                                      BB_stats_smooth             = "EMA",
                                      Wilder                      = T,
                                      Rel_value_cols              = c("atr", "BB_Width", "Volume", "ADX", "RSI"),
                                      Local_extremes_columns      = c("ADX", "BB_Width"),
                                      ADX_threshold               = 45,
                                      Smooth                      = F,
                                      Fibonnaci                   = F,
                                      Remove_leading_NAs          = T, ...){

  # data.table::setDT(Dataset)

  if (all(is.na(Dataset$Volume))) Dataset$Volume <- 0

  if (!("data.table" %in% class(Dataset))) {Dataset <- if ("xts" %in% class(Dataset)) timetk::tk_tbl(Dataset) %>% dplyr::rename(Date = index) %>% data.table::as.data.table(.) else data.table::data.table(Dataset)}

  Roll_LM_columns    <-  c(Roll_LM_columns, c("BB_Width", "atr", "RSI", "ADX"))

  Second_BB_Smooth   <-  if (BB_stats_smooth == "EMA") "SMA" else "EMA"

  First_BBand        <-  Fast_BBands_DT(Dataset = Dataset, Smoothing = BB_stats_smooth)

  Second_BBad        <-  Fast_BBands_DT(Dataset = Dataset, Smoothing = Second_BB_Smooth) %>% set_colnames(paste0(colnames(.), "_", Second_BB_Smooth)) %>% data.frame()

  BBamds             <-  cbind(First_BBand, Second_BBad)

  Dataset            <-  dplyr::bind_cols(Dataset, BBamds)

  Dataset$pctB       <-  ifelse(is.infinite(Dataset$pctB), 0.5, Dataset$pctB)

  Dataset$Returns    <-  c(0, diff(log(Dataset$Close)))

  Dataset$index      <-  1:nrow(Dataset)

  Dataset[,  Roll_SD  := roll::roll_sd(Dataset$Returns, width = 20)]

  Dataset[, c("BB_Up", "BB_Down", "mavg_20") := list(up, dn, mavg)]

  Dataset[, c("dn","up", "mavg") := NULL]

  if (BB_stats_smooth == "EMA"){

    Dataset[, c("BB_Up_SMA", "BB_Down_SMA", "mavg_20_SMA") := list(up_SMA, dn_SMA, mavg_SMA)]

    Dataset[, c("up_SMA","dn_SMA", "mavg_20_SMA") := NULL]

  } else {

    Dataset[, c("BB_Up_EMA", "BB_Down_EMA", "mavg_20_EMA") := list(up_EMA, dn_EMA, mavg_EMA)]

    Dataset[, c("up_EMA","dn_EMA", "mavg_20_EMA") := NULL]

  }

  Dataset$BB_Width                                 <- (Dataset$BB_Up - Dataset$BB_Down)/Dataset$mavg_20

  Dataset[[paste0("BB_Width_", Second_BB_Smooth)]] <- (Dataset[[paste0("BB_Up_", Second_BB_Smooth)]] - Dataset[[paste0("BB_Down_", Second_BB_Smooth)]])/Dataset[[paste0("mavg_", Second_BB_Smooth)]]

  Dataset[[paste0("BB_Width_Ratio")]]              <-  Dataset$BB_Width/Dataset[[paste0("BB_Width_", Second_BB_Smooth)]]

  if ("pctB_SMA" %in% colnames(Dataset)) Dataset <- Dataset[, -match("pctB_SMA", colnames(Dataset)), with = F]

  Dataset$mavg_20       <- (Dataset$Close/Dataset$mavg_20) - 1

  for (i in c(50, 200)){

    if (nrow(Dataset) >= i){

      Dataset[[paste0("mavg_", i)]] <- if (BB_stats_smooth == "EMA"){

        (Dataset$Close/TTR::EMA(x = Dataset$Close, n = i, Wilder = F)) - 1

      } else {

        (Dataset$Close/roll::roll_mean(x = Dataset$Close, width = i)) - 1

      }

    }

  }

  SMA_Cols                 <- grep(paste0("mavg_", "[[:digit:]]"), colnames(Dataset), value = T)

  Dataset$Distance_to_MAs  <- rowMeans(Dataset[,SMA_Cols , with = FALSE])

  Dataset$RSI              <- QuantTools::rsi(Dataset$Close, n = 14)

  Dataset$atr              <- Fast_ATR(Dataset = Dataset, N = 14)

  Dataset$atr              <- Dataset$atr/Dataset$Close

  ADX                      <- TTR::ADX(HLC = Dataset[, c("High", "Low", "Close")],  n = 14, maType = "EMA", Wilder = Wilder) %>% data.frame() %>% .[-3]

  Dataset                  <- dplyr::bind_cols(Dataset, ADX)

  Roll_Min                 <- roll::roll_min(Dataset$Low, width = 10)

  Roll_Max                 <- roll::roll_max(Dataset$High, width = 10)

  Dataset$Max_Min_Range    <- (Roll_Max - Roll_Min)/Roll_Min

  Dataset$Rel_Body         <- roll::roll_mean(abs(Dataset$Open - Dataset$Close)/Dataset$Close, width = 5)

  Dataset$Rel_Candle       <- roll::roll_mean(abs(Dataset$High - Dataset$Low)/Dataset$Low, width = 5)

  Dataset$Ratio_SMA        <- roll::roll_mean((Dataset$Rel_Body/Dataset$Rel_Candle) %>% replace(., is.nan(.), 0), width = 5)

  Dataset$Diff_ADX_DI      <- Dataset$ADX - pmin(Dataset$DIp, Dataset$DIn)

  Dataset$Diff_DIp_DIn     <- Dataset$DIp - Dataset$DIn

  Ema_Volume_Short         <- QuantTools::ema(Dataset$Volume, n = 12)

  Ema_Volume_Long          <- QuantTools::ema(Dataset$Volume, n = 26)

  Dataset$PVO              <- (100*(Ema_Volume_Short - Ema_Volume_Long)/Ema_Volume_Long) %>% replace(., is.na(.), 0)

  Dataset$Sig              <- QuantTools::ema(Dataset$PVO, n = 9)

  Dataset$PVO_Hist         <- Dataset$PVO - Dataset$Sig

  Regression_Data            = tryCatch(expr = {rollRegres::roll_regres(Dataset$Close  ~ index,
                                                                        Dataset,
                                                                        width = 10,
                                                                        do_compute = c("r.squareds"))}, error = function(e) NULL)


  Regression                 = QuantTools::roll_lm(x = Dataset$Close, y = Dataset$index, 10)

  if (!is.null(Regression_Data)){

    Reg_Slope_vector <- as.vector(Regression_Data$coefs[,"index"])/Dataset$Close

    R_Squared_vector <- as.vector(Regression_Data$r.squareds)

    Dataset[, `:=`(Reg_Slope          = Reg_Slope_vector,
                   R_Squared          = R_Squared_vector)]

  } else {

    Dataset[, `:=`(Reg_Slope          = 0,
                   R_Squared          = Regression$r.squared)]

  }

  Dataset[,`:=`(Regression_R           = Regression$r)]

  if (Pct_rank_and_rel_value_stat){

    in_cols     <- if (is.null(Rel_value_cols)) c("atr", grep("BB_Width", colnames(Dataset), value = T), "Volume", "ADX", "RSI") else unique(c(intersect(c("BB_Width","BB_Width_SMA","BB_Width_EMA"), colnames(Dataset)), Rel_value_cols))

    in_cols     <- in_cols[which(in_cols %in% colnames(Dataset))]

    out_cols    <- paste0(rep("Relative_Value_", length(in_cols)),rep(in_cols))

    Dataset[, c(out_cols) := data.frame(lapply(.SD, function(x) Relative_Value(x, N = min(Relative_value_N, length(na.omit(x)))))), .SDcols = in_cols]

    if (all(c("Relative_Value_BB_Width", paste0("Relative_Value_BB_Width_", Second_BB_Smooth)) %in% colnames(Dataset))) Dataset$Rel_BBW_Ratio <- Dataset$Relative_Value_BB_Width/Dataset[[paste0("Relative_Value_BB_Width_", Second_BB_Smooth)]]

    out_cols    = paste0(rep("Pct_Rank_", length(in_cols)),rep(in_cols))

    Dataset[, c(out_cols) := data.frame(lapply(.SD, function(x) Pct_Rank(x, N = min(N, length(na.omit(x)))))), .SDcols = in_cols]

    Roll_LM_columns     <- Roll_LM_columns[which(Roll_LM_columns %in% colnames(Dataset))]

    for (col in Roll_LM_columns) Dataset[is.na(get(col)), (col) := 0]

    Roll_LM_Col_Names <- paste0("Roll_LM_R_", Roll_LM_columns)

    Dataset[, c(Roll_LM_Col_Names) := data.frame(lapply(.SD, function(x) QuantTools::roll_lm(Dataset$index, x, 10)$r)), .SDcols = Roll_LM_columns]

  }

  Sort_out_cols              <-  paste0(rep(c("Local_Max_", "Local_Min_"), length(Local_extremes_columns)),
                                        rep(Local_extremes_columns, each = 2))

  Dataset[, c(Sort_out_cols) := data.frame(lapply(.SD, function(x) Detect_Extremes(x, Threshold = c(0, -9999999)))), .SDcols = Local_extremes_columns]

  Dataset$CS_Reversals <- as.numeric(Dataset$Local_Max_ADX*(Dataset$ADX > ADX_threshold))

  if (Fibonnaci){

    FR100 <- max(Dataset[(nrow(Dataset) - min(9999, nrow(Dataset) - 1)):nrow(Dataset),"High"])
    FR0   <- min(Dataset[(nrow(Dataset) - min(9999, nrow(Dataset) - 1)):nrow(Dataset),"Low"])

    Diff  <- FR100 - FR0

    Dataset[,  `:=`(FR100    = FR100,
                    FR24     = FR100 - (Diff) * 0.236,
                    FR38     = FR100 - (Diff) * 0.382,
                    FR50     = FR100 - (Diff) * 0.500,
                    FR62     = FR100 - (Diff) * 0.618,
                    FR79     = FR100 - (Diff) * 0.786,
                    FR0      = FR0)]

  }

  if (Last_row){

    Dataset <- Dataset[nrow(Dataset),]

  } else {

    if (length(Roll_LM_columns) > 0 & Remove_leading_NAs){

      Min_ok_index <- max(apply(Dataset[,Roll_LM_columns, with = F], 2, function(x) which(x > 0)[1]) %>% as.vector())

      Dataset      <- Dataset[(Min_ok_index + 1):nrow(Dataset), ]

    }

  }

  Compression_Columns       <- c("atr", "BB_Width", "ADX")

  for (Col in Compression_Columns){

    if (all(c(Col, paste0("Relative_Value_", Col)) %in% colnames(Dataset)))   Dataset[[paste0(Col, "_Compression")]] <- Dataset[[Col]]*Dataset[[paste0("Relative_Value_", Col)]]

  }

  return(Dataset)

}


#' Normalize statistical values with Bitcoin statistical values
#'
#' @param Dataset
#' @param Columns_to_normalize
#' @param Columns_to_group_by
#' @param Reverse
#' @param Convert_to_positive
#' @param Merge_with_original
#' @param New_version
#' @param Remove_BTC
#'
#' @return
#' @export

Normalize_By_BTC_Value <- function(Dataset,
                                   Columns_to_normalize = c("Returns", "atr", "BB_Width", "ADX", "pctB", "RSI"),
                                   Columns_to_group_by  = NULL,
                                   Reverse              = F,
                                   Convert_to_positive  = F,
                                   Merge_with_original  = TRUE,
                                   New_version          = T,
                                   Remove_BTC           = F){

  Only_Same_Time       <- TRUE

  Columns_to_normalize <- intersect(colnames(Dataset), Columns_to_normalize)

  if (length(Columns_to_normalize) > 0){

    Dataset           <-  data.frame(Dataset)

    Symbol_Column        <-  Determine_Symbol_Column(Dataset = Dataset)

    Symbol_to_match      <-  grep(pattern = paste0(paste0("^BTC$"), collapse = "|"), x = unique(Dataset[[Symbol_Column]]), value = T)

    if (length(Symbol_to_match) > 0){

      if ("Candle" %in% colnames(Dataset)) Dataset <- Dataset %>% dplyr::mutate(Candle = ifelse(grepl("_Aggregated", !!as.name(Symbol_Column)), "Aggregated", Candle))

      if (is.null(Columns_to_group_by)){

        Columns_to_group_by  <- intersect(c("Time", "Candle"), colnames(Dataset))

        if (!Only_Same_Time)                           Columns_to_group_by    <- "Candle"

        Columns_to_group_by

      }

      Symbols_to_Match_Columns   <- Dataset %>% dplyr::select(all_of(Columns_to_group_by), all_of(Symbol_Column), all_of(Columns_to_normalize))

      BTC_Stats                  <- Symbols_to_Match_Columns %>% dplyr::filter(!!as.name(Symbol_Column) == !!Symbol_to_match) %>%

                                    set_colnames(c(Columns_to_group_by, Symbol_Column,  paste0(Symbol_to_match, "_Normalized_", Columns_to_normalize))) %>%

                                    dplyr::select(-!!as.name(Symbol_Column))

      for (Col in Columns_to_group_by){

        if ("IDate" %in% class(Symbols_to_Match_Columns[[Col]]) | "IDate" %in% class(BTC_Stats[[Col]])){

          Dataset[[Col]]               <- as.Date(Dataset[[Col]])

          Symbols_to_Match_Columns[[Col]] <- as.Date(Symbols_to_Match_Columns[[Col]])

          BTC_Stats[[Col]]                <- as.Date(BTC_Stats[[Col]])

        }

      }

      Symbols_to_Match_Columns   <- dplyr::left_join(Symbols_to_Match_Columns, BTC_Stats, by = Columns_to_group_by)

      BTC_Normalized_Columns     <- Symbols_to_Match_Columns

      for (Col in Columns_to_normalize){

        Normalized_Col       <- if (!Reverse) (Symbols_to_Match_Columns[[Col]]/Symbols_to_Match_Columns[[paste0(Symbol_to_match, "_Normalized_", Col)]]) else (Symbols_to_Match_Columns[[paste0(Symbol_to_match, "_Normalized_", Col)]]/Symbols_to_Match_Columns[[Col]])

        Normalized_Col_Value <- Normalized_Col %>% replace(., is.infinite(.), NA) %>% replace(., is.nan(.), 1)

        if (length(na.omit(Normalized_Col)) > 0){

          Normalized_Col_Value <- Normalized_Col_Value %>% {if (Convert_to_positive & min(na.omit(Normalized_Col)) < 0) abs(.) else . }

        }

        BTC_Normalized_Columns[paste0(Symbol_to_match ,"_Normalized_", Col)] <- Normalized_Col_Value

      }

      BTC_Normalized_Columns <- BTC_Normalized_Columns %>% dplyr::select(all_of(Columns_to_group_by), all_of(Symbol_Column), all_of(paste0(Symbol_to_match, "_Normalized_", Columns_to_normalize)))

      if (nrow(BTC_Normalized_Columns) == 1 & identical(unique(BTC_Normalized_Columns[[Symbol_Column]]), Symbol_to_match))  BTC_Normalized_Columns <- BTC_Normalized_Columns %>% dplyr::mutate_at(dplyr::vars(dplyr::contains(Symbol_to_match)), function(x) 1)

      for (Col in grep(pattern = Symbol_to_match, x = colnames(BTC_Normalized_Columns), value = T)){

        Symbol_to_match_index                                <- BTC_Normalized_Columns[[Symbol_Column]] == Symbol_to_match

        BTC_Normalized_Columns[[Col]][Symbol_to_match_index] <- 1

      }

      Dataset             <- if (Merge_with_original){

                                  dplyr::left_join(Dataset, BTC_Normalized_Columns, by = c(Columns_to_group_by, Symbol_Column))

                                } else {

                                  BTC_Normalized_Columns

                                }
      Dataset  <- Dataset %>% dplyr::ungroup()

      if (Remove_BTC){

        Dataset <- Dataset[Dataset$Symbol != Symbol_to_match, ]

      }

      Dataset

    }

    Dataset

  }

  Dataset

}


#' Join historical quantiles for selected columns
#'
#' @param Dataset
#' @param Past_Quantiles
#' @param Ticker
#' @param N
#' @param Min_N_Data
#' @param Min_Week
#' @param Granularity
#' @param Hist_Quantile_Columns
#' @param Extra_Columns
#' @param Round_Quantile_down
#' @param Filter_Nas
#'
#' @return
#' @export

Add_Historical_Quantiles <- function(Dataset,
                                     Past_Quantiles           = NULL,
                                     Ticker                   = NULL,
                                     N                        = 20,
                                     Min_N_Data               = 500,
                                     Min_Week                 = 50,
                                     Granularity              = NULL,
                                     Hist_Quantile_Columns    = c("Returns", "atr", "BB_Width", "ADX", "pctB", "RSI", "Diff_DIp_DIn", "Volume"),
                                     Extra_Columns            = c(),
                                     Round_Quantile_down      = T,
                                     Filter_Nas               = T){

  Hist_Quantile_Columns    <-  unique(c(Hist_Quantile_Columns, Extra_Columns))

  Signal                   <-  Dataset %>% data.frame()

  Dataset                  <-  Dataset %>% data.frame()

  Filter_At                <-  intersect(Hist_Quantile_Columns, c("BB_Width", "atr")) %>% intersect(colnames(Dataset), . )

  if (length(Filter_At) > 0) Dataset   <- Dataset %>% dplyr::filter_at(intersect(Hist_Quantile_Columns, c("BB_Width", "atr")),  dplyr::all_vars(. > 0))

  if ("Candle" %in% colnames(Dataset)){

    Dataset <- Add_Minute_Factor_Column(Dataset = Dataset) %>% dplyr::arrange(Symbol, Minute_Factor) %>% dplyr::select(-Minute_Factor)

  } else {

    Dataset <- Dataset %>% dplyr::arrange(Symbol)

  }

  if (is.null(Ticker))       Ticker      <-  sort(unique(Dataset$Symbol))

  if (is.null(Granularity))  Granularity <- if ("Candle" %in% colnames(Dataset)) Dataset$Candle  %>% unique()

  if (is.null(Past_Quantiles)) Past_Quantiles      <-  Read_Historical_Quantile(Ticker       = Ticker,

                                                                                Granularity  = Granularity)

  if (nrow(Past_Quantiles) > 0){

    if ("Nrow" %in% colnames(Past_Quantiles)) Past_Quantiles <- Past_Quantiles %>% dplyr::filter(Nrow >= Min_N_Data | Candle == "W" & Nrow >= Min_Week  | Candle == "D" & Nrow >= Min_Week*5)

    if (nrow(Past_Quantiles) > 0){

      Main_Symbol                   <-  "BTC"

      Hist_Quantile_Columns         <-  intersect(union(Hist_Quantile_Columns, sapply(c("Relative_Value_", paste0(Main_Symbol, "_Normalized_")), function(x) paste0(x, Hist_Quantile_Columns)) %>% as.vector() %>% grep(pattern = "R_Squared|Chaikin", x = .,  invert = T, value = T)), colnames(Dataset))

      Hist_Quantile_Columns         <-  Hist_Quantile_Columns[(Hist_Quantile_Columns %>% ifelse(grepl("Relative_Value", .), paste0(., "_", N), .)) %in% colnames(Past_Quantiles)]

      Columns_to_Merge              <-  intersect(c("Candle","Symbol") , colnames(Dataset))

      Filtered_Quantiles            <-  if ("Candle" %in% colnames(Past_Quantiles)) Past_Quantiles[Past_Quantiles$Symbol %in% Ticker & Past_Quantiles$Candle %in% Granularity, ]  %>% dplyr::group_by(Symbol , Candle) else Past_Quantiles[Past_Quantiles$Symbol %in% Ticker, ]  %>% dplyr::group_by(!!as.name(intersect(colnames(Past_Quantiles), c("Symbol", "Candle"))))

      Columns_to_Merge_By           <-  if ( nrow(dplyr::group_keys(Dataset %>% dplyr::group_by(Symbol, Candle))) < nrow(Dataset)) (c("Symbol", "Candle", "Time")) else c("Symbol", "Candle")

      Filteres_Dataset              <-  Dataset[Dataset$Symbol %in% Ticker | grepl(pattern = paste0(paste0(Ticker, "_"), collapse = "|") , x = Dataset$Symbol) , c(Columns_to_Merge_By, Hist_Quantile_Columns)] %>%

                                        set_colnames(c(Columns_to_Merge_By, paste0(Hist_Quantile_Columns, "_now"))) %>% dplyr::mutate(Symbol = gsub("_Aggregated", "", Symbol))

      if ("Symbol" %in% Columns_to_Merge){

        Filtered_Quantiles$Symbol   <- as.character(Filtered_Quantiles$Symbol)

        Filteres_Dataset$Symbol  <- as.character(Filteres_Dataset$Symbol)

      }

      Dataset           <-  dplyr::left_join(Filtered_Quantiles, Filteres_Dataset, by = Columns_to_Merge) %>% dplyr::group_by(across(all_of(Columns_to_Merge_By)))

      if ("Candle" %in% colnames(Dataset))  Dataset <- Dataset %>%  dplyr::mutate(Symbol = ifelse(grepl("Min|D|W|M", Candle), Symbol, paste0(Symbol, "_Aggregated")))

      for (Col in Hist_Quantile_Columns) Dataset[[paste0(Col, "_now")]] <- Dataset[[paste0(Col, "_now")]] > Dataset[[ifelse(grepl("Relative_Value", Col), paste0(Col, "_", N), Col)]]

      Dataset              <-   Dataset %>% dplyr::filter_at(dplyr::vars(all_of(paste0(Hist_Quantile_Columns, "_now"))), any_vars(!is.na(.)))

      Unique_quantile      <-   unique(Past_Quantiles$Quantile)

      Replace_By           <-   if (Round_Quantile_down) tail(Unique_quantile[Unique_quantile < 0.5], 1) else head(Unique_quantile[Unique_quantile > 0.5], 1)

      Dataset              <-   Dataset  %>% dplyr::mutate_at(.vars = dplyr::vars(all_of(paste0(Hist_Quantile_Columns, "_now"))),

                                                                .funs = function(x) {Sum <- sum(x);if (!is.na(Sum)) {if (!Round_Quantile_down) Sum <- Sum + 1;if (Sum > 0) Unique_quantile[Sum] else 0} else 0.5}) %>%

                                dplyr::distinct_at(Columns_to_Merge_By, .keep_all = T) %>%  dplyr::ungroup() %>%

                                dplyr::mutate_at(dplyr::vars(all_of(paste0(Hist_Quantile_Columns, "_now"))), function(x) replace(x, x == Replace_By, 0.5)) %>%

                                .[c(Columns_to_Merge_By,  paste0(Hist_Quantile_Columns, "_now"), intersect( "Nrow", colnames(.)))] %>% set_colnames(gsub("_now", "", colnames(.))) %>%

                                set_colnames(c(Columns_to_Merge_By,  paste0("Past_Quantile_", colnames(.)[-c(1:(length(Columns_to_Merge_By)), ncol(.))]) , if ("Nrow" %in% colnames(.)) "Nrow"))

      Symbol_to_Match      <-   "BTC"

      Dataset              <-   Dataset %>% dplyr::mutate_at(dplyr::vars(dplyr::contains(paste0("Past_Quantile_",Main_Symbol,"_Normalized"))), function(x) ifelse(.$Symbol == Symbol_to_Match, 0.5, x))

      if (nrow(Dataset) > 0){

        Signal            <- dplyr::left_join(Signal, Dataset, by = Columns_to_Merge_By)

        for (Col in Hist_Quantile_Columns){

          Replace_Values_Index <- (Signal[[Col]] == 10000) %>% replace(., is.na(.), FALSE)

          Signal[Replace_Values_Index, paste0("Past_Quantile_", Col)] <- 0.5

        }

        Signal                     <-  Signal %>% dplyr::mutate_at(dplyr::vars(dplyr::contains("Past_Quantile_")), function(x) as.numeric(x))

        if (Filter_Nas)    Signal  <-  Signal %>% dplyr::filter_at(dplyr::vars(dplyr::contains("Past_Quantile")), dplyr::any_vars(!is.na(.)))

        Signal                     <-  Signal %>% set_colnames(gsub("^Past_Quantile_Nrow$", "Nrow", colnames(.)))

        Signal

      } else {

        Signal

      }

    } else Signal

   Signal

  }

  gc()

  Signal

}


#' Read historical quantiles file
#'
#' @param Ticker
#' @param Granularity
#'
#' @return

Read_Historical_Quantile <- function(Ticker,
                                     Granularity  = NULL){

  File              <-   "./data/Cryptocurrencies_Hist_Quantiles.csv"

  Dataset           <-   lapply(File, function(file) data.table::fread(file = file,  sep = ",")) %>% dplyr::bind_rows()

  if (!missing("Ticker")) Dataset <-  Dataset %>% dplyr::filter(Symbol %in% !!Ticker)

  if (!is.null(Granularity)){

    Dataset <- Dataset %>% dplyr::filter(grepl(paste0("^", Granularity) %>% paste0(collapse = "|"), Candle))

  }

  Dataset <- Dataset %>% dplyr::distinct_at(intersect(colnames(.), c("ticker","Symbol", "Candle", "Quantile", "Time_Index", "Timestamp", "By_IV", "Stats", "RTH")), .keep_all = T)

  colnames(Dataset) <- gsub("SPY", "BTC", colnames(Dataset))

  Dataset

}

#' Add numerical minute factor based on candle size for sorting the signal
#'
#' @param Dataset
#' @param Remove_Minute_Column
#' @param Cols_to_group
#'
#' @return

Add_Minute_Factor_Column <- function(Dataset,
                                     Remove_Minute_Column = F,
                                     Cols_to_group        = c("Time", "Symbol")){

  Candle_Column <- if ("Candle" %in% colnames(Dataset)) "Candle" else if ("Combination" %in% colnames(Dataset)) "Combination" else c()

  if (length(Candle_Column) > 0){

    Dataset[["Minute_Factor"]]  <- unlist(Dataset[[Candle_Column]] %>% gsub("_Min", "", .) %>% gsub("^D$", 390, .) %>% gsub("^W$", 390*5, .) %>% gsub("^D_W$", (390/(1 + sqrt(5))) + (390*5*(1 - (1/(1 + sqrt(5))))), .) %>% gsub("^2W$", 390*5*2, .) %>%  strsplit(split = "_") %>% purrr::map(~.x %>% as.numeric %>% mean))

    Dataset <- Dataset %>% dplyr::group_by(across(intersect(Cols_to_group, colnames(Dataset))))

    Dataset <- Dataset %>% dplyr::arrange(Minute_Factor, .by_group = T)

    Dataset <- Dataset %>% dplyr::ungroup()

    if (Remove_Minute_Column) Dataset <- Dataset %>% dplyr::select(-Minute_Factor)

  } else {

    Dataset

  }

  Dataset

}

#' Initiate Parallel
#'
#' @param N_Cores
#' @param Allow_all
#'
#' @return
#' @export

Initiate_Parallel <- function(N_Cores = 12, Allow_all = F){

  suppressPackageStartupMessages(library(doParallel))

  if (missing("Allow_all")) {Allow_all <- if (.Platform$OS.type == "windows") F else T}

  print(Allow_all)

  N_Cores <- min(N_Cores, parallel::detectCores() - {if (!Allow_all) 1 else 0})

  if (!(foreach::getDoParRegistered()) | !exists(x = "Parallel_Session", envir = .GlobalEnv)){

    print(paste0("Starting ", N_Cores, " cores."))

    doParallel::registerDoParallel(cores = N_Cores)

  }

  assign(x = "Parallel_Session", value = TRUE, envir = .GlobalEnv)

}

Unregister_Parallel <- function() {

  env <- foreach:::.foreachGlobals

  rm(list = ls(name = env), pos = env)

}

#' Largest dollar volume coins on Binance
#'
#' @param Volume24h
#' @param Filter_Tickers
#' @param Only_Symbols
#' @param Only_Quotes
#' @param Sort_Column
#'
#' @return
#' @export

Crypto_Symbols_Selection <- function(Volume24h                   = 1000000,
                                     Symbols_with_Hist_Quantiles = F,
                                     Only_Symbols                = T,
                                     Only_Quotes                 = F,
                                     Sort_Column                 = "1h"){

  Crypto_Info       <-  From_JS(URL = paste0("https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?",
                                             "start=1&limit=1000&sortBy=volume_24h&sortType=desc&convert=USD&cryptoType=all&tagType=all&volume24hRange=",
                                             as.integer(Volume24h),"~"))

  Final             <-  dplyr::bind_cols(Crypto_Info$data$cryptoCurrencyList %>% purrr::keep(~!(.x %>% is.list)),

                                         Crypto_Info$data$cryptoCurrencyList$quotes %>% dplyr::bind_rows() %>% .[-match(c("lastUpdated","name"), colnames(.))]) %>%

                        dplyr::mutate_at(dplyr::vars(c("lastUpdated", "dateAdded")), function(x) lubridate::ymd_hms(x) %>% Convert_Time_Zones(From = "UTC")) %>% dplyr::select(-dateAdded) %>%

                        dplyr::filter(marketCap > 0) %>% dplyr::arrange(-Volume24h)

  if (Symbols_with_Hist_Quantiles){

    Hist_Quantiles    <-  Read_Historical_Quantile()

    Hist_Symbols      <-  unique(Hist_Quantiles$Symbol)

    Final             <-  Final %>% dplyr::filter(symbol %in% Hist_Symbols)

  }

  if (Only_Symbols){

    return(Final$symbol)

  } else {

    Final <- Final %>% dplyr::select(name, symbol, id, price, dplyr::contains("volume"),

                                     dplyr::contains("percentChange"), ytdPriceChangePercentage, lastUpdated ) %>%

             dplyr::arrange(-abs(!!as.name(paste0("percentChange", Sort_Column))))

    if (Only_Quotes) Final <- Final %>% dplyr::select(symbol, price, lastUpdated)

    Final

  }

}

#' Title
#'
#' @param Period 1h/24h/7d/30d
#' @param Selection_Columns
#' @param Top_N
#'
#' @return
#' @export

Crypto_Largest_Movers <- function(Period            = "1h",
                                  Top_N             = 10,
                                  Selection_Columns = T){

  Largest_movers <- From_JS(URL = paste0("https://api.coinmarketcap.com/data-api/v3/cryptocurrency/spotlight?dataType=2&limit=",Top_N,"&rankRange=500&timeframe=", Period))

  Largest_movers <- Largest_movers$data %>% purrr::map(~.x  %>% do.call("cbind", .) %>%

                                                         set_colnames(gsub("priceChange\\.", "", colnames(.)))) %>%

                    dplyr::bind_rows() %>% dplyr::arrange(-abs(!!as.name(paste0("priceChange", Period)))) %>% dplyr::select(-id) %>%

                    dplyr::mutate(lastUpdate = Convert_Time_Zones(lubridate::ymd_hms(lastUpdate), From = "UTC")) %>% dplyr::rename(Time = lastUpdate)

  if (Selection_Columns){

    Largest_movers <- Largest_movers %>%  dplyr::select(Time, symbol, name, marketCap, price,
                                                        !!as.name(paste0("priceChange", Period)),
                                                        if (paste0("volume", Period) %in% colnames(.)) paste0("volume", Period))

  }

  Largest_movers

}


#' Binance list of currencies
#'
#' @return
#' @export

Binance_List_of_Currencies <- function(){

  Binance_List_of_Assets <- From_JS(URL = "https://www.binance.com/bapi/asset/v2/public/asset-service/product/get-products?includeEtf=false") %>% .$data %>%

                            dplyr::filter(q == "USDT")

  Binance_List_of_Assets

}


#' OHLC Data from Binance
#'
#' @param Symbols Any of the symbol(s) from Binance_List_of_Currencies
#' @param Interval 1, 5, 15, 30, 60, 1d, 1w; if numeric then minutes
#' @param Count Nr of candles returned
#' @param Add_Candle_Column
#' @param Only_full_candles
#'
#' @return
#' @export

Crypto_OHLC_Data <- function(Symbols,
                             Interval           = 15,
                             Count              = 100,
                             Add_Candle_Column  = F,
                             Only_full_candles  = T){

  Count      <- min(Count, 1000)

  Raw        <- lapply(Interval, function(interval){

    interval <- tolower(interval)

    if (!grepl("\\D", interval)) interval <- paste0(interval, "m")

    if (identical(interval, "60m"))  interval <- "1h"

    if (interval %in% c("d", "w")){

      interval_to_use <- paste0("1", interval)

    } else {

      interval_to_use <- interval

    }

    Curl_Multi_Json_Data(URL_Prefix =  paste0("https://www.binance.com/api/v3/uiKlines?limit=", Count, "&symbol="),
                         Variable   =  Symbols,
                         URL_Suffix =  paste0("USDT&interval=", interval_to_use))

  }) %>% set_names(Interval)

  OHLC_Data  <- purrr::map_depth(.x     = Raw,

                                 .depth = 2,

                                 .f = function(x) tryCatch(expr =  x  %>% data.frame() %>% dplyr::select(1:6) %>%

                                                             set_colnames(c("Time","Open", "High", "Low", "Close", "Volume")) %>%

                                                             dplyr::mutate_at(-1, function(x) round(as.numeric(x), digits = 8)),

                                                           error = function(e) NULL))

  OHLC_Data <- purrr::map_depth(.x     = OHLC_Data,

                                .depth = 1,

                                 function(x) purrr::compact(x))

  OHLC_Data <- OHLC_Data %>% lapply(function(x) Imap_and_rbind(x))

  OHLC_Data <- purrr::imap(OHLC_Data, function(x, y){

                if (length(x) > 0){

                  Interval_for_round_time  <-  if (!grepl("\\D", y)) paste0(y, "m") else tolower(y)

                  if (Interval_for_round_time == "60m") Interval_for_round_time <- "1h"

                  Round_Time               <-  lubridate::floor_date(Sys.time(), unit = toupper(Interval_for_round_time))

                  multiplier               <-  if (grepl("m$", Interval_for_round_time)) as.numeric(gsub("m", "", Interval_for_round_time))*60 else if (grepl("h$", Interval_for_round_time)) as.numeric(gsub("h", "", Interval_for_round_time))*3600 else if (grepl("d$", Interval_for_round_time)) 86400 else 86400*7

                  x                        <-  x %>%

                                              {if (grepl("m$|h$", Interval_for_round_time)) dplyr::mutate(., Time = Numeric_to_Date(Time, Divisor = 1000) %>% Convert_Time_Zones(From = "UTC")) else dplyr::mutate(., Time = Numeric_to_Date(Time, Divisor = 1000) %>% as.Date())} %>%

                                              {if (grepl("m$|h$", Interval_for_round_time)) dplyr::mutate(., Time = Time + multiplier) else . } %>%

                                              {if (grepl("m$|h$", Interval_for_round_time)) dplyr::filter(., Time <= Round_Time) else . }

                  Max_Time                 <- max(x$Time)

                  x                        <- x %>% dplyr::group_by(Symbol) %>% dplyr::filter(dplyr::last(Time) == Max_Time) %>% dplyr::ungroup()

                  x

                }

              }) %>% purrr::compact()

  if (length(OHLC_Data) > 0){

    names(OHLC_Data) <- as.vector(sapply(names(OHLC_Data), function(interval) if (!interval %in% c("D", "1D","W", "1W")) paste0(gsub("M$", "", interval) %>% gsub("1H$", "60", . ), "_Min") else gsub("^1", "", interval)))

    if (Only_full_candles){

      if (any(c("D", "W") %in% names(OHLC_Data))){

        if ("W" %in% names(OHLC_Data)){

          Last_Monday      <- Last_Friday() + 3

          OHLC_Data[["W"]] <- OHLC_Data[["W"]] %>% dplyr::filter(. , as.Date(Time) <= Last_Monday)

        } else {

          OHLC_Data[["D"]] <- OHLC_Data[["D"]] %>% dplyr::filter(. , as.Date(Time) < Sys.Date())

        }

      }

    }

    if (any(c("D","W") %in% names(OHLC_Data))){

      OHLC_Data <- lapply(names(OHLC_Data), function(interval){

                    if (interval %in% c("D","W")) OHLC_Data[[interval]]$Time <- as.Date(OHLC_Data[[interval]]$Time)

                    OHLC_Data[[interval]]

                  }) %>% set_names(names(OHLC_Data))

    }

    if (Add_Candle_Column)  OHLC_Data <- OHLC_Data %>% Imap_and_rbind(New_Column_Name = "Candle")

    OHLC_Data

  } else {

    OHLC_Data <- NULL

  }

  OHLC_Data

}


### Collection of helper functions for calculating trading signal

Detect_Max      <- function(x, Threshold = 0) x < data.table::shift(x, 1) &  data.table::shift(x, 1) > data.table::shift(x, 2) & data.table::shift(x, 1) > Threshold

Detect_Min      <- function(x, Threshold = -99999999) x > data.table::shift(x, 1) &  data.table::shift(x, 1) < data.table::shift(x, 2) & data.table::shift(x, 1) < Threshold

Detect_Extremes <- function(x, Threshold = c(0, -99999999)) list(Max = Detect_Max(x, Threshold = Threshold[1]), Min = Detect_Min(x, Threshold = Threshold[2]))

Pct_Rank        <- function(x, N = 100) TTR::runPercentRank(x, n = min(N, length(na.omit(x))), cumulative = F, exact.multiplier = 1)

Relative_Value  <- function(x, N = 100) x/roll::roll_mean(x = as.matrix(x), width = min(N, length(na.omit(x))))

Percent_Rank_and_Relative_Value = function(x, N = 100) list(Pct_Rank = Pct_Rank(x, N = N), Rel_Value = Relative_Value(x, N = N))

Fast_BBands_DT <- function(Dataset,
                           sd           = 2,
                           width        = 20,
                           Smoothing    = "EMA",
                           Name_Col     = "Close"){

  mavg <- if (Smoothing == "SMA"){

    roll::roll_mean(x = Dataset[[Name_Col]], width = width)

  } else {

    TTR::EMA(x = Dataset[[Name_Col]], n = width, Wilder = F)

  }

  sdev <- if (Smoothing == "SMA"){

    roll::roll_sd(x = Dataset[[Name_Col]], width = width)

  } else if (Smoothing == "EMA"){

    x               <-  Dataset[[Name_Col]]

    roll_sd         <-  numeric(length = length(x))

    roll_sd_values  <-  sapply(width:length(roll_sd), function(i){

      vec         <- x[(i - (width - 1)):i]

      sqrt(sum((vec - mavg[i])^2)/(width - 1))

    })

    roll_sd          <- c(rep(NA, width - 1), roll_sd_values)

  }

  Correction_Factor <- sqrt((width - 1)/width)

  sdev              <- Correction_Factor*sdev

  up                <- mavg + sd * sdev
  dn                <- mavg - sd * sdev

  pctB              <- (Dataset[[Name_Col]] - dn)/(up - dn)

  res               <- cbind(dn, mavg, up, pctB)

  colnames(res)     <- c("dn", "mavg", "up", "pctB")

  res

}

Fast_ATR <- function(Dataset, N = 14){

  tr       <- pmax(Dataset$High, dplyr::lag(Dataset$Close)) - pmin(Dataset$Low, dplyr::lag(Dataset$Close))

  tr[1]    <- 0

  ATR      <- QuantTools::ema(x = tr, n = N)

  ATR

}


Chaikin_Vol <- function(Dataset, Mavg_N = 10, ROC_N = 10){

  Range       <- Dataset$High - Dataset$Low

  mavg        <- TTR::EMA(Range, n = Mavg_N)

  Chaikin_Vol <- TTR::ROC(mavg, n = ROC_N, type = "discrete")

  Chaikin_Vol

}




### Various helper functions


Correct_Colnames <- function(Colnames){

  Colnames <- Colnames %>% stringi::stri_trans_totitle() %>%

              gsub("\\.| |-|\\,|\\(|\\)|/|\\'|\\:|\\|", "_", .) %>%

              gsub("__", "_", .) %>% gsub("_$|\\^", "", .) %>% gsub("&", "and", .) %>% gsub("\\s+", "_", .)

  Colnames

}


From_JS <- function(URL){

  test <- jsonlite::fromJSON(txt = URL)

  test

}


List_to_Assigned_Variables <- function(List){

  invisible(List %>% purrr::imap(function(.x, .y) {assign(x = .y, value = .x, envir = .GlobalEnv)}))

}

Time_Since_US_Market_Open_Close <- function(TOD = "Open", Period_Unit = "mins"){

  Duration <- if (TOD == "Open"){

    as.numeric(difftime(Sys.time(), US_Market_Open_and_Close_Hour() %>% .[[TOD]], units = Period_Unit))

  } else {

    as.numeric(difftime(US_Market_Open_and_Close_Hour() %>% .[[TOD]],Sys.time(),  units = Period_Unit))

  }

  Duration
}


US_Market_Open_and_Close_Hour <- function(Time, Last_Close = F){

  Time_Diff     <- Time_Diff_Ljubljana_New_York()

  if (!Last_Close){

    Open          <- as.POSIXct(paste0(Sys.Date()," ", round(9  + Time_Diff, digits = 0), ":30:00"))

    Hour          <- if (!Sys.Date() %in% Half_Open_Days()) 16 else 13

    Close         <- as.POSIXct(paste0(Sys.Date()," ", round(Hour + Time_Diff, digits = 0), ":00:00"))

    Times         <- list(Open = Open, Close = Close)

    if (!missing("Time")) return(Times[[Time]]) else return(Times)

  } else {

    Yesterdays_Close()

  }

}

Time_Diff_Ljubljana_New_York <- function(){

  Hour_Diff <- as.numeric(Sys.time() - (lubridate::with_tz(Sys.time(), tzone = "America/New_York") %>% lubridate::force_tz(., tzone = "Europe/Ljubljana"))) %>% round(digits = 0)

  Hour_Diff

}


Numeric_to_Date <- function(Numeric_Date, Divisor = 1, TZ = "UTC"){

  Dates <- as.POSIXct(as.numeric(Numeric_Date)/Divisor, origin = "1970-01-01", tz = TZ)

  Dates
}

Convert_Time_Zones <- function(Times, From = "America/New_York", To = Sys.timezone()){

  Times <- Times %>% lubridate::force_tz(., tzone = From) %>% lubridate::with_tz(., tzone = To)

  Times
}


Half_Open_Days <- function() as.Date(c("2023-07-03", "2024-07-03", "2025-07-03", "2023-11-24", "2024-11-29", "2025-11-28", "2024-12-24", "2025-12-24")) %>% sort

Yesterdays_Close <- function() as.POSIXct(paste0(Sys.Date() - 1, "22:00:00"))

Determine_Symbol_Column <- function(Dataset){

  Symbol_Column <- intersect(c("Ticker", "Symbol", "ticker", "symbol"), colnames(Dataset))

  stopifnot(length(Symbol_Column) > 0)

  if (length(Symbol_Column) > 1) Symbol_Column <- "Symbol"

  Symbol_Column

}


Imap_and_rbind <- function(Named_List, New_Column_Name = "Symbol"){

  if (length(Named_List) > 0){

    if (New_Column_Name %in% colnames(Named_List[[1]])){

      Named_List <- Named_List %>% dplyr::bind_rows()

    } else {

      Named_List <- Named_List %>% purrr::imap(~cbind(.x, bla = .y)) %>% dplyr::bind_rows() %>% dplyr::rename(!!as.name(New_Column_Name) := bla)

    }

  }

  Named_List

}


DF <- function(Dataset){

  Dataset <- data.frame(Dataset)

  Dataset

}




