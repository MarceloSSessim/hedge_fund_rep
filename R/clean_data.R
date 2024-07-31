#CLEANING HF

library(readxl)
library(tidyverse)
library(lubridate)

hf = read_xlsx("C:/Users/msses/Desktop/TCC/dados/HF/Series_Novas_XLS.xlsx")

hf = hf %>% 
  rename(fund = `Nome do Fundo`,
         date = Data) %>% 
  mutate(date = as.Date(date))

#so contar de segunda a sexta
hf = hf %>% 
  filter(!weekdays(date) %in% c("saturday", "Sunday"))

#pegar os dias que temos dados para etf
start_date <- as.POSIXct("2008-04-01")
end_date <- as.POSIXct("2023-04-26")
hf <- subset(hf, date >= start_date & date <= end_date)


# Convert Data to Date class if necessary
hf$date <- as.Date(hf$date)

# Selecionar apenas as colunas necessárias
hf <- hf %>% 
  select(date, val_cota, fund)

# Definir as datas de início e fim
start_date <- as.Date("2008-04-01")
end_date <- as.Date("2023-04-26")

# Criar a sequência de datas entre as datas de início e fim
all_dates <- seq.Date(start_date, end_date, by = "day")

# Set the days to exclude, correctly capitalized
weekdays_to_exclude <- c("Saturday", "Sunday")

# Filter out weekends
filtered_dates <- all_dates[!weekdays(all_dates) %in% weekdays_to_exclude]

b = hf %>% distinct(date)
b$date = as.Date(b$date)
b = setdiff(filtered_dates,b$date)
b <- as.Date(b, origin = "1970-01-01")


# Function to create a sequence of all weekdays (excluding Saturdays, Sundays, and holidays) between the first and last date for each fund
create_weekday_sequence <- function(df, b) {
  min_date <- min(df$date)
  max_date <- max(df$date)
  all_dates <- seq(min_date, max_date, by = "day")
  
  # Filter out weekends and holidays
  weekdays <- all_dates[!weekdays(all_dates) %in% c("Saturday", "Sunday")]
  
  
  return(data.frame(date = weekdays))
}

# Apply the function to each ticker and create a complete dataset
complete_dates <- hf %>%
  group_by(fund) %>%
  do(create_weekday_sequence(.)) %>%
  ungroup()

# Merge the complete dataset with the original data
hf_complete <- left_join(complete_dates, hf, by = c("date", "fund"))

# Fill missing val_cota prices by carrying forward the last known price
hf_complete <- hf_complete %>%
  filter(!date %in% b) %>% 
  arrange(fund, date) %>%
  group_by(fund) %>%
  fill(val_cota, .direction = "down") %>%
  ungroup()


#load exchange rate pra colocar em dolar

ex = read.csv2("C:/Users/msses/Desktop/TCC/dados/bcb_br_dol/STI-20240619215953812.csv")

ex$date <- as.Date(ex$Date, format = "%d/%m/%Y")

ex = ex %>% 
  select(-Date) %>% 
  rename(ex = `X1...Exchange.rate...Free...United.States.dollar..sale....1...c.m.u..US.`)

hf_complete = hf_complete %>% 
  left_join(ex, by = c("date"))

hf_complete = hf_complete %>% 
  mutate(val_cota = val_cota/as.numeric(ex))

# Calcular o retorno percentual
hf_complete <- hf_complete %>%
  arrange(fund, date) %>%  # Garantir que os dados estejam ordenados
  group_by(fund) %>%       # Calcular retornos dentro de cada grupo de fund
  mutate(ret = (val_cota / lag(val_cota) - 1) * 100) %>%  # Calcular o retorno percentual
  ungroup()


#load cdi taxa livre de risco
cdi = read.csv2("C:/Users/msses/Desktop/TCC/dados/CDI/STI-20240609204004544.csv")

cdi$Date <- as.Date(cdi$Date, format = "%d/%m/%Y")
cdi = cdi %>% 
  rename(date = Date,
         tx = X12...Interest.rate...CDI.....p.d.)


hf_complete = hf_complete %>% 
  left_join(cdi,
            by= c("date"))

hf_complete = hf_complete %>% 
  mutate(tx = as.numeric(tx),
         ret = ret-tx)

save(hf_complete, file = "C:/Users/msses/Desktop/TCC/dados/HF/hf_complete.RData")


# 
# 
# library(tidyverse)
# library(readxl)
# 
# 
# #CLEANING ETF
# 
# # unzip dados de ETF e criar dataframes com eles (acho que separar em blocos)
# raw.files = list.files("C:/Users/msses/Desktop/TCC/dados/ETF", full.names = T)
# 
# head(raw.files)
# 
# # Define the output directory
# output_dir <- "C:/Users/msses/Desktop/TCC/dados/ETF/unzipped_files"
# 
# # Create the output directory if it doesn't exist
# if (!dir.exists(output_dir)) {
#   dir.create(output_dir, recursive = TRUE)
# }
# 
# # Unzip each file into the output directory
# for (zipfile in raw.files) {
#   unzip(zipfile, exdir = output_dir)
#   cat("Unzipped:", zipfile, "\n")
# }
# 
# cat("All files have been successfully unzipped into", output_dir, "\n")
# 
# # dados de HF de 01-04-2008 a 26-04-2023 --- filtrar dados de ETF
# raw.files = list.files("C:/Users/msses/Desktop/TCC/dados/ETF/unzipped_files", full.names = T)
# 
# # Function to read and process each file
# process_file <- function(file_path) {
#   # Read the data
#   data <- read.csv(file_path, header = FALSE)
# 
#   # Name the columns
#   colnames(data) <- c("DateTime", "Open", "High", "Low", "Close", "Volume")
# 
#   # Convert the DateTime column to the proper format
#   data$DateTime <- as.POSIXct(data$DateTime, format="%Y-%m-%d")
# 
#   # Extract the file name before the first underscore
#   file_name <- basename(file_path)
#   file_name_short <- strsplit(file_name, "_")[[1]][1]
# 
#   # Add the extracted name as a new column
#   data$ticker <- file_name_short
# 
#   return(data)
# }
# 
# # Read and combine all files into one data frame
# etf <- do.call(rbind, lapply(raw.files, process_file))
# 
# # Define the date range for filtering
# start_date <- as.POSIXct("2008-04-01")
# end_date <- as.POSIXct("2023-04-26")
# 
# # Filter the combined data frame
# etf <- subset(etf, DateTime >= start_date & DateTime <= end_date)
# 
# save(etf, file = "C:/Users/msses/Desktop/TCC/dados/ETF/clean_ETF.RData")
# 
# rm(etf)


library(tidyverse)
library(lubridate)
library(timeDate)
load("C:/Users/msses/Desktop/TCC/dados/ETF/clean_ETF.RData")


#load hf to use its dates because of br holidays

load( file = "C:/Users/msses/Desktop/TCC/dados/HF/hf_complete.RData")

dates = hf_complete %>% 
  distinct(date)

rm(hf_complete)



#criando retorno etf

#preenchendo com precos "forward"
# Convert DateTime to Date class if necessary
etf$DateTime <- as.Date(etf$DateTime)
etf = etf %>% 
  select(DateTime,Close,ticker)
# Function to create a sequence of all weekdays between the first and last date for each ticker
create_weekday_sequence <- function(df) {
  min_date <- min(df$DateTime)
  max_date <- max(df$DateTime)
  all_dates <- seq(min_date, max_date, by = "day")
  weekdays <- all_dates[!weekdays(all_dates) %in% c("Saturday", "Sunday")]
  return(data.frame(DateTime = weekdays))
}

# Apply the function to each ticker and create a complete dataset
complete_dates <- etf %>%
  group_by(ticker) %>%
  do(create_weekday_sequence(.)) %>%
  ungroup()

# Merge the complete dataset with the original data
etf_complete <- left_join(complete_dates, etf, by = c("DateTime", "ticker")) %>% 
  filter(DateTime %in% dates$date)

# Fill missing "Close" prices by carrying forward the last known price
etf_complete <- etf_complete %>%
  arrange(ticker, DateTime) %>%
  group_by(ticker) %>%
  fill(Close, .direction = "down") %>%
  ungroup()

#criando coluna retorno
# Calcular o retorno percentual
etf_complete <- etf_complete %>%
  arrange(ticker, DateTime) %>%  # Garantir que os dados estejam ordenados
  group_by(ticker) %>%           # Calcular retornos dentro de cada grupo de ticker
  mutate(Return = ((Close / lag(Close)) - 1) * 100) %>%  # Calcular o retorno percentual
  ungroup()

etf_complete = etf_complete %>% 
  rename(date = DateTime,
         ret = Return)

#load cdi taxa livre de risco
cdi = read.csv2("C:/Users/msses/Desktop/TCC/dados/CDI/STI-20240609204004544.csv")

cdi$Date <- as.Date(cdi$Date, format = "%d/%m/%Y")
cdi = cdi %>% 
  rename(date = Date,
         tx = X12...Interest.rate...CDI.....p.d.)


etf_complete = etf_complete %>% 
  left_join(cdi,
            by= c("date"))

etf_complete = etf_complete %>% 
  mutate(tx = as.numeric(tx),
         ret = ret-tx)


save(etf_complete, file = "C:/Users/msses/Desktop/TCC/dados/ETF/etf_complete.RData")

