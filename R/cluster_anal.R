# Carregar pacotes necessários ----------------
library(tidyverse)
library(cluster)
library(factoextra)
library(glmnet)
library(lubridate)
library(stats)
library(tictoc)
library(parallel)
library(future.apply)

tic()

#load complete etf w returns
load("C:/Users/msses/Desktop/TCC/dados/ETF/etf_complete.RData")

#load tickers that will be used to filter previous db
load(file = "C:/Users/msses/Desktop/TCC/dados/ETF/ticker_liq.RData")

etf_complete = etf_complete %>% 
  filter(ticker %in% tickers$ticker) %>% 
  select(-Close,-tx)

rm(tickers)


#dates vec


# load( file = "C:/Users/msses/Desktop/TCC/dados/HF/hf_complete.RData")
# 
# dates = hf_complete %>% 
#   distinct(date,fund)
# 
# rm(hf_complete)

#hf date

load( file = "C:/Users/msses/Desktop/TCC/dados/HF/hf_complete.RData")


#FILTRAR DADOS DE ACORDO COM O QUANTO SAO TRADED (DECIDIR COM O ALVARO)

# cluster ----------------------

# Garantir que a coluna date esteja em formato Date
etf_complete$date <- as.Date(etf_complete$date)

# Remover linhas com NA

#somente as primeiras obs e um ticker com tudo = 0 que foi excluido tb
# na = etf_complete %>%
#   ungroup() %>%
#   filter(is.na(ret)) %>%
#   mutate(c = 1) %>%
#   dplyr::group_by(ticker) %>%
#   dplyr::summarise(sum(c))

etf_complete <- etf_complete %>% 
  filter(!is.na(ret)) %>% 
  filter(ticker!= "SLVO")

hf_complete = hf_complete %>% 
  mutate(date = as.Date(date)) %>% 
  filter(!is.na(ret)) %>% 
  select(date,ret,fund)




cluster_func = function(t){
  
  ### PARALELIZAR EM f,d ########### hf_complete %>% distinct(fund,date) -- pegar min dates pra cada fundo
  f = paral$fund
  d = paral$date[t]
  i = paral$i
  # Filtrar para os primeiros dois anos de dados
  start_date <- d #+ 500
  end_date <- start_date + years(2) - days(1)
  etf_complete_iter <- etf_complete %>% filter(date >= start_date & date <= end_date)
  
  
  # Verificar se todos os ETFs têm dados completos para cada dia útil no período
  complete_etfs <- etf_complete_iter %>%
    group_by(ticker) %>%
    filter(all(hf_complete_iter$date %in% date)) %>%
    ungroup()
  
  # Padronização para o formato longo (`complete_etfs`)
  complete_etfs <- complete_etfs %>%
    group_by(ticker) %>%
    mutate(ret = scale(ret, center = TRUE, scale = TRUE)) %>%
    ungroup()
  
  # Pivot to wide format
  etf_wide <- complete_etfs %>%
    pivot_wider(names_from = ticker, values_from = ret)
  
  train_means_wide <-
    colMeans(etf_wide[-1], na.rm = TRUE)  # Ignorando a coluna de data
  train_sds_wide <-
    apply(etf_wide[-1], 2, sd, na.rm = TRUE)  # Ignorando a coluna de data
  
  
  # Convert all ETF ret columns to numeric if necessary
  # (Assuming the first column might be the date or identifier that should not be converted)
  etf_wide <- etf_wide %>%
    mutate(across(-1, as.numeric))  # Skipping the first column, typically an index or date
  
  
  
  
  # Calcular a matriz de correlação dos retornos
  cor_matrix <- cor(etf_wide[,-1], use = "pairwise.complete.obs")
  
  ### ITERAÇÃO EM I 1:100
  
  
  
  # Baseado no gráfico do Elbow method, escolher o número de clusters
  optimal_clusters <- i # Substitua pelo número ideal de clusters
  
  # Realizar a clusterização com k-means
  set.seed(123)
  km <- kmeans(cor_matrix, centers = optimal_clusters)
  
  # Adicionar os clusters ao dataframe original
  clustered_etfs <-
    data.frame(ticker = colnames(cor_matrix), cluster = km$cluster)
  
  #rm(cor_matrix,etf_wide,km)
  
  clustered_etfs = clustered_etfs %>%
    left_join(complete_etfs, by = "ticker")
  
  #rm(complete_etfs)
  
  calculate_sdi <- function(cluster_data, etf_data) {
    lista = vector()
    
    # Iteração por cada cluster
    for (i in unique(cluster_data$cluster)) {
      # Filtrar os ETFs pertencentes ao cluster atual
      tickers_in_cluster <- clustered_etfs %>%
        filter(cluster == i) %>%
        select(ticker) %>%
        distinct()
      
      relevant_etfs <-
        etf_data[, tickers_in_cluster$ticker, drop = FALSE]
      
      # Calcular a média dos retornos do cluster
      cluster_ret_mean <- rowMeans(relevant_etfs, na.rm = TRUE)
      
      # Calcular o SDI de cada ETF e encontrar o mínimo
      sdi_values <- sapply(tickers_in_cluster, function(etf) {
        etf_rets <- relevant_etfs[, etf]
        1 - cor(etf_rets, cluster_ret_mean, use = "complete.obs")
        
        
      })
      
      # Encontrar o ETF com o menor SDI
      min_sdi_index <- which.min(sdi_values)
      
      
      
      lista = c(lista, tickers_in_cluster$ticker[min_sdi_index])
    }
    
    # Retornar um vetor ou DataFrame dos ETFs selecionados
    return(lista)
  }
  
  selected_etfs <- calculate_sdi(clustered_etfs, etf_wide)
  
  etf_wide = complete_etfs %>%
    filter(ticker %in% selected_etfs)
  #selecting with LASSO & adaLASSO -----------------------------
  
  #convertendo etf wide para matriz com selecionadas para rodar no glmnet
  # Filtrando as colunas do dataframe 'etf_wide' para incluir somente as ETFs selecionadas
  
  etf_wide <- complete_etfs %>%
    pivot_wider(names_from = ticker, values_from = ret)
  
  etf <- etf_wide %>%
    select(all_of(selected_etfs)) %>%
    as.matrix()
  
  hf = hf_complete_iter$ret
  
  
  
  # Definir uma função para calcular o BIC
  calc_bic <- function(y_true, y_pred, num_params) {
    n <- length(y_true)
    mse <- mean((y_true - y_pred) ^ 2)
    bic <- n * log(mse) + num_params * log(n)
    return(bic)
  }
  
  # Ajustar o modelo LASSO sem intercepto
  lasso_model <- glmnet(etf, hf, alpha = 1, intercept = FALSE)
  
  # Obter valores de lambda usados no ajuste
  lambdas <- lasso_model$lambda
  
  # Calcular o BIC para cada valor de lambda
  bics <- sapply(lambdas, function(lambda) {
    preds <- predict(lasso_model, s = lambda, newx = etf)
    num_nonzero_coefs <-
      sum(coef(lasso_model, s = lambda) != 0) - 1  # Subtrai 1 para ignorar o intercepto
    calc_bic(hf, preds, num_nonzero_coefs)
  })
  
  # Selecionar o lambda que minimiza o BIC
  best_lambda_bic <- lambdas[which.min(bics)]
  
  # Extrair os coeficientes no lambda ótimo
  coefs_optimal_no_intercept <-
    coef(lasso_model, s = best_lambda_bic)
  
  # Converter para data frame para facilitar a manipulação
  coefs_df_optimal_no_intercept <-
    as.data.frame(as.matrix(coefs_optimal_no_intercept))
  coefs_df_optimal_no_intercept$variable <-
    rownames(coefs_df_optimal_no_intercept)
  colnames(coefs_df_optimal_no_intercept)[1] <- "coefficient"
  
  # Identificar coeficientes não-zero
  lasso_non_zero_coefs_optimal_no_intercept <-
    coefs_df_optimal_no_intercept %>%
    filter(coefficient != 0)
  
  rm(coefs_df_optimal_no_intercept,
     coefs_optimal_no_intercept,
     lasso_model)
  
  ##adaLASSO -------------
  
  
  
  # Define function to calculate BIC
  calc_bic <- function(y_true, y_pred, num_params) {
    n <- length(y_true)
    mse <- mean((y_true - y_pred) ^ 2)
    bic <- n * log(mse) + num_params * log(n)
    return(bic)
  }
  
  
  # Fit the Ridge regression model without intercept using glmnet
  ridge_model <- glmnet(etf, hf, alpha = 0, intercept = FALSE)
  
  # Calcular o BIC para cada valor de lambda
  bics <- sapply(lambdas, function(lambda) {
    preds <- predict(ridge_model, s = lambda, newx = etf)
    num_nonzero_coefs <-
      sum(coef(ridge_model, s = lambda) != 0) - 1  # Subtrai 1 para ignorar o intercepto
    calc_bic(hf, preds, num_nonzero_coefs)
  })
  
  # Selecionar o lambda que minimiza o BIC
  best_lambda_bic <- lambdas[which.min(bics)]
  
  # Extract the coefficients at the optimal lambda
  coefs_optimal_no_intercept <-
    coef(ridge_model, s = best_lambda_bic)[-1]  # Exclude intercept
  
  
  # Number of observations
  n <- nrow(etf)
  
  # Compute weights
  omega <- 1 / (abs(coefs_optimal_no_intercept) + 1 / sqrt(n))
  
  # Prepare the adaptive weights
  # glmnet allows for weights using penalty.factor
  adaptive_weights <- omega
  
  # Perform LASSO regression with adaptive weights
  lasso_model <-
    glmnet(
      etf,
      hf,
      alpha = 1,
      penalty.factor = adaptive_weights,
      intercept = F
    )
  
  # Obter valores de lambda usados no ajuste
  lambdas <- lasso_model$lambda
  
  # Calcular o BIC para cada valor de lambda
  bics <- sapply(lambdas, function(lambda) {
    preds <- predict(lasso_model, s = lambda, newx = etf)
    num_nonzero_coefs <-
      sum(coef(lasso_model, s = lambda) != 0) - 1  # Subtrai 1 para ignorar o intercepto
    calc_bic(hf, preds, num_nonzero_coefs)
  })
  
  # Selecionar o lambda que minimiza o BIC
  best_lambda_bic <- lambdas[which.min(bics)]
  
  # Extrair os coeficientes no lambda ótimo
  coefs_optimal_no_intercept <-
    coef(lasso_model, s = best_lambda_bic)
  
  # Converter para data frame para facilitar a manipulação
  coefs_df_optimal_no_intercept <-
    as.data.frame(as.matrix(coefs_optimal_no_intercept))
  coefs_df_optimal_no_intercept$variable <-
    rownames(coefs_df_optimal_no_intercept)
  colnames(coefs_df_optimal_no_intercept)[1] <- "coefficient"
  
  # Identificar coeficientes não-zero
  adalasso_non_zero_coefs_optimal_no_intercept <-
    coefs_df_optimal_no_intercept %>%
    filter(coefficient != 0)
  
  rm(
    lasso_model,
    ridge_model,
    km,
    coefs_df_optimal_no_intercept,
    coefs_optimal_no_intercept
  )
  
  #EWMA---------------------
  #LASSO
  # Function to compute EWMA for each ETF
  
  
  
  
  # Subset etf_wide to include only the selected ETFs
  etf <-
    etf_wide[, lasso_non_zero_coefs_optimal_no_intercept$variable]
  
  
  # Set parameters
  tau <- 0.97
  T <- nrow(etf) - 1
  
  # Compute the exponential weights
  weights <- sqrt(tau ^ (0:T))
  normalization_factor <- sqrt((1 - tau) / (1 - tau ^ (T + 1)))
  weights <- normalization_factor * weights
  
  weights = as.data.frame(weights)
  
  # Supondo que seus dataframes são etf e weights
  result <- sweep(etf, 1, weights$weights, `*`)
  
  result = result %>%
    cbind(hf)
  
  # Fit a linear model
  fit <- lm(hf ~ . - 1, data = as.data.frame(result))
  
  
  # Calculate the Bayesian Information Criterion (BIC)
  bic_value <- BIC(fit)
  
  lasso_bic = bic_value
  
  
  #adaLASSO
  # Function to compute EWMA for each ETF
  
  
  
  # Subset etf_wide to include only the selected ETFs
  etf <-
    etf_wide[, adalasso_non_zero_coefs_optimal_no_intercept$variable]
  
  
  # Set parameters
  tau <- 0.97
  T <- nrow(etf) - 1
  
  # Compute the exponential weights
  weights <- sqrt(tau ^ (0:T))
  normalization_factor <- sqrt((1 - tau) / (1 - tau ^ (T + 1)))
  weights <- normalization_factor * weights
  
  weights = as.data.frame(weights)
  
  # Supondo que seus dataframes são etf e weights
  result <- sweep(etf, 1, weights$weights, `*`)
  
  result = result %>%
    cbind(hf)
  
  # Fit a linear model
  fit <- lm(hf ~ . - 1, data = as.data.frame(result))
  
  
  # Calculate the Bayesian Information Criterion (BIC)
  bic_value <- BIC(fit)
  
  adalasso_bic = bic_value
  
  res = c(f,d,lasso_bic,adalasso_bic,i)
  res
  
  
}


paral = hf_complete %>% 
  distinct(date,fund) %>% 
  group_by(fund) %>% 
  mutate(inicio = min(date),
         final = max(date))

# Adiciona a coluna 'valid' com a verificação de pelo menos 2 anos e 14 dias à frente
paral <- paral %>%
  mutate(valid = (final - date) >= (years(2) + days(14))) %>% 
  filter(valid == T) %>% 
  select(fund,date)

#teste com meos fundos

fundos = paral %>% 
  mutate(c = 1) %>% 
  group_by(fund) %>%
  summarise(soma =sum(c)) %>%
  arrange(-soma) %>% 
  slice_head(n = 10) %>% 
  distinct(fund)

paral = paral %>% 
  filter(fund %in% fundos$fund) 

hf_complete = hf_complete %>% 
  filter(fund %in% fundos$fund)

rm(fundos)


detectCores()

plan(multisession, workers = detectCores()-3)

result = future_lapply(1:5, cluster_func)

save(result, file="C:/Users/msses/Desktop/TCC/dados/result/result_ewma_cdi.RData")

toc()
