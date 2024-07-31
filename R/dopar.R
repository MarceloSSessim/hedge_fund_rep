# Carregar pacotes necessários ----------------
library(tidyverse)
library(cluster)
library(factoextra)
library(glmnet)
library(lubridate)
library(stats)
library(tictoc)
library(parallel)
library(foreach)
library(doParallel)
library(PerformanceAnalytics)
library(moments)
library(dplyr)


tic()

#load complete etf w returns
load("C:/Users/msses/Desktop/TCC/dados/ETF/etf_complete.RData")

#load tickers that will be used to filter previous db
load(file = "C:/Users/msses/Desktop/TCC/dados/ETF/ticker_liq.RData")

etf_complete = etf_complete %>% 
  filter(ticker %in% tickers$ticker) %>% 
  select(-Close,-tx)

rm(tickers)

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

# Remove February 29 from the dataset
etf_complete <- etf_complete %>%
  filter(!(format(date, "%m-%d") == "02-29"))

hf_complete = hf_complete %>% 
  mutate(date = as.Date(date)) %>% 
  filter(!is.na(ret)) %>% 
  select(date,ret,fund)

# Remove February 29 from the dataset
hf_complete <- hf_complete %>%
  filter(!(format(date, "%m-%d") == "02-29"))

hf_complete <- hf_complete %>%
  filter(date > as.Date("2017-12-31"))

etf_complete <- etf_complete %>%
  filter(date > as.Date("2017-12-31"))


#sum(format(etf_complete$date, "%m-%d") == "02-29")

paral = hf_complete %>% 
  distinct(date,fund) %>% 
  group_by(fund) %>% 
  mutate(inicio = min(date),
         final = max(date))

# Adiciona a coluna 'valid' com a verificação de pelo menos 2 anos e 14 dias à frente
paral <- paral %>%
  mutate(valid = (final - date) >= (years(1) + months(1))) %>% 
  filter(valid == T) %>% 
  select(fund,date)

# Filtrar observações de cada fundo espaçadas de mês em mês
paral <- paral %>%
  arrange(fund, date) %>% # Ordena por fund e data
  group_by(fund, year = year(date), month = month(date)) %>% # Agrupa por fundo, ano e mês
  slice(1) %>% # Mantém apenas a primeira observação de cada grupo
  ungroup() %>% # Remove agrupamento
  select(-year,-month) %>%  # Remove as colunas temporárias
  filter(
    !fund %in% c(
      "BLP CRYPTO 100 INVESTIMENTO NO EXTERIOR FI MULTIMERCADO",
      "XPA TÁTICO FI MULTIMERCADO CRÉDITO PRIVADO",
      "SAFRA BONDS INVESTIMENTO NO EXTERIOR FI MULTIMERCADO CRÉDITO PRIVADO"
    )
  )
#teste com meos fundos

fundos = paral %>% 
  mutate(c = 1) %>% 
  group_by(fund) %>%
  summarise(soma =sum(c)) %>%
  arrange(-soma) 

paral = paral %>% 
  filter(fund %in% fundos$fund) 

hf_complete = hf_complete %>% 
  filter(fund %in% fundos$fund)

rm(fundos)

fundos = hf_complete %>% 
  distinct(fund)

paral = paral %>% 
  filter(fund %in% fundos$fund[141:149])

rm(fundos)


# Initialize parallel backend
cl <- makeCluster(detectCores()-1)  # Adjust number of cores as needed
registerDoParallel(cl)


tic()
# Parallel processing for observations
results <-
  foreach(
    t = 1:nrow(paral),
    .packages = c(
      'dplyr',
      'tidyr',
      'glmnet',
      'lubridate',
      'PerformanceAnalytics',
      'moments'
    )
  ) %dopar% {
    
  # Load necessary packages within each worker
  library(dplyr)
  library(tidyr)
  library(glmnet)
  library(lubridate)
  library(foreach)
  library(PerformanceAnalytics)
  library(moments)
  ### PARALELIZAR EM f,d ########### hf_complete %>% distinct(fund,date) -- pegar min dates pra cada fundo
  
  f = paral[t,]$fund
  d = paral[t,]$date
  
  hf_complete_iter = hf_complete %>% 
    filter(fund == f) %>% 
    filter(date >= d & date <= d + years(1) - days(1))
  
  # Definir a função para aplicar o filtro MA(2)
  apply_ma2_filter <- function(data) {
    num_observacoes <- nrow(data)
    
    # Verificar se o número de observações é par
    if (num_observacoes %% 2 == 0) {
      # Se for par, remover a observação mais antiga
      data <- data[-1, ]
    }
    
    # Aplicar o filtro MA(2) apenas se houver pelo menos 3 observações
    if (nrow(data) >= 3) {
      # Calcular o filtro MA(2)
      ma2_filter <- stats::filter(data$ret, c(1/2, 1/2), sides = 2, circular = TRUE)
      
      # Atribuir os valores filtrados de volta aos dados originais
      data$ret <- ma2_filter
    }
    
    return(data)
  }
  
  # Exemplo de uso com o seu dataframe original hf_complete_iter
  hf_complete_iter <- hf_complete_iter %>%
    group_by(fund) %>%
    do(apply_ma2_filter(.)) %>%
    mutate(ret = as.numeric(ret))  # Converter a coluna ret para numérico
  
  
  # Filtrar para os primeiros dois anos de dados
  start_date <- min(hf_complete_iter$date) #+ 500
  end_date <- max(hf_complete_iter$date)
  etf_complete_iter <- etf_complete %>% filter(date >= start_date & date <= end_date)
  
  
  # Verificar se todos os ETFs têm dados completos para cada dia útil no período
  complete_etfs <- etf_complete_iter %>%
    group_by(ticker) %>%
    filter(all(hf_complete_iter$date %in% date)) %>%
    ungroup()
  
  # Calculate means and standard deviations before scaling
  summary_stats <- complete_etfs %>%
    group_by(ticker) %>%
    summarise(mean_ret = mean(ret),
              sd_ret = sd(ret))
  
  # Save in wide format objects
  train_means_wide <- summary_stats %>% select(mean_ret,ticker) %>% pivot_wider(names_from = ticker, values_from = mean_ret)
  train_sds_wide <- summary_stats %>% select(sd_ret,ticker) %>% pivot_wider(names_from = ticker, values_from = sd_ret)
  
  
  # Padronização para o formato longo (`complete_etfs`)
  complete_etfs <- complete_etfs %>%
    group_by(ticker) %>%
    mutate(ret = scale(ret, center = TRUE, scale = TRUE)) %>% 
    ungroup()
  
  # Pivot to wide format
  etf_wide <- complete_etfs %>%
    pivot_wider(names_from = ticker, values_from = ret)
  
  
  # Convert all ETF ret columns to numeric if necessary
  # (Assuming the first column might be the date or identifier that should not be converted)
  etf_wide <- etf_wide %>%
    mutate(across(-1, as.numeric))  # Skipping the first column, typically an index or date
  
  
  # Calcular a matriz de correlação dos retornos
  cor_matrix <- cor(etf_wide[, -1], use = "pairwise.complete.obs")
  
  
  # Parallelize cluster evaluation within each observation
  result <- foreach(i = 2:100,.packages = c('dplyr', 'tidyr', 'glmnet', 'lubridate'), .combine = 'rbind') %dopar% {
    
    
    # Baseado no gráfico do Elbow method, escolher o número de clusters
    optimal_clusters <- i # Substitua pelo número ideal de clusters
    
    # Realizar a clusterização com k-means
    set.seed(123)
    km <- kmeans(cor_matrix, centers = optimal_clusters)
    
    # Adicionar os clusters ao dataframe original
    clustered_etfs <- data.frame(ticker = colnames(cor_matrix), cluster = km$cluster)
    
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
        
        relevant_etfs <- etf_data[, tickers_in_cluster$ticker, drop = FALSE]
        
        # Calcular a média dos retornos do cluster
        cluster_ret_mean <- rowMeans(relevant_etfs, na.rm = TRUE)
        
        # Calcular o SDI de cada ETF e encontrar o mínimo
        sdi_values <- sapply(tickers_in_cluster, function(etf) {
          etf_rets <- relevant_etfs[, etf]
          1 - (cor(etf_rets, cluster_ret_mean, use = "complete.obs"))
          
          
        })
        
        # Encontrar o ETF com o menor SDI
        min_sdi_index <- which.min(sdi_values)
        
        
        
        lista = c(lista,tickers_in_cluster$ticker[min_sdi_index])
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
    
    
    
    cv_model <- cv.glmnet(etf, hf, alpha = 1, intercept = FALSE)
    
    # Obter o valor de lambda que minimiza o erro de validação cruzada
    best_lambda_cv <- cv_model$lambda.min
    
    # Extrair os coeficientes no lambda ótimo
    coefs_optimal_no_intercept <- coef(cv_model, s = best_lambda_cv)
    
    # Converter para data frame para facilitar a manipulação
    coefs_df_optimal_no_intercept <- as.data.frame(as.matrix(coefs_optimal_no_intercept))
    coefs_df_optimal_no_intercept$variable <- rownames(coefs_df_optimal_no_intercept)
    colnames(coefs_df_optimal_no_intercept)[1] <- "coefficient"
    
    # Identificar coeficientes não-zero
    lasso_non_zero_coefs_optimal_no_intercept <- coefs_df_optimal_no_intercept %>%
      filter(coefficient != 0)
    
    # Limpar variáveis não necessárias
    rm(coefs_df_optimal_no_intercept, coefs_optimal_no_intercept, cv_model)
    
    ##adaLASSO -------------
    
    
    
    # Fit the Ridge regression model with cross-validation using glmnet
    cv_ridge_model <- cv.glmnet(etf, hf, alpha = 0, intercept = FALSE, nfolds = 10)
    
    # Get the optimal lambda from cross-validation
    best_lambda_cv_ridge <- cv_ridge_model$lambda.min
    
    # Extract the coefficients at the optimal lambda for Ridge
    coefs_optimal_no_intercept_ridge <- coef(cv_ridge_model, s = best_lambda_cv_ridge)[-1]  # Exclude intercept
    
    # Number of observations
    n <- nrow(etf)
    
    # Compute weights for adaptive Lasso
    omega <- 1 / (abs(coefs_optimal_no_intercept_ridge) + 1 / sqrt(n))
    
    # Prepare the adaptive weights
    adaptive_weights <- omega
    
    # Perform LASSO regression with adaptive weights and cross-validation
    cv_lasso_model <- cv.glmnet(etf, hf, alpha = 1, penalty.factor = adaptive_weights, intercept = FALSE, nfolds = 10)
    
    # Get the optimal lambda from cross-validation for Lasso
    best_lambda_cv_lasso <- cv_lasso_model$lambda.min
    
    # Extract the coefficients at the optimal lambda for Lasso
    coefs_optimal_no_intercept_lasso <- coef(cv_lasso_model, s = best_lambda_cv_lasso)
    
    # Convert to data frame for easier manipulation
    coefs_df_optimal_no_intercept_lasso <- as.data.frame(as.matrix(coefs_optimal_no_intercept_lasso))
    coefs_df_optimal_no_intercept_lasso$variable <- rownames(coefs_df_optimal_no_intercept_lasso)
    colnames(coefs_df_optimal_no_intercept_lasso)[1] <- "coefficient"
    
    # Identify non-zero coefficients
    adalasso_non_zero_coefs_optimal_no_intercept <- coefs_df_optimal_no_intercept_lasso %>%
      filter(coefficient != 0)
    
    # Clean up
    rm(cv_lasso_model, cv_ridge_model, coefs_df_optimal_no_intercept_lasso, coefs_optimal_no_intercept_ridge)
    
    #EWMA---------------------
    #LASSO
    # Function to compute EWMA for each ETF
    
    
    
    if(nrow(lasso_non_zero_coefs_optimal_no_intercept)==0){
      
      lasso_bic = NA
      r2_adjusted_lasso = NA
      
    }else{
      # Subset etf_wide to include only the selected ETFs
      etf <- etf_wide[, lasso_non_zero_coefs_optimal_no_intercept$variable]
      
      # dias <- nrow(etf) - 1  # Adjust T to be the last index of etf_data
      # # Compute the exponential weights
      # tau = 0.97
      # weights <- sqrt(tau^(0:dias))
      # normalization_factor <- sqrt((1 - tau) / (1 - tau^(dias + 1)))
      # weights <- normalization_factor * weights
      # 
      # weights = as.data.frame(weights)
      # # Adicionar um índice de linha
      # weights <- weights %>% mutate(row_number = row_number())
      # 
      # # Inverter a ordem das linhas usando arrange e desc
      # weights_reversed <- weights %>%
      #   arrange(desc(row_number)) %>%
      #   select(-row_number)  # Remove a coluna auxiliar
      # 
      # 
      # # Supondo que seus dataframes são etf e weights
      # etf <- sweep(etf, 1, weights_reversed$weights, `*`)
      
      
      result = etf %>% 
        cbind(hf)
      
      # Fit a linear model
      fit <- lm(hf ~ . -1, data = as.data.frame(result))
      
      
      # Calculate the Bayesian Information Criterion (BIC)
      bic_value <- BIC(fit)
      # Calcular o R^2 ajustado
      r2_adjusted <- summary(fit)$adj.r.squared
      
      lasso_bic = bic_value
      r2_adjusted_lasso = r2_adjusted
    }
    
    if(nrow(adalasso_non_zero_coefs_optimal_no_intercept)==0){
      adalasso_bic = NA
      adalasso_r2_ajusted =NA
    }else{
      #adaLASSO
      
      #EWMA
      # Function to compute EWMA for each ETF
      
      
      
      # Subset etf_wide to include only the selected ETFs
      etf <- etf_wide[, adalasso_non_zero_coefs_optimal_no_intercept$variable]
      
      
      # dias <- nrow(etf) - 1  # Adjust T to be the last index of etf_data
      # # Compute the exponential weights
      # tau = 0.97
      # weights <- sqrt(tau^(0:dias))
      # normalization_factor <- sqrt((1 - tau) / (1 - tau^(dias + 1)))
      # weights <- normalization_factor * weights
      # 
      # weights = as.data.frame(weights)
      # 
      # # Adicionar um índice de linha
      # weights <- weights %>% mutate(row_number = row_number())
      # 
      # # Inverter a ordem das linhas usando arrange e desc
      # weights_reversed <- weights %>%
      #   arrange(desc(row_number)) %>%
      #   select(-row_number)  # Remove a coluna auxiliar
      # 
      # # Supondo que seus dataframes são etf e weights
      # etf <- sweep(etf, 1, weights_reversed$weights, `*`)
      
      result = etf %>% 
        cbind(hf)
      
      # Fit a linear model
      fit <- lm(hf ~ . -1, data = as.data.frame(result))
      
      
      # Calculate the Bayesian Information Criterion (BIC)
      bic_value <- BIC(fit)
      r2_adjusted <- summary(fit)$adj.r.squared
      adalasso_bic = bic_value
      adalasso_r2_ajusted =r2_adjusted
    }
    
    res = c(lasso_bic,adalasso_bic,i,adalasso_r2_ajusted,r2_adjusted_lasso)
    
    # Transpor res para uma linha
    res <- t(res)
    
    # Converter para data frame e nomear as colunas
    res <- as.data.frame(res)
    colnames(res) <- c("lasso", "adalasso", "i","adalasso_r2_ajusted", "r2_adjusted_lasso")
    
    return(res)
    
    
    
    
  }
  
  result = result %>% bind_rows()
  
  position_lasso = result %>% 
    arrange(-r2_adjusted_lasso,i) %>% 
    first() %>% 
    select(i)
  
  position_lasso = position_lasso$i
  
  position_adalasso = result %>% 
    arrange(-adalasso_r2_ajusted,i) %>% 
    first() %>% 
    select(i)
  
  position_adalasso = position_adalasso$i
  
  rm(result)
  
  #BEST LASSO
  
  # Baseado no gráfico do Elbow method, escolher o número de clusters
  optimal_clusters <- position_lasso # Substitua pelo número ideal de clusters
  
  # Realizar a clusterização com k-means
  set.seed(123)
  km <- kmeans(cor_matrix, centers = optimal_clusters)
  
  # Adicionar os clusters ao dataframe original
  clustered_etfs <- data.frame(ticker = colnames(cor_matrix), cluster = km$cluster)
  
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
      
      relevant_etfs <- etf_data[, tickers_in_cluster$ticker, drop = FALSE]
      
      # Calcular a média dos retornos do cluster
      cluster_ret_mean <- rowMeans(relevant_etfs, na.rm = TRUE)
      
      # Calcular o SDI de cada ETF e encontrar o mínimo
      sdi_values <- sapply(tickers_in_cluster, function(etf) {
        etf_rets <- relevant_etfs[, etf]
        1 - cor(etf_rets, cluster_ret_mean, use = "complete.obs")
        
        
      })
      
      # Encontrar o ETF com o menor SDI
      min_sdi_index <- which.min(sdi_values)
      
      
      
      lista = c(lista,tickers_in_cluster$ticker[min_sdi_index])
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
  
  
  
  cv_model <- cv.glmnet(etf, hf, alpha = 1, intercept = FALSE)
  
  # Obter o valor de lambda que minimiza o erro de validação cruzada
  best_lambda_cv <- cv_model$lambda.min
  
  # Extrair os coeficientes no lambda ótimo
  coefs_optimal_no_intercept <- coef(cv_model, s = best_lambda_cv)
  
  # Converter para data frame para facilitar a manipulação
  coefs_df_optimal_no_intercept <- as.data.frame(as.matrix(coefs_optimal_no_intercept))
  coefs_df_optimal_no_intercept$variable <- rownames(coefs_df_optimal_no_intercept)
  colnames(coefs_df_optimal_no_intercept)[1] <- "coefficient"
  
  # Identificar coeficientes não-zero
  lasso_non_zero_coefs_optimal_no_intercept <- coefs_df_optimal_no_intercept %>%
    filter(coefficient != 0)
  
  # Limpar variáveis não necessárias
  rm(coefs_df_optimal_no_intercept, coefs_optimal_no_intercept, cv_model)
  
  
  #EWMA---------------------
  #LASSO
  # Function to compute EWMA for each ETF
  
  
  # Subset etf_wide to include only the selected ETFs
  etf <- etf_wide %>% 
    select(lasso_non_zero_coefs_optimal_no_intercept$variable) %>% 
    as.matrix() %>% 
    as.data.frame()
  
  
  # dias <- nrow(etf) - 1  # Adjust T to be the last index of etf_data
  # # Compute the exponential weights
  # tau = 0.97
  # weights <- sqrt(tau^(0:dias))
  # normalization_factor <- sqrt((1 - tau) / (1 - tau^(dias + 1)))
  # weights <- normalization_factor * weights
  # 
  # weights = as.data.frame(weights)
  # # Adicionar um índice de linha
  # weights <- weights %>% mutate(row_number = row_number())
  # 
  # # Inverter a ordem das linhas usando arrange e desc
  # weights_reversed <- weights %>%
  #   arrange(desc(row_number)) %>%
  #   select(-row_number)  # Remove a coluna auxiliar
  # # Supondo que seus dataframes são etf e weights
  # etf <- sweep(etf, 1, weights_reversed$weights, `*`)
  
  
  result = etf %>% 
    cbind(hf)
  
  # Fit a linear model
  fit_lasso <- lm(hf ~ . -1, data = as.data.frame(result))
  
  # Obter o resumo do modelo
  summary_fit <- summary(fit_lasso)
  
  # Extrair o R²
  r_squared_lasso <- summary_fit$r.squared
  
  #BEST ADALASSO
  
  # Baseado no gráfico do Elbow method, escolher o número de clusters
  optimal_clusters <- position_adalasso # Substitua pelo número ideal de clusters
  
  # Realizar a clusterização com k-means
  set.seed(123)
  km <- kmeans(cor_matrix, centers = optimal_clusters)
  
  # Adicionar os clusters ao dataframe original
  clustered_etfs <- data.frame(ticker = colnames(cor_matrix), cluster = km$cluster)
  
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
      
      relevant_etfs <- etf_data[, tickers_in_cluster$ticker, drop = FALSE]
      
      # Calcular a média dos retornos do cluster
      cluster_ret_mean <- rowMeans(relevant_etfs, na.rm = TRUE)
      
      # Calcular o SDI de cada ETF e encontrar o mínimo
      sdi_values <- sapply(tickers_in_cluster, function(etf) {
        etf_rets <- relevant_etfs[, etf]
        1 - cor(etf_rets, cluster_ret_mean, use = "complete.obs")
        
        
      })
      
      # Encontrar o ETF com o menor SDI
      min_sdi_index <- which.min(sdi_values)
      
      
      
      lista = c(lista,tickers_in_cluster$ticker[min_sdi_index])
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
  
  
  
  
  
  # Fit the Ridge regression model with cross-validation using glmnet
  cv_ridge_model <- cv.glmnet(etf, hf, alpha = 0, intercept = FALSE, nfolds = 10)
  
  # Get the optimal lambda from cross-validation
  best_lambda_cv_ridge <- cv_ridge_model$lambda.min
  
  # Extract the coefficients at the optimal lambda for Ridge
  coefs_optimal_no_intercept_ridge <- coef(cv_ridge_model, s = best_lambda_cv_ridge)[-1]  # Exclude intercept
  
  # Number of observations
  n <- nrow(etf)
  
  # Compute weights for adaptive Lasso
  omega <- 1 / (abs(coefs_optimal_no_intercept_ridge) + 1 / sqrt(n))
  
  # Prepare the adaptive weights
  adaptive_weights <- omega
  
  # Perform LASSO regression with adaptive weights and cross-validation
  cv_lasso_model <- cv.glmnet(etf, hf, alpha = 1, penalty.factor = adaptive_weights, intercept = FALSE, nfolds = 10)
  
  # Get the optimal lambda from cross-validation for Lasso
  best_lambda_cv_lasso <- cv_lasso_model$lambda.min
  
  # Extract the coefficients at the optimal lambda for Lasso
  coefs_optimal_no_intercept_lasso <- coef(cv_lasso_model, s = best_lambda_cv_lasso)
  
  # Convert to data frame for easier manipulation
  coefs_df_optimal_no_intercept_lasso <- as.data.frame(as.matrix(coefs_optimal_no_intercept_lasso))
  coefs_df_optimal_no_intercept_lasso$variable <- rownames(coefs_df_optimal_no_intercept_lasso)
  colnames(coefs_df_optimal_no_intercept_lasso)[1] <- "coefficient"
  
  # Identify non-zero coefficients
  adalasso_non_zero_coefs_optimal_no_intercept <- coefs_df_optimal_no_intercept_lasso %>%
    filter(coefficient != 0)
  
  # Clean up
  rm(cv_lasso_model, cv_ridge_model, coefs_df_optimal_no_intercept_lasso, coefs_optimal_no_intercept_ridge)
  
  #EWMA---------------------
  
  #adaLASSO
  # Function to compute EWMA for each ETF
  
  
  
  # Subset etf_wide to include only the selected ETFs
  etf <- etf_wide[, adalasso_non_zero_coefs_optimal_no_intercept$variable]
  
  
  # dias <- nrow(etf) - 1  # Adjust T to be the last index of etf_data
  # # Compute the exponential weights
  # tau = 0.97
  # weights <- sqrt(tau^(0:dias))
  # normalization_factor <- sqrt((1 - tau) / (1 - tau^(dias + 1)))
  # weights <- normalization_factor * weights
  # 
  # weights = as.data.frame(weights)
  # # Adicionar um índice de linha
  # weights <- weights %>% mutate(row_number = row_number())
  # 
  # # Inverter a ordem das linhas usando arrange e desc
  # weights_reversed <- weights %>%
  #   arrange(desc(row_number)) %>%
  #   select(-row_number)  # Remove a coluna auxiliar
  # # Supondo que seus dataframes são etf e weights
  # etf <- sweep(etf, 1, weights_reversed$weights, `*`)
  
  etf = etf %>% as.matrix %>% as.data.frame()
  
  
  result = etf %>% 
    cbind(hf)
  
  
  # Fit a linear model
  fit_adalasso <- lm(hf ~ . -1, data = as.data.frame(result))
  
  # Obter o resumo do modelo
  summary_fit <- summary(fit_adalasso)
  
  # Extrair o R²
  r_squared_adalasso <- summary_fit$r.squared
  
  #jurema
  
  # final error --------------------
  
  # Nomes das variáveis do modelo fit_adalasso
  variables_fit_adalasso <- names(fit_adalasso$coefficients)
  num_var_adalasso = length(variables_fit_adalasso)
  # Nomes das variáveis do modelo fit_lasso
  variables_fit_lasso <- names(fit_lasso$coefficients)
  num_var_lasso = length(variables_fit_lasso)
  
  
  # Calcular a data do último dia do próximo mês
  next_month_date <- end_date %m+% months(1)
  end_of_next_month <- ceiling_date(next_month_date, "month") - days(1)
  
  hf_complete_iter = hf_complete %>% 
    filter(fund == f) %>% 
    filter(date >= end_date + days(1) & date<= end_of_next_month)
  
  apply_ma2_filter_test <- function(data) {
    num_observacoes <- nrow(data)
    
    # Verificar se o número de observações é par
    if (num_observacoes %% 2 == 0) {
      # Se for par, remover a observação mais recente
      data <- data[-num_observacoes, ]
    }
    
    # Aplicar o filtro MA(2) apenas se houver pelo menos 3 observações
    if (nrow(data) >= 3) {
      # Calcular o filtro MA(2)
      ma2_filter <- stats::filter(data$ret, c(1/2, 1/2), sides = 2, circular = TRUE)
      
      # Atribuir os valores filtrados de volta aos dados originais
      data$ret <- ma2_filter
    }
    
    return(data)
  }
  
  # Supomos que hf_test_data tenha a mesma estrutura que hf_complete_iter
  hf_complete_iter <- hf_complete_iter %>%
    group_by(fund) %>%
    do(apply_ma2_filter_test(.)) %>%
    mutate(ret = as.numeric(ret))  # Converter a coluna ret para numérico
  
  
  # Supondo que 'hf_complete_iter' já esteja definido e contenha todas as datas
  all_dates <- hf_complete_iter %>% distinct(date)
  
  # Filtrar para os primeiros dois anos de dados
  start_date <- min(hf_complete_iter$date) #+ 500
  end_date <- max(hf_complete_iter$date)
  etf_complete_iter <- etf_complete %>% filter(date >= start_date & date <= end_date)
  
  # Fazer o 'left_join' para garantir que todas as datas estejam presentes
  etf_complete_iter <- all_dates %>%
    left_join(etf_complete_iter, by = "date")
  
  # Pivotar para o formato wide
  etf_wide <- etf_complete_iter %>%
    pivot_wider(names_from = ticker, values_from = ret, values_fill = list(ret = 0))
  
  
  
  # Convertendo as colunas de retorno para numérico, exceto a primeira coluna (data)
  etf_wide <- etf_wide %>%
    mutate(across(-date, as.numeric))
  
  etf_lasso = etf_wide[,variables_fit_lasso]
  
  etf_adalasso = etf_wide[,variables_fit_adalasso]
  
  #NORMALIZAR DADOS LASSO DE ACORDO COM A NORMALIZACAO FEITA ANTERIORMENTE
  # Filtrar as médias e desvios padrão correspondentes às ETFs selecionadas
  selected_means <- train_means_wide[variables_fit_lasso]
  selected_sds <- train_sds_wide[variables_fit_lasso]
  
  
  for(i in 1:ncol(etf_lasso)){
    
    mean = selected_means[i] %>% unlist %>% unname()
    sd = selected_sds[i]%>% unlist %>% unname()
    temp = etf_lasso[,i]
    
    temp = temp %>%
      mutate_all(~ (. - mean) / sd)
    
    if(i==1){
      lasso = temp
    }else{
      lasso = lasso %>% bind_cols(temp)
      
    }
    
  }
  
  etf_lasso = lasso  
  
  rm(lasso)
  
  # Convertendo todas as colunas para numeric
  etf_lasso <- etf_lasso %>%
    mutate(across(everything(), as.numeric))
  
  # Extraindo os coeficientes do modelo fit_lasso
  coeficientes <- coef(fit_lasso)
  
  # Convertendo os coeficientes para um vetor
  coef_lasso <- as.vector(coeficientes)
  selected_sds = selected_sds %>% as.vector() %>% unlist() %>% unname()
  
  betas_originais <- cbind(coef_lasso,selected_sds) %>% 
    as.data.frame() %>% 
    mutate(betas = coef_lasso*selected_sds) %>% 
    summarise(beta0 = 1-(sum(betas)))
  
  b0_lasso = betas_originais$beta0
  
  #NORMALIZAR DADOS adaLASSO DE ACORDO COM A NORMALIZACAO FEITA ANTERIORMENTE
  # Filtrar as médias e desvios padrão correspondentes às ETFs selecionadas
  selected_means <- train_means_wide[variables_fit_adalasso]
  selected_sds <- train_sds_wide[variables_fit_adalasso]
  
  for(i in 1:ncol(etf_adalasso)){
    
    mean = selected_means[i] %>% unlist %>% unname()
    sd = selected_sds[i]%>% unlist %>% unname()
    temp = etf_adalasso[,i]
    
    temp = temp %>%
      mutate_all(~ (. - mean) / sd)
    
    if(i==1){
      adalasso = temp
    }else{
      adalasso = adalasso %>% bind_cols(temp)
      
    }
    
  }
  
  etf_adalasso = adalasso  
  
  rm(adalasso)
  # Extraindo os coeficientes do modelo fit_lasso
  coeficientes <- coef(fit_adalasso)
  
  # Convertendo os coeficientes para um vetor
  coef_adalasso <- as.vector(coeficientes)
  selected_sds = selected_sds %>% as.vector() %>% unlist() %>% unname()
  
  betas_originais <- cbind(coef_adalasso,selected_sds) %>% 
    as.data.frame() %>% 
    mutate(betas = coef_adalasso*selected_sds) %>% 
    summarise(beta0 = 1-(sum(betas)))
  
  b0_adalasso = betas_originais$beta0
  
  # #EWMA
  # dias <- nrow(etf_lasso) - 1  # Adjust T to be the last index of etf_data
  # # Compute the exponential weights
  # tau = 0.97
  # weights <- sqrt(tau^(0:dias))
  # normalization_factor <- sqrt((1 - tau) / (1 - tau^(dias + 1)))
  # weights <- normalization_factor * weights
  # 
  # weights = as.data.frame(weights)
  # # Adicionar um índice de linha
  # weights <- weights %>% mutate(row_number = row_number())
  # 
  # # Inverter a ordem das linhas usando arrange e desc
  # weights_reversed <- weights %>%
  #   arrange(desc(row_number)) %>%
  #   select(-row_number)  # Remove a coluna auxiliar
  # Supondo que seus dataframes são etf e weights
  #etf_lasso <- sweep(etf_lasso, 1, weights_reversed$weights, `*`)
  
  
  # Predict hedge fund returns using the coefficients from the fitted model
  lasso_predicted_hf_returns <- stats::predict(fit_lasso, newdata = as.data.frame(etf_lasso))
  
  
  #EWMA ADALASSO
  # #EWMA
  # dias <- nrow(etf_adalasso) - 1  # Adjust T to be the last index of etf_data
  # # Compute the exponential weights
  # tau = 0.97
  # weights <- sqrt(tau^(0:dias))
  # normalization_factor <- sqrt((1 - tau) / (1 - tau^(dias + 1)))
  # weights <- normalization_factor * weights
  # 
  # weights = as.data.frame(weights)
  # # Adicionar um índice de linha
  # weights <- weights %>% mutate(row_number = row_number())
  # 
  # # Inverter a ordem das linhas usando arrange e desc
  # weights_reversed <- weights %>%
  #   arrange(desc(row_number)) %>%
  #   select(-row_number)  # Remove a coluna auxiliar
  # Supondo que seus dataframes são etf e weights
  #etf_adalasso <- sweep(etf_adalasso, 1, weights_reversed$weights, `*`)

  
  # Predict hedge fund returns using the coefficients from the fitted model
  adalasso_predicted_hf_returns <- predict(fit_adalasso, newdata = as.data.frame(etf_adalasso))
  
  # Compare predicted hedge fund returns with actual hedge fund returns (assuming you have actual data for the 2 weeks)
  actual_hf_returns <- hf_complete_iter$ret  # Example actual returns for demonstration
  
  # Calculate evaluation metrics
  mse_lasso <- mean((lasso_predicted_hf_returns - actual_hf_returns)^2)
  rmse_lasso <- sqrt(mse_lasso)
  mae_lasso <- mean(abs(lasso_predicted_hf_returns - actual_hf_returns))
  track_error_lasso = sum(lasso_predicted_hf_returns) - sum(actual_hf_returns)
  # Calculate evaluation metrics
  mse_adalasso <- mean((adalasso_predicted_hf_returns - actual_hf_returns)^2)
  rmse_adalasso <- sqrt(mse_adalasso)
  mae_adalasso <- mean(abs(adalasso_predicted_hf_returns - actual_hf_returns))
  track_error_adalasso = sum(adalasso_predicted_hf_returns) - sum(actual_hf_returns)
  
  # Sharpe Ratio function
  sharpe_ratio <- function(returns, rf_rate = 0) {
    excess_returns <- returns - rf_rate
    mean_excess_return <- mean(excess_returns)
    sd_excess_return <- sd(excess_returns)
    sharpe_ratio <- mean_excess_return / sd_excess_return
    return(sharpe_ratio)
  }
  
  # Sortino Ratio function
  sortino_ratio <- function(returns, rf_rate = 0) {
    downside_deviation <- sqrt(mean(pmin(lasso_predicted_hf_returns - rf_rate, 0)^2))
    sortino_ratio <- (mean(returns - rf_rate)) / downside_deviation
    return(sortino_ratio)
  }
  
  # Information Ratio function
  information_ratio <- function(returns, benchmark_returns) {
    tracking_error <- sqrt(mean((returns - benchmark_returns)^2))
    information_ratio <- (mean(returns - benchmark_returns)) / tracking_error
    return(information_ratio)
  }
  
  # Skewness function with NA handling
  skewness_custom <- function(returns) {
    returns <- returns[!is.na(returns)]  # Remove NA values
    skew <- moments::skewness(returns)
    return(skew)
  }
  
  # Attrition Rate function
  attrition_rate <- function(returns) {
    attrition <- sum(returns < 0) / length(returns)
    return(attrition)
  }
  
  # Calculate metrics for LASSO predicted returns
  sharpe_lasso <- sharpe_ratio(lasso_predicted_hf_returns)
  sortino_lasso <- sortino_ratio(lasso_predicted_hf_returns)
  info_ratio_lasso <- information_ratio(lasso_predicted_hf_returns, actual_hf_returns)
  skewness_lasso <- skewness_custom(lasso_predicted_hf_returns)
  attrition_rate_lasso <- attrition_rate(lasso_predicted_hf_returns)
  mean_error_lasso <- mean(lasso_predicted_hf_returns - actual_hf_returns)
  volatility_error_lasso <- sd(lasso_predicted_hf_returns - actual_hf_returns)
  
  # Calculate metrics for adaLASSO predicted returns
  sharpe_adalasso <- sharpe_ratio(adalasso_predicted_hf_returns)
  sortino_adalasso <- sortino_ratio(adalasso_predicted_hf_returns)
  info_ratio_adalasso <- information_ratio(adalasso_predicted_hf_returns, actual_hf_returns)
  skewness_adalasso <- skewness_custom(adalasso_predicted_hf_returns)
  attrition_rate_adalasso <- attrition_rate(adalasso_predicted_hf_returns)
  mean_error_adalasso <- mean(adalasso_predicted_hf_returns - actual_hf_returns)
  volatility_error_adalasso <- sd(adalasso_predicted_hf_returns - actual_hf_returns)
  
  # Calculate metrics for actual HF returns (excluding information ratio)
  sharpe_actual <- sharpe_ratio(actual_hf_returns)
  sortino_actual <- sortino_ratio(actual_hf_returns)
  skewness_actual <- skewness_custom(actual_hf_returns)
  
  # Create the DataFrame
  model_results <- data.frame(
    model = c("lasso", "adalasso"),
    fund = f,
    num_var = c(num_var_lasso , num_var_adalasso),
    r2 = c(r_squared_lasso,r_squared_adalasso),
    start_train = paral[t,]$date,
    start_pred = start_date,
    end_pred = end_date,
    mse = c(mse_lasso, mse_adalasso),
    rmse = c(rmse_lasso, rmse_adalasso),
    mae = c(mae_lasso, mae_adalasso),
    sharpe_ratio = c(sharpe_lasso, sharpe_adalasso),
    sortino_ratio = c(sortino_lasso, sortino_adalasso),
    information_ratio = c(info_ratio_lasso, info_ratio_adalasso),
    skewness = c(skewness_lasso, skewness_adalasso),
    attrition_rate = c(attrition_rate_lasso, attrition_rate_adalasso),
    mean_error = c(mean_error_lasso, mean_error_adalasso),
    volatility_error = c(volatility_error_lasso, volatility_error_adalasso),
    stringsAsFactors = FALSE
  )
  
  # Add metrics for actual HF returns as new columns (excluding information ratio)
  model_results$sharpe_ratio_actual <- sharpe_actual
  model_results$sortino_ratio_actual <- sortino_actual
  model_results$skewness_actual <- skewness_actual
  model_results$actual_ret <- list(actual_hf_returns)
  
  # Create the list of selected variables (assuming variables_fit_lasso and variables_fit_adalasso are defined)
  variables_list <- list(variables_fit_lasso, variables_fit_adalasso)
  
  # Add the list to the data frame
  model_results$variables_fitted <- variables_list
  
  # Create the list of selected variables (assuming variables_fit_lasso and variables_fit_adalasso are defined)
  variables_list <- list(lasso_predicted_hf_returns, adalasso_predicted_hf_returns)
  
  # Add the list to the data frame
  model_results$predicted_returns <- variables_list
  
  
  save(model_results, file = file.path("C:/Users/msses/Desktop/TCC/dados/result",
                                       paste0("result_ewma_cdi_test", unique(model_results$fund), unique(model_results$start_train), ".RData")
  ))
  
  
}

# Stop the cluster after processing
stopCluster(cl)


toc()





