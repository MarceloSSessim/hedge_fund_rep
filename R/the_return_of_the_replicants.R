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
library(PerformanceAnalytics)


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
k = 9
l = 10

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

parallel_func = function(t) {
  tryCatch({
    
    ### PARALELIZAR EM f,d ########### hf_complete %>% distinct(fund,date) -- pegar min dates pra cada fundo
    
    f = paral[t,]$fund
    d = paral[t,]$date
    
    
    hf_complete_iter = hf_complete %>% 
      filter(fund == f) %>% 
      filter(date >= d & date<= d + years(2) - days(1))
    
    # Filtrar para os primeiros dois anos de dados
    start_date <- min(hf_complete_iter$date) #+ 500
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
    
    train_means_wide <- colMeans(etf_wide[-1], na.rm = TRUE)  # Ignorando a coluna de data
    train_sds_wide <- apply(etf_wide[-1], 2, sd, na.rm = TRUE)  # Ignorando a coluna de data
    
    
    # Convert all ETF ret columns to numeric if necessary
    # (Assuming the first column might be the date or identifier that should not be converted)
    etf_wide <- etf_wide %>%
      mutate(across(-1, as.numeric))  # Skipping the first column, typically an index or date
    
    
    
    
    # Calcular a matriz de correlação dos retornos
    cor_matrix <- cor(etf_wide[, -1], use = "pairwise.complete.obs")
    
    ### ITERAÇÃO EM I 1:100
    
    cluster_func = function(i){
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
      
      
      
      # Definir uma função para calcular o BIC
      calc_bic <- function(y_true, y_pred, num_params) {
        n <- length(y_true)
        mse <- mean((y_true - y_pred)^2)
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
        num_nonzero_coefs <- sum(coef(lasso_model, s = lambda) != 0) - 1  # Subtrai 1 para ignorar o intercepto
        calc_bic(hf, preds, num_nonzero_coefs)
      })
      
      # Selecionar o lambda que minimiza o BIC
      best_lambda_bic <- lambdas[which.min(bics)]
      
      # Extrair os coeficientes no lambda ótimo
      coefs_optimal_no_intercept <- coef(lasso_model, s = best_lambda_bic)
      
      # Converter para data frame para facilitar a manipulação
      coefs_df_optimal_no_intercept <- as.data.frame(as.matrix(coefs_optimal_no_intercept))
      coefs_df_optimal_no_intercept$variable <- rownames(coefs_df_optimal_no_intercept)
      colnames(coefs_df_optimal_no_intercept)[1] <- "coefficient"
      
      # Identificar coeficientes não-zero
      lasso_non_zero_coefs_optimal_no_intercept <- coefs_df_optimal_no_intercept %>%
        filter(coefficient != 0)
      
      rm(coefs_df_optimal_no_intercept,coefs_optimal_no_intercept,lasso_model)
      
      ##adaLASSO -------------
      
      
      
      # Define function to calculate BIC
      calc_bic <- function(y_true, y_pred, num_params) {
        n <- length(y_true)
        mse <- mean((y_true - y_pred)^2)
        bic <- n * log(mse) + num_params * log(n)
        return(bic)
      }
      
      
      # Fit the Ridge regression model without intercept using glmnet
      ridge_model <- glmnet(etf, hf, alpha = 0, intercept = FALSE)
      
      # Calcular o BIC para cada valor de lambda
      bics <- sapply(lambdas, function(lambda) {
        if (lambda == 0) {
          preds <- predict(ridge_model, newx = etf)
          num_nonzero_coefs <- sum(coef(ridge_model) != 0) - 1  # Subtrai 1 para ignorar o intercepto
        } else {
          preds <- predict(ridge_model, s = lambda, newx = etf)
          num_nonzero_coefs <- sum(coef(ridge_model, s = lambda) != 0) - 1  # Subtrai 1 para ignorar o intercepto
        }
        calc_bic(hf, preds, num_nonzero_coefs)
      })
      
      # Selecionar o lambda que minimiza o BIC
      best_lambda_bic <- lambdas[which.min(bics)]
      
      # Extract the coefficients at the optimal lambda
      coefs_optimal_no_intercept <- coef(ridge_model, s = best_lambda_bic)[-1]  # Exclude intercept
      
      
      # Number of observations
      n <- nrow(etf)
      
      # Compute weights
      omega <- 1 / (abs(coefs_optimal_no_intercept) + 1 / sqrt(n))
      
      # Prepare the adaptive weights
      # glmnet allows for weights using penalty.factor
      adaptive_weights <- omega
      
      # Perform LASSO regression with adaptive weights
      lasso_model <- glmnet(etf, hf, alpha = 1, penalty.factor = adaptive_weights, intercept = F)
      
      # Obter valores de lambda usados no ajuste
      lambdas <- lasso_model$lambda
      
      # Calcular o BIC para cada valor de lambda
      bics <- sapply(lambdas, function(lambda) {
        preds <- predict(lasso_model, s = lambda, newx = etf)
        num_nonzero_coefs <- sum(coef(lasso_model, s = lambda) != 0) - 1  # Subtrai 1 para ignorar o intercepto
        calc_bic(hf, preds, num_nonzero_coefs)
      })
      
      # Selecionar o lambda que minimiza o BIC
      best_lambda_bic <- lambdas[which.min(bics)]
      
      # Extrair os coeficientes no lambda ótimo
      coefs_optimal_no_intercept <- coef(lasso_model, s = best_lambda_bic)
      
      # Converter para data frame para facilitar a manipulação
      coefs_df_optimal_no_intercept <- as.data.frame(as.matrix(coefs_optimal_no_intercept))
      coefs_df_optimal_no_intercept$variable <- rownames(coefs_df_optimal_no_intercept)
      colnames(coefs_df_optimal_no_intercept)[1] <- "coefficient"
      
      # Identificar coeficientes não-zero
      adalasso_non_zero_coefs_optimal_no_intercept <- coefs_df_optimal_no_intercept %>%
        filter(coefficient != 0)
      
      rm(lasso_model,ridge_model,km,coefs_df_optimal_no_intercept,coefs_optimal_no_intercept)
      
      #EWMA---------------------
      #LASSO
      # Function to compute EWMA for each ETF
      
      
      
      
      # Subset etf_wide to include only the selected ETFs
      etf <- etf_wide[, lasso_non_zero_coefs_optimal_no_intercept$variable]
      
      
      # Set parameters
      tau <- 0.97
      T <- nrow(etf) - 1  # Assuming etf_adalasso is your dataframe
      
      # Compute the exponential weights
      weights <- sqrt(tau^(0:T))
      normalization_factor <- sqrt((1 - tau) / (1 - tau^(T + 1)))
      weights <- normalization_factor * weights
      
      # Apply weights to ETF data
      for (j in 0:T) {
        etf[, -(1:j)] <- sweep(etf[, -(1:j)], 2, weights[j + 1], `*`)
      }
      
      result = etf %>% 
        cbind(hf)
      
      # Fit a linear model
      fit <- lm(hf ~ . -1, data = as.data.frame(result))
      
      
      # Calculate the Bayesian Information Criterion (BIC)
      bic_value <- BIC(fit)
      
      lasso_bic = bic_value
      
      
      #adaLASSO
      
      #EWMA
      # Function to compute EWMA for each ETF
      
      
      
      # Subset etf_wide to include only the selected ETFs
      etf <- etf_wide[, adalasso_non_zero_coefs_optimal_no_intercept$variable]
      
      
      # Set parameters
      tau <- 0.97
      T <- nrow(etf) - 1  # Assuming etf_adalasso is your dataframe
      
      # Compute the exponential weights
      weights <- sqrt(tau^(0:T))
      normalization_factor <- sqrt((1 - tau) / (1 - tau^(T + 1)))
      weights <- normalization_factor * weights
      
      # Apply weights to ETF data
      for (j in 0:T) {
        etf[, -(1:j)] <- sweep(etf[, -(1:j)], 2, weights[j + 1], `*`)
      }
      
      result = etf %>% 
        cbind(hf)
      
      # Fit a linear model
      fit <- lm(hf ~ . -1, data = as.data.frame(result))
      
      
      # Calculate the Bayesian Information Criterion (BIC)
      bic_value <- BIC(fit)
      
      adalasso_bic = bic_value
      
      res = c(lasso_bic,adalasso_bic,i)
      
      # Transpor res para uma linha
      res <- t(res)
      
      # Converter para data frame e nomear as colunas
      res <- as.data.frame(res)
      colnames(res) <- c("lasso", "adalasso", "i")
      
      res
    }
    
    plan(multisession, workers = 10)
    
    result = future_lapply(2:100, cluster_func)
    result = result %>% bind_rows()
    
    position_lasso = result %>% 
      arrange(lasso,-i) %>% 
      first() %>% 
      select(i)
    
    position_lasso = position_lasso$i
    
    position_adalasso = result %>% 
      arrange(adalasso,-i) %>% 
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
    
    
    
    # Definir uma função para calcular o BIC
    calc_bic <- function(y_true, y_pred, num_params) {
      n <- length(y_true)
      mse <- mean((y_true - y_pred)^2)
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
      num_nonzero_coefs <- sum(coef(lasso_model, s = lambda) != 0) - 1  # Subtrai 1 para ignorar o intercepto
      calc_bic(hf, preds, num_nonzero_coefs)
    })
    
    # Selecionar o lambda que minimiza o BIC
    best_lambda_bic <- lambdas[which.min(bics)]
    
    # Extrair os coeficientes no lambda ótimo
    coefs_optimal_no_intercept <- coef(lasso_model, s = best_lambda_bic)
    
    # Converter para data frame para facilitar a manipulação
    coefs_df_optimal_no_intercept <- as.data.frame(as.matrix(coefs_optimal_no_intercept))
    coefs_df_optimal_no_intercept$variable <- rownames(coefs_df_optimal_no_intercept)
    colnames(coefs_df_optimal_no_intercept)[1] <- "coefficient"
    
    # Identificar coeficientes não-zero
    lasso_non_zero_coefs_optimal_no_intercept <- coefs_df_optimal_no_intercept %>%
      filter(coefficient != 0)
    
    rm(coefs_df_optimal_no_intercept,coefs_optimal_no_intercept,lasso_model)
    
    
    #EWMA---------------------
    #LASSO
    # Function to compute EWMA for each ETF
    
    
    # Subset etf_wide to include only the selected ETFs
    etf <- etf_wide[, lasso_non_zero_coefs_optimal_no_intercept$variable]
    
    
    # Set parameters
    tau <- 0.97
    T <- nrow(etf) - 1  # Assuming etf_adalasso is your dataframe
    
    # Compute the exponential weights
    weights <- sqrt(tau^(0:T))
    normalization_factor <- sqrt((1 - tau) / (1 - tau^(T + 1)))
    weights <- normalization_factor * weights
    
    # Apply weights to ETF data
    for (j in 0:T) {
      etf[, -(1:j)] <- sweep(etf[, -(1:j)], 2, weights[j + 1], `*`)
    }
    
    result = etf %>% 
      cbind(hf)
    
    # Fit a linear model
    fit_lasso <- lm(hf ~ . -1, data = as.data.frame(result))
    
    
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
    
    
    
    
    
    # Define function to calculate BIC
    calc_bic <- function(y_true, y_pred, num_params) {
      n <- length(y_true)
      mse <- mean((y_true - y_pred)^2)
      bic <- n * log(mse) + num_params * log(n)
      return(bic)
    }
    
    
    # Fit the Ridge regression model without intercept using glmnet
    ridge_model <- glmnet(etf, hf, alpha = 0, intercept = FALSE)
    
    # Calcular o BIC para cada valor de lambda
    bics <- sapply(lambdas, function(lambda) {
      if (lambda == 0) {
        preds <- predict(ridge_model, newx = etf)
        num_nonzero_coefs <- sum(coef(ridge_model) != 0) - 1  # Subtrai 1 para ignorar o intercepto
      } else {
        preds <- predict(ridge_model, s = lambda, newx = etf)
        num_nonzero_coefs <- sum(coef(ridge_model, s = lambda) != 0) - 1  # Subtrai 1 para ignorar o intercepto
      }
      calc_bic(hf, preds, num_nonzero_coefs)
    })
    
    # Selecionar o lambda que minimiza o BIC
    best_lambda_bic <- lambdas[which.min(bics)]
    
    # Extract the coefficients at the optimal lambda
    coefs_optimal_no_intercept <- coef(ridge_model, s = best_lambda_bic)[-1]  # Exclude intercept
    
    
    # Number of observations
    n <- nrow(etf)
    
    # Compute weights
    omega <- 1 / (abs(coefs_optimal_no_intercept) + 1 / sqrt(n))
    
    # Prepare the adaptive weights
    # glmnet allows for weights using penalty.factor
    adaptive_weights <- omega
    
    # Perform LASSO regression with adaptive weights
    lasso_model <- glmnet(etf, hf, alpha = 1, penalty.factor = adaptive_weights, intercept = F)
    
    # Obter valores de lambda usados no ajuste
    lambdas <- lasso_model$lambda
    
    # Calcular o BIC para cada valor de lambda
    bics <- sapply(lambdas, function(lambda) {
      preds <- predict(lasso_model, s = lambda, newx = etf)
      num_nonzero_coefs <- sum(coef(lasso_model, s = lambda) != 0) - 1  # Subtrai 1 para ignorar o intercepto
      calc_bic(hf, preds, num_nonzero_coefs)
    })
    
    # Selecionar o lambda que minimiza o BIC
    best_lambda_bic <- lambdas[which.min(bics)]
    
    # Extrair os coeficientes no lambda ótimo
    coefs_optimal_no_intercept <- coef(lasso_model, s = best_lambda_bic)
    
    # Converter para data frame para facilitar a manipulação
    coefs_df_optimal_no_intercept <- as.data.frame(as.matrix(coefs_optimal_no_intercept))
    coefs_df_optimal_no_intercept$variable <- rownames(coefs_df_optimal_no_intercept)
    colnames(coefs_df_optimal_no_intercept)[1] <- "coefficient"
    
    # Identificar coeficientes não-zero
    adalasso_non_zero_coefs_optimal_no_intercept <- coefs_df_optimal_no_intercept %>%
      filter(coefficient != 0)
    
    rm(lasso_model,ridge_model,km,coefs_df_optimal_no_intercept,coefs_optimal_no_intercept)
    
    #EWMA---------------------
    
    #adaLASSO
    # Function to compute EWMA for each ETF
    
    
    
    # Subset etf_wide to include only the selected ETFs
    etf <- etf_wide[, adalasso_non_zero_coefs_optimal_no_intercept$variable]
    
    
    # Set parameters
    tau <- 0.97
    T <- nrow(etf) - 1  # Assuming etf_adalasso is your dataframe
    
    # Compute the exponential weights
    weights <- sqrt(tau^(0:T))
    normalization_factor <- sqrt((1 - tau) / (1 - tau^(T + 1)))
    weights <- normalization_factor * weights
    
    # Apply weights to ETF data
    for (j in 0:T) {
      etf[, -(1:j)] <- sweep(etf[, -(1:j)], 2, weights[j + 1], `*`)
    }
    
    result = etf %>% 
      cbind(hf)
    
    
    # Fit a linear model
    fit_adalasso <- lm(hf ~ . -1, data = as.data.frame(result))
    
    summary(fit_lasso)
    
    # final error --------------------
    
    # Nomes das variáveis do modelo fit_adalasso
    variables_fit_adalasso <- names(fit_adalasso$coefficients)
    
    # Nomes das variáveis do modelo fit_lasso
    variables_fit_lasso <- names(fit_lasso$coefficients)
    
    hf_complete_iter = hf_complete %>% 
      filter(fund == f) %>% 
      filter(date >= end_date + days(1) & date<= end_date +days(15))
    
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
    
    # Função para normalizar os dados com base nas médias e desvios padrão fornecidos
    normalize_data <- function(data, means, sds) {
      normalized_data <- data
      for (etf in names(data)) {
        if (!is.na(means[etf]) && !is.na(sds[etf]) && sds[etf] != 0) {
          normalized_data[[etf]] <- (data[[etf]] - means[etf]) / sds[etf]
        }
        # } else {
        #   warning(paste("Skipped normalizing ETF:", etf, "due to zero standard deviation or missing values"))
        # }
      }
      return(normalized_data)
    }
    
    # Aplicar a normalização aos dados de previsão
    etf_lasso <- normalize_data(etf_lasso, selected_means, selected_sds)
    
    #NORMALIZAR DADOS adaLASSO DE ACORDO COM A NORMALIZACAO FEITA ANTERIORMENTE
    # Filtrar as médias e desvios padrão correspondentes às ETFs selecionadas
    selected_means <- train_means_wide[variables_fit_adalasso]
    selected_sds <- train_sds_wide[variables_fit_adalasso]
    
    # Função para normalizar os dados com base nas médias e desvios padrão fornecidos
    normalize_data <- function(data, means, sds) {
      normalized_data <- data
      for (etf in names(data)) {
        if (!is.na(means[etf]) && !is.na(sds[etf]) && sds[etf] != 0) {
          normalized_data[[etf]] <- (data[[etf]] - means[etf]) / sds[etf]
        }
        # } else {
        #   warning(paste("Skipped normalizing ETF:", etf, "due to zero standard deviation or missing values"))
        # }
      }
      return(normalized_data)
    }
    
    # Aplicar a normalização aos dados de previsão
    etf_adalasso <- normalize_data(etf_adalasso, selected_means, selected_sds)
    
    
    
    # Set parameters
    tau <- 0.97
    T <- nrow(etf_lasso) - 1  # Assuming etf_lasso is your dataframe
    
    # Compute the exponential weights
    weights <- sqrt(tau^(0:T))
    normalization_factor <- sqrt((1 - tau) / (1 - tau^(T + 1)))
    weights <- normalization_factor * weights
    
    # Apply weights to ETF data
    for (j in 0:T) {
      etf_lasso[, -(1:j)] <- sweep(etf_lasso[, -(1:j)], 2, weights[j + 1], `*`)
    }
    
    # Predict hedge fund returns using the coefficients from the fitted model
    lasso_predicted_hf_returns <- predict(fit_lasso, newdata = as.data.frame(etf_lasso))
    
    
    #EWMA ADALASSO
    # Set parameters
    tau <- 0.97
    T <- nrow(etf_adalasso) - 1  # Assuming etf_adalasso is your dataframe
    
    # Compute the exponential weights
    weights <- sqrt(tau^(0:T))
    normalization_factor <- sqrt((1 - tau) / (1 - tau^(T + 1)))
    weights <- normalization_factor * weights
    
    # Apply weights to ETF data
    for (j in 0:T) {
      etf_adalasso[, -(1:j)] <- sweep(etf_adalasso[, -(1:j)], 2, weights[j + 1], `*`)
    }
    
    
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
    
    # Function to calculate Sortino Ratio
    sortino_ratio <- function(returns, rf_rate = 0) {
      downside_deviation <- sqrt(mean(pmin(returns - rf_rate, 0)^2))
      sortino_ratio <- (mean(returns - rf_rate)) / downside_deviation
      return(sortino_ratio)
    }
    
    # Function to calculate Information Ratio
    information_ratio <- function(returns, benchmark_returns) {
      tracking_error <- sqrt(mean((returns - benchmark_returns)^2))
      information_ratio <- (mean(returns - benchmark_returns)) / tracking_error
      return(information_ratio)
    }
    
    # Function to calculate skewness
    skewness <- function(returns) {
      skew <- skewness(returns, na.rm = TRUE)
      return(skew)
    }
    
    # Function to calculate attrition rate
    attrition_rate <- function(returns) {
      attrition <- sum(returns < 0) / length(returns)
      return(attrition)
    }
    
    # Calculate metrics for LASSO predicted returns
    sharpe_lasso <- SharpeRatio(lasso_predicted_hf_returns)
    sortino_lasso <- sortino_ratio(lasso_predicted_hf_returns)
    info_ratio_lasso <- information_ratio(lasso_predicted_hf_returns, actual_hf_returns)
    skewness_lasso <- skewness(lasso_predicted_hf_returns)
    attrition_rate_lasso <- attrition_rate(lasso_predicted_hf_returns)
    mean_error_lasso <- mean(lasso_predicted_hf_returns - actual_hf_returns)
    volatility_error_lasso <- sd(lasso_predicted_hf_returns - actual_hf_returns)
    
    # Calculate metrics for adaLASSO predicted returns
    sharpe_adalasso <- SharpeRatio(adalasso_predicted_hf_returns)
    sortino_adalasso <- sortino_ratio(adalasso_predicted_hf_returns)
    info_ratio_adalasso <- information_ratio(adalasso_predicted_hf_returns, actual_hf_returns)
    skewness_adalasso <- skewness(adalasso_predicted_hf_returns)
    attrition_rate_adalasso <- attrition_rate(adalasso_predicted_hf_returns)
    mean_error_adalasso <- mean(adalasso_predicted_hf_returns - actual_hf_returns)
    volatility_error_adalasso <- sd(adalasso_predicted_hf_returns - actual_hf_returns)
    
    
    # Create the DataFrame
    model_results <- data.frame(
      model = c("lasso", "adalasso"),
      fund = f,
      start_prediction = start_date,
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
    
    # Create the list of selected variables (assuming variables_fit_lasso and variables_fit_adalasso are defined)
    variables_list <- list(variables_fit_lasso, variables_fit_adalasso)
    
    # Add the list to the data frame
    model_results$variables_fitted <- variables_list
    
    save(model_results, file = file.path("C:/Users/msses/Desktop/TCC/dados/result",
                                         paste0("result_ewma_cdi_test", unique(model_results$fund), unique(model_results$start_prediction), ".RData")
    ))
    
    
  },
  error = function(e) {
    cat("Error in iteration", t, ":", conditionMessage(e), "\n")
    return(NULL)
  })
  
  
  
  
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

tic()

detectCores()

plan(multisession, workers = 8)


future_lapply(1:10, parallel_func,future.seed=TRUE)


toc()



