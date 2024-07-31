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
library(scales)


tic()

options(scipen = 999)

#load complete etf w returns
load("C:/Users/msses/Desktop/TCC/dados/ETF/etf_complete_semCDI.RData")

#load tickers that will be used to filter previous db
load(file = "C:/Users/msses/Desktop/TCC/dados/ETF/ticker_liq.RData")

etf_complete = etf_complete %>% 
  filter(ticker %in% tickers$ticker) %>% 
  select(-Close)

rm(tickers)

# load( file = "C:/Users/msses/Desktop/TCC/dados/HF/hf_complete.RData")
# 
# dates = hf_complete %>% 
#   distinct(date,fund)
# 
# rm(hf_complete)

#hf date

load( file = "C:/Users/msses/Desktop/TCC/dados/HF/hf_complete_semCDI.RData")


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

hf_complete=hf_complete %>% 
  filter(
    !fund %in% c(
      "BLP CRYPTO 100 INVESTIMENTO NO EXTERIOR FI MULTIMERCADO",
      "XPA TÁTICO FI MULTIMERCADO CRÉDITO PRIVADO",
      "SAFRA BONDS INVESTIMENTO NO EXTERIOR FI MULTIMERCADO CRÉDITO PRIVADO"
    )
  )

# Assumindo que os dados já estão carregados na variável hf_complete

# Criar um resumo estatístico geral
summary_stats_general <- hf_complete %>%
  summarise(
    total_obs = n(),
    total_funds = hf_complete %>% distinct(fund) %>% nrow(),
    start_date = min(date),
    end_date = max(date),
    mean_return = mean(ret, na.rm = TRUE),
    sd_return = sd(ret, na.rm = TRUE),
    min_return = min(ret, na.rm = TRUE),
    max_return = max(ret, na.rm = TRUE),
    median_return = median(ret, na.rm = TRUE),
    total_positive_returns = sum(ret > 0, na.rm = TRUE),
    total_negative_returns = sum(ret < 0, na.rm = TRUE),
    percl_positive_returns = total_positive_returns/(total_positive_returns+total_negative_returns),
    percl_negative_returns = total_negative_returns/(total_positive_returns+total_negative_returns)
    
  )

# Exibir o resumo geral
print(summary_stats_general)


# Assumindo que os dados já estão carregados na variável etf_complete

# Criar um resumo estatístico geral
summary_stats_etf <- etf_complete %>%
  summarise(
    total_obs = n(),
    total_etf = etf_complete %>% distinct(ticker) %>% nrow(),
    start_date = min(date),
    end_date = max(date),
    mean_return = mean(ret, na.rm = TRUE),
    sd_return = sd(ret, na.rm = TRUE),
    min_return = min(ret, na.rm = TRUE),
    max_return = max(ret, na.rm = TRUE),
    median_return = median(ret, na.rm = TRUE),
    total_positive_returns = sum(ret > 0, na.rm = TRUE),
    total_negative_returns = sum(ret < 0, na.rm = TRUE),
    percl_positive_returns = total_positive_returns/(total_positive_returns+total_negative_returns),
    percl_negative_returns = total_negative_returns/(total_positive_returns+total_negative_returns)
  )

# Exibir o resumo geral
print(summary_stats_etf)

#HISTOGRAMA RETORNOS HF
# Supondo que seu dataframe esteja em uma variável chamada hf_complete
# Definindo os limites para as bandas
limite_inferior <- -3
limite_superior <- 3
passo <- 0.15

# Criando os breaks para categorizar os valores
breaks <- c(-Inf, seq(limite_inferior, limite_superior, by = passo), Inf)

# Gerando os rótulos para as bandas (limites superiores)
labels <- c(paste0("<", limite_inferior),
            as.character(seq(limite_inferior + passo, limite_superior, by = passo)),
            paste0(">", limite_superior))

# Categorizar os valores de ret em bandas
hf_complete$bandas <- cut(hf_complete$ret,
                          breaks = breaks,
                          include.lowest = TRUE,
                          labels = labels)

# Verificando a distribuição das bandas
table(hf_complete$bandas)

# Criar o histograma com as bandas
p = ggplot(hf_complete, aes(x = bandas)) +
  geom_bar(fill = "blue", color = "black", alpha = 0.7) +
  labs(
    title = "Histograma dos Retornos dos Hedge Funds",
    x = "Limite Superior da Banda de Retorno (%)",
    y = "Frequência (milhares)"
  ) +
  theme_minimal() +
  scale_y_continuous(labels = scales::label_number(scale = 1/1000)) +
  theme(
    plot.background = element_rect(fill = "white"),  # Fundo branco
    plot.title = element_text(size = 24, face = "bold", hjust = 0.5, color = "black"),  # Título maior e centralizado
    axis.title.x = element_text(size = 16, face = "bold", color = "black"),  # Eixo x em negrito e preto
    axis.title.y = element_text(size = 16, face = "bold", color = "black"),  # Eixo y em negrito e preto
    axis.text.x = element_text(size = 14, angle = 45, hjust = 1, color = "black"),  # Texto do eixo x em preto
    axis.text.y = element_text(size = 14, color = "black")  # Texto do eixo y em preto
  )

# Salvar o gráfico com fundo branco
ggsave(filename = "C:/Users/msses/Desktop/TCC/dados/grafs/histogram_hf.png", plot = p, width = 18, height = 10, units = "in", dpi = 300)# #quantis


#HISTOGRAMA RETORNOS etf
# Supondo que seu dataframe esteja em uma variável chamada hf_complete
# Definindo os limites para as bandas
limite_inferior <- -3
limite_superior <- 3
passo <- 0.15

# Criando os breaks para categorizar os valores
breaks <- c(-Inf, seq(limite_inferior, limite_superior, by = passo), Inf)

# Gerando os rótulos para as bandas (limites superiores)
labels <- c(paste0("<", limite_inferior),
            as.character(seq(limite_inferior + passo, limite_superior, by = passo)),
            paste0(">", limite_superior))

# Categorizar os valores de ret em bandas
etf_complete$bandas <- cut(etf_complete$ret,
                           breaks = breaks,
                           include.lowest = TRUE,
                           labels = labels)

# Verificando a distribuição das bandas
table(etf_complete$bandas)

# Criar o histograma com as bandas
p=ggplot(etf_complete, aes(x = bandas)) +
  geom_bar(fill = "blue", color = "black", alpha = 0.7) +
  labs(title = "Histograma dos retornos de ETFs",
       x = "Limite Superior da Banda de Retorno (%)",
       y = "Frequência (milhares)") +
  theme_minimal() +
  scale_y_continuous(labels = label_number(scale = 1/1000))+  # Tema mais limpo
  theme(
    plot.background = element_rect(fill = "white"),  # Fundo branco
    plot.title = element_text(size = 24, face = "bold", hjust = 0.5, color = "black"),  # Título maior e centralizado
    axis.title.x = element_text(size = 16, face = "bold", color = "black"),  # Eixo x em negrito e preto
    axis.title.y = element_text(size = 16, face = "bold", color = "black"),  # Eixo y em negrito e preto
    axis.text.x = element_text(size = 14, angle = 45, hjust = 1, color = "black"),  # Texto do eixo x em preto
    axis.text.y = element_text(size = 14, color = "black")  # Texto do eixo y em preto
  ) 
# Salvar o gráfico com fundo branco
ggsave(filename = "C:/Users/msses/Desktop/TCC/dados/grafs/histogram_etf.png", plot = p, width = 18, height = 10, units = "in", dpi = 300)# #quantis
