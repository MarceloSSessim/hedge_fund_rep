
library(readxl)
library(tidyverse)
library(lubridate)



# ETFs -----------------------
options(scipen = 999)

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

hf_complete=hf_complete %>% 
  filter(
    !fund %in% c(
      "BLP CRYPTO 100 INVESTIMENTO NO EXTERIOR FI MULTIMERCADO",
      "XPA TÁTICO FI MULTIMERCADO CRÉDITO PRIVADO",
      "SAFRA BONDS INVESTIMENTO NO EXTERIOR FI MULTIMERCADO CRÉDITO PRIVADO"
    )
  )


etf = etf_complete
rm(etf_complete)
# Assumindo que os dados já estão carregados na variável hf_complete
#grafico que o alvaro tinha falado
# Converter coluna date para formato de data
etf$date <- as.Date(etf$date)

# Extrair o mês de cada data e agrupar em formato de data imediatamente
etf$Month <- as.Date(format(etf$date, "%Y-%m-01"))

# Contar o número de ETFs por mês
etf_count <- etf %>% group_by(Month) %>% summarise(ETF_Count = n_distinct(ticker))

# Ordenar os meses (não é estritamente necessário se todos os valores já estão em order date format)
etf_count <- etf_count[order(etf_count$Month), ]

# Criar o gráfico
p <- ggplot(etf_count, aes(x = Month, y = ETF_Count)) +
  geom_col(fill = "blue") +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  labs(x = "Ano", y = "Número de ETFs", title = "Número de ETFs negociadas por mês") +
  theme_minimal() +
  theme(
    plot.background = element_rect(fill = "white"),  # Fundo branco
    plot.title = element_text(size = 24, face = "bold", hjust = 0.5, color = "black"),  # Título maior e centralizado
    axis.title.x = element_text(size = 16, face = "bold", color = "black"),  # Eixo x em negrito e preto
    axis.title.y = element_text(size = 16, face = "bold", color = "black"),  # Eixo y em negrito e preto
    axis.text.x = element_text(size = 14, angle = 45, hjust = 1, color = "black"),  # Texto do eixo x em preto
    axis.text.y = element_text(size = 14, color = "black")  # Texto do eixo y em preto
  ) 

# Salvar o gráfico com fundo branco
ggsave(filename = "C:/Users/msses/Desktop/TCC/dados/grafs/num_etfs_ano.png", plot = p, width = 18, height = 10, units = "in", dpi = 300)# #quantis
# etf_quantiles <- etf %>%
#   group_by(ticker) %>%
#   summarise(Q1 = quantile(Volume, 0.25),
#             Median = quantile(Volume, 0.5),
#             Q3 = quantile(Volume, 0.75))
# 
# etf_quantiles
# 
# # Passo 1: Agregar o volume mensal por ETF
# monthly_volume <- etf %>%
#   group_by(ticker, Month) %>%
#   summarise(MonthlyVolume = sum(Volume), .groups = 'drop')
# 
# # Passo 2: Calcular quantis de volume mensal por ETF
# etf_quantiles <- monthly_volume %>%
#   group_by(ticker) %>%
#   summarise(Q1 = quantile(MonthlyVolume, 0.25),
#             Median = quantile(MonthlyVolume, 0.5),
#             Q3 = quantile(MonthlyVolume, 0.75),
#             .groups = 'drop')



#grafico dos missings das etfs
# Convertendo date para formato de data, se ainda não estiver
etf$date <- as.Date(etf$date)

# Calculando o número total de dias por ticker
num_total_dias_por_ticker <- etf %>%
  group_by(ticker) %>%
  summarize(num_total_dias = as.numeric(difftime(max(date), min(date), units = "days")) + 1,
            num_dias_semana = sum(wday(seq(min(date), max(date), by = "day")) %in% 2:6))

# Contando o número de observações por ticker
num_obs_por_ticker <- etf %>%
  group_by(ticker) %>%
  summarize(num_obs = n_distinct(date),
            vol = sum(Volume))

# Verificando se há lacunas de observações
tickers_com_lacunas <- num_obs_por_ticker %>%
  left_join(num_total_dias_por_ticker, by = "ticker") %>%
  mutate(perc_traded = num_obs / num_total_dias,
         perc_traded_semana = num_obs / num_dias_semana,
         vol_day_semana = vol / num_dias_semana)

# Adicionando uma nova coluna que categoriza `perc_traded_semana` em grupos de 5%
tickers_com_lacunas$perc_traded_semana_group <- cut(tickers_com_lacunas$perc_traded_semana, seq(0, 1, by = 0.05))

# Adicionando uma nova coluna que categoriza `perc_traded_semana` em grupos de 5% com valores de 0.85 para baixo agrupados
tickers_com_lacunas$perc_traded_semana_group_combined <- cut(tickers_com_lacunas$perc_traded_semana, c(-Inf, seq(0.85, 1, by = 0.05)))

# Criando o gráfico original
ggplot(tickers_com_lacunas, aes(x = perc_traded_semana_group)) +
  geom_bar() + # Esta função cria um histograma/barra de contagem de ocorrências por grupo
  theme_minimal() + # Tema minimalista para o gráfico
  labs(
    title = "Quantidade de ETFs por Percentual de Dias Negociados por Semana (Original)",
    x = "Percentual de Dias Negociados por Semana (Intervalo de 5%)",
    y = "Quantidade de ETFs"
  ) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) # Rotação dos textos do eixo x para melhor visualização
  
# Criando o gráfico com a mudança
ggplot(tickers_com_lacunas, aes(x = perc_traded_semana_group_combined)) +
  geom_bar() + # Esta função cria um histograma/barra de contagem de ocorrências por grupo
  theme_minimal() + # Tema minimalista para o gráfico
  labs(
    title = "Quantidade de ETFs por Percentual de Dias Negociados por Semana (Alterado)",
    x = "Percentual de Dias Negociados por Semana",
    y = "Quantidade de ETFs"
  ) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) # Rotação dos textos do eixo x para melhor visualização


tickers_com_lacunas %>% filter(perc_traded_semana <.85) %>% nrow()
# # 349 de 2519 (0.138547) com menos de 90% dos dias sendo "trocados" 
# # pra 85% --- 0.05835649

#salvar rdata com o ticker escolhidos 
tickers = tickers_com_lacunas %>% 
  filter(perc_traded_semana >.9) %>% 
  distinct(ticker)
save(tickers,file = "C:/Users/msses/Desktop/TCC/dados/ETF/ticker_liq.RData")


#HF ------------------

hf = hf_complete

rm(hf_complete,paral)
# Converter coluna date para formato de data
hf$date <- as.Date(hf$date)

# Extrair o mês de cada date e agrupar em formato de date imediatamente
hf$Month <- as.Date(format(hf$date, "%Y-%m-01"))

# Contar o número de fundos por mês
hf_count <- hf %>% group_by(Month) %>% summarise(Fund_Count = n_distinct(fund))

# Ordenar os meses (não é estritamente necessário se todos os valores já estão em order date format)
hf_count <- hf_count[order(hf_count$Month), ]

# Plotar o gráfico
p = ggplot(hf_count, aes(x = Month, y = Fund_Count)) +
  geom_col(fill = "blue") +  # geom_col é um atalho para geom_bar(stat="identity")
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") + # Configura quebras a cada ano
  labs(x = "Ano", y = "Número de Fundos", title = "Número de Fundos negociados por mês") +
  theme_minimal() +  # Tema mais limpo
  theme(
    plot.background = element_rect(fill = "white"),  # Fundo branco
    plot.title = element_text(size = 24, face = "bold", hjust = 0.5, color = "black"),  # Título maior e centralizado
    axis.title.x = element_text(size = 16, face = "bold", color = "black"),  # Eixo x em negrito e preto
    axis.title.y = element_text(size = 16, face = "bold", color = "black"),  # Eixo y em negrito e preto
    axis.text.x = element_text(size = 14, angle = 45, hjust = 1, color = "black"),  # Texto do eixo x em preto
    axis.text.y = element_text(size = 14, color = "black")  # Texto do eixo y em preto
  ) 

# Salvar o gráfico com fundo branco
ggsave(filename = "C:/Users/msses/Desktop/TCC/dados/grafs/num_hfs_ano.png", plot = p, width = 18, height = 10, units = "in", dpi = 300)# #quantis


# Converter date para formato de date, se ainda não estiver
hf$date <- as.Date(hf$date)

# Calcular o número total de dias por fundo
num_total_dias_por_fundo <- hf %>%
  group_by(fund) %>%
  summarize(
    num_total_dias = as.numeric(difftime(max(date), min(date), units = "days")) + 1,
    num_dias_semana = sum(wday(seq(min(date), max(date), by = "day")) %in% 2:6)
  )

# Contar o número de observações por fundo
num_obs_por_fundo <- hf %>%
  group_by(fund) %>%
  summarize(num_obs = n_distinct(date))

# Verificar se há lacunas de observações
fundos_com_lacunas <- num_obs_por_fundo %>%
  left_join(num_total_dias_por_fundo, by = "fund") %>%
  mutate(
    perc_traded = num_obs / num_total_dias,
    perc_traded_semana = num_obs / num_dias_semana
  )

# Adicionando uma nova coluna que categoriza `perc_traded_semana` em grupos de 5%
fundos_com_lacunas$perc_traded_semana_group <- cut(fundos_com_lacunas$perc_traded_semana, seq(0, 1, by = 0.05))

# Criando um gráfico
ggplot(fundos_com_lacunas, aes(x = perc_traded_semana_group)) +
  geom_bar() + # Cria um histograma de contagem de ocorrências por grupo
  theme_minimal() + # Tema minimalista para o gráfico
  labs(
    title = "Quantidade de Fundos por Percentual de Dias Negociados por Semana",
    x = "Percentual de Dias Negociados por Semana (Intervalo de 5%)",
    y = "Quantidade de Fundos"
  ) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) # Melhora a visualização dos rótu
# Exibir os fundos com lacunas
print(fundos_com_lacunas)

rm(hf)

