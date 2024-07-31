library(tidyverse)
library(lubridate)


options(scipen = 999)

#load complete etf w returns
load("C:/Users/msses/Desktop/TCC/dados/ETF/etf_complete_semCDI.RData")

#load tickers that will be used to filter previous db
load(file = "C:/Users/msses/Desktop/TCC/dados/ETF/ticker_liq.RData")

etf_complete = etf_complete %>% 
  filter(ticker %in% tickers$ticker) %>% 
  select(-Close)

rm(tickers)

# Garantir que a coluna date esteja em formato Date
etf_complete$date <- as.Date(etf_complete$date)

etf_complete <- etf_complete %>% 
  filter(!is.na(ret)) %>% 
  filter(ticker!= "SLVO")

# Remove February 29 from the dataset
etf_complete <- etf_complete %>%
  filter(!(format(date, "%m-%d") == "02-29"))

etf_complete <- etf_complete %>%
  filter(date > as.Date("2017-12-31"))

paral = etf_complete %>% 
  distinct(date,ticker) %>% 
  group_by(ticker) %>% 
  mutate(inicio = min(date),
         final = max(date))

# Adiciona a coluna 'valid' com a verificação de pelo menos 2 anos e 14 dias à frente
paral <- paral %>%
  mutate(valid = (final - date) >= (years(1) + months(1))) %>% 
  filter(valid == T) %>% 
  select(ticker,date)

# Filtrar observações de cada fundo espaçadas de mês em mês
paral <- paral %>%
  arrange(ticker, date) %>% # Ordena por fund e data
  group_by(ticker, year = year(date), month = month(date)) %>% # Agrupa por fundo, ano e mês
  slice(1) %>% # Mantém apenas a primeira observação de cada grupo
  ungroup() %>% # Remove agrupamento
  select(-year,-month)
#teste com meos fundos

etfs = paral %>% 
  mutate(c = 1) %>% 
  group_by(ticker) %>%
  summarise(soma =sum(c)) %>%
  arrange(-soma) 

paral = paral %>% 
  filter(ticker %in% etfs$ticker) 

etf_complete = etf_complete %>% 
  filter(ticker %in% etfs$ticker) 

# Agora, podemos criar a nova variável ano_start_pred
paral <- paral %>% 
  mutate(ano_start_pred = format(date, "%Y") %>% as.integer() + 1) 

netf = paral %>%
  distinct(ticker,ano_start_pred) %>% 
  mutate(c=1) %>% 
  group_by(ano_start_pred) %>% 
  summarise(tot_fundo_ano = sum(c))

rm(etfs,etf_complete,paral)

a = list.files("C:/Users/msses/Desktop/TCC/dados/result",full.names = T)

res = data.frame()
for(i in 1:length(a)){
  
  load(a[i])
  
  res = res %>% bind_rows(model_results)
  
}

rm(model_results)

load( file = "C:/Users/msses/Desktop/TCC/dados/HF/hf_complete.RData")

hf_complete =hf_complete %>% 
  distinct(date)


datas = distinct(res, start_train) %>% 
  mutate(end_train = start_train +  years(1) - days(1))

# Criar uma coluna para contar as observações em cada intervalo
datas <- datas %>%
  mutate(start_train = as.Date(start_train),
         end_train = as.Date(end_train)) %>%
  rowwise() %>%
  mutate(obs = sum(hf_complete$date >= start_train & hf_complete$date <= end_train)) %>%
  ungroup() %>% 
  select(-end_train)

res = res %>% 
  left_join(datas, by = c("start_train"))

res = res %>% 
  mutate(r2_adjusted = 1-(((1-r2)*obs)/(obs-num_var)))


res <- res %>%
  mutate(ano_start_pred = format(as.Date(start_pred), "%Y"))

#tabela com as medias

# Selecionar apenas as colunas desejadas
res_subset <- res %>%
  select(model, ano_start_pred, mse, r2,r2_adjusted, num_var, sharpe_ratio, sortino_ratio, information_ratio, 
         sharpe_ratio_actual, sortino_ratio_actual,mean_error,mae)

# Agrupar por ano e modelo, calcular as médias (ou qualquer estatística desejada)
res_agrupado <- res_subset %>%
  group_by(model, ano_start_pred) %>%
  summarise_all(mean, na.rm = TRUE) %>%
  ungroup()

# Ordenar se necessário
res_agrupado <- res_agrupado %>%
  arrange(ano_start_pred, model)

nfundo = distinct(res,ano_start_pred,fund) %>% 
  mutate(c=1) %>% 
  group_by(ano_start_pred) %>% 
  summarise(tot_fundo_ano = sum(c))

fundos = res_agrupado %>% 
  distinct(sharpe_ratio_actual,sortino_ratio_actual,ano_start_pred) %>% 
  left_join(nfundo, by = c("ano_start_pred"))

netf = netf %>% 
  rename(tot_etf_ano = tot_fundo_ano)

netf = netf %>% 
  mutate(ano_start_pred = as.character(ano_start_pred))

replicantes = res_agrupado %>% 
  left_join(netf) %>% 
  select(model,ano_start_pred,tot_etf_ano,num_var,mse,r2,r2_adjusted,sharpe_ratio,sortino_ratio,information_ratio)

## scatterplot r2 lasso adalasso

# Filtrando os dados para lasso e adalasso
res_lasso <- res %>% filter(model == "lasso")
res_adalasso <- res %>% filter(model == "adalasso")

# Unindo os dados pelo start_pred
res_comparado <- merge(res_lasso, res_adalasso, by = c("start_pred","fund"), suffixes = c("_lasso", "_adalasso"))

p <- ggplot(res_comparado, aes(x = r2_adjusted_lasso, y = r2_adjusted_adalasso)) +
  geom_point(color = "blue", size = 2) +
  labs(
    title = "Comparação de R² Ajustado: Lasso vs Adalasso",
    x = expression(R^2 ~ " Adalasso"),
    y = expression(R^2 ~ " Lasso")
  ) +
  geom_abline(intercept = 0, slope = 1, linetype = "dashed", color = "red") +  # Linha de 45 graus
  scale_x_continuous(limits = c(0, 1), breaks = seq(0, 1, by = 0.1)) +  # Escala do eixo x de 0 a 1
  scale_y_continuous(limits = c(0, 1), breaks = seq(0, 1, by = 0.1)) +  # Escala do eixo y de 0 a 1
  coord_fixed() +  # Mantém a proporção 1:1 entre os eixos x e y
  theme(
    plot.background = element_rect(fill = "white"),  # Fundo branco
    plot.title = element_text(size = 36, face = "bold", hjust = 0.5, color = "black"),  # Título maior e centralizado
    axis.title.x = element_text(size = 28, face = "bold", color = "black"),  # Eixo x em negrito e preto
    axis.title.y = element_text(size = 28, face = "bold", color = "black"),  # Eixo y em negrito e preto
    axis.text.x = element_text(size = 24, angle = 45, hjust = 1, color = "black"),  # Texto do eixo x em preto
    axis.text.y = element_text(size = 24, color = "black")  # Texto do eixo y em preto
  )

# Salvando o gráfico em maior tamanho e alta resolução
ggsave(filename = "C:/Users/msses/Desktop/TCC/dados/grafs/r2ajustado.png", plot = p, width = 28, height = 22, units = "in", dpi = 300)

# Criando o scatterplot para Sharpe Ratio
p1 <- ggplot(res %>% filter(model == "adalasso"), aes(x = sharpe_ratio * 100, y = sharpe_ratio_actual * 100)) +
  geom_point(color = "blue", size = 2) +
  labs(
    title = "Comparação de Sharpe Ratio: Replicante (adaLASSO) vs Fundo",
    x = expression("Sharpe Ratio Replicantes (adaLASSO) (*100)"),
    y = expression("Sharpe Ratio Fundo (*100)")
  ) +
  geom_abline(intercept = 0, slope = 1, linetype = "dashed", color = "red") +  # Linha de 45 graus
  coord_fixed() +  # Mantém a proporção 1:1 entre os eixos x e y
  theme(
    plot.background = element_rect(fill = "white"),  # Fundo branco
    plot.title = element_text(size = 24, hjust = 0.5),  # Tamanho do título e centralização
    axis.text = element_text(size = 20),  # Tamanho do texto nos eixos x e y
    axis.title = element_text(size = 22),  # Tamanho dos títulos dos eixos x e y
    legend.text = element_text(size = 18),  # Tamanho do texto na legenda
    legend.title = element_text(size = 20)  # Tamanho do título da legenda
  )

# Exibindo o gráfico
print(p1)

# Salvando o gráfico com fundo branco e tamanho maior
ggsave(
  filename = "C:/Users/msses/Desktop/TCC/dados/grafs/histogram_sharpe.png",
  plot = p1,
  width = 24, height = 16, units = "in", dpi = 300
)


# Criando o scatterplot para Sortino Ratio
p2 <- ggplot(res %>% filter(model == "adalasso"), aes(x = sortino_ratio * 100, y = sortino_ratio_actual * 100)) +
  geom_point(color = "blue", size = 2) +
  labs(
    title = "Comparação de Sortino Ratio: Replicante (adaLASSO) vs Fundo",
    x = expression("Sortino Ratio Replicantes (adaLASSO) (*100)"),
    y = expression("Sortino Ratio Fundo (*100)")
  ) +
  geom_abline(intercept = 0, slope = 1, linetype = "dashed", color = "red") +  # Linha de 45 graus
  coord_fixed() +  # Mantém a proporção 1:1 entre os eixos x e y
  theme(
    plot.background = element_rect(fill = "white"),  # Fundo branco
    plot.title = element_text(size = 24, hjust = 0.5),  # Tamanho do título e centralização
    axis.text = element_text(size = 20),  # Tamanho do texto nos eixos x e y
    axis.title = element_text(size = 22),  # Tamanho dos títulos dos eixos x e y
    legend.text = element_text(size = 18),  # Tamanho do texto na legenda
    legend.title = element_text(size = 20)  # Tamanho do título da legenda
  )

# Exibindo o gráfico
print(p2)

# Salvando o gráfico com fundo branco e tamanho maior
ggsave(
  filename = "C:/Users/msses/Desktop/TCC/dados/grafs/histogram_sortino.png",
  plot = p2,
  width = 24, height = 16, units = "in", dpi = 300
)


##graf scatterplot mean percentage error
t = res %>% select(model,fund,actual_ret,predicted_returns,r2,start_pred,start_train,mse)

# # Converta as strings em listas de números
# t$actual_ret <- lapply(strsplit(as.character(t$actual_ret), ", "), as.numeric)
# t$predicted_returns <- lapply(strsplit(as.character(t$predicted_returns), ", "), as.numeric)

# Função para expandir as listas em linhas
expand_df <- function(df) {
  df %>%
    # Transformar as listas em tibble
    mutate(row_id = row_number()) %>%  # Adiciona uma coluna de identificação para manter a ordem
    unnest(c(actual_ret, predicted_returns)) %>%
    select(-row_id)
}

# Aplicar a função para expandir o dataframe
t_expanded <- expand_df(t)

# Calculando o MAPE para lasso e adalasso
t_expanded <- t_expanded %>%
  mutate(
    MAPE = abs((actual_ret - predicted_returns) / actual_ret)
  ) %>% 
  group_by(start_pred,fund,model) %>% 
  mutate(c = row_number()) %>% 
  ungroup()

# Verificando as primeiras linhas para confirmar o cálculo
head(t_expanded)

# Filtrando os dados para lasso e adalasso
res_lasso <- t_expanded %>% filter(model == "lasso")
res_adalasso <- t_expanded %>% filter(model == "adalasso")

# Unindo os dados pelo start_pred
res_comparado <- merge(res_lasso, res_adalasso, by = c("start_pred","fund","c"), suffixes = c("_lasso", "_adalasso"))


# Criando o scatterplot com MAPE
ggplot(res_comparado, aes(x = mse_lasso, y = mse_adalasso)) +
  geom_point(color = "blue", size = 3, na.rm = TRUE) +  # Remover NA explicitamente
  labs(
    title = "Comparação de MSE: Lasso vs Adalasso",
    x = "MSE Lasso",
    y = "MSE Adalasso"
  ) +
  theme_minimal() +
  geom_abline(intercept = 0, slope = 1, linetype = "dashed", color = "red") +  # Linha de 45 graus
  expand_limits(x = c(0, 50), y = c(0, 50)) +  # Expande os limites para 50x50
  scale_x_continuous(limits = c(0, 50), breaks = seq(0, 50, by = 10)) +  # Escala do eixo x de 0 a 50
  scale_y_continuous(limits = c(0, 50), breaks = seq(0, 50, by = 10)) +  # Escala do eixo y de 0 a 50
  coord_fixed() +  # Mantém a proporção 1:1 entre os eixos x e y
  theme(
    plot.title = element_text(hjust = 0.5, size = 14),  # Título centralizado e tamanho da fonte
    axis.text = element_text(size = 12),  # Tamanho da fonte dos textos dos eixos
    axis.title = element_text(size = 12)  # Tamanho da fonte dos títulos dos eixos
  )


## grafico temp e retorno replicante e retorno fundo
t = res %>% select(model,fund,actual_ret,predicted_returns,r2,start_pred,start_train)

# # Converta as strings em listas de números
# t$actual_ret <- lapply(strsplit(as.character(t$actual_ret), ", "), as.numeric)
# t$predicted_returns <- lapply(strsplit(as.character(t$predicted_returns), ", "), as.numeric)

# Função para expandir as listas em linhas
expand_df <- function(df) {
  df %>%
    # Transformar as listas em tibble
    mutate(row_id = row_number()) %>%  # Adiciona uma coluna de identificação para manter a ordem
    unnest(c(actual_ret, predicted_returns)) %>%
    select(-row_id)
}

# Aplicar a função para expandir o dataframe
t_expanded <- expand_df(t)





# Função para criar e salvar os gráficos para cada combinação de model e fund
create_and_save_plots <- function(df, save_path) {
  # Obter os modelos e fundos únicos
  unique_models <- unique(df$model)
  unique_funds <- unique(df$fund)
  
  # Verificar se o diretório existe, se não, criar
  if (!dir.exists(save_path)) {
    dir.create(save_path, recursive = TRUE)
  }
  
  # Loop sobre cada combinação de modelo e fundo
  for (modelo in unique_models) {
    for (fundo in unique_funds) {
      # Filtrar o dataframe para o modelo e fundo atuais
      df_filtered <- df %>%
        filter(modelo == model & fund == fundo) %>%
        arrange(start_pred)
      
      # Criar uma coluna de data incremental
      df_filtered <- df_filtered %>%
        #group_by(start_pred) %>%
        mutate(date = row_number()) %>%
        ungroup()
      
      
      # Converte as colunas necessárias para numeric e ajusta a escala para percentual
      df_filtered$actual_ret <- as.numeric(df_filtered$actual_ret)
      df_filtered$predicted_returns <- as.numeric(df_filtered$predicted_returns)
      
      # Cria o gráfico ggplot
      p <- ggplot(df_filtered, aes(x = date)) +
        geom_line(aes(y = actual_ret, color = "Fundo"), alpha = 0.2) +  # Linha mais transparente para actual_ret
        geom_line(aes(y = predicted_returns, color = "Replicante"), alpha = 0.2) +  # Linha mais transparente para predicted_returns
        labs(x = "Data", y = "Retorno(%)", 
             title = paste("Retorno do Replicante vs Fundo", unique(df_filtered$fund), "com o modelo", unique(df_filtered$model))) +
        geom_smooth(aes(y = actual_ret), method = "loess", color = "blue", se = FALSE, span = 0.05) +  # Ajuste do span para 0.3
        geom_smooth(aes(y = predicted_returns), method = "loess", color = "red", se = FALSE, span = 0.05) +  # Ajuste do span para 0.3
        scale_color_manual(values = c("blue", "red"), labels = c("Fundo", "Replicante")) +  # Define as cores e rótulos
        theme_minimal() +  # Estilo minimalista
        theme(
          plot.background = element_rect(fill = "white"),  # Fundo branco
          plot.title = element_text(size = 18, hjust = 0.5),  # Tamanho do título e centralização
          axis.text = element_text(size = 14),  # Tamanho do texto nos eixos x e y
          axis.title = element_text(size = 16),  # Tamanho dos títulos dos eixos x e y
          legend.text = element_text(size = 12),  # Tamanho do texto na legenda
          legend.title = element_text(size = 14)  # Tamanho do título da legenda
        )
      
      # print(p)
      # print(p)
      
      # Salvar o gráfico no diretório especificado
      ggsave(filename = paste0(save_path, "/", modelo, "_", gsub(" ", "_", fundo), ".png"), plot = p, width = 18, height = 10, units = "in", dpi = 300)
    }
  }
}

# Caminho para salvar os gráficos
save_path <- "C:/Users/msses/Desktop/TCC/dados/grafs/returns_replicant_actual"

# Aplicar a função ao dataframe e salvar os gráficos na pasta especificada
create_and_save_plots(t_expanded, save_path)


#criar grafico para janela preditiva

hf_complete = hf_complete %>% 
  filter(fund == "BTG PACTUAL HEDGE FI MULTIMERCADO")

hf_complete <- hf_complete %>%
  filter(date < as.Date("2019-01-02"))


# Filtrar o dataframe para o modelo e fundo atuais
df_filtered <- t_expanded %>%
  filter(model == "adalasso" & fund == "BTG PACTUAL HEDGE FI MULTIMERCADO") %>%
  arrange(start_pred)
df_filtered = df_filtered %>% 
  filter(start_pred < as.Date("2021-12-31"))

# Criar uma coluna de data incremental
df_filtered <- df_filtered %>%
  #group_by(start_pred) %>%
  mutate(date = row_number()) %>%
  ungroup() %>% 
  group_by(start_pred) %>%
  mutate(new_pred = row_number() == 1) %>%
  ungroup()

df_filtered = df_filtered %>% 
  select(ret = actual_ret,
         predicted_returns,
         new_pred)

hf_complete = hf_complete %>% 
  mutate(date = row_number(),
         new_pred = FALSE) %>% 
  select(-fund)

hf_complete = hf_complete %>% 
  mutate(predicted_returns = NA) %>% 
  select(-date)

hf_complete = hf_complete %>% 
  bind_rows(df_filtered) %>% 
  mutate(date = row_number())

hf_complete = hf_complete%>% 
  mutate(date = row_number())


# Calculando médias móveis de 27 dias para ret e predicted_returns
test <- hf_complete %>%
  arrange(date) %>%
  mutate(ret = rollmean(ret, k = 27, fill = NA, align = "right"),
         predicted_returns = rollmean(predicted_returns, k = 27, fill = NA, align = "right"))

test = test %>% 
  filter(!is.na(ret)) %>% 
  mutate(date = row_number())

test = test %>% 
  mutate(predicted_returns = ifelse(date<265,NA,predicted_returns)) %>% 
  rename(actual_ret = ret)

test = test[16:nrow(test),]

test = test %>% 
  mutate(date = row_number())

# Converte as colunas necessárias para numeric e ajusta a escala para percentual
test$actual_ret <- as.numeric(test$actual_ret)
test$predicted_returns <- as.numeric(test$predicted_returns)

test = test[1:350,]

# Identifica as datas onde new_pred é TRUE
dates_new_pred_true <- test %>% filter(new_pred == TRUE) %>% pull(date)

dates_new_pred_true <- dates_new_pred_true[2:length(dates_new_pred_true)]

dates_new_pred_true = dates_new_pred_true[2:length(dates_new_pred_true)]

# Cria o gráfico ggplot
p <- ggplot(test, aes(x = date)) +
  geom_line(aes(y = actual_ret, color = "Fundo"),size = 0.6,alpha=0.6) +  # Linha mais transparente para actual_ret
  geom_line(aes(y = predicted_returns, color = "Replicante"),size = 0.6,alpha=0.6) +  # Linha mais transparente para predicted_returns
  labs(x = "Data", y = "Retorno(%)", 
       title = "Janelas Preditivas (média móvel de 27 dias)") +
  # Adiciona linhas verticais tracejadas onde new_pred é TRUE
  geom_vline(xintercept = dates_new_pred_true, linetype = "dashed", color = "black", size = 0.8) +
  scale_color_manual(values = c("blue", "red"), labels = c("Fundo", "Replicante")) +  # Define as cores e rótulos
  theme_minimal() +  # Estilo minimalista
  theme(
    plot.background = element_rect(fill = "white"),  # Fundo branco
    plot.title = element_text(size = 28, hjust = 0.5),  # Tamanho do título e centralização
    axis.text = element_text(size = 20),  # Tamanho do texto nos eixos x e y
    axis.title = element_text(size = 20),  # Tamanho dos títulos dos eixos x e y
    legend.text = element_text(size = 20),  # Tamanho do texto na legenda
    legend.title = element_text(size = 20)  # Tamanho do título da legenda
  )

print(p)

# Salvar o gráfico com fundo branco
ggsave(filename = "C:/Users/msses/Desktop/TCC/dados/grafs/JANELAS_PRED.png", plot = p, width = 18, height = 10, units = "in", dpi = 300)# #quantis

#graficos retorno acumulado

a = "VERSA TRACKER FI MULTIMERCADO" 
b="ÓRAMA TÁTICO FI MULTIMERCADO"
c="TM3 LONG BIASED FI MULTIMERCADO"

test = t_expanded %>% 
  filter(fund == a,
         model == "adalasso") %>% 
  select(actual_ret,predicted_returns) %>% 
  mutate(actual_ret = (actual_ret/100)+1,
         predicted_returns = (predicted_returns/100)+1) %>% 
  mutate(date = row_number())

test1 = test %>% 
  mutate(acuta_res = as.numeric(0),
         predicted_res= as.numeric(0),
         predicted_returns = as.numeric(predicted_returns),
         actual_ret = as.numeric(actual_ret))

for(i in 1:nrow(test)){ 
  
  if(i == 1){
    test1[i,4] = test1[i,1]*1000
    test1[i,5] = test1[i,2]*1000
  }else{
    test1[i,4] = test1[i,1]*test1[i-1,4]
    test1[i,5] = test1[i,2]*test1[i-1,5]
    
  }
  
}

# Criar o gráfico de linha
p = ggplot(data = test1, aes(x = date)) +
  geom_line(aes(y = acuta_res, color = "Fundo")) +
  geom_line(aes(y = predicted_res, color = "Replicante")) +
  labs(title = "Evolução de Retornos Acumulados (Fundo 1)",
       x = "Data",
       y = "Retornos Acumulados",
       color = "Legenda") +
  theme_minimal()+
  theme(
    plot.background = element_rect(fill = "white"),  # Fundo branco
    plot.title = element_text(size = 36, face = "bold", hjust = 0.5, color = "black"),  # Título maior e centralizado
    axis.title.x = element_text(size = 28, face = "bold", color = "black"),  # Eixo x em negrito e preto
    axis.title.y = element_text(size = 28, face = "bold", color = "black"),  # Eixo y em negrito e preto
    axis.text.x = element_text(size = 24, angle = 45, hjust = 1, color = "black"),  # Texto do eixo x em preto
    axis.text.y = element_text(size = 24, color = "black"),  # Texto do eixo y em preto
    legend.title = element_text(size = 28, face = "bold", color = "black"),  # Título da legenda em negrito e preto
    legend.text = element_text(size = 24, color = "black")  # Texto da legenda em preto  
  )
print(p)

# Salvar o gráfico com fundo branco
ggsave(filename = "C:/Users/msses/Desktop/TCC/dados/grafs/ret_acum_fundo1.png", plot = p, width = 18, height = 10, units = "in", dpi = 300)# #quantis

test = t_expanded %>% 
  filter(fund == b,
         model == "adalasso") %>% 
  select(actual_ret,predicted_returns) %>% 
  mutate(actual_ret = (actual_ret/100)+1,
         predicted_returns = (predicted_returns/100)+1) %>% 
  mutate(date = row_number())

test1 = test %>% 
  mutate(acuta_res = as.numeric(0),
         predicted_res= as.numeric(0),
         predicted_returns = as.numeric(predicted_returns),
         actual_ret = as.numeric(actual_ret))

for(i in 1:nrow(test)){ 
  
  if(i == 1){
    test1[i,4] = test1[i,1]*1000
    test1[i,5] = test1[i,2]*1000
  }else{
    test1[i,4] = test1[i,1]*test1[i-1,4]
    test1[i,5] = test1[i,2]*test1[i-1,5]
    
  }
  
}

# Criar o gráfico de linha
p = ggplot(data = test1, aes(x = date)) +
  geom_line(aes(y = acuta_res, color = "Fundo")) +
  geom_line(aes(y = predicted_res, color = "Replicante")) +
  labs(title = "Evolução de Retornos Acumulados (Fundo 2)",
       x = "Data",
       y = "Retornos Acumulados",
       color = "Legenda") +
  theme_minimal()+
  theme(
    plot.background = element_rect(fill = "white"),  # Fundo branco
    plot.title = element_text(size = 36, face = "bold", hjust = 0.5, color = "black"),  # Título maior e centralizado
    axis.title.x = element_text(size = 28, face = "bold", color = "black"),  # Eixo x em negrito e preto
    axis.title.y = element_text(size = 28, face = "bold", color = "black"),  # Eixo y em negrito e preto
    axis.text.x = element_text(size = 24, angle = 45, hjust = 1, color = "black"),  # Texto do eixo x em preto
    axis.text.y = element_text(size = 24, color = "black"),  # Texto do eixo y em preto
    legend.title = element_text(size = 28, face = "bold", color = "black"),  # Título da legenda em negrito e preto
    legend.text = element_text(size = 24, color = "black")  # Texto da legenda em preto  
  )
print(p)

# Salvar o gráfico com fundo branco
ggsave(filename = "C:/Users/msses/Desktop/TCC/dados/grafs/ret_acum_fundo2.png", plot = p, width = 18, height = 10, units = "in", dpi = 300)# #quantis

test = t_expanded %>% 
  filter(fund == c,
         model == "adalasso") %>% 
  select(actual_ret,predicted_returns) %>% 
  mutate(actual_ret = (actual_ret/100)+1,
         predicted_returns = (predicted_returns/100)+1) %>% 
  mutate(date = row_number())

test1 = test %>% 
  mutate(acuta_res = as.numeric(0),
         predicted_res= as.numeric(0),
         predicted_returns = as.numeric(predicted_returns),
         actual_ret = as.numeric(actual_ret))

for(i in 1:nrow(test)){ 
  
  if(i == 1){
    test1[i,4] = test1[i,1]*1000
    test1[i,5] = test1[i,2]*1000
  }else{
    test1[i,4] = test1[i,1]*test1[i-1,4]
    test1[i,5] = test1[i,2]*test1[i-1,5]
    
  }
  
}

# Criar o gráfico de linha
p = ggplot(data = test1, aes(x = date)) +
  geom_line(aes(y = acuta_res, color = "Fundo")) +
  geom_line(aes(y = predicted_res, color = "Replicante")) +
  labs(title = "Evolução de Retornos Acumulados (Fundo 3)",
       x = "Data",
       y = "Retornos Acumulados",
       color = "Legenda") +
  theme_minimal()+
  theme(
    plot.background = element_rect(fill = "white"),  # Fundo branco
    plot.title = element_text(size = 36, face = "bold", hjust = 0.5, color = "black"),  # Título maior e centralizado
    axis.title.x = element_text(size = 28, face = "bold", color = "black"),  # Eixo x em negrito e preto
    axis.title.y = element_text(size = 28, face = "bold", color = "black"),  # Eixo y em negrito e preto
    axis.text.x = element_text(size = 24, angle = 45, hjust = 1, color = "black"),  # Texto do eixo x em preto
    axis.text.y = element_text(size = 24, color = "black"),  # Texto do eixo y em preto
    legend.title = element_text(size = 28, face = "bold", color = "black"),  # Título da legenda em negrito e preto
    legend.text = element_text(size = 24, color = "black")  # Texto da legenda em preto  
  )
print(p)

# Salvar o gráfico com fundo branco
ggsave(filename = "C:/Users/msses/Desktop/TCC/dados/grafs/ret_acum_fundo3.png", plot = p, width = 18, height = 10, units = "in", dpi = 300)# #quantis

test = t_expanded %>%
  filter(model == "adalasso") %>%
  group_by(fund) %>%
  mutate(date = row_number()) %>%
  ungroup() %>%
  group_by(date) %>%
  summarise(
    predicted_returns = mean(predicted_returns),
    actual_ret = mean(actual_ret)
  ) %>%
  mutate(
    actual_ret = rollmean(
      actual_ret,
      k = 27,
      fill = NA,
      align = "right"
    ),
    predicted_returns = rollmean(
      predicted_returns,
      k = 27,
      fill = NA,
      align = "right"
    )
  ) %>% 
  filter(!is.na(predicted_returns)) %>% 
  mutate(date = row_number())

# Criar o gráfico de linha
p = ggplot(data = test, aes(x = date)) +
  geom_line(aes(y = actual_ret, color = "Fundo")) +
  geom_line(aes(y = predicted_returns, color = "Replicante")) +
  labs(title = "Evolução de Retornos Médios (%) - média móvel 27 dias",
       x = "Data",
       y = "Retornos Acumulados",
       color = "Legenda") +
  theme_minimal()+
  theme(
    plot.background = element_rect(fill = "white"),  # Fundo branco
    plot.title = element_text(size = 36, face = "bold", hjust = 0.5, color = "black"),  # Título maior e centralizado
    axis.title.x = element_text(size = 28, face = "bold", color = "black"),  # Eixo x em negrito e preto
    axis.title.y = element_text(size = 28, face = "bold", color = "black"),  # Eixo y em negrito e preto
    axis.text.x = element_text(size = 24, angle = 45, hjust = 1, color = "black"),  # Texto do eixo x em preto
    axis.text.y = element_text(size = 24, color = "black"),  # Texto do eixo y em preto
    legend.title = element_text(size = 28, face = "bold", color = "black"),  # Título da legenda em negrito e preto
    legend.text = element_text(size = 24, color = "black")  # Texto da legenda em preto  
  )
print(p)

# Salvar o gráfico com fundo branco
ggsave(filename = "C:/Users/msses/Desktop/TCC/dados/grafs/ret_mean.png", plot = p, width = 18, height = 10, units = "in", dpi = 300)# #quantis

#graf retorno medio acumulado

test = t_expanded %>%
  filter(model == "adalasso") %>%
  group_by(fund) %>%
  mutate(date = row_number()) %>%
  ungroup() %>%
  group_by(date) %>%
  summarise(
    predicted_returns = mean(predicted_returns),
    actual_ret = mean(actual_ret)
  )


test1 = test %>% 
  mutate(acuta_res = as.numeric(0),
         predicted_res= as.numeric(0),
         predicted_returns = ((as.numeric(predicted_returns))/100)+1,
         actual_ret = (as.numeric(actual_ret)/100)+1)

for(i in 1:nrow(test)){ 
  
  if(i == 1){
    test1[i,4] = test1[i,3]*1000
    test1[i,5] = test1[i,2]*1000
  }else{
    test1[i,4] = test1[i,3]*test1[i-1,4]
    test1[i,5] = test1[i,2]*test1[i-1,5]
    
  }
  
}

# Criar o gráfico de linha
p = ggplot(data = test1, aes(x = date)) +
  geom_line(aes(y = acuta_res, color = "Fundo")) +
  geom_line(aes(y = predicted_res, color = "Replicante")) +
  labs(title = "Evolução de Retornos Médios Acumulados",
       x = "Data",
       y = "Retornos Acumulados",
       color = "Legenda") +
  theme_minimal()+
  theme(
    plot.background = element_rect(fill = "white"),  # Fundo branco
    plot.title = element_text(size = 36, face = "bold", hjust = 0.5, color = "black"),  # Título maior e centralizado
    axis.title.x = element_text(size = 28, face = "bold", color = "black"),  # Eixo x em negrito e preto
    axis.title.y = element_text(size = 28, face = "bold", color = "black"),  # Eixo y em negrito e preto
    axis.text.x = element_text(size = 24, angle = 45, hjust = 1, color = "black"),  # Texto do eixo x em preto
    axis.text.y = element_text(size = 24, color = "black"),  # Texto do eixo y em preto
    legend.title = element_text(size = 28, face = "bold", color = "black"),  # Título da legenda em negrito e preto
    legend.text = element_text(size = 24, color = "black")  # Texto da legenda em preto  
  )
print(p)

# Salvar o gráfico com fundo branco
ggsave(filename = "C:/Users/msses/Desktop/TCC/dados/grafs/ret_acum_mean.png", plot = p, width = 18, height = 10, units = "in", dpi = 300)# #quantis

