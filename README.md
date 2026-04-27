# Inteligência de escolha de telefones no WhatsApp

## Objetivo

Este repositório entrega uma solução reproduzível para criar a Inteligência de Escolha no motor de disparos de WhatsApp.

O problema é a multiplicidade de telefones por cidadão. Um mesmo CPF pode ter vários números com origens e datas de atualização diferentes. A decisão de para quais números enviar uma mensagem impacta custo e efetividade.

A solução aqui produz

1. Métricas de qualidade por sistema de origem com intervalos de confiança
2. Uma análise de decaimento de qualidade por idade do cadastro no sistema
3. Um ranking de confiabilidade por sistema com suavização estatística para reduzir instabilidade
4. Um algoritmo de scoring que escolhe automaticamente os dois melhores telefones por CPF
5. Um desenho completo de experimento A B para validação em produção

## Dados e modelo mental

Tabelas usadas

1. `base_disparo_mascarado` contém o histórico real de disparos e o `status_disparo`
2. `dim_telefone_mascarado` contém metadados de telefone e a lista `telefone_aparicoes` com uma aparição por sistema de origem, incluindo `id_sistema` e `registro_data_atualizacao`

Chave de join

`base_disparo_mascarado.contato_telefone` com `dim_telefone_mascarado.telefone_numero`

Métrica alvo

Entrega é aproximada por `status_disparo in {delivered, read}`. Nesta base, os valores estão em minúsculo e `read` implica que a mensagem foi entregue.

Viés de seleção

Alguns sistemas aparecem mais no log porque já são priorizados. O repositório evita conclusões ingênuas de duas formas

1. Reporta taxa com volume e intervalo de confiança
2. Recalcula a comparação em um subconjunto mais comparável, telefones que aparecem em dois ou mais sistemas

## Arquitetura do repositório

Diretórios

1. `data/raw` contém os arquivos Parquet baixados do GCS e o `schema.yml`
2. `data/processed` é reservado para artefatos derivados, se necessário
3. `notebooks` contém o Notebook principal com a análise ponta a ponta
4. `src` contém o código reutilizável, com separação clara entre IO, métricas e scoring
5. `scripts` contém automações auxiliares, como download

Código fonte em `src`

1. `src/config.py` resolve caminhos do projeto de forma consistente
2. `src/io.py` localiza e carrega os Parquets em `data/raw`
3. `src/metrics.py` centraliza a definição de sucesso e cálculo de taxas com intervalo de confiança
4. `src/scoring.py` implementa o ranking de sistemas e o score por telefone para seleção por CPF

Notebook principal

`notebooks/01_inteligencia_escolha_whatsapp.ipynb`

Este Notebook executa

1. Leitura das tabelas
2. Desnormalização de `telefone_aparicoes` para obter uma linha por telefone por sistema
3. Join com a base de disparos
4. Escolha de um sistema representativo por disparo baseado em maior `registro_data_atualizacao` consistente com a data do envio
5. Taxas de entrega por sistema e análise de viés
6. Decaimento por idade do dado e estimação de meia vida
7. Ranking de sistemas e score final por telefone
8. Desenho do experimento A B

## Como executar no Windows

### Passo 1 baixar os dados do Google Cloud Storage

Recomendado instalar o Google Cloud CLI porque ele copia recursivamente

1. Instale

```
winget install -e --id Google.CloudSDK
```

2. Autentique sua conta

```
gcloud auth login
```

3. Copie o bucket para `data/raw`

```
mkdir data\raw
gcloud storage cp -r gs://case_vagas/whatsapp/* data/raw/
```

Resultado esperado em `data/raw`

1. `base_disparo_mascarado`
2. `dim_telefone_mascarado`
3. `schema.yml`

### Passo 2 criar ambiente e instalar dependências

Por conta de caminhos longos no Windows, prefira rodar este repositório em um caminho curto. Exemplo `C:\case-whatsapp`.

Na raiz do repositório

```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

### Passo 3 abrir o Notebook

Execute a partir da raiz do repositório para o Python conseguir importar `src`

```
python -m jupyter lab
```

Abra o arquivo `notebooks/01_inteligencia_escolha_whatsapp.ipynb` e rode todas as células em ordem.

## Como validar que está tudo certo

Checklist no Jupyter

1. No Launcher selecione o kernel que corresponde ao seu ambiente virtual, normalmente aparece como Python com venv
2. No Notebook, rode as duas primeiras células e verifique que `paths.data_raw` aponta para a pasta correta
3. Rode a célula de leitura de schema e confirme que não há erro
4. Execute o Notebook inteiro e confirme que você obteve
   1. Tabela com taxa de DELIVERED por `id_sistema_mask` e colunas `n` e intervalo de confiança
   2. Gráfico de decaimento por faixa de idade
   3. Tabela `system_rank` com `score` ordenado
   4. Exemplo de seleção de telefones para um CPF com `final_score` e `rank`
   5. Cálculo de tamanho de amostra para o teste

Se algum passo falhar, a causa mais comum é o kernel errado ou o Jupyter sendo executado fora da raiz do repositório.

## Detalhes metodológicos

### Taxa por sistema

Para cada sistema calculo

1. \(n\) tentativas
2. \(s\) entregas
3. \(p = s / n\) taxa observada

Reporto intervalo de confiança de Wilson porque é mais estável do que aproximações normais quando a proporção está perto de zero ou um e quando o volume é moderado.

### Decaimento por atualidade

Defino a idade do dado no momento do envio em dias

\(age\_days = envio\_data - registro\_data\_atualizacao\)

Modelo uma regressão logística simples de `delivered` em função de `age_days` e estimo uma meia vida em odds.

### Ranking de sistemas

Para reduzir instabilidade em sistemas com pouco volume, estimo uma taxa suavizada via média a posteriori Beta

\(\hat p = (s + \alpha) / (n + \alpha + \beta)\)

Depois aplico regularização por volume para evitar que sistemas com poucas tentativas dominem o topo do ranking.

### Algoritmo de escolha por CPF

O score final por telefone combina

1. Score do sistema de origem do telefone
2. Peso de recência com decaimento exponencial usando meia vida
3. Ajuste opcional por DDD com base em desempenho histórico agregado

A decisão é escolher os dois telefones com maior `final_score`.

## Experimento A B

Resumo do desenho recomendado

1. Unidade de randomização CPF
2. Controle escolhe dois telefones aleatórios elegíveis
3. Tratamento escolhe dois telefones pelo `final_score`
4. Métrica primária taxa de DELIVERED por CPF, ao menos uma entrega em duas tentativas
5. Métricas secundárias taxa por tentativa e custo por entrega

O Notebook calcula um tamanho de amostra inicial para um lift relativo configurável.

## Resultados esperados

Esta seção serve como checklist final para revisão e para orientar outra pessoa que abra o repositório pela primeira vez.

### Prints principais que devem aparecer ao executar o Notebook

1. Schemas das tabelas sem erro
   1. `base_disparo_mascarado` com colunas incluindo `contato_telefone`, `envio_datahora`, `status_disparo`
   2. `dim_telefone_mascarado` com `telefone_aparicoes` como lista de structs contendo `id_sistema` e `registro_data_atualizacao`

2. Tabela de performance por sistema
   1. DataFrame `rates_system` com colunas `id_sistema_mask`, `n`, `rate`, `ci_low`, `ci_high`
   2. Interpretação esperada sistemas com maior taxa e intervalo de confiança mais estreito tendem a ser fontes mais quentes

3. Decaimento por idade do cadastro
   1. DataFrame `rates_age` com `age_bucket`, `n`, `rate` e intervalo de confiança
   2. Gráfico de taxa por faixa de idade sem erros de barras negativas

4. Ranking de sistemas
   1. DataFrame `system_rank` com colunas `id_sistema_mask`, `n`, `successes`, `posterior_mean` e `score`
   2. Ordenação por `score` em ordem decrescente

5. Seleção automática de telefones por CPF
   1. DataFrame `scored` com colunas `telefone`, `id_sistema_mask`, `registro_data_atualizacao`, `final_score`, `rank`
   2. A checagem `scored.filter(rank <= 2)` retorna exatamente 2 linhas e `n_unique(telefone) == 2`

6. Experimento A B
   1. Célula de amostragem retorna `(baseline_p, p_t, n_per_group)` com `baseline_p` entre 0 e 1 e `n_per_group` maior que 0
   2. Exemplo visto durante execução `baseline_p ≈ 0.9085`, `p_t = baseline_p + 0.01`, `n_per_group ≈ 9769`
