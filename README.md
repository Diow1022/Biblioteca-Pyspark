# Biblioteca-Pyspark

PySpark é a API do Apache Spark para Python, desenvolvida para processamento distribuído de grandes volumes de dados. Ela permite trabalhar com grandes conjuntos de dados utilizando Python, aproveitando a capacidade de processamento paralelo do Apache Spark.

Este repositório apresenta os principais conceitos do PySpark, sua instalação, configuração e exemplos de utilização para análise e processamento de dados.

---

## Sobre o PySpark

O PySpark permite executar operações de processamento de dados de forma distribuída, utilizando múltiplos núcleos de processamento ou diferentes computadores em um cluster.

Entre suas principais aplicações estão:

* Processamento de grandes volumes de dados
* Engenharia de dados
* ETL (Extract, Transform, Load)
* Limpeza e transformação de dados
* Análise de dados
* Processamento de arquivos CSV, JSON e Parquet
* Processamento de dados em larga escala
* Machine Learning distribuído
* Integração com bancos de dados e serviços de armazenamento

---

## Tecnologias utilizadas

* Python
* Apache Spark
* PySpark
* Java
* Jupyter Notebook ou VS Code

---

## Instalação

Antes de instalar o PySpark, certifique-se de que o Python está instalado em seu sistema.

Verifique a versão do Python:

```bash
python --version
```

Em algumas distribuições Linux, pode ser necessário utilizar:

```bash
python3 --version
```

### Instalação do PySpark

Utilize o `pip`:

```bash
pip install pyspark
```

Ou:

```bash
pip3 install pyspark
```

Para verificar se a instalação foi realizada corretamente:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

---

## Criando uma SparkSession

A `SparkSession` é o principal ponto de entrada para trabalhar com PySpark.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MeuProjetoPySpark") \
    .getOrCreate()

print("Spark iniciado com sucesso!")
```

Para encerrar a sessão:

```python
spark.stop()
```

---

## Criando um DataFrame

O PySpark utiliza DataFrames para trabalhar com dados estruturados.

Exemplo:

```python
dados = [
    ("Jonatas", 25, "Tecnologia"),
    ("Carlos", 30, "Dados"),
    ("Ana", 28, "Desenvolvimento")
]

colunas = ["Nome", "Idade", "Area"]

df = spark.createDataFrame(dados, colunas)

df.show()
```

Resultado esperado:

```text
+-------+-----+---------------+
|   Nome|Idade|           Area|
+-------+-----+---------------+
|Jonatas|   25|     Tecnologia|
| Carlos|   30|          Dados|
|    Ana|   28|Desenvolvimento|
+-------+-----+---------------+
```

---

## Visualizando os dados

Para visualizar um DataFrame:

```python
df.show()
```

Para visualizar uma quantidade específica de registros:

```python
df.show(5)
```

Para visualizar o esquema dos dados:

```python
df.printSchema()
```

Para obter informações sobre as colunas:

```python
print(df.columns)
```

---

## Selecionando colunas

Podemos selecionar uma ou mais colunas utilizando `select()`.

```python
df.select("Nome", "Area").show()
```

Também é possível utilizar:

```python
df.select(df.Nome, df.Idade).show()
```

---

## Filtrando dados

O método `filter()` permite selecionar registros de acordo com determinadas condições.

```python
df.filter(df.Idade >= 28).show()
```

Também podemos utilizar `where()`:

```python
df.where(df.Idade >= 28).show()
```

---

## Criando novas colunas

Para criar uma nova coluna, utilizamos `withColumn()`.

```python
from pyspark.sql.functions import col

df = df.withColumn(
    "Idade_Dobrada",
    col("Idade") * 2
)

df.show()
```

---

## Renomeando colunas

Utilize `withColumnRenamed()`:

```python
df = df.withColumnRenamed(
    "Area",
    "Departamento"
)
```

---

## Removendo colunas

Para remover uma coluna:

```python
df = df.drop("Idade_Dobrada")
```

---

## Ordenando dados

O método `orderBy()` permite ordenar os registros.

```python
df.orderBy("Idade").show()
```

Para ordenar de forma decrescente:

```python
from pyspark.sql.functions import desc

df.orderBy(desc("Idade")).show()
```

---

## Trabalhando com arquivos CSV

O PySpark permite carregar arquivos CSV diretamente.

```python
df = spark.read.csv(
    "dados.csv",
    header=True,
    inferSchema=True
)

df.show()
```

### Parâmetros utilizados

* `header=True`: utiliza a primeira linha como nome das colunas.
* `inferSchema=True`: tenta identificar automaticamente os tipos dos dados.

---

## Salvando dados em CSV

Para salvar um DataFrame:

```python
df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("resultado")
```

O Spark normalmente cria uma pasta contendo os arquivos de saída e informações relacionadas ao processamento.

---

## Trabalhando com JSON

Leitura:

```python
df = spark.read.json("dados.json")

df.show()
```

Escrita:

```python
df.write \
    .mode("overwrite") \
    .json("resultado_json")
```

---

## Trabalhando com Parquet

O formato Parquet é muito utilizado em ambientes de engenharia de dados por ser otimizado para armazenamento e processamento analítico.

### Leitura

```python
df = spark.read.parquet("dados.parquet")
```

### Escrita

```python
df.write \
    .mode("overwrite") \
    .parquet("resultado_parquet")
```

---

## Operações de agregação

O PySpark permite realizar operações como soma, média, mínimo, máximo e contagem.

```python
from pyspark.sql.functions import avg, sum, max, min, count

df.select(
    avg("Idade").alias("Media"),
    max("Idade").alias("Maior_Idade"),
    min("Idade").alias("Menor_Idade"),
    count("Nome").alias("Quantidade")
).show()
```

---

## Agrupamento de dados

O método `groupBy()` permite agrupar informações.

```python
df.groupBy("Area").count().show()
```

Também podemos realizar agregações:

```python
df.groupBy("Area") \
    .agg(avg("Idade").alias("Media_Idade")) \
    .show()
```

---

## SQL com PySpark

O PySpark também permite utilizar SQL para consultar DataFrames.

Primeiro, registre o DataFrame como uma tabela temporária:

```python
df.createOrReplaceTempView("funcionarios")
```

Depois, execute uma consulta SQL:

```python
resultado = spark.sql("""
    SELECT Nome, Idade, Area
    FROM funcionarios
    WHERE Idade >= 28
""")

resultado.show()
```

Essa abordagem é bastante útil para profissionais que já possuem conhecimento em SQL.

---

## Exemplo completo

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .appName("ExemploPySpark") \
    .getOrCreate()

dados = [
    ("Jonatas", 25, "Tecnologia"),
    ("Carlos", 30, "Dados"),
    ("Ana", 28, "Desenvolvimento"),
    ("Maria", 32, "Dados")
]

colunas = ["Nome", "Idade", "Area"]

df = spark.createDataFrame(dados, colunas)

print("Dados originais:")
df.show()

print("Pessoas com idade igual ou superior a 28:")
df.filter(df.Idade >= 28).show()

print("Média de idade por área:")
df.groupBy("Area") \
    .agg(avg("Idade").alias("Media_Idade")) \
    .show()

spark.stop()
```

---

## Estrutura recomendada do projeto

Uma estrutura simples para um projeto PySpark pode ser:

```text
projeto-pyspark/
│
├── data/
│   ├── entrada/
│   └── saida/
│
├── src/
│   └── main.py
│
├── notebooks/
│   └── analise.ipynb
│
├── requirements.txt
│
├── .gitignore
│
└── README.md
```

---

## requirements.txt

Para registrar as dependências do projeto:

```text
pyspark
```

A instalação pode ser realizada com:

```bash
pip install -r requirements.txt
```

---

## Boas práticas

Ao desenvolver projetos utilizando PySpark, algumas boas práticas são:

1. Utilizar `SparkSession` como ponto de entrada da aplicação.
2. Evitar utilizar `collect()` em grandes volumes de dados.
3. Preferir operações distribuídas.
4. Utilizar o formato Parquet quando apropriado.
5. Evitar carregar grandes conjuntos de dados para a memória local.
6. Separar os dados de entrada, processamento e saída.
7. Manter as dependências do projeto no `requirements.txt`.
8. Utilizar ambientes virtuais para isolar as dependências.
9. Criar transformações claras e reutilizáveis.
10. Monitorar o desempenho das aplicações Spark.

---

## PySpark e Engenharia de Dados

O PySpark é uma ferramenta importante dentro do ecossistema de Engenharia de Dados.

Um fluxo comum pode ser:

```text
Fonte de dados
      |
      v
  Extração
      |
      v
   PySpark
      |
      +------> Limpeza
      |
      +------> Transformação
      |
      +------> Agregação
      |
      v
 Armazenamento
      |
      v
   Analytics
```

Esse tipo de arquitetura permite processar grandes quantidades de informações de maneira distribuída.

---

## Principais módulos

Alguns dos módulos mais utilizados do PySpark são:

### pyspark.sql

Utilizado para trabalhar com DataFrames e SQL.

```python
from pyspark.sql import SparkSession
```

### pyspark.sql.functions

Disponibiliza diversas funções para transformação e análise de dados.

```python
from pyspark.sql.functions import col, avg, sum, count
```

### pyspark.ml

Disponibiliza ferramentas para Machine Learning.

```python
from pyspark.ml.feature import VectorAssembler
```

### pyspark.streaming

Utilizado para processamento de dados em fluxo.

---

## Objetivo deste repositório

Este projeto tem como objetivo servir como material de estudo e referência para utilização do PySpark com Python, apresentando desde conceitos básicos até operações comuns de processamento e análise de dados.

---

## Autor

**Jonatas Nascimento**

Estudante de Tecnologia com interesse em:

* Python
* Engenharia de Dados
* Banco de Dados
* Automação
* Inteligência Artificial
* Desenvolvimento de Sistemas

---

## Licença

Este projeto pode ser utilizado para fins educacionais e de estudo.
