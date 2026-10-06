from py4j.protocol import CONSTRUCTOR_COMMAND_NAME
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,desc,asc,when

spark = SparkSession.builder\
 .appName('Gestao_De_cidade')\
 .getOrCreate()

dados_cidadaos = [
     (1,"Ana Silva",28,10),
     (2,"João Bento",45,10),
     (3,"Maria Luz",19,20),
     (4,"Calos Vaz",35,20),
     (5,"Sofia Rosa",22,20),
]

dados_bairro = [
     (10,"Centro Historico",5000),
     (20,"Zona Rieirinha",3500),
     (30,"parque das Nações",7000),
     (40,"Bairro Alto",2000),
]

cidadaos = spark.createDataFrame(dados_cidadaos,["id_cicadao","nome","idade", "id_bairro"])
bairro = spark.createDataFrame(dados_bairro,["id_bairro","nome_bairro","consumo"])

print("registro")
cidadaos.show()
bairro.show()
cidadaos.filter(col("idade") >30).select("nome", "idade").show()
cidadaos.select("nome", "idade").show()
bairro.orderBy("id_bairro").show()

cidadaos_classificados = cidadaos.withColumn(
    "faixa_etaria",
    when(col("idade")<12, "Criança")
    .when(col("idade")<20, "Jovem")
    .when(col("idade")<40, "Adulto")
    .otherwise ("Terceira Idade")

)
cidadaos_classificados.show()

cidadaos.createOrReplaceTempView("tb_cidadaos")
bairro.createOrReplaceTempView("tb_Bairros")

resultado_sql = spark.sql("""
  SELECT b.nome_bairro, COUNT(c.id_cidadaos)AS qtd_cidadaos, b.cor
  FROM tb_Bairro b
  LEFT JOIN tb_cidadaos c ON b.id_bairro = c.id_bairro
  GROUP BY b.nome_bairro, b.CONSTRUCTOR_COMMAND_NAME
  ORDER BY b.consumo DESC                       
  """)

resultado_sql.show()
    
