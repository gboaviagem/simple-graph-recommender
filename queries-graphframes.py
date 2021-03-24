# Recomendação de produtos usando Spark GraphFrames
# =================================================

# %%
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 pyspark-shell'

# from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
spark = SparkSession.builder.master("local[1]").appName("gboaviagemApp").getOrCreate()


# %%
sqlc = SQLContext(spark.sparkContext)

# %% [markdown]
# Uma vez que este notebook foi executado dentro de um container, rodando a imagem `jupyter/pyspark-notebook`, o path estará diferente. Para enviar o arquivo da máquina host para o sistema de arquivos do container, basta usar o comando `sudo docker cp`.

# %%
wdir = "/home/jovyan/work/neo4jdata-200/"

n_users = spark.read.csv(wdir + "neo4j_node_list-Usuario.csv", header=True, inferSchema=True)
n_prods = spark.read.csv(wdir + "neo4j_node_list-Produto.csv", header=True, inferSchema=True)
e_comprou = spark.read.csv(wdir + "neo4j_edge_list-COMPROU.csv", header=True, inferSchema=True)


# %%
n_users.show()


# %%
n_users.printSchema()


# %%
n_prods.printSchema()


# %%
e_comprou.printSchema()

# %% [markdown]
# ## Dando casting da coluna "quando" de `str` para `timestamp`
# 
# Isso poderia ser importante, caso fosse preciso alguma query explorando a natureza temporal das compras. Para a estratégia simples de recomendação construída aqui, isso não era necessário.

# %%
from pyspark.sql import functions as F

e_comprou = e_comprou.withColumn('quando', F.current_timestamp())


# %%
e_comprou.printSchema()

# %% [markdown]
# ## Criando o GraphFrame

# %%
nodes1 = n_users.    drop_duplicates(subset=['email']).    withColumnRenamed('email', 'id').    withColumn('tipo', F.lit('Usuario'))

nodes2 = n_prods.    drop_duplicates().    withColumnRenamed('produto_id', 'id').    withColumn('tipo', F.lit('Produto'))


# %%
nodes1.count()


# %%
nodes2.count()


# %%
nodes = nodes1.join(nodes2, ['id', 'tipo'], 'outer')


# %%
nodes.printSchema()


# %%
nodes.limit(5).show()


# %%
e_comprou.count()


# %%
e_comprou.printSchema()


# %%
edges = e_comprou.    withColumnRenamed('email', 'src').    withColumnRenamed('produto_id', 'dst').    withColumn('tipo', F.lit('COMPROU'))


# %%
edges.show()


# %%
from graphframes import GraphFrame

g = GraphFrame(nodes, edges)


# %%
q = g.find("(usuario)-[]->(prod)").withColumn('usuario', F.col('usuario').id)


# %%
# Top compradores
# q.groupBy('user').count().sort(F.col('count').desc(), 'user').show()
q = q.groupBy('usuario')    .agg(F.count('prod').alias('qtd_produtos'))    .sort(F.col('qtd_produtos').desc(), 'usuario')


# %%
q.show()


# %%
q.write.csv("top_compradores")

# %% [markdown]
# Reference: https://sparkbyexamples.com/pyspark/pyspark-groupby-explained-with-example/

# %%
# Top produtos
q = g.find("(u)-[e]->(produto)") #.withColumn('user', F.col('user').id)

# %% [markdown]
# Para filtrar, bastaria adicionar antes do `show`
# 
# ```python
# where(F.col('qtd_compradores') > 10)
# ```

# %%
q = q.withColumn('valor_pago', F.col('e').valor_pago)    .groupBy("produto")    .agg(
        F.count("u").alias("qtd_compradores"),
        F.avg("valor_pago").alias("preco_medio"))\
    .withColumn('produto', F.col('produto').id)\
    .sort(F.col('qtd_compradores').desc(), 'produto')
    #.write.csv("top_produtos")


# %%
q.show()

# %% [markdown]
# ### Recomendação em grão usuário (offline)
# 
# Dado um usuário A, quais os produtos comprados por usuários B que compraram os mesmos produtos que A?

# %%
q = g.find("(u1)-[]->(p1); (u2)-[]->(p1); (u2)-[]->(p2)")    .withColumn('u1', F.col('u1').id)    .withColumn('u2', F.col('u2').id)    .withColumn('p1', F.col('p1').id)    .withColumn('p2', F.col('p2').id)    .where(
        (F.col('u1') == 'user003@gmail.com') &
        (F.col('u1') != F.col('u2')) &
        (F.col('p1') != F.col('p2')))
    

# More complex queries can be expressed by applying filters.
# motifs.filter("b.age > 30").show()


# %%
q.limit(5).show()


# %%
q.write.csv('graph_recomendacao_usuario003.csv')


# %%



