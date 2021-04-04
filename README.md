# simple-graph-recommender

- [PT-BR] Implementação da recomendação de produtos "quem comprou X também comprou Y" usando grafos, tanto em Spark GraphFrames como Neo4j.

- [EN] Implementation of the simple strategy for product recommendation using graphs: "people also bought this". The same result was obtained using Spark GraphFrames and Neo4j.

## Ambiente

Eu rodei meu código localmente com Neo4j Desktop 4.2 e, para o Spark GraphFrames, utilizei um container docker com a imagem [jupyter/pyspark-notebook](https://hub.docker.com/r/jupyter/pyspark-notebook). É necessário informar ao docker que a porta 8888 do container deve se conectar à porta 8888 local, através da flag -p,

```sh
sudo docker run -it -p 8888:8888 jupyter/pyspark-notebook
```

e, ao entrar no Jupyter Notebook (que é executado imediatamente), é preciso atualizar uma variável de ambiente, para indicar ao Spark que usaremos o GraphFrames:

```python
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 pyspark-shell’
```

Para quem deseja rodar seus serviços na AWS, o Neo4j Community Edition pode ser instalado gratuitamente (tirando o preço da máquina) numa instância EC2. O Spark GraphFrames pode ser naturalmente instalado num cluster EMR, para aproveitar a paralelização.