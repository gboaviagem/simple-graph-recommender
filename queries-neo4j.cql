"""
// ===================================================================
// Queries no Neo4j

200 arestas
10 emails
50 produtos

Arquivos:
neo4j_node_list-Usuario.csv - email, uf
neo4j_node_list-Produto.csv - produto_id
neo4j_edge_list-COMPROU.csv - email, produto_id, valor_pago, quando

No Neo4j Desktop, deve-se adicionar os arquivos CSV ao ambiente e depois copiar a URL, para o carregamento.

"""

// Carregando o grafo no Neo4j

LOAD CSV WITH HEADERS FROM "http://localhost:11001/project-8fefc2db-f3a3-4b5a-b9c7-e2fdf340b852/neo4j_node_list-Usuario.csv" as row
MERGE (n :Usuario {email: row.email})
ON CREATE SET n.uf = row.uf
RETURN count(n);
CREATE CONSTRAINT ON (n:Usuario) ASSERT n.produto_id IS UNIQUE;

LOAD CSV WITH HEADERS FROM "http://localhost:11001/project-8fefc2db-f3a3-4b5a-b9c7-e2fdf340b852/neo4j_node_list-Produto.csv" as row
MERGE (n :Produto {produto_id: row.produto_id})
RETURN count(n);
CREATE CONSTRAINT ON (n:Produto) ASSERT n.produto_id IS UNIQUE;

LOAD CSV WITH HEADERS FROM "http://localhost:11001/project-8fefc2db-f3a3-4b5a-b9c7-e2fdf340b852/neo4j_edge_list-COMPROU.csv" as row
MATCH (n:Usuario {email: row.email})
MATCH (m:Produto {produto_id: row.produto_id})
MERGE (n)-[r:COMPROU {quando: datetime(row.quando_neo4j)}]->(m)
ON CREATE SET r.valor_pago = toFloat(row.valor_pago)
RETURN count(r);

// ------------------------------------------------------------------
// TOP COMPRADORES
MATCH (n :Usuario) -[r]-> (p :Produto)
RETURN
    n.email as user_email,
    count(p) as qtd_itens,
    collect(p.produto_id) as quais_itens
ORDER BY qtd_itens DESC

// ------------------------------------------------------------------
// TOP PRODUTOS
MATCH (n :Usuario) -[r :COMPROU]-> (p :Produto)
RETURN
    p.produto_id as produto,
    avg(r.valor_pago) as preco_medio,
    count(n) as qtd_compradores,
    collect(n.email) as quais_compradores
ORDER BY qtd_compradores DESC


// ==================================================================
// Recomendação em grão usuário
// Dado um usuário A, quais os produtos comprados por usuários B que compraram os mesmos produtos que A?
// Para o usuário user003@gmail.com
MATCH (u1 :Usuario) --> (p1 :Produto) <-- (u2 :Usuario) --> (p2 :Produto)
WHERE
    u1.email = "user003@gmail.com"
    AND
    u1.email <> u2.email
    AND
    p1.produto_id <> p2.produto_id
RETURN u1, p1, u2, p2
