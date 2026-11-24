-- Preço de venda por UF e produto em out/2026 (partição publicada pelo DAG anp_precos_mensal).
-- Rodar no workgroup aws-airflow-orquestracao-anp (limite de 100 MB por consulta).
-- O filtro por ano/mes usa as partições: só o Parquet do mês é lido.
SELECT
    uf,
    produto,
    ROUND(AVG(valor_venda), 2) AS preco_medio,
    MIN(valor_venda)           AS preco_minimo,
    MAX(valor_venda)           AS preco_maximo,
    COUNT(*)                   AS coletas
FROM anp_combustiveis.precos_gasolina_etanol
WHERE ano = '2026'
  AND mes = '10'
GROUP BY uf, produto
ORDER BY produto, preco_medio DESC;
