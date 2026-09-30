-- Запрос получения доп цен
SELECT DISTINCT
    TRIM(bar.[BarCode]) AS barcode,
    REPLACE(CAST(goods.[PrekesKaina2] AS VARCHAR(20)), ',', '.') AS additional_price,
    goods.Laikas AS effectivedate
FROM [rdata].[dbo].[BarKodai] bar
JOIN [rdata].[dbo].[Prekes] goods ON bar.[PrekesKodas] = goods.[PrekesKodas]
WHERE goods.[Aktyvi] = 1 AND bar.[Dep] = ? AND goods.[PrekesKaina2] > 0;