    SELECT DISTINCT
        TRIM(bar.[BarCode]) AS barcode,
        REPLACE(CAST(goods.[PrekesKaina] AS VARCHAR(20)), ',', '.') AS price,
        goods.Laikas AS effectivedate
    FROM [rdata].[dbo].[BarKodai] bar
    INNER JOIN [rdata].[dbo].[Prekes] goods ON bar.[PrekesKodas] = goods.[PrekesKodas] AND bar.[Dep] = goods.[Dep]
    WHERE goods.[Aktyvi] = 1 AND bar.[Dep] = ? AND goods.[PrekesKaina] > 0;