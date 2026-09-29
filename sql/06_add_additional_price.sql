    SELECT DISTINCT
        TRIM(bar.[BarCode]) AS barcode,
        REPLACE(CAST(goods.[PrekesKaina2] AS VARCHAR(20)), ',', '.') AS additional_price
    FROM [rdata].[dbo].[BarKodai] bar
    INNER JOIN [rdata].[dbo].[Prekes] goods ON bar.[PrekesKodas] = goods.[PrekesKodas] AND bar.[Dep] = goods.[Dep]
    WHERE goods.[Aktyvi] = 1 AND bar.[Dep] = ? AND goods.[PrekesKaina2] > 0;