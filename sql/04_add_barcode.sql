    SELECT DISTINCT
        goods.[PrekesKodas] AS code,
        TRIM(bar.[BarCode]) AS barcode,
        TRIM(REPLACE(REPLACE(SUBSTRING(CONCAT(goods.[PrekesPavadinimas], ' ', goods.[PrekesKomentaras]), 0, 
             CHARINDEX('[', CONCAT(goods.[PrekesPavadinimas], ' ', goods.[PrekesKomentaras]))), '\', '\\'), CHAR(9), '')) AS name,
        goods.[PrekesMatas] AS measure,
        CASE WHEN goods.[N_Type] = '8' THEN 7 ELSE 0 END AS tmctype,
        1.0 AS quantdefault
    FROM [rdata].[dbo].[BarKodai] bar
    INNER JOIN [rdata].[dbo].[Prekes] goods ON bar.[PrekesKodas] = goods.[PrekesKodas] AND bar.[Dep] = goods.[Dep]
    WHERE goods.[Aktyvi] = 1 AND bar.[Dep] = ?;