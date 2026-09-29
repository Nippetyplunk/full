    SELECT 
        goods.[PrekesKodas] AS inventcode,
        goods.[PrekesMatas] AS measurecode,
        1 AS isinventitem,
        TRIM(REPLACE(REPLACE(SUBSTRING(CONCAT(goods.[PrekesPavadinimas], ' ', goods.[PrekesKomentaras]), 0, 
             CHARINDEX('[', CONCAT(goods.[PrekesPavadinimas], ' ', goods.[PrekesKomentaras]))), '\', '\\'), CHAR(9), '')) AS name,
        gp_cat.[ItemCategoryId] AS inventgroup,
        CASE WHEN goods.[PrekesMatas] = 1 THEN 0 ELSE 1 END AS requirequantityscales,
        CASE WHEN goods.[N_Type] = '8' THEN 18 ELSE 0 END AS age,
        CASE WHEN goods.[N_Type] = '8' THEN 32 ELSE 0 END AS opmode
    FROM [rdata].[dbo].[Prekes] goods
    INNER JOIN [rdata].[dbo].[MatavimoVienetai] unit ON goods.[PrekesMatas] = unit.[MatoKodas]
    JOIN [GPBO].[dbo].[GP_Items] gp_item ON SUBSTRING(gp_item.[ItemCode], PATINDEX('%[^0]%', gp_item.[ItemCode] + 'a'), LEN(gp_item.[ItemCode])) = goods.[PrekesKodas]
    LEFT JOIN [GPBO].[dbo].[GP_ItemCategories] gp_cat ON gp_item.[ItemCategoryId] = gp_cat.[ItemCategoryId]
    WHERE goods.[Aktyvi] = 1 AND goods.[Dep] = ?;

