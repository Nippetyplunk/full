-- ============================================================================
-- ИЗВЛЕЧЕНИЕ ТОВАРОВ ИЗ MS SQL ДЛЯ ЗАПОЛНЕНИЯ ЛОКАЛЬНОЙ БД
-- ============================================================================
SELECT 
    goods.[PrekesKodas] AS inventcode,
    goods.[PrekesMatas] AS measurecode,
    TRIM(REPLACE(REPLACE(
        SUBSTRING(
            CONCAT(goods.[PrekesPavadinimas], ' ', goods.[PrekesKomentaras]), 0, 
            CHARINDEX('[', CONCAT(goods.[PrekesPavadinimas], ' ', goods.[PrekesKomentaras]))
        ), '\', '\\'), CHAR(9), '')) AS name,
    gp_cat.[ItemCategoryId] AS inventgroup
FROM [rdata].[dbo].[Prekes] goods
INNER JOIN [GPBO].[dbo].[GP_Items] gp_item 
    ON SUBSTRING(gp_item.[ItemCode], PATINDEX('%[^0]%', gp_item.[ItemCode] + 'a'), LEN(gp_item.[ItemCode])) = goods.[PrekesKodas]
LEFT JOIN [GPBO].[dbo].[GP_ItemCategories] gp_cat 
    ON gp_item.[ItemCategoryId] = gp_cat.[ItemCategoryId]
WHERE goods.[Aktyvi] = 1 
  AND goods.[Dep] = ?;

