-- Запрос для получения штри-кодов товаров и признака маркировки 
WITH PreparedData AS (
    SELECT 
        goods.[PrekesKodas] AS code,
        TRIM(bar.[BarCode]) AS barcode,
        -- Очистка наименования выполняется один раз
        TRIM(REPLACE(REPLACE(SUBSTRING(CONCAT(goods.[PrekesPavadinimas], ' ', goods.[PrekesKomentaras]), 0, 
             CHARINDEX('[', CONCAT(goods.[PrekesPavadinimas], ' ', goods.[PrekesKomentaras]))), '\', '\\'), CHAR(9), '')) AS name,
        goods.[PrekesMatas] AS measure,
        CASE WHEN goods.[N_Type] = '8' THEN 7 ELSE 0 END AS tmctype,
        -- Флаг совпадения отдела товара с целевым отделом штрихкода
        CASE WHEN goods.[Dep] = bar.[Dep] THEN 1 ELSE 0 END AS is_main_dep
    FROM [rdata].[dbo].[BarKodai] bar
    INNER JOIN [rdata].[dbo].[Prekes] goods 
        ON bar.[PrekesKodas] = goods.[PrekesKodas]
    WHERE goods.[Aktyvi] = 1 
      AND goods.[Dep] = ?
)
SELECT 
    code,
    barcode,
    -- Приоритет наименования из целевого отдела, иначе из любого доступного
    ISNULL(MAX(CASE WHEN is_main_dep = 1 THEN name END), MAX(name)) AS name,
    -- Приоритет единицы измерения из целевого отдела
    ISNULL(MAX(CASE WHEN is_main_dep = 1 THEN measure END), MAX(measure)) AS measure,
    -- Если хотя бы в одном отделе стоит маркировка (7), MAX вернет 7
    MAX(tmctype) AS tmctype,
    1.0 AS quantdefault
FROM PreparedData
GROUP BY code, barcode;

