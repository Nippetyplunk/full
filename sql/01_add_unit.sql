-- Запрос для поулчения едениц измерения
SELECT 
    unit.[MatoKodas] AS unitcode,
    TRIM(REPLACE(REPLACE(REPLACE(unit.[MatoPavad], CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS name,
    CASE WHEN ISNULL(unit.[MatoTikslumas], 0) > 0 THEN 1 ELSE 0 END AS fractional
FROM [rdata].[dbo].[MatavimoVienetai] unit
WHERE unit.[MatoKodas] IS NOT NULL;