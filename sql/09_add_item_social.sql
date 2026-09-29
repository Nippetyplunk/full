--- Запрос товаров с признаком SOCIAL
SELECT SUBSTRING([MATNR], PATINDEX('%[^0]%', [MATNR] + 'a'), LEN([MATNR])) AS inventcode
  FROM [GPBO].[dbo].[GP_SAPArticlesAttributes]
  WHERE ATTRIBUTE='SOCIAL' AND VALUE='true'
