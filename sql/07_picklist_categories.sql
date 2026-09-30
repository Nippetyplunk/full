-- Запрос для получения родительских категорий для пиклиста с картинками
SELECT 
    CAST(hcn.[HierarchyCatalogId] AS VARCHAR(50)) AS code,
    TRIM(hct.[HierarchyCatalogName]) AS name,
    bf.[BinaryFileId] AS binary_id,
    bf.[BinaryFileContent] AS image_bytes
FROM [GPBO].[dbo].[GP_HierarchyCatalogNodes] hcn
INNER JOIN [GPBO].[dbo].[GP_CustomPictureObjectAssigns] cpos ON hcn.[HierarchyCatalogNodeId] = cpos.[HierarchyCatalogNodeId]
INNER JOIN [GPBO_Pictures].[dbo].[GP_BinaryFiles] bf ON cpos.[BinaryFileId] = bf.[BinaryFileId]
INNER JOIN [GPBO].[dbo].[GP_HierarchyCatalogs] hc ON hcn.[HierarchyCatalogId] = hc.[HierarchyCatalogId]
INNER JOIN [GPBO].[dbo].[GP_HierarchyCatalogsT] hct ON hc.[HierarchyCatalogId] = hct.[HierarchyCatalogId]
WHERE cpos.[DeletionDate] IS NULL;
