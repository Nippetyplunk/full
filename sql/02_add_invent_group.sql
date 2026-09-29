    SELECT DISTINCT
        gp_cat.[ItemCategoryId] AS groupcode,
        TRIM(REPLACE(REPLACE(REPLACE(ISNULL(gp_catt.[ItemCategoryName], 'Без названия'), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS groupname,
        gp_cat.[ItemCategoryId:Parent] AS parentgroupcode
    FROM [GPBO].[dbo].[GP_ItemCategories] gp_cat
    OUTER APPLY (
        SELECT TOP 1 [ItemCategoryName]
        FROM [GPBO].[dbo].[GP_ItemCategoriesT]
        WHERE [ItemCategoryId] = gp_cat.[ItemCategoryId]
    ) gp_catt
    WHERE gp_cat.[DeletionDate] IS NULL;