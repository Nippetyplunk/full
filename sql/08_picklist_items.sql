   SELECT 
        gp_item.[ItemCode] AS raw_code,
        TRIM(gp_itemt.[ItemPOSName]) AS name,
        bf.[BinaryFileId] AS binary_id,
        bf.[BinaryFileContent] AS image_bytes,
        CAST(hcn.[HierarchyCatalogId] AS VARCHAR(50)) AS parent,
        gp_item.[ItemCode] AS tmccode,
        ean.[ItemEAN] AS barcode
    FROM [GPBO].[dbo].[GP_CustomPictureObjectAssigns] cpos
    INNER JOIN [GPBO].[dbo].[GP_Items] gp_item ON cpos.[ItemId] = gp_item.[ItemId] AND gp_item.[DeletionDate] IS NULL
    LEFT JOIN [GPBO].[dbo].[GP_ItemsT] gp_itemt ON gp_item.[ItemId] = gp_itemt.[ItemId]
    INNER JOIN [GPBO_Pictures].[dbo].[GP_BinaryFiles] bf ON cpos.[BinaryFileId] = bf.[BinaryFileId]
    OUTER APPLY (
        SELECT TOP 1 ie.[ItemEAN]
        FROM [GPBO].[dbo].[GP_ItemUnitsOfMeasure] iuom
        INNER JOIN [GPBO].[dbo].[GP_ItemEANs] ie ON iuom.[ItemUnitOfMeasureId] = ie.[ItemUnitOfMeasureId]
        WHERE iuom.[ItemId] = gp_item.[ItemId] AND iuom.[DeletionDate] IS NULL
        ORDER BY ie.[ItemEANId]
    ) ean
    OUTER APPLY (
        SELECT TOP 1 hcn_sub.[HierarchyCatalogId]
        FROM [GPBO].[dbo].[GP_HierarchyCatalogNodeItemAssigns] assign_sub
        INNER JOIN [GPBO].[dbo].[GP_HierarchyCatalogNodes] hcn_sub ON assign_sub.[HierarchyCatalogNodeId] = hcn_sub.[HierarchyCatalogNodeId]
        WHERE assign_sub.[ItemId] = cpos.[ItemId]
    ) hcn
    INNER JOIN (
        SELECT s_sub.[MATNR], ROW_NUMBER() OVER (PARTITION BY s_sub.[MATNR] ORDER BY s_sub.[timestamp] DESC) AS rn
        FROM [GPBO].[dbo].[BO_Stock_On_Hand] s_sub
        WHERE s_sub.[timestamp] >= '2026-01-01' AND s_sub.[timestamp] < '2026-01-12'
    ) stk ON stk.[MATNR] = gp_item.[ItemCode] AND stk.rn = 1
    WHERE cpos.[DeletionDate] IS NULL AND cpos.[ItemId] IS NOT NULL;