DECLARE @sql NVARCHAR(MAX);

IF COL_LENGTH('dbo.CarteraFacturaLatest', 'Cuota') IS NOT NULL
BEGIN
  SELECT @sql = N'ALTER TABLE dbo.CarteraFacturaLatest DROP CONSTRAINT ' + QUOTENAME(dc.name)
  FROM sys.default_constraints dc
  INNER JOIN sys.columns c ON c.default_object_id = dc.object_id
  INNER JOIN sys.tables t ON t.object_id = dc.parent_object_id
  INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
  WHERE s.name = 'dbo' AND t.name = 'CarteraFacturaLatest' AND c.name = 'Cuota';

  IF @sql IS NOT NULL EXEC sp_executesql @sql;
  ALTER TABLE dbo.CarteraFacturaLatest DROP COLUMN Cuota;
END

SET @sql = NULL;

IF COL_LENGTH('dbo.CarteraFacturaLatest', 'Monto') IS NOT NULL
BEGIN
  SELECT @sql = N'ALTER TABLE dbo.CarteraFacturaLatest DROP CONSTRAINT ' + QUOTENAME(dc.name)
  FROM sys.default_constraints dc
  INNER JOIN sys.columns c ON c.default_object_id = dc.object_id
  INNER JOIN sys.tables t ON t.object_id = dc.parent_object_id
  INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
  WHERE s.name = 'dbo' AND t.name = 'CarteraFacturaLatest' AND c.name = 'Monto';

  IF @sql IS NOT NULL EXEC sp_executesql @sql;
  ALTER TABLE dbo.CarteraFacturaLatest DROP COLUMN Monto;
END

