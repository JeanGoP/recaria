IF COL_LENGTH('dbo.CarteraFacturaLatest', 'Saldo') IS NULL
BEGIN
  ALTER TABLE dbo.CarteraFacturaLatest ADD Saldo DECIMAL(18, 2) NOT NULL CONSTRAINT DF_CarteraFacturaLatest_Saldo DEFAULT (0);
END

IF COL_LENGTH('dbo.CarteraFacturaLatest', 'Cuota') IS NULL
BEGIN
  ALTER TABLE dbo.CarteraFacturaLatest ADD Cuota DECIMAL(18, 2) NOT NULL CONSTRAINT DF_CarteraFacturaLatest_Cuota DEFAULT (0);
END

