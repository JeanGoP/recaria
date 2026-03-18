IF OBJECT_ID('dbo.CarteraFacturaLatest', 'U') IS NOT NULL
BEGIN
  DROP TABLE dbo.CarteraFacturaLatest;
END

CREATE TABLE dbo.CarteraFacturaLatest (
  EmpresaId INT NOT NULL,
  NumFactura NVARCHAR(64) NOT NULL,
  Identificacion NVARCHAR(64) NOT NULL,
  Cliente NVARCHAR(256) NULL,
  FechaFac DATETIME2(0) NULL,
  AnoMes NVARCHAR(20) NULL,
  Cuota INT NULL,
  Vencimiento DATE NOT NULL,
  Dias INT NULL,
  PorVencer DECIMAL(18, 2) NOT NULL CONSTRAINT DF_CarteraFacturaLatest_PorVencer DEFAULT (0),
  Treinta_Dias DECIMAL(18, 2) NOT NULL CONSTRAINT DF_CarteraFacturaLatest_Treinta_Dias DEFAULT (0),
  Sesenta_Dias DECIMAL(18, 2) NOT NULL CONSTRAINT DF_CarteraFacturaLatest_Sesenta_Dias DEFAULT (0),
  Noventa_Dias DECIMAL(18, 2) NOT NULL CONSTRAINT DF_CarteraFacturaLatest_Noventa_Dias DEFAULT (0),
  Mas_de_Noventa DECIMAL(18, 2) NOT NULL CONSTRAINT DF_CarteraFacturaLatest_Mas_de_Noventa DEFAULT (0),
  UpdatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_CarteraFacturaLatest_UpdatedAt DEFAULT (SYSUTCDATETIME()),
  CONSTRAINT PK_CarteraFacturaLatest PRIMARY KEY CLUSTERED (EmpresaId, NumFactura, Identificacion, Vencimiento),
  CONSTRAINT FK_CarteraFacturaLatest_Empresa FOREIGN KEY (EmpresaId) REFERENCES dbo.Empresa(EmpresaId)
);

CREATE INDEX IX_CarteraFacturaLatest_EmpresaId ON dbo.CarteraFacturaLatest (EmpresaId);
CREATE INDEX IX_CarteraFacturaLatest_Dias ON dbo.CarteraFacturaLatest (EmpresaId, Dias);
CREATE INDEX IX_CarteraFacturaLatest_Vencimiento ON dbo.CarteraFacturaLatest (EmpresaId, Vencimiento);

