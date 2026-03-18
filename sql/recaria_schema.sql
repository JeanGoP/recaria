CREATE TABLE dbo.Empresa (
  EmpresaId INT IDENTITY(1,1) NOT NULL,
  TokenHash CHAR(64) NOT NULL,
  CreatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_Empresa_CreatedAt DEFAULT (SYSUTCDATETIME()),
  CONSTRAINT PK_Empresa PRIMARY KEY CLUSTERED (EmpresaId),
  CONSTRAINT UQ_Empresa_TokenHash UNIQUE (TokenHash)
);

CREATE TABLE dbo.CarteraLatestMeta (
  EmpresaId INT NOT NULL,
  LastSyncAt DATETIME2(0) NULL,
  LastSyncOk BIT NOT NULL CONSTRAINT DF_CarteraLatestMeta_LastSyncOk DEFAULT (0),
  LastSyncCount INT NULL,
  LastSyncTotalMonto DECIMAL(18, 2) NULL,
  LastError NVARCHAR(4000) NULL,
  LastPayloadJson NVARCHAR(MAX) NULL,
  CONSTRAINT PK_CarteraLatestMeta PRIMARY KEY CLUSTERED (EmpresaId),
  CONSTRAINT FK_CarteraLatestMeta_Empresa FOREIGN KEY (EmpresaId) REFERENCES dbo.Empresa(EmpresaId)
);

CREATE TABLE dbo.CarteraFacturaLatest (
  EmpresaId INT NOT NULL,
  NumFactura NVARCHAR(64) NOT NULL,
  Identificacion NVARCHAR(64) NOT NULL,
  Cliente NVARCHAR(256) NULL,
  Vencimiento DATE NULL,
  Dias INT NULL,
  Monto DECIMAL(18, 2) NOT NULL CONSTRAINT DF_CarteraFacturaLatest_Monto DEFAULT (0),
  UpdatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_CarteraFacturaLatest_UpdatedAt DEFAULT (SYSUTCDATETIME()),
  CONSTRAINT PK_CarteraFacturaLatest PRIMARY KEY CLUSTERED (EmpresaId, NumFactura, Identificacion),
  CONSTRAINT FK_CarteraFacturaLatest_Empresa FOREIGN KEY (EmpresaId) REFERENCES dbo.Empresa(EmpresaId)
);

CREATE INDEX IX_CarteraFacturaLatest_EmpresaId ON dbo.CarteraFacturaLatest (EmpresaId);
CREATE INDEX IX_CarteraFacturaLatest_Dias ON dbo.CarteraFacturaLatest (EmpresaId, Dias);
CREATE INDEX IX_CarteraFacturaLatest_Vencimiento ON dbo.CarteraFacturaLatest (EmpresaId, Vencimiento);
