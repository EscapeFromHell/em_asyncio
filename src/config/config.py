class Config:
    URL = "https://spimex.com/upload/reports/oil_xls/oil_xls_"
    DATABASE_DSN = "postgresql+asyncpg://postgres:password@localhost:5432/spimex"


settings = Config()
