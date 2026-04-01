import os
from datetime import datetime
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass
class Settings:
    # Parametros de extracao na API do Banco Mundial.
    wb_api_base_url: str = os.getenv("WB_API_BASE_URL", "https://api.worldbank.org/v2")
    wb_indicator_codes: str = os.getenv(
        "WB_INDICATOR_CODES",
        "NY.GDP.PCAP.KD,SP.POP.TOTL,SH.XPD.CHEX.GD.ZS,SE.XPD.TOTL.GD.ZS,EG.ELC.ACCS.ZS",
    )
    wb_format: str = os.getenv("WB_FORMAT", "json")
    wb_country_per_page: int = int(os.getenv("WB_COUNTRY_PER_PAGE", 300))
    wb_indicator_per_page: int = int(os.getenv("WB_INDICATOR_PER_PAGE", 100))
    wb_max_pages: int = int(os.getenv("WB_MAX_PAGES", 300))
    wb_mrv: int = int(os.getenv("WB_MRV", 10))
    wb_request_timeout: int = int(os.getenv("WB_REQUEST_TIMEOUT", 30))
    wb_retry_attempts: int = int(os.getenv("WB_RETRY_ATTEMPTS", 3))
    wb_retry_sleep_seconds: int = int(os.getenv("WB_RETRY_SLEEP_SECONDS", 2))
    min_year: int = int(os.getenv("MIN_YEAR", 2010))
    max_year: int = int(os.getenv("MAX_YEAR", datetime.now().year))
    allowed_income_groups: str = os.getenv("ALLOWED_INCOME_GROUPS", "LIC,MIC,HIC")

    # Parametros de conexao com o banco de destino.
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", 5432))
    db_name: str = os.getenv("DB_NAME", "DB_NAME")
    db_user: str = os.getenv("DB_USER", "DB_USER")
    db_password: str = os.getenv("DB_PASSWORD", "DB_PASSWORD")

    def __post_init__(self) -> None:
        required_db_envs = {
            "DB_NAME": self.db_name,
            "DB_USER": self.db_user,
            "DB_PASSWORD": self.db_password,
        }
        missing_or_placeholder = [
            key
            for key, value in required_db_envs.items()
            if not value or value.startswith("CHANGE_ME_")
        ]
        if missing_or_placeholder:
            joined = ", ".join(missing_or_placeholder)
            raise ValueError(
                f"Variaveis obrigatorias ausentes ou invalidas no .env: {joined}."
            )

    @property
    def indicator_code_list(self) -> list[str]:
        return [item.strip() for item in self.wb_indicator_codes.split(",") if item.strip()]

    @property
    def allowed_income_group_list(self) -> list[str]:
        return [item.strip().upper() for item in self.allowed_income_groups.split(",") if item.strip()]

    @property
    def sqlalchemy_database_url(self) -> str:
        # DSN usada pelo SQLAlchemy para criar engine e sessoes transacionais.
        return (
            f"postgresql+psycopg://{self.db_user}:{self.db_password}@"
            f"{self.db_host}:{self.db_port}/{self.db_name}"
        )


settings = Settings()
