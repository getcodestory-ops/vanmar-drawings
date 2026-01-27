import os
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""
    
    # Procore API Configuration
    PROCORE_CLIENT_ID: str
    PROCORE_CLIENT_SECRET: str
    PROCORE_BASE_URL: str = "https://api.procore.com"
    PROCORE_COMPANY_ID: int = 4907  # Default, can be overridden
    
    # Database Configuration
    DATABASE_URL: str = "sqlite:///./jobs.db"
    
    # Application Configuration
    ENVIRONMENT: str = "development"
    DEBUG: bool = False
    
    # Server Configuration
    BASE_URL: Optional[str] = None  # Auto-detected from RENDER_EXTERNAL_URL or defaults to localhost
    PORT: int = 8000
    
    # Redis/Cache Configuration (Optional)
    REDIS_URL: Optional[str] = None
    CACHE_TTL: int = 900  # 15 minutes
    
    # Security
    SECRET_KEY: Optional[str] = None  # For token encryption
    
    # Performance (Memory-Tunable for low-RAM environments like Render.com)
    MAX_CONCURRENT_DOWNLOADS: int = 3  # Reduced from 10 to prevent OOM on 2GB RAM
    MERGE_BATCH_SIZE: int = 10  # Batch size for PDF merging (lower = less memory, was 15)
    LOW_MEMORY_MODE: bool = True  # Enable conservative memory usage (skips TOC, aggressive gc)
    DOWNLOAD_TIMEOUT: int = 300  # 5 minutes
    JOB_TIMEOUT: int = 1800  # 30 minutes
    DRAWING_DOWNLOAD_RETRIES: int = 3  # Retries per drawing when fetch/download fails
    
    # Rate Limiting
    RATE_LIMIT_PER_MINUTE: int = 5
    
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=True
    )
    
    @property
    def redirect_uri(self) -> str:
        """Generate OAuth redirect URI based on environment."""
        if self.BASE_URL:
            base = self.BASE_URL.rstrip('/')  # Remove trailing slash
            return f"{base}/callback"
        
        # Check for Render environment variable
        render_url = os.getenv("RENDER_EXTERNAL_URL")
        if render_url:
            render_url = render_url.rstrip('/')  # Remove trailing slash
            return f"{render_url}/callback"
        
        # Default to localhost for development
        return f"http://localhost:{self.PORT}/callback"
    
    @property
    def login_url(self) -> str:
        """Procore OAuth login URL."""
        return "https://login.procore.com/oauth/authorize"
    
    @property
    def token_url(self) -> str:
        """Procore OAuth token URL."""
        return "https://login.procore.com/oauth/token"
    
    @property
    def is_production(self) -> bool:
        """Check if running in production."""
        return self.ENVIRONMENT.lower() == "production"
    
    def get_encryption_key(self) -> bytes:
        """Get or generate encryption key for token storage."""
        from cryptography.fernet import Fernet
        
        if self.SECRET_KEY:
            # Use provided key (must be base64 encoded)
            return self.SECRET_KEY.encode()
        
        # Generate a key (should be stored in env for production)
        if self.is_production:
            raise ValueError("SECRET_KEY must be set in production environment")
        
        # For development, use a static key (DO NOT USE IN PRODUCTION)
        return Fernet.generate_key()


# Global settings instance
settings = Settings()
