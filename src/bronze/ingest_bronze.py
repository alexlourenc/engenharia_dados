import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

# Configuração de Logging para monitorar a ingestão
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

def validate_env_vars():
    """Valida se todas as variáveis de ambiente necessárias estão presentes."""
    required_vars = ["ACCOUNT_URL", "CONTAINER_NAME", "BLOB_NAME", "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"]
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise EnvironmentError(f"Variáveis ausentes no .env: {', '.join(missing)}")

def ingest_bronze():
    validate_env_vars()
    
    # Configurações de caminhos
    base_dir = Path(__file__).resolve().parents[2]
    target_path = base_dir / "data" / "bronze" / os.getenv("BLOB_NAME")
    target_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Autenticação
        credential = ClientSecretCredential(
            tenant_id=os.getenv("AZURE_TENANT_ID"),
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET")
        )

        blob_service_client = BlobServiceClient(os.getenv("ACCOUNT_URL"), credential=credential)
        blob_client = blob_service_client.get_blob_client(
            container=os.getenv("CONTAINER_NAME"), 
            blob=os.getenv("BLOB_NAME")
        )

        logger.info(f"Iniciando download do blob: {os.getenv('BLOB_NAME')}")
        
        with open(target_path, "wb") as f:
            data = blob_client.download_blob()
            f.write(data.readall())

        logger.info(f"✅ Ingestão concluída! Arquivo salvo em: {target_path}")
        return True

    except Exception as e:
        logger.error(f"❌ Falha na ingestão: {str(e)}")
        return False

if __name__ == "__main__":
    ingest_bronze()