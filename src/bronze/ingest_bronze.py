import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

# Logging configuration to monitor the ingestion process
# Configuração de Logging para monitorar o processo de ingestão
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env file
# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

def validate_env_vars():
    """
    Validates if all required environment variables are present.
    Valida se todas as variáveis de ambiente necessárias estão presentes.
    """
    required_vars = [
        "ACCOUNT_URL", "CONTAINER_NAME", "BLOB_NAME", 
        "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"
    ]
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise EnvironmentError(f"Missing variables in .env: {', '.join(missing)}")

def ingest_bronze():
    """
    Main function to ingest raw data from Azure Blob Storage to the Bronze layer.
    Função principal para ingerir dados brutos do Azure Blob Storage para a camada Bronze.
    """
    validate_env_vars()
    
    # Path configurations for local storage
    # Configurações de caminhos para armazenamento local
    base_dir = Path(__file__).resolve().parents[2]
    target_path = base_dir / "data" / "bronze" / os.getenv("BLOB_NAME")
    target_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Azure Authentication using Service Principal
        # Autenticação Azure utilizando Service Principal
        credential = ClientSecretCredential(
            tenant_id=os.getenv("AZURE_TENANT_ID"),
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET")
        )

        # Initialize Blob Service Client and Get Blob Client
        # Inicializa o Cliente de Serviço Blob e obtém o Cliente do Blob
        blob_service_client = BlobServiceClient(os.getenv("ACCOUNT_URL"), credential=credential)
        blob_client = blob_service_client.get_blob_client(
            container=os.getenv("CONTAINER_NAME"), 
            blob=os.getenv("BLOB_NAME")
        )

        logger.info(f"Starting blob download: {os.getenv('BLOB_NAME')}")
        
        # Download the data and write it to the local bronze layer
        # Baixa os dados e os escreve na camada bronze local
        with open(target_path, "wb") as f:
            data = blob_client.download_blob()
            f.write(data.readall())

        logger.info(f"✅ Ingestion completed! File saved at: {target_path}")
        return True

    except Exception as e:
        logger.error(f"❌ Ingestion failed: {str(e)}")
        return False

if __name__ == "__main__":
    ingest_bronze()