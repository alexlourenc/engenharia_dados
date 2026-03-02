"""
PROJECT: JIRA SLA Management Console
AUTHOR: Alex Lourenço (https://github.com/alexlourenc/)
DESCRIPTION: Streamlit interface for executive dashboard, pipeline audit logs, and data governance.
"""
import subprocess
import sys
import platform
import logging
from pathlib import Path

# Configuring logging for environment setup
# Configurando o logging para o setup do ambiente
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def run_command(command):
    """
    Executes terminal commands safely, protecting paths with spaces.
    Executa comandos no terminal de forma segura, protegendo caminhos com espaços.
    """
    try:
        # shell=True is kept for Windows shell built-ins but used with caution
        # shell=True é mantido para comandos nativos do shell Windows, usado com cautela
        subprocess.run(command, check=True, shell=True)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Command failed / Comando falhou: {e}")
        return False

def get_venv_info():
    """
    Detects if a venv already exists and returns the correct paths.
    Detecta se uma venv já existe e retorna os caminhos corretos.
    """
    os_system = platform.system()
    for name in ["venv", ".venv"]:
        venv_path = Path(name)
        if venv_path.exists():
            # Determining the pip path based on the OS
            # Determinando o caminho do pip baseado no SO
            if os_system == "Windows":
                pip_path = venv_path / "Scripts" / "pip.exe"
            else:
                pip_path = venv_path / "bin" / "pip"
            return name, pip_path, os_system
    
    return None, None, os_system

def configure_dotenv():
    """
    Manages the creation and update of the .env file with mandatory credentials.
    Gerencia a criação e atualização do arquivo .env com as credenciais obrigatórias.
    """
    env_file = Path(".env")
    required_keys = [
        "ACCOUNT_URL", "CONTAINER_NAME", "BLOB_NAME",
        "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"
    ]
    
    existing_values = {}
    if env_file.exists():
        with open(env_file, "r", encoding="utf-8") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    key, val = line.strip().split("=", 1)
                    if val:
                        existing_values[key] = val

    missing = [k for k in required_keys if k not in existing_values]
    if not missing:
        logger.info("✅ .env file credentials are complete / Credenciais no .env estão completas.")
        return

    logger.info(f"🛠️ CONFIGURING CREDENTIALS / CONFIGURAÇÃO DE CREDENCIAIS ({len(missing)} pending)")
    for key in required_keys:
        if key in existing_values:
            continue
        while True:
            val = input(f"➤ {key}: ").strip()
            if val:
                existing_values[key] = val
                break
            print("❌ Mandatory field / Campo obrigatório.")

    # Persisting values back to .env
    # Persistindo os valores de volta para o .env
    with open(env_file, "w", encoding="utf-8") as f:
        for key in required_keys:
            f.write(f"{key}={existing_values[key]}\n")
    logger.info("✅ .env file updated / Arquivo .env atualizado.")

def setup_environment():
    """
    Main orchestration function for the project environment setup.
    Função principal de orquestração para o setup do ambiente do projeto.
    """
    logger.info("🚀 STARTING PROJECT SETUP / INICIANDO SETUP DO PROJETO")
    logger.info("="*50)
    
    venv_name, pip_path, os_system = get_venv_info()

    # 1. VENV Creation (if it doesn't exist)
    # 1. Criação da VENV (se não existir)
    if not venv_name:
        venv_name = "venv"
        logger.info(f"📦 Creating virtual environment / Criando ambiente virtual: '{venv_name}'")
        if not run_command(f'"{sys.executable}" -m venv {venv_name}'):
            logger.error("❌ Failed to create venv / Falha ao criar venv.")
            return
        
        # Recalculate pip path after creation
        # Recalcular o caminho do pip após a criação
        pip_path = Path(venv_name) / ("Scripts/pip.exe" if os_system == "Windows" else "bin/pip")
    else:
        logger.info(f"✅ Venv detected / Venv detectada: '{venv_name}'")

    # 2. Dependency Installation
    # 2. Instalação de dependências
    requirements_file = Path("requirements.txt")
    if requirements_file.exists():
        logger.info("📥 Installing dependencies / Instalando dependências...")
        # Upgrading pip first for stability
        # Atualizando o pip primeiro por estabilidade
        run_command(f'"{pip_path}" install --upgrade pip')
        run_command(f'"{pip_path}" install -r {requirements_file}')
    else:
        logger.warning("⚠️ requirements.txt not found / não encontrado. Skipping installation.")

    # 3. Environment variables configuration
    # 3. Configuração de variáveis de ambiente
    configure_dotenv()

    # 4. Finalization
    # 4. Finalização
    logger.info("="*50)
    logger.info("✨ SETUP COMPLETE / TUDO PRONTO!")
    
    activate_cmd = f".\\{venv_name}\\Scripts\\activate" if os_system == "Windows" else f"source {venv_name}/bin/activate"
    logger.info(f"To activate use / Para ativar use: {activate_cmd}")
    logger.info("Then run / Depois execute: python main.py")

if __name__ == "__main__":
    setup_environment()