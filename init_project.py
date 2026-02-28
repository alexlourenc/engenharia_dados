import os
import platform
import subprocess
import sys

def run_command(command):
    """Executa comandos no terminal protegendo caminhos com espaços."""
    try:
        # shell=True permite que o Windows interprete o comando corretamente
        subprocess.run(command, check=True, shell=True)
        return True
    except subprocess.CalledProcessError:
        return False

def get_venv_info():
    """Detecta se já existe uma venv e retorna os caminhos corretos."""
    sistema = platform.system()
    for name in ["venv", ".venv"]:
        if os.path.exists(name):
            pip_path = os.path.join(name, "Scripts", "pip.exe") if sistema == "Windows" else os.path.join(name, "bin", "pip")
            return name, pip_path, sistema
    
    return None, None, sistema

def setup_environment():
    print("🚀 INICIANDO SETUP DO PROJETO\n" + "="*40)
    
    venv_name, pip_path, sistema = get_venv_info()

    # 1. Criação da VENV (se não existir)
    if not venv_name:
        venv_name = "venv"
        print(f"📦 Criando ambiente virtual '{venv_name}'...")
        # CORREÇÃO: Aspas duplas em volta do sys.executable para evitar erro de espaços
        if not run_command(f'"{sys.executable}" -m venv {venv_name}'):
            print("❌ Falha ao criar ambiente virtual.")
            return
        # Após criar, precisamos recalcular o pip_path
        pip_path = os.path.join(venv_name, "Scripts", "pip.exe") if sistema == "Windows" else os.path.join(venv_name, "bin", "pip")
    else:
        print(f"✅ Ambiente virtual '{venv_name}' detectado.")

    # 2. Instalação de dependências
    if os.path.exists("requirements.txt"):
        print("📥 Atualizando PIP e instalando dependências...")
        # CORREÇÃO: Aspas duplas em volta do pip_path
        run_command(f'"{pip_path}" install --upgrade pip')
        run_command(f'"{pip_path}" install -r requirements.txt')
    else:
        print("⚠️  requirements.txt não encontrado. Pulando instalação.")

    # 3. Configuração do arquivo .env
    configure_dotenv()

    # 4. Finalização
    print("\n" + "="*40)
    print("✨ TUDO PRONTO!")
    
    activate_cmd = f".\\{venv_name}\\Scripts\\activate" if sistema == "Windows" else f"source {venv_name}/bin/activate"
    print(f"Para ativar o ambiente use: {activate_cmd}")
    print("Depois, execute sua aplicação: python main.py")

def configure_dotenv():
    env_file = ".env"
    required_keys = [
        "ACCOUNT_URL", "CONTAINER_NAME", "BLOB_NAME",
        "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"
    ]
    
    existing_values = {}
    if os.path.exists(env_file):
        with open(env_file, "r", encoding="utf-8") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    key, val = line.strip().split("=", 1)
                    if val: existing_values[key] = val

    missing = [k for k in required_keys if k not in existing_values]
    if not missing:
        print("✅ Credenciais no arquivo .env estão completas.")
        return

    print(f"\n--- 🛠️ CONFIGURAÇÃO DE CREDENCIAIS ({len(missing)} pendentes) ---")
    for key in required_keys:
        if key in existing_values: continue
        while True:
            val = input(f"➤ {key}: ").strip()
            if val:
                existing_values[key] = val
                break
            print("❌ Campo obrigatório.")

    with open(env_file, "w", encoding="utf-8") as f:
        for key in required_keys:
            f.write(f"{key}={existing_values[key]}\n")
    print("✅ Arquivo .env atualizado.")

if __name__ == "__main__":
    setup_environment()