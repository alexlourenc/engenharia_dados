import os
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent / '.env'
load_dotenv(dotenv_path=env_path)

print(f"Caminho do .env: {env_path}")
print(f"Account URL: {os.getenv('ACCOUNT_URL')}")