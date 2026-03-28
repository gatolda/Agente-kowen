"""
Agente Kowen - Punto de entrada.
Clasifica correos de Gmail usando Claude como cerebro de IA.
"""

import sys
from dotenv import load_dotenv

load_dotenv()

from agent import run_agent


def main():
    max_emails = 20

    # Permitir pasar numero de correos como argumento
    if len(sys.argv) > 1:
        try:
            max_emails = int(sys.argv[1])
        except ValueError:
            print(f"Uso: python main.py [max_correos]")
            sys.exit(1)

    print("=" * 50)
    print("  AGENTE KOWEN - Clasificador de Correos")
    print("=" * 50)
    print(f"Procesando hasta {max_emails} correos...\n")

    run_agent(max_emails=max_emails)


if __name__ == "__main__":
    main()
