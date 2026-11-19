
import base64
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

ENV_FILE = ROOT / ".env"
ENCODED_FILE = ROOT / ".env_encoded"

REQUIRED = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "POSTGRES_PASSWORD",
    "KESTRA_BASIC_AUTH_PASSWORD",
]

DEFAULTS = {
    "SNOWFLAKE_ROLE": "PSET2_ROLE",
    "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
    "SNOWFLAKE_DATABASE": "PSET2_DB",
    "SNOWFLAKE_SCHEMA": "RAW",
    "KESTRA_BASIC_AUTH_USERNAME": "admin@kestra.io",
}

SNOWFLAKE_SECRETS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]


def read_env_file():
    """Lee las variables de un archivo .env existente."""
    values = {}

    if not ENV_FILE.exists():
        return values

    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if len(value) >= 2 and value[0] == value[-1]:
            if value[0] == '"':
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    value = value[1:-1]
            elif value[0] == "'":
                value = value[1:-1].replace("\\'", "'")

        values[key] = value

    return values


def main():
    # Reutilizar .env local si existe.
    values = read_env_file()

    # En Codespaces, las variables del entorno tienen prioridad.
    for key in REQUIRED + list(DEFAULTS):
        if key in os.environ:
            values[key] = os.environ[key]

    # Aplicar valores predeterminados.
    for key, default in DEFAULTS.items():
        values.setdefault(key, default)

    # Comprobar las credenciales obligatorias.
    missing = [
        key for key in REQUIRED
        if not values.get(key, "").strip()
    ]

    if missing:
        raise ValueError(
            "Faltan variables obligatorias: "
            + ", ".join(missing)
        )

    # Evitar saltos de línea y errores de formato.
    for key, value in values.items():
        if "\n" in value or "\r" in value:
            raise ValueError(
                f"La variable {key} contiene saltos de línea."
            )

    ordered_keys = REQUIRED + list(DEFAULTS)

    # Crear .env.
    env_lines = [
        "# Archivo generado automáticamente. No versionar."
    ]

    for key in ordered_keys:
        value = values[key].replace("'", "\\'")
        env_lines.append(f"{key}='{value}'")

    ENV_FILE.write_text(
        "\n".join(env_lines) + "\n",
        encoding="utf-8"
    )

    # Crear secretos Base64 para Kestra.
    encoded_lines = [
        "# Generado automáticamente. No versionar."
    ]

    for key in SNOWFLAKE_SECRETS:
        encoded = base64.b64encode(
            values[key].encode("utf-8")
        ).decode("ascii")

        encoded_lines.append(f"SECRET_{key}={encoded}")

    ENCODED_FILE.write_text(
        "\n".join(encoded_lines) + "\n",
        encoding="utf-8"
    )

    # Proteger los archivos en sistemas Linux.
    if os.name == "posix":
        os.chmod(ENV_FILE, 0o600)
        os.chmod(ENCODED_FILE, 0o600)

    print("Archivos de configuración generados correctamente.")
    print(".env: creado.")
    print(".env_encoded: creado.")
    print("No se han mostrado credenciales.")


if __name__ == "__main__":
    main()
