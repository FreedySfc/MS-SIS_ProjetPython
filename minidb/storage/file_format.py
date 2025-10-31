import json
import struct
from typing import Dict, Any, Tuple

MAGIC = b"MINIDB\x00"  # 8 octets
VERSION = 1  # 1 octet

# Structures packées (big endian)
U8 = ">B"
U32 = ">I"
I64 = ">q"
F64 = ">d"


def write_header(f, schema: Dict[str, Any]) -> None:
    # Écrit l’en-tête: MAGIC, VERSION, len(schema), schema JSON
    f.seek(0)
    f.write(MAGIC)
    f.write(struct.pack(U8, VERSION))
    schema_bytes = json.dumps(schema, ensure_ascii=False).encode("utf-8")
    f.write(struct.pack(U32, len(schema_bytes)))
    f.write(schema_bytes)


def read_header(f) -> Tuple[Dict[str, Any], int]:
    # Lit l’en-tête et retourne (schema, offset_data)
    f.seek(0)
    magic = f.read(len(MAGIC))
    if magic != MAGIC:
        raise ValueError("Fichier invalide: MAGIC incorrect")
    version_byte = f.read(1)
    if not version_byte:
        raise ValueError("Fichier invalide: VERSION manquant")
    version = struct.unpack(U8, version_byte)[0]
    if version != VERSION:
        raise ValueError(f"Version non supportée: {version}")
    schema_len_bytes = f.read(4)
    if len(schema_len_bytes) != 4:
        raise ValueError("Fichier invalide: longueur schéma manquante")
    schema_len = struct.unpack(U32, schema_len_bytes)[0]
    schema_bytes = f.read(schema_len)
    if len(schema_bytes) != schema_len:
        raise ValueError("Fichier invalide: schéma tronqué")
    schema = json.loads(schema_bytes.decode("utf-8"))
    return schema, f.tell()


def encode_text(value: str) -> bytes:
    data = value.encode("utf-8")
    return struct.pack(U32, len(data)) + data


def decode_text(f) -> str:
    (length,) = struct.unpack(U32, f.read(4))
    data = f.read(length)
    if len(data) != length:
        raise ValueError("Texte tronqué")
    return data.decode("utf-8")
