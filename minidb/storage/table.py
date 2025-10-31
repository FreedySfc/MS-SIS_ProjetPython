import os
import uuid
import struct
import threading
from typing import Dict, Any, List, Tuple, Iterable

from .file_format import (
    write_header,
    read_header,
    encode_text,
    decode_text,
    U8,
    I64,
    F64,
)
from ..types import ColumnType, coerce_value


class Table:
    # Table basée fichier binaire append-only, avec schéma JSON en en-tête

    def __init__(self, path: str, name: str, schema: Dict[str, Any] | None = None):
        self.path = path
        self.name = name
        self._lock = threading.RLock()
        self._schema: Dict[str, Any] | None = None
        self._data_offset = 0
        if os.path.exists(self.path):
            with open(self.path, "rb") as f:
                self._schema, self._data_offset = read_header(f)
        else:
            if schema is None:
                raise ValueError("Schéma requis pour créer une nouvelle table")
            # S’assure que _id SERIAL existe
            cols = schema.get("columns", [])
            has_id = any(c["name"] == "_id" for c in cols)
            if not has_id:
                cols = [{"name": "_id", "type": ColumnType.SERIAL.value}] + cols
            self._schema = {"table": name, "columns": cols}
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            with open(self.path, "wb") as f:
                write_header(f, self._schema)
                self._data_offset = f.tell()

    @property
    def schema(self) -> Dict[str, Any]:
        if self._schema is None:
            raise RuntimeError("Schéma non initialisé")
        return self._schema

    @property
    def columns(self) -> List[Dict[str, str]]:
        return list(self.schema["columns"])  # [{name, type}]

    def _iter_records(self) -> Iterable[Tuple[bool, bytes, List[Any]]]:
        # Itère sur toutes les lignes: (tombstone, id_bytes, values)
        with open(self.path, "rb") as f:
            f.seek(self._data_offset)
            while True:
                flag = f.read(1)
                if not flag:
                    break
                tombstone = struct.unpack(U8, flag)[0] == 1
                id_bytes = f.read(16)
                if len(id_bytes) != 16:
                    break
                values: List[Any] = []
                for col in self.columns:
                    if col["name"] == "_id":
                        # _id est stocké séparément
                        continue
                    ctype = ColumnType(col["type"])
                    if ctype == ColumnType.INT:
                        (v,) = struct.unpack(I64, f.read(8))
                        values.append(v)
                    elif ctype == ColumnType.FLOAT:
                        (v,) = struct.unpack(F64, f.read(8))
                        values.append(v)
                    elif ctype == ColumnType.BOOL:
                        (b,) = struct.unpack(U8, f.read(1))
                        values.append(bool(b))
                    elif ctype == ColumnType.TEXT:
                        values.append(decode_text(f))
                    elif ctype == ColumnType.SERIAL:
                        # non écrit ici (c’est l’ID)
                        continue
                    else:
                        raise ValueError("Type inconnu")
                yield tombstone, id_bytes, values

    def read_all(self) -> List[Dict[str, Any]]:
        # Reconstruit l’état courant: dernière version active par _id, en ignorant tombstones
        latest: Dict[bytes, Dict[str, Any]] = {}
        for tombstone, id_bytes, values in self._iter_records():
            row = {"_id": uuid.UUID(bytes=id_bytes).hex}
            vi = 0
            for col in self.columns:
                if col["name"] == "_id":
                    continue
                row[col["name"]] = values[vi]
                vi += 1
            if tombstone:
                latest.pop(id_bytes, None)
            else:
                latest[id_bytes] = row
        return list(latest.values())

    def insert(self, values_by_col: Dict[str, Any]) -> Dict[str, Any]:
        # Génère _id, vérifie types, écrit l’enregistrement
        with self._lock:
            row_id = uuid.uuid4()
            encoded: List[bytes] = []
            for col in self.columns:
                name = col["name"]
                ctype = ColumnType(col["type"])
                if name == "_id":
                    continue
                if name not in values_by_col:
                    raise ValueError(f"Colonne manquante: {name}")
                val = coerce_value(values_by_col[name], ctype)
                if ctype == ColumnType.INT:
                    encoded.append(struct.pack(I64, int(val)))
                elif ctype == ColumnType.FLOAT:
                    encoded.append(struct.pack(F64, float(val)))
                elif ctype == ColumnType.BOOL:
                    encoded.append(struct.pack(U8, 1 if val else 0))
                elif ctype == ColumnType.TEXT:
                    encoded.append(encode_text(str(val)))
                elif ctype == ColumnType.SERIAL:
                    # ignoré; géré par _id
                    pass
                else:
                    raise ValueError("Type inconnu")
            with open(self.path, "r+b") as f:
                f.seek(0, os.SEEK_END)
                f.write(struct.pack(U8, 0))
                f.write(row_id.bytes)
                for part in encoded:
                    f.write(part)
            result = {"_id": row_id.hex}
            result.update({k: values_by_col[k] for k in values_by_col})
            return result

    def delete(self, id_hex: str) -> bool:
        # Écrit une tombstone pour l’_id donné
        with self._lock:
            try:
                row_id = uuid.UUID(hex=id_hex)
            except Exception as exc:
                raise ValueError("_id invalide") from exc
            with open(self.path, "r+b") as f:
                f.seek(0, os.SEEK_END)
                f.write(struct.pack(U8, 1))
                f.write(row_id.bytes)
                # pas de payload
            return True

    def update(self, id_hex: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        # Lit la dernière version, applique, écrit une nouvelle version
        all_rows = {r["_id"]: r for r in self.read_all()}
        if id_hex not in all_rows:
            raise ValueError("Ligne introuvable")
        merged = dict(all_rows[id_hex])
        for col in self.columns:
            name = col["name"]
            if name == "_id":
                continue
            if name in updates:
                ctype = ColumnType(col["type"])
                merged[name] = coerce_value(updates[name], ctype)
        # Supprime l’ancienne (tombstone) et écrit comme nouvel insert
        self.delete(id_hex)
        values_by_col = {k: v for k, v in merged.items() if k != "_id"}
        return self.insert(values_by_col)

    def describe(self) -> Dict[str, Any]:
        return {"table": self.name, "columns": self.columns}
