from enum import Enum
from typing import Any


class ColumnType(str, Enum):
    INT = "INT"
    FLOAT = "FLOAT"
    TEXT = "TEXT"
    BOOL = "BOOL"
    SERIAL = "SERIAL"


def python_type_for(column_type: ColumnType):
    # Retourne le type Python attendu pour validation
    if column_type == ColumnType.INT:
        return int
    if column_type == ColumnType.FLOAT:
        return float
    if column_type == ColumnType.TEXT:
        return str
    if column_type == ColumnType.BOOL:
        return bool
    if column_type == ColumnType.SERIAL:
        return (bytes, str)
    raise ValueError(f"Type inconnu: {column_type}")


def coerce_value(value: Any, column_type: ColumnType) -> Any:
    # Convertit/valide une valeur Python en fonction du type logique
    if column_type == ColumnType.INT:
        if isinstance(value, bool):
            # bool hérite d'int en Python; on refuse implicitement
            raise TypeError("BOOL non autorisé comme INT")
        return int(value)
    if column_type == ColumnType.FLOAT:
        return float(value)
    if column_type == ColumnType.TEXT:
        return str(value)
    if column_type == ColumnType.BOOL:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            v = value.strip().lower()
            if v in {"true", "1", "t", "yes", "y"}:
                return True
            if v in {"false", "0", "f", "no", "n"}:
                return False
        raise TypeError("Valeur invalide pour BOOL")
    if column_type == ColumnType.SERIAL:
        # Géré par le moteur, pas de conversion externe
        return value
    raise ValueError(f"Type inconnu: {column_type}")
