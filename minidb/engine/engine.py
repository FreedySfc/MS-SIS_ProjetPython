import os
import json
import operator
from typing import Any, Dict, List, Tuple

from ..sql import parser
from ..storage.table import Table
from ..types import ColumnType, coerce_value


class DatabaseEngine:
    # Moteur regroupant tables par data_dir

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

    def _table_path(self, name: str) -> str:
        return os.path.join(self.data_dir, f"table_{name}.db")

    def _open_table(self, name: str) -> Table:
        path = self._table_path(name)
        if not os.path.exists(path):
            raise ValueError("Table inexistante")
        return Table(path, name)

    def _create_table(self, name: str, columns: List[Tuple[str, str]]):
        # Normalise types et construit schéma
        cols: List[Dict[str, str]] = []
        seen = set()
        for cname, ctype in columns:
            utype = ctype.upper()
            if utype not in ColumnType.__members__:
                raise ValueError(f"Type inconnu: {ctype}")
            if cname in seen:
                raise ValueError(f"Colonne dupliquée: {cname}")
            seen.add(cname)
            cols.append({"name": cname, "type": utype})
        path = self._table_path(name)
        if os.path.exists(path):
            raise ValueError("Table existe déjà")
        _ = Table(path, name, {"table": name, "columns": cols})
        return {"status": "ok", "message": f"Table {name} créée"}

    def execute(self, sql: str) -> Dict[str, Any]:
        try:
            ast = parser.parse(sql)
            kind = ast["type"]
            if kind == "create":
                return self._create_table(ast["table"], ast["columns"])
            if kind == "drop":
                path = self._table_path(ast["table"])
                if not os.path.exists(path):
                    raise ValueError("Table inexistante")
                os.remove(path)
                return {"status": "ok", "message": f"Table {ast['table']} supprimée"}
            if kind == "describe":
                t = self._open_table(ast["table"])
                return {"status": "ok", "data": t.describe()}
            if kind == "insert":
                t = self._open_table(ast["table"]) 
                row = self._prepare_insert(t, ast)
                created = t.insert(row)
                return {"status": "ok", "data": created}
            if kind == "select":
                t = self._open_table(ast["table"]) 
                rows = self._select(t, ast)
                return {"status": "ok", "data": rows}
            if kind == "update":
                t = self._open_table(ast["table"]) 
                count, updated = self._update(t, ast)
                return {"status": "ok", "message": f"{count} ligne(s) modifiée(s)", "data": updated}
            if kind == "delete":
                t = self._open_table(ast["table"]) 
                count = self._delete(t, ast)
                return {"status": "ok", "message": f"{count} ligne(s) supprimée(s)"}
            raise ValueError("Commande non gérée")
        except Exception as exc:
            return {"status": "error", "message": str(exc)}

    def _prepare_insert(self, table: Table, ast: Dict[str, Any]) -> Dict[str, Any]:
        # Construit dict {col: val} en validant types
        if ast["columns"] is None:
            # Valeurs dans l’ordre du schéma hors _id
            expected_cols = [c["name"] for c in table.columns if c["name"] != "_id"]
            if len(expected_cols) != len(ast["values"]):
                raise ValueError("Nombre de valeurs invalide")
            row = {}
            vi = 0
            for col in table.columns:
                if col["name"] == "_id":
                    continue
                ctype = ColumnType(col["type"])
                row[col["name"]] = coerce_value(ast["values"][vi], ctype)
                vi += 1
            return row
        else:
            # Mapping par nom
            row = {}
            for col in table.columns:
                if col["name"] == "_id":
                    continue
                if col["name"] not in ast["columns"]:
                    raise ValueError(f"Valeur manquante pour {col['name']}")
            for name, value in zip(ast["columns"], ast["values"]):
                ctype = next((ColumnType(c["type"]) for c in table.columns if c["name"] == name), None)
                if ctype is None:
                    raise ValueError(f"Colonne inconnue: {name}")
                row[name] = coerce_value(value, ctype)
            return row

    def _eval_where(self, expr: str | None, row: Dict[str, Any]) -> bool:
        if not expr:
            return True
        # Évaluation très simple et limitée: remplace identifiants par valeurs JSON puis évalue opérateurs via python
        # Sécurité: on ne fait pas eval brut; on parse opérateurs connus.
        tokens = re_tokenize(expr)
        return eval_tokens(tokens, row)

    def _select(self, table: Table, ast: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = table.read_all()
        filtered = [r for r in rows if self._eval_where(ast.get("where"), r)]
        # ORDER BY
        if ast.get("order"):
            col, direction = ast["order"]
            reverse = direction.upper() == "DESC"
            filtered.sort(key=lambda r: r.get(col), reverse=reverse)
        # LIMIT/OFFSET
        off = ast.get("offset") or 0
        lim = ast.get("limit")
        sliced = filtered[off : (off + lim) if lim is not None else None]
        # Colonnes
        cols = ast["columns"]
        if cols == ["*"]:
            return sliced
        proj: List[Dict[str, Any]] = []
        for r in sliced:
            proj.append({c: r.get(c) for c in cols})
        return proj

    def _update(self, table: Table, ast: Dict[str, Any]) -> tuple[int, List[Dict[str, Any]]]:
        rows = table.read_all()
        targets = [r for r in rows if self._eval_where(ast.get("where"), r)]
        updated: List[Dict[str, Any]] = []
        for r in targets:
            new_vals = dict(ast["assigns"])  # déjà typé par coerce dans update()
            # On retape ici pour cohérence stricte
            typed = {}
            for col in table.columns:
                name = col["name"]
                if name == "_id":
                    continue
                if name in new_vals:
                    typed[name] = coerce_value(new_vals[name], ColumnType(col["type"]))
            updated.append(table.update(r["_id"], typed))
        return len(updated), updated

    def _delete(self, table: Table, ast: Dict[str, Any]) -> int:
        rows = table.read_all()
        targets = [r for r in rows if self._eval_where(ast.get("where"), r)]
        for r in targets:
            table.delete(r["_id"])
        return len(targets)


# WHERE ultra minimal: tokenizer + éval sur opérateurs logiques/comparaisons
import re

_token_spec = [
    ("LPAR", r"\("),
    ("RPAR", r"\)"),
    ("AND", r"(?i:AND)"),
    ("OR", r"(?i:OR)"),
    ("NOT", r"(?i:NOT)"),
    ("OP", r"<=|>=|!=|=|<|>"),
    ("BOOL", r"(?i:TRUE|FALSE)"),
    ("NUM", r"-?\d+(?:\.\d+)?"),
    ("STR", r"'(?:[^']|''*)*'|\"(?:[^\"]|\"\"*)*\""),
    ("IDENT", r"[A-Za-z_][A-Za-z0-9_]*"),
    ("WS", r"\s+"),
]
_tok_re = re.compile("|".join(f"(?P<{n}>{p})" for n, p in _token_spec))


def re_tokenize(expr: str) -> List[Tuple[str, str]]:
    tokens: List[Tuple[str, str]] = []
    for m in _tok_re.finditer(expr):
        kind = m.lastgroup
        text = m.group()
        if kind == "WS":
            continue
        tokens.append((kind, text))
    return tokens


def eval_tokens(tokens: List[Tuple[str, str]], row: Dict[str, Any]) -> bool:
    # Shunting-yard vers RPN puis éval; opérateurs: NOT, AND, OR; comparaisons binaires
    prec = {"NOT": 3, "AND": 2, "OR": 1}
    output: List[Tuple[str, str]] = []
    ops: List[Tuple[str, str]] = []

    def push_op(op: Tuple[str, str]):
        while ops and ops[-1][0] in ("NOT", "AND", "OR") and prec[ops[-1][0]] >= prec[op[0]]:
            output.append(ops.pop())
        ops.append(op)

    for kind, text in tokens:
        if kind in ("BOOL", "NUM", "STR", "IDENT"):
            output.append((kind, text))
        elif kind == "OP":
            # binaire -> on pousse tel quel, géré dans éval
            ops.append((kind, text))
        elif kind == "NOT":
            push_op((kind, text.upper()))
        elif kind in ("AND", "OR"):
            push_op((kind, text.upper()))
        elif kind == "LPAR":
            ops.append((kind, text))
        elif kind == "RPAR":
            while ops and ops[-1][0] != "LPAR":
                output.append(ops.pop())
            if not ops:
                raise ValueError("Parenthèses déséquilibrées")
            ops.pop()
    while ops:
        if ops[-1][0] in ("LPAR", "RPAR"):
            raise ValueError("Parenthèses déséquilibrées")
        output.append(ops.pop())

    # Évaluation de la RPN
    stack: List[Any] = []

    def val_of(tok: Tuple[str, str]) -> Any:
        kind, text = tok
        if kind == "BOOL":
            return text.lower() == "true"
        if kind == "NUM":
            return float(text) if "." in text else int(text)
        if kind == "STR":
            return text[1:-1].replace("''", "'").replace('\"\"', '"')
        if kind == "IDENT":
            return row.get(text)
        raise ValueError("Jeton invalide")

    cmp_ops = {
        "=": operator.eq,
        "!=": operator.ne,
        "<": operator.lt,
        "<=": operator.le,
        ">": operator.gt,
        ">=": operator.ge,
    }

    for kind, text in output:
        if kind in ("BOOL", "NUM", "STR", "IDENT"):
            stack.append(val_of((kind, text)))
        elif kind == "OP":
            b = stack.pop()
            a = stack.pop()
            # Comparaison typée avec conversion simple pour chaînes -> nombres/bool si possible
            opf = cmp_ops[text]
            stack.append(opf(a, b))
        elif kind == "NOT":
            v = stack.pop()
            stack.append(not bool(v))
        elif kind == "AND":
            b = stack.pop(); a = stack.pop()
            stack.append(bool(a) and bool(b))
        elif kind == "OR":
            b = stack.pop(); a = stack.pop()
            stack.append(bool(a) or bool(b))
        else:
            raise ValueError("Jeton invalide dans évaluation")
    if len(stack) != 1:
        raise ValueError("Expression WHERE invalide")
    return bool(stack[0])
