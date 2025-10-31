import re
from typing import Any, Dict, List, Tuple, Optional

# Parser SQL minimaliste pour un sous-ensemble des commandes
# Note: Ce parser est volontairement simple, non tolérant aux ambiguïtés du SQL complet.

_whitespace = re.compile(r"\s+")
_ident = r"[A-Za-z_][A-Za-z0-9_]*"
_value_text = r"'(?:[^']|''*)*'|\"(?:[^\"]|\"\"*)*\""
_number = r"-?\d+(?:\.\d+)?"
_bool = r"TRUE|FALSE|true|false"


def _strip_quotes(s: str) -> str:
    if len(s) >= 2 and ((s[0] == s[-1] == '\'') or (s[0] == s[-1] == '"')):
        return s[1:-1].replace("''", "'").replace('\"\"', '"')
    return s


def _parse_value(tok: str) -> Any:
    if re.fullmatch(_bool, tok):
        return tok.lower() == "true"
    if re.fullmatch(_number, tok):
        return float(tok) if "." in tok else int(tok)
    if re.fullmatch(_value_text, tok):
        return _strip_quotes(tok)
    # Ident nu -> traité comme texte
    return tok


def parse(sql: str) -> Dict[str, Any]:
    s = sql.strip().rstrip(";")
    # CREATE TABLE
    m = re.fullmatch(rf"CREATE\s+TABLE\s+({_ident})\s*\((.*?)\)", s, re.IGNORECASE | re.DOTALL)
    if m:
        table = m.group(1)
        cols_raw = m.group(2)
        cols: List[Tuple[str, str]] = []
        for part in re.split(r",", cols_raw):
            part = part.strip()
            cm = re.fullmatch(rf"({_ident})\s+({_ident})", part)
            if not cm:
                raise ValueError("Colonne mal formée dans CREATE TABLE")
            cols.append((cm.group(1), cm.group(2).upper()))
        return {"type": "create", "table": table, "columns": cols}

    # DROP TABLE
    m = re.fullmatch(rf"DROP\s+TABLE\s+({_ident})", s, re.IGNORECASE)
    if m:
        return {"type": "drop", "table": m.group(1)}

    # DESCRIBE
    m = re.fullmatch(rf"DESCRIBE\s+({_ident})", s, re.IGNORECASE)
    if m:
        return {"type": "describe", "table": m.group(1)}

    # INSERT INTO table [(cols...)] VALUES (vals...)
    m = re.fullmatch(
        rf"INSERT\s+INTO\s+({_ident})(?:\s*\((.*?)\))?\s+VALUES\s*\((.*?)\)", s, re.IGNORECASE | re.DOTALL
    )
    if m:
        table = m.group(1)
        cols_part = m.group(2)
        vals_part = m.group(3)
        cols: Optional[List[str]] = None
        if cols_part:
            cols = [c.strip() for c in cols_part.split(",") if c.strip()]
        # Valeurs: on sépare par virgule sans casser les quotes simples/doubles
        tokens: List[str] = []
        buf = []
        in_s = None
        for ch in vals_part:
            if ch in ("'", '"'):
                if in_s is None:
                    in_s = ch
                elif in_s == ch:
                    in_s = None
                buf.append(ch)
                continue
            if ch == "," and in_s is None:
                token = "".join(buf).strip()
                if token:
                    tokens.append(token)
                buf = []
            else:
                buf.append(ch)
        token = "".join(buf).strip()
        if token:
            tokens.append(token)
        values = [_parse_value(t) for t in tokens]
        return {"type": "insert", "table": table, "columns": cols, "values": values}

    # SELECT cols FROM table ...
    m = re.fullmatch(rf"SELECT\s+(.*?)\s+FROM\s+({_ident})(.*)", s, re.IGNORECASE | re.DOTALL)
    if m:
        cols_raw = m.group(1).strip()
        table = m.group(2)
        tail = m.group(3) or ""
        cols = ["*"] if cols_raw == "*" else [c.strip() for c in cols_raw.split(",")]
        where = None
        order = None
        limit = None
        offset = None
        # WHERE
        wm = re.search(r"\bWHERE\b(.*)$", tail, re.IGNORECASE | re.DOTALL)
        if wm:
            after_where = wm.group(1)
            cut = re.split(r"\bORDER\s+BY\b|\bLIMIT\b|\bOFFSET\b", after_where, flags=re.IGNORECASE)[0]
            where = cut.strip()
        # ORDER BY
        om = re.search(r"ORDER\s+BY\s+([A-Za-z_][A-Za-z0-9_]*)(?:\s+(ASC|DESC))?", tail, re.IGNORECASE)
        if om:
            order = (om.group(1), (om.group(2) or "ASC").upper())
        # LIMIT
        lm = re.search(r"\bLIMIT\b\s+(\d+)", tail, re.IGNORECASE)
        if lm:
            limit = int(lm.group(1))
        # OFFSET
        ofm = re.search(r"\bOFFSET\b\s+(\d+)", tail, re.IGNORECASE)
        if ofm:
            offset = int(ofm.group(1))
        return {
            "type": "select",
            "table": table,
            "columns": cols,
            "where": where,
            "order": order,
            "limit": limit,
            "offset": offset,
        }

    # UPDATE table SET ... [WHERE ...]
    m = re.fullmatch(rf"UPDATE\s+({_ident})\s+SET\s+(.*)", s, re.IGNORECASE | re.DOTALL)
    if m:
        table = m.group(1)
        tail = m.group(2)
        where = None
        wm = re.search(r"\bWHERE\b(.*)$", tail, re.IGNORECASE | re.DOTALL)
        if wm:
            where = wm.group(1).strip()
            tail = tail[: wm.start()].strip()
        assigns: Dict[str, Any] = {}
        parts = [p.strip() for p in tail.split(",") if p.strip()]
        for p in parts:
            am = re.fullmatch(rf"({_ident})\s*=\s*(.+)", p)
            if not am:
                raise ValueError("Affectation invalide dans UPDATE")
            col = am.group(1)
            val = _parse_value(am.group(2).strip())
            assigns[col] = val
        return {"type": "update", "table": table, "assigns": assigns, "where": where}

    # DELETE FROM table [WHERE ...]
    m = re.fullmatch(rf"DELETE\s+FROM\s+({_ident})(?:\s+WHERE\s+(.*))?", s, re.IGNORECASE | re.DOTALL)
    if m:
        table = m.group(1)
        where = (m.group(2) or "").strip() or None
        return {"type": "delete", "table": table, "where": where}

    raise ValueError("Commande SQL non reconnue ou non supportée")
