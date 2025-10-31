import argparse
import json
import socket
import threading
from typing import Tuple

from ..auth.security import ensure_admin, verify_admin
from ..engine.engine import DatabaseEngine


class ClientHandler(threading.Thread):
    # Gère une connexion (auth + boucle requêtes SQL)

    def __init__(self, conn: socket.socket, addr: Tuple[str, int], engine: DatabaseEngine, data_dir: str):
        super().__init__(daemon=True)
        self.conn = conn
        self.addr = addr
        self.engine = engine
        self.data_dir = data_dir

    def run(self):
        try:
            f = self.conn.makefile("rwb")
            # Auth attendue: 1ère ligne JSON {action:"auth", username, password}
            auth_line = f.readline()
            if not auth_line:
                return
            try:
                msg = json.loads(auth_line.decode("utf-8"))
            except Exception:
                self._send_json(f, {"status": "error", "message": "Protocole invalide: JSON attendu"})
                return
            if msg.get("action") != "auth":
                self._send_json(f, {"status": "error", "message": "Authentification requise"})
                return
            if not verify_admin(self.data_dir, msg.get("username", ""), msg.get("password", "")):
                self._send_json(f, {"status": "error", "message": "Identifiants invalides"})
                return
            self._send_json(f, {"status": "ok", "message": "Authentifié"})
            # Boucle: chaque ligne => requête SQL (texte), réponse JSON
            while True:
                line = f.readline()
                if not line:
                    break
                sql = line.decode("utf-8").strip()
                if not sql:
                    continue
                if sql.lower() in {"quit", "exit"}:
                    self._send_json(f, {"status": "ok", "message": "Bye"})
                    break
                result = self.engine.execute(sql)
                self._send_json(f, result)
        finally:
            try:
                self.conn.close()
            except Exception:
                pass

    def _send_json(self, f, obj):
        data = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
        f.write(data)
        f.flush()


def main():
    parser = argparse.ArgumentParser(description="MiniDB Server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5555)
    parser.add_argument("--data-dir", default="./data")
    args = parser.parse_args()

    ensure_admin(args.data_dir)
    engine = DatabaseEngine(args.data_dir)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((args.host, args.port))
        s.listen(5)
        print(f"Serveur MiniDB en écoute sur {args.host}:{args.port}")
        while True:
            conn, addr = s.accept()
            handler = ClientHandler(conn, addr, engine, args.data_dir)
            handler.start()


if __name__ == "__main__":
    main()
