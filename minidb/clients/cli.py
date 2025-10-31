import argparse
import getpass
import json
import socket


def main():
    parser = argparse.ArgumentParser(description="MiniDB CLI Client")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5555)
    parser.add_argument("--user", required=True)
    args = parser.parse_args()

    password = getpass.getpass("Mot de passe: ")

    with socket.create_connection((args.host, args.port)) as s:
        f = s.makefile("rwb")
        auth = {"action": "auth", "username": args.user, "password": password}
        f.write((json.dumps(auth) + "\n").encode("utf-8"))
        f.flush()
        line = f.readline().decode("utf-8").strip()
        print(line)
        resp = json.loads(line)
        if resp.get("status") != "ok":
            return
        print("Connecté. Tapez du SQL, ou 'exit' pour quitter.")
        while True:
            try:
                sql = input(">> ").strip()
            except EOFError:
                break
            if not sql:
                continue
            f.write((sql + "\n").encode("utf-8"))
            f.flush()
            line = f.readline()
            if not line:
                break
            print(line.decode("utf-8").strip())


if __name__ == "__main__":
    main()
