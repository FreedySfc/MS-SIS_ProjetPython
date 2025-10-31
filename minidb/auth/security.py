import os
import json
import getpass
import hashlib
import secrets
from typing import Tuple

SECURE_FILE = "secure.db"  # Stocké dans data_dir


def _hash_password(password: str, salt: bytes) -> str:
    # Hachage sécurisé via PBKDF2-HMAC-SHA256
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return dk.hex()


def ensure_admin(data_dir: str) -> None:
    # Si secure.db absent, demande en console la création admin
    path = os.path.join(data_dir, SECURE_FILE)
    if os.path.exists(path):
        return
    os.makedirs(data_dir, exist_ok=True)
    print("Aucun administrateur défini. Création d’un compte administrateur.")
    username = input("Identifiant admin: ").strip()
    while not username:
        username = input("Identifiant admin: ").strip()
    while True:
        pwd1 = getpass.getpass("Mot de passe: ")
        pwd2 = getpass.getpass("Confirmer le mot de passe: ")
        if pwd1 != pwd2:
            print("Les mots de passe ne correspondent pas. Réessayez.")
            continue
        if len(pwd1) < 6:
            print("Mot de passe trop court (>= 6 caractères)")
            continue
        break
    salt = secrets.token_bytes(16)
    hpw = _hash_password(pwd1, salt)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"username": username, "salt": salt.hex(), "hash": hpw}, f)
    print("Administrateur créé.")


def verify_admin(data_dir: str, username: str, password: str) -> bool:
    path = os.path.join(data_dir, SECURE_FILE)
    if not os.path.exists(path):
        # Par sécurité, refuser si le fichier n’existe pas
        return False
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    if obj.get("username") != username:
        return False
    salt = bytes.fromhex(obj["salt"]) if isinstance(obj.get("salt"), str) else obj["salt"]
    expected = obj.get("hash")
    return _hash_password(password, salt) == expected
