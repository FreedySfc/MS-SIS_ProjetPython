# Mini Moteur de Base de Données en Python

Ce projet implémente un mini moteur de base de données en Python (librairie standard côté serveur) avec:
- stockage binaire par table
- exécution d’un sous-ensemble SQL
- serveur TCP avec authentification administrateur (fichier sécurisé `secure.db`)
- clients: CLI et GUI (PySide6)
- tests unitaires

## Architecture

- `minidb/`
  - `auth/` — gestion admin, hachage, fichier sécurisé
  - `engine/` — moteur d’exécution SQL (validation types, opérations)
  - `server/` — serveur TCP (protocole JSON ligne-à-ligne)
  - `sql/` — parser SQL minimal (sous-ensemble)
  - `storage/` — format binaire, lecture/écriture de tables
  - `clients/` — `cli.py` (console), `gui.py` là j'ai préalablement créé un environnemment virtuel avant de faire pip install Pyside6 pour l'interface pour les commandes dans la bd
  - `types.py` — types supportés, vérifications

## Format binaire de stockage

Chaque table est stockée dans un fichier: `table_<nom>.db`.

Structure:
1. En-tête (header)
   - MAGIC: 8 octets `b"MINIDB\x00"`
   - VERSION: 1 octet (actuel: 1)
   - Longueur schéma (uint32, big endian)
   - Schéma JSON UTF-8 (définition: nom table, colonnes avec types; chaque colonne inclut `name` et `type` parmi `INT`, `FLOAT`, `TEXT`, `BOOL`, `SERIAL`) ; la colonne `_id` de type `SERIAL` est toujours présente
2. Enregistrements (append-only)
   - TOMBSTONE: 1 octet (0 = actif, 1 = supprimé)
   - ID: 16 octets (UUID v4 pour `SERIAL`)
   - Valeurs colonnes dans l’ordre du schéma (hors `_id` qui est l’ID ci-dessus)
     - INT: entier signé 8 octets (big endian)
     - FLOAT: double IEEE-754 8 octets (big endian)
     - BOOL: 1 octet (0/1)
     - TEXT: longueur uint32 big endian + octets UTF-8

Justification:
- En-tête auto-descriptif (JSON) facilitant évolutivité et introspection
- Alignement simple et déterministe, sans padding arbitraire
- Append-only: écriture séquentielle, mises à jour via nouvelle version + tombstone éventuelle; lecture reconstitue l’état courant
- `_id` en UUID évite collisions et simplifie génération côté moteur

## Protocoles et API

- Serveur TCP (texte):
  - Connexion: envoyer une ligne JSON `{action: "auth", username, password}`
  - Requête: envoyer une ligne de texte avec SQL
  - Réponse: une ligne JSON `{status: "ok"|"error", message, data?}`

## SQL supporté (sous-ensemble)
- `CREATE TABLE table (col TYPE, ...)`
- `DROP TABLE table`
- `DESCRIBE table`
- `INSERT INTO table VALUES (...)`
- `INSERT INTO table (c1, c2, ...) VALUES (...)`
- `SELECT cols FROM table [WHERE ...] [ORDER BY col [ASC|DESC]] [LIMIT n OFFSET m]`
- `UPDATE table SET col=value [, ...] [WHERE ...]`
- `DELETE FROM table [WHERE ...]`

WHERE: opérateurs `=, !=, <, <=, >, >=`, `AND`, `OR`, optionnel `NOT`, parenthèses basiques.

## Lancer le serveur

```bash
python -m minidb.server.tcp_server --host 127.0.0.1 --port 5555 --data-dir ./data
```
- Si `secure.db` manque, le serveur demandera la création d’un administrateur (login/mot de passe) via la console (hachage PBKDF2-HMAC).

## Client CLI

```bash
python -m minidb.clients.cli --host 127.0.0.1 --port 5555 --user freedy
```
Saisir le mot de passe (le mien c'est freedy), puis taper des requêtes SQL.

## Client GUI (PySide6)
- Requiert `PySide6` installé (`pip install PySide6`).
```bash
python -m minidb.clients.gui
```

## Tests

```bash
python -m unittest discover -s tests -p "test_*.py"
```

