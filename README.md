# Mini Moteur de Base de Données en Python

Ce projet implémente un mini moteur de base de données en Python (librairie standard côté serveur) avec:
- stockage binaire par table
- exécution d’un sous-ensemble SQL
- serveur TCP avec authentification administrateur (fichier sécurisé `secure.db`)
- clients: CLI et GUI (PySide6)
- tests unitaires J'ai pas encore fini

## Architecture

- `minidb/`
  - `auth/` — gestion admin, hachage, fichier sécurisé
  - `engine/` — moteur d’exécution SQL (validation types, opérations)
  - `server/` — serveur TCP (protocole JSON ligne-à-ligne)
  - `sql/` — parser SQL minimal (sous-ensemble)
  - `storage/` — format binaire, lecture/écriture de tables
  - `clients/` — `cli.py` (console), `gui.py` là j'ai préalablement créé un environnemment virtuel avant de faire pip install Pyside6 pour l'interface pour les commandes dans la bd
  - `types.py` — types supportés, vérifications


## Protocoles

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
## Les manipulations qu'on peut faire


Lancer le serveur :

python -m minidb.server.tcp_server --host 127.0.0.1 --port 5555 --data-dir ./data

Le client CLI

python -m minidb.clients.cli --host 127.0.0.1 --port 5555 --user freedy
Mot de passe: ****
{"status": "ok", "message": "Authentifié"}
Connecté. Tapez du SQL, ou 'exit' pour quitter.

Créer une table
CREATE TABLE users (firstname TEXT, age INT, salary FLOAT, disabled BOOL);

Insérer dans une table
INSERT INTO users (firstname, age, salary, disabled) VALUES ('Richard', 25, 20000, false);

Lire dans une table
SELECT firstname, age FROM users WHERE age < 30;

Mettre à jour une table
UPDATE users SET salary = 30000 WHERE firstname = 'Richard';

Supprimer
DELETE FROM users WHERE age > 40;

Supprimer la table
DROP TABLE users;
