import json
import sys

try:
    from PySide6.QtWidgets import (
        QApplication,
        QWidget,
        QLabel,
        QLineEdit,
        QTextEdit,
        QPushButton,
        QGridLayout,
        QMessageBox,
    )
except Exception as exc:  # pragma: no cover
    raise SystemExit("PySide6 requis: pip install PySide6") from exc

import socket


class MiniDBGui(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("MiniDB Client")
        layout = QGridLayout(self)

        self.host = QLineEdit("127.0.0.1")
        self.port = QLineEdit("5555")
        self.user = QLineEdit("admin")
        self.password = QLineEdit()
        self.password.setEchoMode(QLineEdit.Password)
        self.sql = QTextEdit()
        self.output = QTextEdit()
        self.output.setReadOnly(True)
        self.btn = QPushButton("Exécuter")
        self.btn.clicked.connect(self.execute)

        layout.addWidget(QLabel("Hôte"), 0, 0)
        layout.addWidget(self.host, 0, 1)
        layout.addWidget(QLabel("Port"), 1, 0)
        layout.addWidget(self.port, 1, 1)
        layout.addWidget(QLabel("Utilisateur"), 2, 0)
        layout.addWidget(self.user, 2, 1)
        layout.addWidget(QLabel("Mot de passe"), 3, 0)
        layout.addWidget(self.password, 3, 1)
        layout.addWidget(QLabel("SQL"), 4, 0, 1, 2)
        layout.addWidget(self.sql, 5, 0, 1, 2)
        layout.addWidget(self.btn, 6, 0, 1, 2)
        layout.addWidget(QLabel("Réponse"), 7, 0, 1, 2)
        layout.addWidget(self.output, 8, 0, 1, 2)

    def execute(self):
        host = self.host.text().strip()
        port = int(self.port.text().strip())
        user = self.user.text().strip()
        pwd = self.password.text()
        sql = self.sql.toPlainText().strip()
        if not sql:
            QMessageBox.warning(self, "Erreur", "Veuillez saisir une requête SQL")
            return
        try:
            with socket.create_connection((host, port), timeout=5) as s:
                f = s.makefile("rwb")
                auth = {"action": "auth", "username": user, "password": pwd}
                f.write((json.dumps(auth) + "\n").encode("utf-8"))
                f.flush()
                line = f.readline().decode("utf-8")
                resp = json.loads(line)
                if resp.get("status") != "ok":
                    self.output.setPlainText(json.dumps(resp, ensure_ascii=False, indent=2))
                    return
                f.write((sql + "\n").encode("utf-8"))
                f.flush()
                line = f.readline().decode("utf-8")
                self.output.setPlainText(line.strip())
        except Exception as exc:
            QMessageBox.critical(self, "Erreur", str(exc))


def main():
    app = QApplication(sys.argv)
    w = MiniDBGui()
    w.resize(700, 600)
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
