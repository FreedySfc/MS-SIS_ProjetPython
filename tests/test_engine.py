import os
import shutil
import tempfile
import unittest

from minidb.engine.engine import DatabaseEngine


class TestEngine(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="minidb_")
        self.engine = DatabaseEngine(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_crud_flow(self):
        r = self.engine.execute("CREATE TABLE users (firstname TEXT, age INT, salary FLOAT, disabled BOOL)")
        self.assertEqual(r["status"], "ok")

        r = self.engine.execute("DESCRIBE users")
        self.assertEqual(r["status"], "ok")
        cols = [c["name"] for c in r["data"]["columns"]]
        self.assertIn("_id", cols)
        self.assertIn("firstname", cols)

        r = self.engine.execute("INSERT INTO users (firstname, age, salary, disabled) VALUES ('Richard', 25, 20000, false)")
        self.assertEqual(r["status"], "ok")
        rid = r["data"]["_id"]
        self.assertTrue(rid)

        r = self.engine.execute("SELECT firstname, age FROM users WHERE age < 30")
        self.assertEqual(r["status"], "ok")
        self.assertEqual(len(r["data"]), 1)
        self.assertEqual(r["data"][0]["firstname"], "Richard")

        r = self.engine.execute(f"UPDATE users SET salary = 30000, age = 40 WHERE _id = '{rid}'")
        self.assertEqual(r["status"], "ok")

        r = self.engine.execute("SELECT * FROM users WHERE age >= 40")
        self.assertEqual(len(r["data"]), 1)
        self.assertEqual(r["data"][0]["salary"], 30000.0)

        r = self.engine.execute(f"DELETE FROM users WHERE _id = '{rid}'")
        self.assertEqual(r["status"], "ok")

        r = self.engine.execute("SELECT * FROM users")
        self.assertEqual(len(r["data"]), 0)

        r = self.engine.execute("DROP TABLE users")
        self.assertEqual(r["status"], "ok")
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "table_users.db")))


if __name__ == "__main__":
    unittest.main()
