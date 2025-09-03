import mysql.connector
import math
import csv

# DBED Assignment 2
# Student ID:
# Name:

class DBEDAssign2():
    def __init__(self, name):
        self.name = name

    def disp(self):
        print(self.name)

    def setUp(self):
        # Use socket authentication in container/grader environment
        self.connection = mysql.connector.connect(database='postal')
        self.cursor = self.connection.cursor(buffered=True)

    def syncDB(self):
        self.connection.commit()

    def tearDown(self):
        self.cursor.close()
        self.connection.close()

    def show_all(self):
        query = "SELECT * FROM pcode;"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def select_by_pcode(self, pcode):
        query = "SELECT * FROM pcode WHERE postcode = %s;"
        self.cursor.execute(query, (pcode,))
        return self.cursor.fetchall()

    def insert_data(self, pcode, locality, state):
        query = "INSERT INTO pcode (postcode, locality, state) VALUES (%s, %s, %s);"
        self.cursor.execute(
            query,
            (str(pcode).strip()[:4], str(locality).strip()[:40], str(state).strip()[:3].upper())
        )

    def readData(self, fname):
        with open(fname, newline="", encoding="utf-8") as f:
            rdr = csv.reader(f)
            next(rdr, None)  # skip header
            for row in rdr:
                if len(row) >= 3 and row[0] and row[1] and row[2]:
                    self.insert_data(row[0], row[1], row[2])
        # Commit at the end
        self.syncDB()

    def entropyCalc(self):
        # total rows
        self.cursor.execute("SELECT COUNT(*) FROM pcode;")
        total = self.cursor.fetchone()[0]
        if total == 0:
            return 0.0

        # counts for last digit of postcode
        H = 0.0
        for d in "0123456789":
            self.cursor.execute("SELECT COUNT(*) FROM pcode WHERE RIGHT(postcode,1) = %s;", (d,))
            c = self.cursor.fetchone()[0]
            if c > 0:
                p = c / total
                H -= p * math.log2(p)
        return H
