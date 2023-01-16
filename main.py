import sqlite3
import sys
import atexit


# DTOs
class Hat:
    def __init__(self, id, topping, supplier, quantity):
        self.id = id
        self.topping = topping
        self.supplier = supplier
        self.quantity = quantity


class Supplier:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class Order:
    def __init__(self, id, location, hat):
        self.id = id
        self.location = location
        self.hat = hat


# DAOs
class _Hats:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, hat):
        self._conn.execute("""
            INSERT INTO hats (id, topping, supplier, quantity) VALUES (?, ?, ?, ?)
        """, [hat.id, hat. topping, hat.supplier, hat.quantity])

    def updateQunatity(self,hat_id):
        self._conn.execute("""
            UPDATE hats SET quantity = quantity -1 WHERE id = ?
        """, [hat_id])
        self._conn.execute("""
                   DELETE FROM hats WHERE quantity = 0 """)

    def find(self, hat_topping):
        c = self._conn.cursor()
        c.execute("""
            SELECT * FROM hats WHERE topping = ? ORDER BY supplier
        """, [hat_topping])

        return Hat(*c.fetchone())


class _Suppliers:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, supplier):
        self._conn.execute("""
              INSERT INTO suppliers (id, name) VALUES (?, ?)
          """, [supplier.id, supplier.name])

    def find(self, supplier_id):
        c = self._conn.cursor()
        c.execute("""
              SELECT * FROM suppliers WHERE id = ?
          """, [supplier_id])

        return Supplier(*c.fetchone())


class _Orders:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, _order):
        self._conn.execute("""
              INSERT INTO orders (id, location, hat) VALUES (?, ?, ?)
          """, [_order.id, _order.location, _order.hat])

    def find(self, order_id):
        c = self._conn.cursor()
        c.execute("""
              SELECT * FROM orders WHERE id = ?
          """, [order_id])

        return Order(*c.fetchone())


# Repository
class _Repository:
    def __init__(self, db):
        self._conn = sqlite3.connect(db)
        self.hats = _Hats(self._conn)
        self.suppliers = _Suppliers(self._conn)
        self.orders = _Orders(self._conn)

    def close(self):
        self._conn.commit()
        self._conn.close()

    def create_tables(self):
        self._conn.executescript("""
        CREATE TABLE hats(
        id          INT     PRIMARY KEY,
        topping     TEXT    NOT NULL,
        supplier    INT     NOT NULL,
        quantity    INT     NOT NULL,
        
        FOREIGN KEY(supplier)       REFERENCES suppliers(id)
        );
        
        CREATE TABLE suppliers(
        id      INT     PRIMARY KEY,
        name    TEXT    NOT NULL
        );
        
        CREATE TABLE orders(
        id          INT     PRIMARY KEY,
        location    TEXT    NOT NULL,
        hat         INT     NOT NULL,
        
        FOREIGN KEY(hat)        REFERENCES hats(id)
        );
        """)


def order(order_location, order_topping, file, repo, index):
    matchinghat = repo.hats.find(order_topping)
    repo.hats.updateQunatity(matchinghat.id)
    order1 = Order(index, order_location, matchinghat.id)
    repo.orders.insert(order1)
    name = repo.suppliers.find(matchinghat.supplier).name
    file.write("%s,%s,%s\n" % (order_topping, name, order_location))


def main(args):
    repo = _Repository(args[4])
    atexit.register(repo.close)
    index = 1
    repo.create_tables()
    inputfilename = args[1]
    with open(inputfilename, 'r') as inputfile:
        line = inputfile.readline()
        line = line.strip()
        nums = line.split(',')
        lines = inputfile.read().split('\n')
        for i in range(0, int(nums[0])):
            line = lines[i]
            str1 = line.split(',')
            hat = Hat(int(str1[0]), str1[1], int(str1[2]), int(str1[3]))
            repo.hats.insert(hat)

        for i in range(int(nums[0]), int(nums[1])+int(nums[0])):
            line = lines[i]
            str1 = line.split(',')
            sup = Supplier(int(str1[0]), str1[1])
            repo.suppliers.insert(sup)

    inputfile.close()

    output = open(args[3], "a+")
    ordersfile = args[2]
    with open(ordersfile, 'r') as orders:
        lines = orders.read().split('\n')
        for line in lines:
            line = line.strip()
            str1 = line.split(',')
            order(str1[0], str1[1], output, repo, index)
            index += 1

    orders.close()
    output.close()


if __name__ == '__main__':
    main(sys.argv)












