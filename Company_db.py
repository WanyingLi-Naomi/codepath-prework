import sqlite3
import pandas as pd
from abc import ABC, abstractmethod
from db_base import DBbase

# CSV files
hourly_employees_df = pd.read_csv('hourly_employees.csv')
salaried_employees_df = pd.read_csv('salaried_employees.csv')
managers_df = pd.read_csv('managers.csv')
executives_df = pd.read_csv('executives.csv')


def reset_database(cursor):
    # sql script for 5 tables
    sql = """
    DROP TABLE IF EXISTS Employee;
    DROP TABLE IF EXISTS HourlyEmployee;
    DROP TABLE IF EXISTS SalariedEmployee;
    DROP TABLE IF EXISTS Manager;
    DROP TABLE IF EXISTS Executive;

    CREATE TABLE Employee (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name TEXT,
        type TEXT,
        base_salary REAL
    );

    CREATE TABLE HourlyEmployee (
        employee_id INTEGER,
        hourly_rate REAL,
        hours_worked REAL,
        FOREIGN KEY (employee_id) REFERENCES Employee(id)
    );

    CREATE TABLE SalariedEmployee (
        employee_id INTEGER,
        annual_salary REAL,
        bonus REAL,
        FOREIGN KEY (employee_id) REFERENCES Employee(id)
    );

    CREATE TABLE Manager (
        employee_id INTEGER,
        annual_salary REAL,
        bonus REAL,
        stock_options REAL,
        FOREIGN KEY (employee_id) REFERENCES Employee(id)
    );

    CREATE TABLE Executive (
        employee_id INTEGER,
        annual_salary REAL,
        bonus REAL,
        stock_options REAL,
        company_car TEXT,
        FOREIGN KEY (employee_id) REFERENCES Employee(id)
    );
    """
    cursor.executescript(sql)


def populate_hourly_employees(cursor, hourly_employees_df):
    for _, row in hourly_employees_df.iterrows():
        cursor.execute("""
            INSERT INTO Employee (id, name, type, base_salary)
            VALUES (?, ?, ?, ?)
        """, (row['id'], row['name'], 'Hourly', row['hourly_rate'] * row['hours_worked']))
        cursor.execute("""
            INSERT INTO HourlyEmployee (employee_id, hourly_rate, hours_worked)
            VALUES (?, ?, ?)
        """, (row['id'], row['hourly_rate'], row['hours_worked']))


def populate_salaried_employees(cursor, salaried_employees_df):
    for _, row in salaried_employees_df.iterrows():
        cursor.execute("""
            INSERT INTO Employee (id, name, type, base_salary)
            VALUES (?, ?, ?, ?)
        """, (row['id'], row['name'], 'Salaried', row['annual_salary']))
        cursor.execute("""
            INSERT INTO SalariedEmployee (employee_id, annual_salary, bonus)
            VALUES (?, ?, ?)
        """, (row['id'], row['annual_salary'], row['bonus']))


def populate_managers(cursor, managers_df):
    for _, row in managers_df.iterrows():
        cursor.execute("""
            INSERT INTO Employee (id, name, type, base_salary)
            VALUES (?, ?, ?, ?)
        """, (row['id'], row['name'], 'Manager', row['annual_salary']))
        cursor.execute("""
            INSERT INTO Manager (employee_id, annual_salary, bonus, stock_options)
            VALUES (?, ?, ?, ?)
        """, (row['id'], row['annual_salary'], row['bonus'], row['stock_options']))


def populate_executives(cursor, executives_df):
    for _, row in executives_df.iterrows():
        cursor.execute("""
            INSERT INTO Employee (id, name, type, base_salary)
            VALUES (?, ?, ?, ?)
        """, (row['id'], row['name'], 'Executive', row['annual_salary']))
        cursor.execute("""
            INSERT INTO Executive (employee_id, annual_salary, bonus, stock_options, company_car)
            VALUES (?, ?, ?, ?, ?)
        """, (row['id'], row['annual_salary'], row['bonus'], row['stock_options'], row['company_car']))


# Initialize database
conn = sqlite3.connect('employee.db')
cursor = conn.cursor()

# Reset and populate database
reset_database(cursor)
populate_hourly_employees(cursor, hourly_employees_df)
populate_salaried_employees(cursor, salaried_employees_df)
populate_managers(cursor, managers_df)
populate_executives(cursor, executives_df)

# close connection
conn.commit()
conn.close()


# Hierachy of classses, abstract Employee class and its subclasses
class Employee(ABC):
    def __init__(self, id, name):
        self.id = id
        self.name = name

    @abstractmethod
    def calculate_pay(self):
        pass


class HourlyEmployee(Employee):
    def __init__(self, id, name, hourly_rate, hours_worked):
        super().__init__(id, name)
        self.hourly_rate = hourly_rate
        self.hours_worked = hours_worked

    def calculate_pay(self):
        return self.hourly_rate * self.hours_worked


class SalariedEmployee(Employee):
    def __init__(self, id, name, annual_salary, bonus):
        super().__init__(id, name)
        self.annual_salary = annual_salary
        self.bonus = bonus

    def calculate_pay(self):
        return self.annual_salary / 12 + self.bonus / 12


class Manager(SalariedEmployee):
    def __init__(self, id, name, annual_salary, bonus, stock_options):
        super().__init__(id, name, annual_salary, bonus)
        self.stock_options = stock_options


class Executive(Manager):
    def __init__(self, id, name, annual_salary, bonus, stock_options, company_car):
        super().__init__(id, name, annual_salary, bonus, stock_options)
        self.company_car = company_car


# Company class with CRUD operations & error handling
class Company:
    def __init__(self, db_name):
        self.db = DBbase(db_name)

    def hire_employee(self, employee):
        cursor = self.db.get_cursor
        try:
            cursor.execute("INSERT INTO Employee (id, name, type, base_salary) VALUES (?, ?, ?, ?)",
                           (employee.id, employee.name, type(employee).__name__, employee.calculate_pay()))
            if isinstance(employee, HourlyEmployee):
                cursor.execute("INSERT INTO HourlyEmployee (employee_id, hourly_rate, hours_worked) VALUES (?, ?, ?)",
                               (employee.id, employee.hourly_rate, employee.hours_worked))
            elif isinstance(employee, SalariedEmployee):
                cursor.execute("INSERT INTO SalariedEmployee (employee_id, annual_salary, bonus) VALUES (?, ?, ?)",
                               (employee.id, employee.annual_salary, employee.bonus))
            elif isinstance(employee, Manager):
                cursor.execute(
                    "INSERT INTO Manager (employee_id, annual_salary, bonus, stock_options) VALUES (?, ?, ?, ?)",
                    (employee.id, employee.annual_salary, employee.bonus, employee.stock_options))
            elif isinstance(employee, Executive):
                cursor.execute(
                    "INSERT INTO Executive (employee_id, annual_salary, bonus, stock_options, company_car) VALUES (?, "
                    "?, ?, ?, ?)",
                    (employee.id, employee.annual_salary, employee.bonus, employee.stock_options, employee.company_car))
            self.db.get_connection.commit()
            print(f"Employee {employee.name} hired successfully.")
        except Exception as e:
            print(f"Error hiring employee {employee.name}: {e}")

    def fire_employee(self, employee_id):
        cursor = self.db.get_cursor
        try:
            cursor.execute("DELETE FROM Employee WHERE id = ?", (employee_id,))
            cursor.execute("DELETE FROM HourlyEmployee WHERE employee_id = ?", (employee_id,))
            cursor.execute("DELETE FROM SalariedEmployee WHERE employee_id = ?", (employee_id,))
            cursor.execute("DELETE FROM Manager WHERE employee_id = ?", (employee_id,))
            cursor.execute("DELETE FROM Executive WHERE employee_id = ?", (employee_id,))
            self.db.get_connection.commit()
            print(f"Employee with ID {employee_id} fired successfully.")
        except Exception as e:
            print(f"Error firing employee with ID {employee_id}: {e}")

    def give_raise(self, employee_id, amount):
        cursor = self.db.get_cursor
        try:
            cursor.execute("UPDATE Employee SET base_salary = base_salary + ? WHERE id = ?", (amount, employee_id))
            self.db.get_connection.commit()
            print(f"Employee with ID {employee_id} received a raise of {amount}.")
        except Exception as e:
            print(f"Error giving raise to employee with ID {employee_id}: {e}")

    def list_employees(self):
        cursor = self.db.get_cursor
        try:
            cursor.execute("SELECT * FROM Employee")
            employees = cursor.fetchall()
            for emp in employees:
                print(emp)
        except Exception as e:
            print(f"Error listing employees: {e}")


# User Interactive Menu
def interactive_menu():
    company = Company('employee.db')
    while True:
        print("\nCompany Management System")
        print("1. Hire Employee")
        print("2. Fire Employee")
        print("3. Give Raise")
        print("4. List Employees")
        print("5. Exit")

        choice = input("Enter choice: ")
        if choice == '1':
            try:
                id = int(input("Enter ID: "))
                name = input("Enter name: ")
                emp_type = input("Enter type (Hourly, Salaried, Manager, Executive): ")
                if emp_type == "Hourly":
                    hourly_rate = float(input("Enter hourly rate: "))
                    hours_worked = float(input("Enter hours worked: "))
                    employee = HourlyEmployee(id, name, hourly_rate, hours_worked)
                elif emp_type == "Salaried":
                    annual_salary = float(input("Enter annual salary: "))
                    bonus = float(input("Enter bonus: "))
                    employee = SalariedEmployee(id, name, annual_salary, bonus)
                elif emp_type == "Manager":
                    annual_salary = float(input("Enter annual salary: "))
                    bonus = float(input("Enter bonus: "))
                    stock_options = float(input("Enter stock options: "))
                    employee = Manager(id, name, annual_salary, bonus, stock_options)
                elif emp_type == "Executive":
                    annual_salary = float(input("Enter annual salary: "))
                    bonus = float(input("Enter bonus: "))
                    stock_options = float(input("Enter stock options: "))
                    company_car = input("Enter company car: ")
                    employee = Executive(id, name, annual_salary, bonus, stock_options, company_car)
                else:
                    print("Invalid employee type.")
                    continue
                company.hire_employee(employee)
            except Exception as e:
                print(f"Error: {e}")

        elif choice == '2':
            try:
                employee_id = int(input("Enter employee ID to fire: "))
                company.fire_employee(employee_id)
            except Exception as e:
                print(f"Error: {e}")

        elif choice == '3':
            try:
                employee_id = int(input("Enter employee ID to give raise: "))
                amount = float(input("Enter raise amount: "))
                company.give_raise(employee_id, amount)
            except Exception as e:
                print(f"Error: {e}")

        elif choice == '4':
            company.list_employees()

        elif choice == '5':
            break

        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    interactive_menu()
