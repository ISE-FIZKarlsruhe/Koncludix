# Alice: employee from January, manager from June, leaves at the end of August
Employee(Alice)           [2025-01-01, 2025-08-31]
Manager(Alice)            [2025-06-01, inf)
worksFor(Alice, ACME)     [2025-01-01, 2025-08-31]
# Bob: employee for two months, never a manager
Employee(Bob)             [2025-02-01, 2025-03-31]
# Carla: contractor (an employee by the TBox) from March
Contractor(Carla)         [2025-03-01, inf)
# Dan: a student, then an employee after studying
Student(Dan)              [2025-01-01, 2025-05-31]
Employee(Dan)             [2025-07-01, inf)
# Yan was an employee, and is a student again from May: as a former employee (derived) he is an Alumnus, which the ontology says is disjoint from Student
Employee(Yan)             [2025-02-01, 2025-03-31]
Student(Yan)              [2025-05-01, inf)
