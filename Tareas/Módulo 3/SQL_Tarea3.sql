
-- Creaci�n de la tabla Empleados.
CREATE TABLE Empleados(
Id INT PRIMARY KEY IDENTITY,
Nombre NVARCHAR(50),
Edad INT,
Departamento NVARCHAR(50)
);
--  Inserci�n de registros en la tabla Empleados
INSERT INTO Empleados(Nombre, Edad, Departamento)
VALUES 
('Juan P�rez', 28, 'IT'),
('Ana G�mez', 34, 'HR'),
('Luis Mart�nez', 29, 'Finance'),
('Marta Rodr�guez', 22, 'IT'),
('Carlos L�pez', 40, 'Marketing')

-- Actualizaci�n de registros en la tabla. Cambiar el departamento de IT por Research and Development
UPDATE Empleados
SET Departamento='Research and Development'
WHERE Departamento='IT'


-- Consultas de registros en la tabla.
SELECT * FROM Empleados;

SELECT * FROM Empleados
  ORDER BY Departamento;

  SELECT * FROM Empleados
  ORDER BY Nombre;

  -- Eliminaci�n de registros en la tabla. Eliminar a Carlos Lopez

  DELETE FROM Empleados WHERE Nombre='Carlos Lopez';

  SELECT * FROM Empleados;


