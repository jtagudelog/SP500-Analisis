
-- Creación de la tabla Empleados.
CREATE TABLE Empleados(
Id INT PRIMARY KEY IDENTITY,
Nombre NVARCHAR(50),
Edad INT,
Departamento NVARCHAR(50)
);
--  Inserción de registros en la tabla Empleados
INSERT INTO Empleados(Nombre, Edad, Departamento)
VALUES 
('Juan Pérez', 28, 'IT'),
('Ana Gómez', 34, 'HR'),
('Luis Martínez', 29, 'Finance'),
('Marta Rodríguez', 22, 'IT'),
('Carlos López', 40, 'Marketing')

-- Actualización de registros en la tabla. Cambiar el departamento de IT por Research and Development
UPDATE Empleados
SET Departamento='Research and Development'
WHERE Departamento='IT'


-- Consultas de registros en la tabla.
SELECT * FROM Empleados;

SELECT * FROM Empleados
  ORDER BY Departamento;

  SELECT * FROM Empleados
  ORDER BY Nombre;

  -- Eliminación de registros en la tabla. Eliminar a Carlos Lopez

  DELETE FROM Empleados WHERE Nombre='Carlos Lopez';

  SELECT * FROM Empleados;


