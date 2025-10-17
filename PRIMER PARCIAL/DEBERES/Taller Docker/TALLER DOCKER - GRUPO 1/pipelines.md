# Pipelines de la práctica (MongoDB con Replica Set)

Este documento contiene los **Aggregation Pipelines** de la práctica, cada uno acompañado de un comentario que explica la lógica aplicada.  

---

## 1. Empleados con salario mayor al promedio de la empresa

### Método 1 — Con ventanas (`$setWindowFields`)

db.empleados.aggregate([
  {
    $setWindowFields: {
      partitionBy: null,
      output: {
        promEmpresa: { $avg: "$salario" }
      }
    }
  },
  { $match: { $expr: { $gt: ["$salario", "$promEmpresa"] } } }
])

### Método 2 — Sin ventanas ($expr)
// Calcular promedio global primero
var promEmpresa = db.empleados.aggregate([
  { $group: { _id: null, prom: { $avg: "$salario" } } }
]).toArray()[0].prom;

// Luego filtrar empleados
db.empleados.aggregate([
  { $match: { $expr: { $gt: ["$salario", promEmpresa] } } }
])

### 2. Departamentos sin empleados asignados
db.departamentos.aggregate([
  { $lookup: {
      from: "empleados",
      localField: "_id",
      foreignField: "departamento_id",
      as: "empleados"
  }},
  { $addFields: { count: { $size: "$empleados" } }},
  { $match: { count: 0 } }
])

### 3. Empleado con salario más alto

### - Opción A — $sort descendente + $limit

db.empleados.aggregate([
  { $sort: { salario: -1 } },
  { $limit: 1 }
])

### - Opción B — $group + $max

db.empleados.aggregate([
  { $group: { _id: null, maxSalario: { $max: "$salario" } } },
  { $lookup: {
      from: "empleados",
      let: { max: "$maxSalario" },
      pipeline: [
        { $match: { $expr: { $eq: ["$salario", "$$max"] } } }
      ],
      as: "empleadoTop"
  }},
  { $unwind: "$empleadoTop" },
  { $replaceRoot: { newRoot: "$empleadoTop" } }
])

### 4. Para cada empleado, mostrar el salario promedio de su departamento

### - Opción con ventanas ($setWindowFields)

db.empleados.aggregate([
  {
    $setWindowFields: {
      partitionBy: "$departamento_id",
      output: {
        promDep: { $avg: "$salario" }
      }
    }
  },
  { $project: { nombre: 1, salario: 1, departamento_id: 1, promDep: 1 } }
])

### Opción sin ventanas ($lookup)

db.empleados.aggregate([
  { $lookup: {
      from: "empleados",
      let: { dep: "$departamento_id" },
      pipeline: [
        { $match: { $expr: { $eq: ["$departamento_id", "$$dep"] } } },
        { $group: { _id: null, promDep: { $avg: "$salario" } } }
      ],
      as: "promDep"
  }},
  { $unwind: "$promDep" },
  { $project: { nombre: 1, salario: 1, departamento_id: 1, promDep: "$promDep.promDep" } }
])


### 5. Departamentos cuyo promedio salarial es mayor al promedio general

### - Opción A — Con ventanas

db.empleados.aggregate([
  {
    $setWindowFields: {
      partitionBy: "$departamento_id",
      output: {
        promDep: { $avg: "$salario" }
      }
    }
  },
  {
    $setWindowFields: {
      partitionBy: null,
      output: {
        promGlobal: { $avg: "$salario" }
      }
    }
  },
  { $match: { $expr: { $gt: ["$promDep", "$promGlobal"] } } },
  { $group: { _id: "$departamento_id" } }
])

### - Opción B — Sin ventanas

// Promedio global
var promGlobal = db.empleados.aggregate([
  { $group: { _id: null, prom: { $avg: "$salario" } } }
]).toArray()[0].prom;

// Promedios por departamento y comparación
db.empleados.aggregate([
  { $group: { _id: "$departamento_id", promDep: { $avg: "$salario" } } },
  { $match: { promDep: { $gt: promGlobal } } }
])

### 6. Ventas: sucursal “top” por mes

### - Método con $sort + $group

db.ventas.aggregate([
  { $group: { _id: { mes: "$_id.mes", sucursal: "$_id.sucursal" }, total: { $sum: "$total" } } },
  { $sort: { "_id.mes": 1, total: -1 } },
  { $group: { _id: "$_id.mes", topSucursal: { $first: "$_id.sucursal" }, maxTotal: { $first: "$total" } } }
])

### - Método con $topN (si está disponible en la versión)

db.ventas.aggregate([
  { $group: {
      _id: "$_id.mes",
      topSucursal: {
        $topN: {
          output: { sucursal: "$_id.sucursal", total: "$total" },
          sortBy: { total: -1 },
          n: 1
        }
      }
  }}
])