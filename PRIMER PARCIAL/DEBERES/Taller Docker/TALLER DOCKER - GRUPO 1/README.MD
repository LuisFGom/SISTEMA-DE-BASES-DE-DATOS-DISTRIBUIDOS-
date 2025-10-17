# Práctica: Base de Datos Distribuidas con Docker y MongoDB

Este proyecto implementa un Replica Set de MongoDB en Docker y permite realizar consultas con Aggregation Pipelines sobre un conjunto de datos de empleados, departamentos y ventas.

##  Pasos para levantar el entorno y cargar datos

### 1. Requisitos previos
- Docker Desktop instalado y en ejecución.
- Virtualización activada en BIOS.
- Archivos docker-compose.yml, departamentos.json, empleados.json, ventas.json.

### 2. Preparación de archivos
Colocar los archivos JSON dentro de la carpeta data/ junto al docker-compose.yml.

Estructura:
Practica_Docker/
├── docker-compose.yml
├── data/
    ├── departamentos.json
    ├── empleados.json
    ├── ventas.json

### 3. Levantar el clúster
docker compose up -d
docker ps

### 4. Inicializar el Replica Set
docker exec -it mongo1 mongosh --eval "rs.initiate({
  _id: 'rs0',
  members: [
    { _id: 0, host: 'mongo1:27017' },
    { _id: 1, host: 'mongo2:27017' },
    { _id: 2, host: 'mongo3:27017' }
  ]
})"

Verificar:
docker exec -it mongo1 mongosh --eval "rs.status()"

### 5. Importar datos
docker exec -it mongo1 mongoimport --db escuela --collection departamentos --file /data/departamentos.json
docker exec -it mongo1 mongoimport --db escuela --collection empleados --file /data/empleados.json
docker exec -it mongo1 mongoimport --db escuela --collection ventas --file /data/ventas.json

### 6. Acceder a la base de datos
docker exec -it mongo1 mongosh
use escuela

---

## Versiones usadas
- Windows 10/11 + PowerShell
- Docker Desktop v27.x
- Docker Compose v2.x
- MongoDB 7.0
- mongosh CLI

---

## Dificultades y soluciones
1. Problema de comillas en PowerShell al usar --eval.  
   Solución: usar comillas dobles externas y simples internas.

2. Error con redirección `< archivo.json`.  
   Solución: usar --file de mongoimport o montar un volumen.

3. Archivos no encontrados dentro del contenedor.  
   Solución: montar carpeta ./data en docker-compose.yml o usar docker cp.

4. Confusión entre comandos de MongoDB y PowerShell.  
   Solución: ejecutar en mongosh o con --eval.

---
