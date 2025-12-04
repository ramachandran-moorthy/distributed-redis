# Distributed Cache System

This is a distributed cache system inspired by Redis, designed to provide high availability and fault tolerance. It consists of several components that work together to cache and serve data efficiently.

## Table of Contents

- [System Architecture](#system-architecture)
- [Prerequisites](#prerequisites)
- [Database Setup](#database-setup)
- [Starting the System](#starting-the-system)
- [Running the Client](#running-the-client)
- [Notes](#notes)

## System Architecture

The system is composed of the following components:

- **Client**: Sends queries to the gateway server.
- **Gateway Server**: Handles client requests, checks the cache via the load balancer, and queries the backend server if necessary.
- **Load Balancer**: Distributes requests to the cache servers and manages cache hits and misses.
- **Cache Servers**: Store cached data in memory. Organized in clusters with primary and replica servers for redundancy.
- **Sentinel**: Monitors cache servers, handles failover by promoting replicas to primaries, and notifies the load balancer.
- **Backend Server**: A MySQL database that serves as the persistent storage.

Service discovery is handled by Consul, which allows components to find and communicate with each other dynamically.

## Prerequisites

- **Python 3.x**: Ensure Python is installed.
- **MySQL Server**: Installed and running.
- **Consul**: Installed and available
- **Python Dependencies**: Install via the following command:

  ```bash
  pip install grpcio mysql-connector-python python-consul
  ```

## Database Setup

1. **Create the Database**:

   ```bash
   mysql -u root -p
   ```

   ```sql
   CREATE DATABASE university;
   ```

2. **Create a User**:

   ```sql
   CREATE USER 'cacheuser'@'localhost' IDENTIFIED BY 'yourpassword';
   GRANT ALL PRIVILEGES ON university.* TO 'cacheuser'@'localhost';
   FLUSH PRIVILEGES;
   EXIT;
   ```

3. **Create Tables**:

   Connect to the `university` database:

   ```bash
   mysql -u cacheuser -p university
   ```

   Create a sample `student` table:

   ```sql
   CREATE TABLE student (
       id INT AUTO_INCREMENT PRIMARY KEY,
       first_name VARCHAR(50),
       last_name VARCHAR(50),
       program VARCHAR(100)
   );
   ```

   Optionally, insert sample data:

   ```sql
   INSERT INTO student (first_name, last_name, program) VALUES
   ('John', 'Doe', 'Computer Science'),
   ('Jane', 'Smith', 'Mathematics');
   ```

## Starting the System

Run each component in a separate terminal window in the following order:

1. **Start Consul**:

   ```bash
   consul agent -dev
   ```

2. **Start the Sentinel**:

   ```bash
   python sentinel.py
   ```

   Runs on port `70100`.

3. **Start the Load Balancer**:

   ```bash
   python load_balancer.py
   ```

   Runs on port `70000`.

4. **Start Cache Servers**:

   Start multiple cache servers with different `server_id`s. For example, to create a cluster (cluster 0) with one primary and two replicas:

   ```bash
   python cache_server.py 0
   python cache_server.py 1
   python cache_server.py 2
   ```

   Ports are `50051 + server_id` (e.g., `50051`, `50052`, `50053`). The sentinel assigns roles.

5. **Start the Backend Server**:

   ```bash
   python backend_server.py
   ```

   Runs on port `70200`.

6. **Start the Gateway Server**:

   ```bash
   python gateway_server.py
   ```

   Runs on port `60000`.

## Running the Client

Use the client to interact with the system. The client supports `create`, `read`, `update`, and `delete` operations.

- **Read all students**:

  ```bash
  python client.py student read
  ```

- **Create a new student**:

  ```bash
  python client.py student create '{"first_name": "Alice", "last_name": "Johnson", "program": "Physics"}'
  ```

- **Update a student** (assuming `id=1` exists):

  ```bash
  python client.py student update '{"id": 1, "program": "Engineering"}'
  ```

- **Delete a student** (assuming `id=1` exists):

  ```bash
  python client.py student delete '{"id": 1}'
  ```

## Notes

- **Scaling**: Add more cache servers with unique `server_id`s. Cluster ID is `server_id // 4` (e.g., `0-3` for cluster 0).
- **Fault Tolerance**: The sentinel promotes replicas to primaries on failure, ensuring high availability.
