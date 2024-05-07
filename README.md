# Distributed NoSQL Triple Store with Conflict Resolution and Data Consistency

This project aims to develop a prototype of a distributed NoSQL triple store capable of handling massive TSV datasets. The primary objectives include:

1. Design and implement a scalable and distributed NoSQL triple store for storing and querying TSV data (subject-predicate-object triples).
2. Utilize a client-server model where each server acts as both a client and a server, providing specific services to users.
3. Leverage state-based objects for efficient management of TSV triples across multiple servers.
4. Implement merge operations and conflict resolution mechanisms to ensure data consistency and integrity across distributed servers.
5. Support querying, updating, and synchronizing triples across multiple servers.
6. Integrate distinct data processing frameworks (MongoDB, MySQL, Hive) to showcase flexibility and interoperability.
7. Provide comprehensive documentation, testing methodologies, and a user-friendly interface.

## Features

- **Distributed Architecture**: The prototype comprises multiple servers hosting different NoSQL database management systems (e.g., MongoDB, MySQL), acting as independent nodes for storing and managing triples.
- **State-Based Objects**: Triples are represented as state-based objects (Triplet class) to track changes and timestamps efficiently.
- **Merge Operations and Conflict Resolution**: Servers can initiate merge operations to synchronize data, resolving conflicts based on timestamps and updating triple values accordingly.
- **Client Interface**: Users can interact with servers through a client interface, performing operations such as querying, updating, merging triples, and fetching logs.
- **Scalability and Flexibility**: The system is designed to handle large-scale TSV datasets and support seamless integration with existing infrastructure through the utilization of distinct frameworks.

## Technologies Used

- **Programming Language**: Python
- **Frameworks and Libraries**: PyMongo (MongoDB integration), mysql.connector (MySQL integration), PyHive (Hive integration)
- **Databases**: MongoDB (NoSQL), MySQL (SQL), Hive (Distributed SQL)

## Installation and Usage

1. Clone the repository: `git clone https://github.com/your-repo/distributed-nosql-triple-store.git`
2. Install the required dependencies: `pip install pymongo mysql-connector pyhive`
3. Configure the database connections in the codebase.
4. Run the application and follow the terminal-based user interface prompts to interact with the distributed NoSQL triple store.

## Documentation

Detailed documentation, including system architecture, implementation details, testing methodologies, and advanced features, is available in the project report.

## Contributing

Contributions to this project are welcome. If you encounter any issues or have suggestions for improvements, please open an issue or submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).
