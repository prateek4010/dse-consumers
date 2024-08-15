# paidy-dse-consumers
This project is an ETL (Extract, Transform, Load) job in Scala/Scpark that processes consumer event data, applies transformations, and updates a Delta Lake table with the latest consumer information. The primary goal is to provide the marketing team with a current and accurate dataset of consumers for various email marketing campaigns. The ETL job to be run using Docker and a Makefile. Well, you can run it in your local as well.


## Project Structure

- **`src/main/scala/LoadConsumers.scala`**: Contains the main ETL logic for loading, transforming, and updating consumer data.
- **`src/main/scala/ConsumerUtils.scala`**: Contains utility functions used in the ETL process.
- **`src/main/scala/QualityChecks.scala`**: Contains utility functions for quality checks for the source and transformed data.
- **`test/resources/consumer_events/`**: Contains example test data for consumer events.
- **`Makefile`**: Defines commands to build, test, and run the application.
- **`Dockerfile`**: Defines the Docker image configuration for running the application.


## Dependencies

- **Apache Spark**: Version 3.5.1
- **Delta Lake**: Version 3.1.0
- **Scala**: Version 2.12.17


## Documentation
- **[Delta Lake Documentation](https://docs.delta.io/latest/index.html)**
- **[Apache Spark Documentation](https://spark.apache.org/docs/latest/)**
- **[Scala Documentation](https://docs.scala-lang.org/)**


## Setup
### Local Development
1. **Clone the Repository**
    ```bash
    git clone https://github.com/prateek4010/paidy-dse-consumers.git
    cd paidy-dse-consumers
    ```
2. **Build the Project**

    Use `sbt` to build the project:
    ```bash
    sbt clean compile
    ```
3. **Run the Application**

    You can run the application locally with or without arguments using `sbt`:
    ```bash
    sbt run
    sbt 'run "2023-11-01 00:00:00"'
    sbt 'run "2023-11-01 00:00:00" "2024-08-01 00:00:00"'
    ```
4. **Testing (TBD)**

    Unit tests will be located in the `src/test/scala` directory. Run tests using `sbt`:
    ```bash
    sbt test
    ```

### Makefile Commands

The `Makefile` provides shortcuts for common tasks:
- **Build & Run**: 
    
    Use the commands below: 
    ```bash
    make run
    make run START_DATE='"2023-11-01 00:00:00"'
    make run START_DATE='"2023-11-01 00:00:00"' END_DATE='"2024-08-01 00:00:00"'
    ```
- **Cleanup docker**: 

    Clean up the containers and images:
    ```bash
    make clean
    ```

- **Test (tbd)**: 

    ```bash
    make clean
    ```


## Configuration

The application expects datetime strings formatted as `yyyy-MM-dd HH:mm:ss` to define the time interval for processing data. The second argument defining the end of the interval is optional and defaults to the current time.


## Explanation / Execution Steps
- **parseArgs Method**: Handles argument parsing to ensure that the time interval is correctly interpreted.

- **createSparkSession Method**: Initializes a Spark session with necessary configurations.
- **createTargetTableIfNeeded Method**: Creates the target consumers table if it doesn't exist, based on the schema.
- **loadEventData Method**: Loads and filters the event-based data using the specified time interval.
- **transformData Method**: Transforms the event data into the required format for the consumers table.
- **deduplicating Data**: Drops duplicates based on the id column to prevent merge conflicts.
- **quality checks**: checks for consistency, uniqueness, accuracy and completeness of source and transformed data.
- **saveToTargetTable Method**: Saves the final DataFrame back into the target table, updating closed and non closed events accordingly.
- **running the Job**: Package the Scala project using sbt. Run the job with arguments specifying the start and optionally end date.


## Quality Checks

To ensure data quality, implement the following checks so that your system is fault-tolerant:
- **Schema Validation**: Ensure that data conforms to the target schema.

- **Data Completeness**: Verify that no important fields are missing.
- **Data Consistency**: Ensure that there are no conflicting records.
- **Data Uniqueness**: Ensure that there are no duplicates.
- **Data Timeliness**: Verify that timesatmp is in valid time window (not required here currently).
- **Data Accuracy**: Verify that the field is in correct format.


## Troubleshooting
- **Table Not Found**: If you encounter errors related to table not found, verify the table's metadata and path. Ensure that the `_delta_log` directory exists and is correctly populated.

- **Delta Table Issues**: If encountering issues with Delta tables, ensure compatibility between Spark and Delta Lake versions and verify the table's creation process.
- **Quality Checks**: These quality checks utility functions can be used for debugging and troubleshooting purposes.


## Assumptions
- **Event-Based Data**: The source data is event-based, meaning each record contains changes to a consumer's attributes rather than the full state.

- **Time Interval Filtering**: The ETL job will filter and process data based on the provided start and end dates. If no end date is provided, the job will use the current time.
- **Handling Duplicate Keys**: Duplicates in the source data are dropped based on the id column before merging with the target table.
- **Schema Evolution**: The schema of the target table is predefined, and any new columns or structural changes are not part of this task.
- **Delta Lake for Storage**: Delta Lake is used for the consumers table for efficient updates.
- **Consumer Status**: The consumer's status in the target table will be updated based on the latest event.


## Limitations / ToDo
- **Use of Delta Lake**: If you're using another format (e.g., Parquet, ORC), will need to adjust the saveAsTable and format options accordingly.

- **Complex Data**: You might need to further refine the transformation logic, especially if the event data includes complex update logic.
- **Performance Considerations**: If the event data is large, consider partitioning strategies for both source data and the Delta table.
- **Error Handling**: Add error handling to manage exceptions during the ETL process.
- **Test Coverage**: Ensure the ETL job is tested with various scenarios, including edge cases like missing data, duplicate records, and time intervals.
- **Monitoring & Alerting**: Track and log process attributes and performance to enhance it, send alerts and notifications to teams for quick response and setting up campaigns.
- **Deployment(if any)**: Automating the process of deployment using ci/cd pipelines and cloud services.


## Final Thoughts

This setup should give the marketing team the flexibility they need to select and update consumer data efficiently based on the latest available events. By this process of structuring the ETL job, transforming the data, and merging it into the Delta table, the code effectively handles the requirements of filtering based on time intervals, transforming the data, and performing the necessary upserts.
