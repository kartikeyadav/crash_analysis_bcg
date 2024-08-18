# Crash Analysis Project

## Overview

This project performs various analytics on crash data using Apache Spark. The analysis is conducted using Data Frame APIs and follows best practices in software engineering. The project structure is modular and config-driven, ensuring easy maintenance and scalability.

## Project Structure
```sh
CrashAnalysisProject/
│
├── src/                        # Source code directory
│   ├── main.py                 # Main entry point for the application
│   ├── data_ingestion/         # Data ingestion module
│   │   ├── __init__.py         # Initialization file for data_ingestion module
│   │   └── data_ingestion.py   # Contains data ingestion logic
│   ├── analytics/              # Analytics module
│   │   ├── __init__.py         # Initialization file for analytics module
│   │   └── car_crash_analysis.py # Contains analytics functions
│   ├── config.yml              # Configuration file for input and output paths
│
├── inputs/                     # Input data files directory
│   ├── Charges_use.csv          # Example input CSV files
│   ├── Damages_use.csv
│   └── ...                      # Other input files
│
├── outputs/                    # Output results directory
│   ├── sol1.csv                 # Example output CSV files
│   ├── sol2.csv
│   └── ...                      # Other output files
│
├── .gitignore                  # Git ignore file
├── requirements.txt            # Python package dependencies
└── README.md                   # Project documentation
```

## Setup

### Prerequisites

- Python 3.x
- Apache Spark
- Java 8 or later

### Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/kartikeyadav/crash_analysis_bcg.git
    ```

2. Navigate to the project directory:

    ```sh
    cd CrashAnalysisProject/src
    ```

3. Install required Python packages:

    ```sh
    pip install -r requirements.txt
    ```

4. Ensure that Apache Spark is properly installed and configured.

## Configuration

Update the `config.yml` file to set the paths for input and output data files. This file should include:

- `input_path`: Path to the directory containing input CSV files.
- `output_path`: Path to the directory where output results will be saved.

## Running the Application

To execute the application, use the following command:

```sh
spark-submit --conf spark.driver.extraClassPath=<path_to_spark_jars> src/main.py
Analytics
The application performs the following analyses:

Number of crashes where males killed > 2:

Analysis to find crashes with more than 2 male deaths.
Count of two-wheelers booked for crashes:

Analysis to determine the number of two-wheelers involved in crashes.
Top 5 Vehicle Makes with driver deaths and non-deployed airbags:

Determines the top 5 vehicle makes where the driver died and airbags did not deploy.
Count of vehicles with valid licenses involved in hit-and-run:

Analysis to count vehicles with valid licenses involved in hit-and-run incidents.
State with the highest number of accidents without female involvement:

Finds the state with the most accidents where no female was involved.
Top 3rd to 5th VEH_MAKE_IDs contributing to injuries including death:

Determines the top 3rd to 5th vehicle makes contributing to injuries and deaths.
Top ethnic user group for each body style involved in crashes:

Analysis of the top ethnic user group for each unique body style.
Top 5 Zip Codes with highest crashes involving alcohol:

Identifies the top 5 zip codes with crashes where alcohol was a contributing factor.
Count of distinct Crash IDs with no damaged property and high damage level:

Analysis of crash IDs with no damage and high damage levels, where the car has insurance.
Top 5 Vehicle Makes with speeding offences, licensed drivers, and specific vehicle characteristics:

Determines the top 5 vehicle makes involved in speeding offences, with licensed drivers and specific vehicle attributes.
License
This project is licensed under the MIT License - see the LICENSE file for details.

Contact
For any questions or feedback, please contact kartikeyadav.
