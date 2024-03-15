
# 1. Setting up Spark and Hadoop with PySpark - Installation Guide

This guide will walk you through the process of setting up Spark 3.0 and Hadoop on your system and integrating them with PySpark.

## 1. Install Java and Python

Make sure Java and Python are installed on your system.

## 2. Download Spark 3.0 and Hadoop

After downloading Spark and Hadoop, create folders on your C drive named 'Spark' and 'hadoop'. Move the downloaded 'spark-3.0.1-bin-hadoop2.7.tgz' file into the 'Spark' folder and extract it. Place the 'winutils.exe' file into the 'bin' folder inside the 'hadoop' folder.

## 3. Folder Setup

Create the necessary folders on your C drive as mentioned in the previous step.

## 4. Set Environment Variables

Configure environment variables by accessing 'Control Panel (View by Large Icons) > System > Advanced System Settings > Environment Variables'.

- Under User Variables (U), create 'HADOOP_HOME' and 'SPARK_HOME' variables and set their values according to Figure 3, referring to the locations created in the folder setup step.
- Select 'Path' under User Variables (U) and click 'Edit'. Add '%SPARK_HOME%\bin' and '%HADOOP_HOME%\bin' as new entries.

## 5. Install and Run PySpark

Open the command prompt (cmd) and run 'pip install pyspark'. Initially, you might encounter errors related to duplicate folder creation mentioned in step 2. Ensure correct paths are configured. If 'pyspark' command displays output similar to Figure 4 after installation, it indicates successful installation.

## 6. Integrate PyCharm with PySpark

Considering PyCharm's convenience over Jupyter Notebook, integrate PyCharm with PySpark as follows:

- Install PyCharm and create a project.
- Navigate to File > Settings > Project: (project name) > Project Structure.
- Click '+Add Content Root' on the right side, and add the following paths:

  (1) C:\Spark\spark-3.0.1-bin-hadoop2.7\python
  (2) C:\Spark\spark-3.0.1-bin-hadoop2.7\python\lib\py4j-0.10.9-src.zip

Once these paths are added, you may be prompted to install 'py4j'. Click to complete the integration of PyCharm with PySpark.

Now you're all set to work with PySpark seamlessly in your development environment!


# 2. Setting up Spark and Hadoop with PySpark - Installation Guide
# Instrument Data Processor

This script processes instrument data from an input file using PySpark and applies price modifiers fetched from an SQLite database.

## Setup Instructions

1. **Prepare Input Data:**
   - Ensure you have an input file named "instrument_data.txt" containing the instrument data.

2. **Set up Database:**
   - Make sure you have SQLite installed.
   - Create a database file named "instrument_price_modifiers.db" in the same directory as the script.

3. **Insert Sample Data:**
   - Open the script in a Python environment.
   - Run the script.
   - Sample data will be automatically inserted into the database.

4. **Execute the Script:**
   - Ensure Spark and PySpark are installed and configured.
   - Run the script.
   - The script will process the input data using Spark, applying any price modifiers fetched from the database.

5. **Review Output:**
   - The script will print the processed instrument data, including the instrument name and its modified value.

6. **Clean Up:**
   - After execution, the script will automatically close the database connection.

## Requirements

- Python 3.x
- PySpark
- SQLite

## Usage

1. Clone the repository or download the script and required files.
2. Follow the setup instructions mentioned above.
3. Execute the script using a Python environment.

## Example

```bash
python instrument_data_processor.py

