
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
```

## Example

```bash
Calculate mean for INSTRUMENT1: 3.3675917318899224
Calculate mean for INSTRUMENT2 for November 2014: 9.413481179393493
Count distinct dates for INSTRUMENT3: 4942
Processed: Instrument: INSTRUMENT1, Value: 2.71205
Processed: Instrument: INSTRUMENT1, Value: 2.7153500000000004
Processed: Instrument: INSTRUMENT1, Value: 2.7203
Processed: Instrument: INSTRUMENT1, Value: 2.73295
Processed: Instrument: INSTRUMENT1, Value: 2.7354800000000004
Processed: Instrument: INSTRUMENT1, Value: 2.73075
Processed: Instrument: INSTRUMENT1, Value: 2.7357000000000005
Processed: Instrument: INSTRUMENT1, Value: 2.73515
Processed: Instrument: INSTRUMENT1, Value: 2.73295
Processed: Instrument: INSTRUMENT1, Value: 2.7907
Processed: Instrument: INSTRUMENT1, Value: 2.7940000000000005
Processed: Instrument: INSTRUMENT1, Value: 2.8039
Processed: Instrument: INSTRUMENT1, Value: 2.7918
Processed: Instrument: INSTRUMENT1, Value: 2.7973000000000003
Processed: Instrument: INSTRUMENT1, Value: 2.8017000000000003
Processed: Instrument: INSTRUMENT1, Value: 2.8121500000000004
Processed: Instrument: INSTRUMENT1, Value: 2.82535
Processed: Instrument: INSTRUMENT1, Value: 2.82337
Processed: Instrument: INSTRUMENT1, Value: 2.8264500000000004
Processed: Instrument: INSTRUMENT1, Value: 2.8259000000000003
Processed: Instrument: INSTRUMENT1, Value: 2.8347
Processed: Instrument: INSTRUMENT1, Value: 2.82975
Processed: Instrument: INSTRUMENT1, Value: 2.83415
Processed: Instrument: INSTRUMENT1, Value: 2.83745
Processed: Instrument: INSTRUMENT1, Value: 2.8259000000000003
Processed: Instrument: INSTRUMENT1, Value: 2.827
Processed: Instrument: INSTRUMENT1, Value: 2.8292
Processed: Instrument: INSTRUMENT1, Value: 2.8292
Processed: Instrument: INSTRUMENT1, Value: 2.8275500000000005
Processed: Instrument: INSTRUMENT1, Value: 2.82975
Processed: Instrument: INSTRUMENT1, Value: 2.8358
Processed: Instrument: INSTRUMENT1, Value: 2.8396500000000002
Processed: Instrument: INSTRUMENT1, Value: 2.83745
Processed: Instrument: INSTRUMENT1, Value: 2.83822
Processed: Instrument: INSTRUMENT1, Value: 2.8380000000000005
Processed: Instrument: INSTRUMENT1, Value: 2.8512000000000004
Processed: Instrument: INSTRUMENT1, Value: 2.8451500000000003
Processed: Instrument: INSTRUMENT1, Value: 2.84625
Processed: Instrument: INSTRUMENT1, Value: 2.7461500000000005
Processed: Instrument: INSTRUMENT1, Value: 2.7461500000000005
Processed: Instrument: INSTRUMENT1, Value: 2.7566
Processed: Instrument: INSTRUMENT1, Value: 2.7632000000000003
Processed: Instrument: INSTRUMENT1, Value: 2.77244
Processed: Instrument: INSTRUMENT1, Value: 2.7826700000000004
Processed: Instrument: INSTRUMENT1, Value: 2.7901500000000006
Processed: Instrument: INSTRUMENT1, Value: 2.78168
Processed: Instrument: INSTRUMENT1, Value: 2.7918
Processed: Instrument: INSTRUMENT1, Value: 2.7885000000000004
Processed: Instrument: INSTRUMENT1, Value: 2.8017000000000003
Processed: Instrument: INSTRUMENT1, Value: 2.80555
Processed: Instrument: INSTRUMENT1, Value: 2.8006
Processed: Instrument: INSTRUMENT1, Value: 2.8017000000000003
Processed: Instrument: INSTRUMENT1, Value: 2.8061000000000003
Processed: Instrument: INSTRUMENT1, Value: 2.80599
Processed: Instrument: INSTRUMENT1, Value: 2.79433
Processed: Instrument: INSTRUMENT1, Value: 2.7857500000000006
Processed: Instrument: INSTRUMENT1, Value: 2.79422
Processed: Instrument: INSTRUMENT1, Value: 2.8011500000000003
Processed: Instrument: INSTRUMENT1, Value: 2.7945500000000005
Processed: Instrument: INSTRUMENT1, Value: 2.7981800000000003
Processed: Instrument: INSTRUMENT1, Value: 2.8007100000000005
Processed: Instrument: INSTRUMENT1, Value: 2.7967500000000003
Processed: Instrument: INSTRUMENT1, Value: 2.7944400000000003
Processed: Instrument: INSTRUMENT1, Value: 2.7962000000000002
Processed: Instrument: INSTRUMENT1, Value: 2.7962000000000002
Processed: Instrument: INSTRUMENT1, Value: 2.7841000000000005
Processed: Instrument: INSTRUMENT1, Value: 2.8391
Processed: Instrument: INSTRUMENT1, Value: 2.8446000000000002
Processed: Instrument: INSTRUMENT1, Value: 2.8523
Processed: Instrument: INSTRUMENT1, Value: 2.8523
Processed: Instrument: INSTRUMENT1, Value: 2.85197
Processed: Instrument: INSTRUMENT1, Value: 2.8567
Processed: Instrument: INSTRUMENT1, Value: 2.8732
Processed: Instrument: INSTRUMENT1, Value: 2.8773800000000005
Processed: Instrument: INSTRUMENT1, Value: 2.8820000000000006
Processed: Instrument: INSTRUMENT1, Value: 2.88893
Processed: Instrument: INSTRUMENT1, Value: 2.8859600000000003
Processed: Instrument: INSTRUMENT1, Value: 2.8897
Processed: Instrument: INSTRUMENT1, Value: 2.8919
Processed: Instrument: INSTRUMENT1, Value: 2.88673
Processed: Instrument: INSTRUMENT1, Value: 2.8985
Processed: Instrument: INSTRUMENT1, Value: 2.9018
Processed: Instrument: INSTRUMENT1, Value: 2.90455
Processed: Instrument: INSTRUMENT1, Value: 2.9139000000000004
Processed: Instrument: INSTRUMENT1, Value: 2.9194
Processed: Instrument: INSTRUMENT1, Value: 2.9106
Processed: Instrument: INSTRUMENT1, Value: 2.9221500000000002
Processed: Instrument: INSTRUMENT1, Value: 2.9243500000000004
Processed: Instrument: INSTRUMENT1, Value: 2.9293
Processed: Instrument: INSTRUMENT1, Value: 2.9227000000000003
Processed: Instrument: INSTRUMENT1, Value: 2.92028
Processed: Instrument: INSTRUMENT1, Value: 2.9180800000000002
Processed: Instrument: INSTRUMENT1, Value: 2.9188500000000004
Processed: Instrument: INSTRUMENT1, Value: 2.91115
Processed: Instrument: INSTRUMENT1, Value: 2.9122500000000002
Processed: Instrument: INSTRUMENT1, Value: 2.92655
Processed: Instrument: INSTRUMENT1, Value: 2.9282000000000004
Processed: Instrument: INSTRUMENT1, Value: 2.93095
Processed: Instrument: INSTRUMENT1, Value: 2.9348000000000005
Processed: Instrument: INSTRUMENT1, Value: 2.9348000000000005
Processed: Instrument: INSTRUMENT1, Value: 2.93799
Processed: Instrument: INSTRUMENT1, Value: 2.9386500000000004
Processed: Instrument: INSTRUMENT1, Value: 2.95515
Processed: Instrument: INSTRUMENT1, Value: 2.96043
Processed: Instrument: INSTRUMENT1, Value: 2.96197
Processed: Instrument: INSTRUMENT1, Value: 2.96197
Processed: Instrument: INSTRUMENT1, Value: 2.97495
Processed: Instrument: INSTRUMENT1, Value: 2.9854000000000003
Processed: Instrument: INSTRUMENT1, Value: 2.97605
Processed: Instrument: INSTRUMENT1, Value: 2.9689
Processed: Instrument: INSTRUMENT1, Value: 2.9524000000000004
Processed: Instrument: INSTRUMENT1, Value: 2.9689
Processed: Instrument: INSTRUMENT1, Value: 2.97495
Processed: Instrument: INSTRUMENT1, Value: 2.9720900000000006
Processed: Instrument: INSTRUMENT1, Value: 2.9722000000000004
Processed: Instrument: INSTRUMENT1, Value: 2.9722000000000004
Processed: Instrument: INSTRUMENT1, Value: 2.98254
Processed: Instrument: INSTRUMENT1, Value: 2.9920000000000004
Processed: Instrument: INSTRUMENT1, Value: 3.0019000000000005
Processed: Instrument: INSTRUMENT1, Value: 3.0008000000000004
Processed: Instrument: INSTRUMENT1, Value: 2.9755000000000003
Processed: Instrument: INSTRUMENT1, Value: 2.9722000000000004
Processed: Instrument: INSTRUMENT1, Value: 2.98045
Processed: Instrument: INSTRUMENT1, Value: 2.99255
Processed: Instrument: INSTRUMENT1, Value: 2.99464
Processed: Instrument: INSTRUMENT1, Value: 2.99695
Processed: Instrument: INSTRUMENT1, Value: 2.9958500000000003
Processed: Instrument: INSTRUMENT1, Value: 2.9898000000000002
Processed: Instrument: INSTRUMENT1, Value: 2.9859500000000003
Processed: Instrument: INSTRUMENT1, Value: 2.9914500000000004
Processed: Instrument: INSTRUMENT1, Value: 2.9986
Processed: Instrument: INSTRUMENT1, Value: 3.0046500000000003
Processed: Instrument: INSTRUMENT1, Value: 3.00685
Processed: Instrument: INSTRUMENT1, Value: 3.00685
Processed: Instrument: INSTRUMENT1, Value: 2.99255
Processed: Instrument: INSTRUMENT1, Value: 2.9975000000000005
Processed: Instrument: INSTRUMENT1, Value: 3.00135
Processed: Instrument: INSTRUMENT1, Value: 3.003
Processed: Instrument: INSTRUMENT1, Value: 3.0041
Processed: Instrument: INSTRUMENT1, Value: 2.98925
Processed: Instrument: INSTRUMENT1, Value: 2.9960700000000005
Processed: Instrument: INSTRUMENT1, Value: 2.9700000000000006
Processed: Instrument: INSTRUMENT1, Value: 2.96505
Processed: Instrument: INSTRUMENT1, Value: 2.9777
Processed: Instrument: INSTRUMENT1, Value: 2.9815500000000004
Processed: Instrument: INSTRUMENT1, Value: 2.9810000000000003
Processed: Instrument: INSTRUMENT1, Value: 2.9711000000000003
Processed: Instrument: INSTRUMENT1, Value: 2.9755000000000003
Processed: Instrument: INSTRUMENT1, Value: 2.9799
Processed: Instrument: INSTRUMENT1, Value: 2.9741800000000005
Processed: Instrument: INSTRUMENT1, Value: 2.97176
Processed: Instrument: INSTRUMENT1, Value: 2.97165
Processed: Instrument: INSTRUMENT1, Value: 2.9683500000000005
Processed: Instrument: INSTRUMENT1, Value: 2.97605
Processed: Instrument: INSTRUMENT1, Value: 2.9799
Processed: Instrument: INSTRUMENT1, Value: 2.9766000000000004
Processed: Instrument: INSTRUMENT1, Value: 2.9903500000000003
Processed: Instrument: INSTRUMENT1, Value: 2.9958500000000003
Processed: Instrument: INSTRUMENT1, Value: 2.9986
Processed: Instrument: INSTRUMENT1, Value: 2.9914500000000004
Processed: Instrument: INSTRUMENT1, Value: 2.98815
Processed: Instrument: INSTRUMENT1, Value: 2.9942
Processed: Instrument: INSTRUMENT1, Value: 3.0035500000000006
Processed: Instrument: INSTRUMENT1, Value: 3.0063000000000004
Processed: Instrument: INSTRUMENT1, Value: 3.0145500000000003
Processed: Instrument: INSTRUMENT1, Value: 3.0129
Processed: Instrument: INSTRUMENT1, Value: 3.0140000000000007
Processed: Instrument: INSTRUMENT1, Value: 3.0079500000000006
Processed: Instrument: INSTRUMENT1, Value: 3.01125
Processed: Instrument: INSTRUMENT1, Value: 3.01345
Processed: Instrument: INSTRUMENT1, Value: 3.00267
Processed: Instrument: INSTRUMENT1, Value: 3.00267
Processed: Instrument: INSTRUMENT1, Value: 3.0085
Processed: Instrument: INSTRUMENT1, Value: 3.0101500000000003
Processed: Instrument: INSTRUMENT1, Value: 3.01565
Processed: Instrument: INSTRUMENT1, Value: 3.01565
Processed: Instrument: INSTRUMENT1, Value: 3.02445
...

