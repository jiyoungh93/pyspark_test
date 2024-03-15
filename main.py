from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
import sqlite3

class InstrumentPriceModifierDB:
    def __init__(self, database_path):
        self.database_path = database_path
        self.connection = sqlite3.connect(database_path)
        self.cursor = self.connection.cursor()
        self.create_table()

    def create_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS INSTRUMENT_PRICE_MODIFIER (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                NAME TEXT NOT NULL,
                MULTIPLIER REAL NOT NULL
            )
        """)
        self.connection.commit()

    def get_multiplier(self, instrument_name):
        self.cursor.execute("SELECT MULTIPLIER FROM INSTRUMENT_PRICE_MODIFIER WHERE NAME=?", (instrument_name,))
        row = self.cursor.fetchone()
        return row[0] if row else None

    def close(self):
        self.connection.close()

class CalculationEngine:
    @staticmethod
    def calculate_mean(df, instrument_name):
        """
        Calculate mean value for a specific instrument.
        """
        mean_value = df.filter(df.Instrument == instrument_name).agg({"Value": "avg"}).collect()[0][0]
        return mean_value

    @staticmethod
    def calculate_mean_for_month(df, instrument_name, month_year):
        """
        Calculate mean value for a specific instrument for a given month.
        """
        start_date = f"01-{month_year}"
        end_date = f"30-{month_year}"
        mean_value = df.filter((col("Instrument") == instrument_name) & (col("Date").between(start_date, end_date))).agg({"Value": "avg"}).collect()[0][0]
        return mean_value

    @staticmethod
    def calculate_statistic(df, instrument_name, statistic):
        """
        Calculate specified statistic for a specific instrument.
        """
        if statistic == "distinct_dates":
            result = df.filter(df.Instrument == instrument_name).agg(countDistinct("Date")).collect()[0][0]
            return result
        else:
            raise ValueError("Unsupported statistic type")

    @staticmethod
    def calculate_sum_of_newest_elements(df, instrument_name, num_elements):
        """
        Calculate sum of the newest elements for a specific instrument.
        """
        sum_value = df.filter(df.Instrument == instrument_name).orderBy(col("Date").desc()).limit(num_elements).agg({"Value": "sum"}).collect()[0][0]
        return sum_value

class InstrumentDataProcessor:
    def __init__(self, input_file, db):
        self.input_file = input_file
        self.db = db

    def process(self):
        spark = SparkSession.builder \
            .appName("InstrumentDataProcessor") \
            .getOrCreate()

        # Read the line from the input file
        df = spark.read.csv(self.input_file, header=False, inferSchema=True)
        df = df.withColumnRenamed("_c0", "Instrument") \
               .withColumnRenamed("_c1", "Date") \
               .withColumnRenamed("_c2", "Value")

        # Use CalculationEngine for various calculations
        mean_instrument1 = CalculationEngine.calculate_mean(df, "INSTRUMENT1")
        print(f"Calculate mean for INSTRUMENT1: {mean_instrument1}")

        mean_instrument2_november = CalculationEngine.calculate_mean_for_month(df, "INSTRUMENT2", "Nov-2014")
        print(f"Calculate mean for INSTRUMENT2 for November 2014: {mean_instrument2_november}")

        statistic_instrument3 = CalculationEngine.calculate_statistic(df, "INSTRUMENT3", "distinct_dates")
        print(f"Count distinct dates for INSTRUMENT3: {statistic_instrument3}")

        newest_10_sum = {}
        for instrument in df.select("Instrument").distinct().collect():
            instrument_name = instrument[0]
            if instrument_name not in ["INSTRUMENT1", "INSTRUMENT2", "INSTRUMENT3"]:
                newest_10_sum[instrument_name] = CalculationEngine.calculate_sum_of_newest_elements(df, instrument_name, 10)

        # Process data with price modifiers
        for row in df.collect():
            instrument_name = row["Instrument"]
            value = row["Value"]
            multiplier = self.db.get_multiplier(instrument_name)
            if multiplier:
                value *= multiplier
            print(f"Processed: Instrument: {instrument_name}, Value: {value}")

        spark.stop()


if __name__ == "__main__":
    input_file = "instrument_data.txt"
    database_path = "instrument_price_modifiers.db"

    # Create and insert sample data into database
    db = InstrumentPriceModifierDB(database_path)

    if not db.cursor.execute("SELECT COUNT(*) FROM INSTRUMENT_PRICE_MODIFIER").fetchone()[0]:
        db.cursor.execute("INSERT INTO INSTRUMENT_PRICE_MODIFIER (NAME, MULTIPLIER) VALUES (?, ?)",
                          ("INSTRUMENT1", 1.1))
        db.cursor.execute("INSERT INTO INSTRUMENT_PRICE_MODIFIER (NAME, MULTIPLIER) VALUES (?, ?)",
                          ("INSTRUMENT2", 0.9))
        db.cursor.execute("INSERT INTO INSTRUMENT_PRICE_MODIFIER (NAME, MULTIPLIER) VALUES (?, ?)",
                          ("INSTRUMENT3", 1.5))
        db.connection.commit()

    processor = InstrumentDataProcessor(input_file, db)
    processor.process()
    db.close()
