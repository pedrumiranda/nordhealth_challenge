import pandas as pd
import sqlite3
import os
from datetime import datetime
from typing import Optional


class StagingEventsProcessor:
    """
    A class to handle the staging of events data with schema validation and data cleaning.
    """
    
    def __init__(self, input_csv_path: Optional[str] = None):
        """
        Initialize the staging processor.
        
        Args:
            input_csv_path: Path to the input CSV file. If None, uses default path.
        """
        if input_csv_path is None:
            self.input_csv_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), 
                'a_raw_data', 
                'Dummy dataset - Sheet1.csv'
            )
        else:
            self.input_csv_path = input_csv_path
            
        self.df = None
        self.conn = None
        self.output_dir = os.path.join(os.path.dirname(__file__), 'data_output')
        
    def load_data(self) -> pd.DataFrame:
        """
        Load data from the CSV file.
        
        Returns:
            pd.DataFrame: The loaded data
        """
        print(f"Loading data from: {self.input_csv_path}")
        self.df = pd.read_csv(self.input_csv_path)
        return self.df
    
    def validate_and_cast_schema(self) -> None:
        """
        Validate and cast data types according to the expected schema.
        
        This method handles:
        - Date conversion for event_date
        - Numeric casting for ID fields
        """
        print("Validating and casting schema...")

        # Convert date column
        self.df['event_date'] = pd.to_datetime(self.df['event_date'], errors='coerce')

        # Cast numeric columns
        numeric_columns = ['record_id', 'client_id', 'sales_rep_id']

        for col in numeric_columns:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                
        print(f"Schema validation completed. Shape: {self.df.shape}")
    
    def fill_missing_values(self) -> None:
        """
        Fill missing values with appropriate defaults.
        
        This method handles:
        - Plan field: 'unknown' string with title case
        - Source system: 'unknown' for missing values
        - Marketing channel: 'unknown' for missing values  
        - Sales rep ID: -1 for missing values
        """
        print("Filling missing values...")
        
        # Fill blanks with appropriate defaults
        if 'plan' in self.df.columns:
            self.df['plan'] = self.df['plan'].fillna('unknown').str.title()
            
        if 'source_system' in self.df.columns:
            self.df['source_system'] = self.df['source_system'].fillna('unknown')
            
        if 'marketing_channel' in self.df.columns:
            self.df['marketing_channel'] = self.df['marketing_channel'].fillna('unknown')
            
        if 'sales_rep_id' in self.df.columns:
            self.df['sales_rep_id'] = self.df['sales_rep_id'].fillna(-1).astype(int)
            
        print("Missing values filled successfully.")
    
    def create_staging_table(self) -> pd.DataFrame:
        """
        Create the staging table using SQL and return the DataFrame directly.
        
        Returns:
            pd.DataFrame: The staging events dataframe
        """
        print("Creating staging table...")
        
        # Create in-memory SQLite DB and load data
        self.conn = sqlite3.connect(':memory:')
        self.df.to_sql('raw_events', self.conn, index=False, if_exists='replace')
        
        # Create and query staging table in a single operation
        staging_sql = """
        SELECT
            record_id,
            client_id,
            event_type,
            event_date,
            plan,
            region,
            marketing_channel,
            sales_rep_id,
            source_system,
            
            -- Row number to handle duplicates
            ROW_NUMBER() OVER (
                PARTITION BY client_id, event_type
                ORDER BY event_date
            ) AS event_rank
        FROM raw_events
        WHERE event_date IS NOT NULL
        """
        
        # Execute the query and return DataFrame directly (no intermediate table creation)
        staging_df = pd.read_sql_query(staging_sql, self.conn)
        print(f"Staging table created successfully. Shape: {staging_df.shape}")
        return staging_df
    
    def export_to_csv(self, staging_df: pd.DataFrame) -> str:
        """
        Export the staging dataframe to CSV.
        
        Args:
            staging_df: The staging events dataframe to export
            
        Returns:
            str: Path to the exported CSV file
        """
        print("Exporting staging events to CSV...")
        
        print(f"\nStaging Events Table Preview:")
        print(staging_df.head())
        
        # Create output directory and write to CSV
        os.makedirs(self.output_dir, exist_ok=True)
        output_path = os.path.join(self.output_dir, 'f_staging_events.csv')
        staging_df.to_csv(output_path, index=False)
        
        print(f"✅ Staging events table written to '{output_path}'")
        return output_path
    
    def process_staging_events(self) -> str:
        """
        Execute the complete staging process.
        
     
        Returns:
            str: Path to the exported CSV file
        """
        try:
            # Execute all steps in sequence
            self.load_data()
            self.validate_and_cast_schema()
            self.fill_missing_values()
            staging_df = self.create_staging_table()
            output_path = self.export_to_csv(staging_df)
            
            print("Staging process completed successfully!")
            return output_path
            
        except Exception as e:
            print(f"Error during staging process: {str(e)}")
            raise
        finally:
            if self.conn:
                self.conn.close()


# -------------------------------
# Execute the staging process
# -------------------------------
if __name__ == "__main__":
    processor = StagingEventsProcessor()
    output_file = processor.process_staging_events()

