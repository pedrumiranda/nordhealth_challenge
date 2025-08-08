import pandas as pd
import sqlite3
import os
from typing import Optional


class ChurnDataProcessor:
    """
    A class to handle churn analysis from staging events data.
    """
    
    def __init__(self, staging_csv_path: Optional[str] = None):
        """
        Initialize the churn data processor.
        
        Args:
            staging_csv_path: Path to the staging events CSV file. If None, uses default path.
        """
        if staging_csv_path is None:
            self.staging_csv_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), 
                'b_staging', 
                'data_output', 
                'f_staging_events.csv'
            )
        else:
            self.staging_csv_path = staging_csv_path
            
        self.staging_df = None
        self.conn = None
        self.output_dir = os.path.join(os.path.dirname(__file__), 'data_output')
        
    def load_staging_data(self) -> pd.DataFrame:
        """
        Load staging data from CSV file.
        
        Returns:
            pd.DataFrame: The loaded staging data
        """
        print(f"Loading staging data from: {self.staging_csv_path}")
        self.staging_df = pd.read_csv(self.staging_csv_path)
        
        # Convert event_date to datetime
        self.staging_df['event_date'] = pd.to_datetime(self.staging_df['event_date'])
        print(f"Staging data loaded. Shape: {self.staging_df.shape}")
        return self.staging_df
    
    def prepare_database(self) -> None:
        """
        Create in-memory SQLite database and load staging data.
        """
        print("Preparing database...")
        self.conn = sqlite3.connect(':memory:')
        self.staging_df.to_sql('f_staging_events', self.conn, index=False, if_exists='replace')
        print("Database prepared successfully.")
    
    def create_churn_analysis(self) -> pd.DataFrame:
        """
        Create churn analysis using SQL to calculate days since various events.
        
        Returns:
            pd.DataFrame: The churn analysis data
        """
        print("Creating churn analysis...")
        
        churn_sql = """
        WITH last_events AS (
            SELECT
                client_id,
                MAX(event_date) AS last_event_date,
                MAX(CASE WHEN event_type = 'applied' THEN event_date END) AS applied_date,
                MAX(CASE WHEN event_type = 'signed' THEN event_date END) AS signed_date,
                MAX(CASE WHEN event_type = 'churned' THEN event_date END) AS churned_date
            FROM f_staging_events
            GROUP BY client_id
        ),
        last_event_details AS (
            SELECT 
                le.client_id,
                le.last_event_date,
                le.applied_date,
                le.signed_date,
                le.churned_date,
                se.event_type AS last_event_type
            FROM last_events le
            LEFT JOIN f_staging_events se ON le.client_id = se.client_id 
                AND le.last_event_date = se.event_date
                AND se.event_rank = 1
        )
        SELECT
            client_id,
            last_event_date,
            applied_date,
            signed_date,
            churned_date,
            last_event_type,
            CASE 
                WHEN churned_date IS NOT NULL THEN 1
                ELSE 0
            END AS is_churned,
            julianday('now') - julianday(last_event_date) AS days_since_last_event,
            CASE 
                WHEN signed_date IS NOT NULL THEN julianday('now') - julianday(signed_date)
                ELSE NULL 
            END AS days_since_signed
        FROM last_event_details
        """
        
        churn_df = pd.read_sql_query(churn_sql, self.conn)
        print(f"Churn analysis created. Shape: {churn_df.shape}")
        return churn_df
    
    def export_to_csv(self, churn_df: pd.DataFrame) -> str:
        """
        Export churn analysis to CSV.
        
        Args:
            churn_df: The churn analysis dataframe to export
            
        Returns:
            str: Path to the exported CSV file
        """
        print("Exporting churn data to CSV...")
        
        print(f"\nChurn Analysis Table Preview:")
        print(churn_df.head())
        
        # Create output directory and write to CSV
        os.makedirs(self.output_dir, exist_ok=True)
        output_path = os.path.join(self.output_dir, 'f_churn_data.csv')
        churn_df.to_csv(output_path, index=False)
        
        print(f"Churn data written to '{output_path}'")
        return output_path
    
    def process_churn_analysis(self) -> str:
        """
        Execute the complete churn analysis process.
        
        Returns:
            str: Path to the exported CSV file
        """
        try:
            # Execute all steps in sequence
            self.load_staging_data()
            self.prepare_database()
            churn_df = self.create_churn_analysis()
            output_path = self.export_to_csv(churn_df)
            
            print("\nChurn analysis completed successfully!")
            return output_path
            
        except Exception as e:
            print(f"Error during churn analysis: {str(e)}")
            raise
        finally:
            if self.conn:
                self.conn.close()
    
    @staticmethod
    def get_business_questions() -> list:
        """
        Get relevant business questions for churn analysis.
        
        Returns:
            list: List of business questions
        """
        return [
            "Q3: Do some churned clients never signed? If so, what does 'churn' mean in that context?",
            "Q5: Is there a typical duration between applied → signed → churned that defines healthy onboarding?",
            "Q6: Can we classify users with no 'churned' event but long inactivity as 'at risk'?"
        ]


# -------------------------------
# Execute the churn analysis
# -------------------------------
if __name__ == "__main__":
    processor = ChurnDataProcessor()
    output_file = processor.process_churn_analysis()
    
