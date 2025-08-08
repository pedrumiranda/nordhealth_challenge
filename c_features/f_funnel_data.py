import pandas as pd
import sqlite3
import os
from typing import Optional


class FunnelDataProcessor:
    """
    A class to handle funnel analysis from staging events data.
    """
    
    def __init__(self, staging_csv_path: Optional[str] = None):
        """
        Initialize the funnel data processor.
        
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
    
    def create_funnel_analysis(self) -> pd.DataFrame:
        """
        Create comprehensive funnel analysis including all event types.
        
        Returns:
            pd.DataFrame: The funnel analysis data with all event types
        """
        print("Creating comprehensive funnel analysis...")
        
        funnel_sql = """
        SELECT
            client_id,
            MIN(CASE WHEN event_type = 'applied' THEN event_date END) AS applied_date,
            MIN(CASE WHEN event_type = 'docs_submitted' THEN event_date END) AS docs_submitted_date,
            MIN(CASE WHEN event_type = 'rejected' THEN event_date END) AS rejected_date,
            MIN(CASE WHEN event_type = 'signed' THEN event_date END) AS signed_date,
            MIN(CASE WHEN event_type = 'churned' THEN event_date END) AS churned_date
        FROM f_staging_events
        WHERE event_rank = 1
        GROUP BY client_id
        """
        
        funnel_df = pd.read_sql_query(funnel_sql, self.conn)
        print(f"Comprehensive funnel analysis created. Shape: {funnel_df.shape}")
        return funnel_df
    
    def export_to_csv(self, funnel_df: pd.DataFrame) -> str:
        """
        Export funnel analysis to CSV.
        
        Args:
            funnel_df: The funnel analysis dataframe to export
            
        Returns:
            str: Path to the exported CSV file
        """
        print("Exporting funnel data to CSV...")
        
        print(f"\nFunnel Analysis Table Preview:")
        print(funnel_df.head())
        
        # Create output directory and write to CSV
        os.makedirs(self.output_dir, exist_ok=True)
        output_path = os.path.join(self.output_dir, 'f_funnel_data.csv')
        funnel_df.to_csv(output_path, index=False)
        
        print(f"Funnel data written to '{output_path}'")
        return output_path
    
    def analyze_funnel_metrics(self, funnel_df: pd.DataFrame) -> dict:
        """
        Calculate comprehensive funnel metrics including all event types.
        
        Args:
            funnel_df: The funnel analysis dataframe
            
        Returns:
            dict: Dictionary containing funnel metrics
        """
        print("Calculating comprehensive funnel metrics...")
        
        total_clients = len(funnel_df)
        applied_clients = funnel_df['applied_date'].notna().sum()
        docs_submitted_clients = funnel_df['docs_submitted_date'].notna().sum()
        rejected_clients = funnel_df['rejected_date'].notna().sum()
        signed_clients = funnel_df['signed_date'].notna().sum()
        churned_clients = funnel_df['churned_date'].notna().sum()
        
        metrics = {
            'total_clients': total_clients,
            'applied_clients': applied_clients,
            'docs_submitted_clients': docs_submitted_clients,
            'rejected_clients': rejected_clients,
            'signed_clients': signed_clients,
            'churned_clients': churned_clients,
            'application_rate': applied_clients / total_clients if total_clients > 0 else 0,
            'docs_submission_rate': docs_submitted_clients / applied_clients if applied_clients > 0 else 0,
            'rejection_rate': rejected_clients / applied_clients if applied_clients > 0 else 0,
            'conversion_rate': signed_clients / applied_clients if applied_clients > 0 else 0,
            'churn_rate': churned_clients / signed_clients if signed_clients > 0 else 0,
            'active_clients': signed_clients - churned_clients if signed_clients >= churned_clients else 0
        }
        
        print(f"Comprehensive funnel metrics calculated: {metrics}")
        return metrics
    
    def process_funnel_analysis(self) -> tuple[str, dict]:
        """
        Execute the complete funnel analysis process.
        
        Returns:
            tuple: (Path to exported CSV file, Funnel metrics dictionary)
        """
        try:
            # Execute all steps in sequence
            self.load_staging_data()
            self.prepare_database()
            funnel_df = self.create_funnel_analysis()
            output_path = self.export_to_csv(funnel_df)
            metrics = self.analyze_funnel_metrics(funnel_df)
            
            print("\nFunnel analysis completed successfully!")
            return output_path, metrics
            
        except Exception as e:
            print(f"Error during funnel analysis: {str(e)}")
            raise
        finally:
            if self.conn:
                self.conn.close()
    
    @staticmethod
    def get_business_questions() -> list:
        """
        Get relevant business questions for funnel analysis.
        
        Returns:
            list: List of business questions
        """
        return [
            "Q1: Why do some clients have multiple 'applied' events? Are re-applications valid or system noise?",
            "Q2: Can clients sign without applying first? (e.g., 1009 case)",
            "Q4: Should we enforce funnel ordering in the data, or allow flexible lifecycles?"
        ]


# -------------------------------
# Execute the funnel analysis
# -------------------------------
if __name__ == "__main__":
    processor = FunnelDataProcessor()
    output_file, funnel_metrics = processor.process_funnel_analysis()
    
    # Print funnel metrics
    print("\nFunnel Metrics:")
    for metric, value in funnel_metrics.items():
        if 'rate' in metric:
            print(f"   {metric}: {value:.2%}")
        else:
            print(f"   {metric}: {value}")
    
    # Print business questions for reference
    print("\nBusiness Questions for Funnel Analysis:")
    for question in processor.get_business_questions():
        print(f"   {question}")
