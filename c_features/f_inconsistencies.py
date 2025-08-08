import pandas as pd
import sqlite3
import os
from typing import Optional, Dict, List


class InconsistenciesProcessor:
    """
    A class to analyze data inconsistencies and business rule violations.
    """
    
    def __init__(self, staging_csv_path: Optional[str] = None):
        """
        Initialize the inconsistencies processor.
        
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
    
    def analyze_churned_without_signed(self) -> pd.DataFrame:
        """
        Q3: Find clients who churned but never signed.
        
        Returns:
            pd.DataFrame: Clients with churn events but no signed events
        """
        print("Analyzing Q3: Churned clients who never signed...")
        
        query = """
        WITH client_events AS (
            SELECT 
                client_id,
                COUNT(CASE WHEN event_type = 'signed' THEN 1 END) as signed_count,
                COUNT(CASE WHEN event_type = 'churned' THEN 1 END) as churned_count,
                MIN(CASE WHEN event_type = 'churned' THEN event_date END) as first_churn_date
            FROM f_staging_events
            GROUP BY client_id
        )
        SELECT 
            client_id,
            'Q3_churned_without_signed' as inconsistency_type,
            'Client churned without ever signing' as description,
            first_churn_date as relevant_date,
            signed_count,
            churned_count
        FROM client_events
        WHERE churned_count > 0 AND signed_count = 0
        """
        
        result_df = pd.read_sql_query(query, self.conn)
        print(f"Found {len(result_df)} clients who churned without signing")
        return result_df
    
    def analyze_long_inactive_unsigned(self) -> pd.DataFrame:
        """
        Q6: Find unsigned clients with long inactivity (potential at-risk).
        
        Returns:
            pd.DataFrame: Unsigned clients who are inactive for >60 days
        """
        print("Analyzing Q6: Long inactive unsigned clients...")
        
        query = """
        WITH client_activity AS (
            SELECT 
                client_id,
                MAX(event_date) as last_event_date,
                COUNT(CASE WHEN event_type = 'signed' THEN 1 END) as signed_count,
                julianday('now') - julianday(MAX(event_date)) as days_since_last_event
            FROM f_staging_events
            GROUP BY client_id
        )
        SELECT 
            client_id,
            'Q6_long_inactive_unsigned' as inconsistency_type,
            'Unsigned client with long inactivity (>60 days) - potential at-risk' as description,
            last_event_date as relevant_date,
            CAST(days_since_last_event AS INTEGER) as days_inactive,
            signed_count
        FROM client_activity
        WHERE signed_count = 0 AND days_since_last_event > 60
        """
        
        result_df = pd.read_sql_query(query, self.conn)
        print(f"Found {len(result_df)} unsigned clients with long inactivity")
        return result_df
    
    def analyze_signed_without_applied(self) -> pd.DataFrame:
        """
        Q2: Find clients who signed without applying first.
        
        Returns:
            pd.DataFrame: Clients with signed events but no applied events
        """
        print("Analyzing Q2: Clients who signed without applying...")
        
        query = """
        WITH client_events AS (
            SELECT 
                client_id,
                COUNT(CASE WHEN event_type = 'applied' THEN 1 END) as applied_count,
                COUNT(CASE WHEN event_type = 'signed' THEN 1 END) as signed_count,
                MIN(CASE WHEN event_type = 'signed' THEN event_date END) as first_signed_date
            FROM f_staging_events
            GROUP BY client_id
        )
        SELECT 
            client_id,
            'Q2_signed_without_applied' as inconsistency_type,
            'Client signed without applying first' as description,
            first_signed_date as relevant_date,
            applied_count,
            signed_count
        FROM client_events
        WHERE signed_count > 0 AND applied_count = 0
        """
        
        result_df = pd.read_sql_query(query, self.conn)
        print(f"Found {len(result_df)} clients who signed without applying")
        return result_df
    
    def analyze_unknown_values(self) -> pd.DataFrame:
        """
        Analyze fields with unknown/missing values.
        
        Returns:
            pd.DataFrame: Records with unknown or problematic values
        """
        print("Analyzing unknown/missing values across fields...")
        
        query = """
        SELECT 
            client_id,
            record_id,
            event_type,
            event_date,
            'unknown_values' as inconsistency_type,
            CASE 
                WHEN plan = 'Unknown' THEN 'Plan field has Unknown value'
                WHEN sales_rep_id = -1 THEN 'Sales rep ID is -1 (missing/unknown)'
                WHEN region IS NULL OR region = '' THEN 'Region field is missing'
                WHEN marketing_channel IS NULL OR marketing_channel = '' THEN 'Marketing channel is missing'
                WHEN source_system IS NULL OR source_system = '' THEN 'Source system is missing'
                ELSE 'Other unknown value detected'
            END as description,
            plan,
            sales_rep_id,
            region,
            marketing_channel,
            source_system
        FROM f_staging_events
        WHERE plan = 'Unknown' 
           OR sales_rep_id = -1 
           OR region IS NULL OR region = ''
           OR marketing_channel IS NULL OR marketing_channel = ''
           OR source_system IS NULL OR source_system = ''
        ORDER BY client_id, event_date
        """
        
        result_df = pd.read_sql_query(query, self.conn)
        print(f"Found {len(result_df)} records with unknown/missing values")
        return result_df
    
    def analyze_event_sequence_violations(self) -> pd.DataFrame:
        """
        Analyze logical sequence violations (e.g., signed before applied, churned before signed).
        
        Returns:
            pd.DataFrame: Events that violate logical sequence
        """
        print("Analyzing event sequence violations...")
        
        query = """
        WITH client_event_dates AS (
            SELECT 
                client_id,
                MIN(CASE WHEN event_type = 'applied' THEN event_date END) as first_applied_date,
                MIN(CASE WHEN event_type = 'signed' THEN event_date END) as first_signed_date,
                MIN(CASE WHEN event_type = 'docs_submitted' THEN event_date END) as first_docs_date,
                MIN(CASE WHEN event_type = 'rejected' THEN event_date END) as first_rejected_date,
                MIN(CASE WHEN event_type = 'churned' THEN event_date END) as first_churned_date
            FROM f_staging_events
            GROUP BY client_id
        ),
        violations AS (
            SELECT 
                client_id,
                CASE 
                    WHEN first_signed_date < first_applied_date THEN 'signed_before_applied'
                    WHEN first_docs_date < first_applied_date THEN 'docs_submitted_before_applied'
                    WHEN first_rejected_date < first_applied_date THEN 'rejected_before_applied'
                    WHEN first_churned_date IS NOT NULL AND first_signed_date IS NOT NULL 
                         AND first_churned_date < first_signed_date THEN 'churned_before_signed'
                    ELSE NULL
                END as violation_type,
                first_applied_date,
                first_signed_date,
                first_docs_date,
                first_rejected_date,
                first_churned_date
            FROM client_event_dates
        )
        SELECT 
            client_id,
            'sequence_violation' as inconsistency_type,
            CASE violation_type
                WHEN 'signed_before_applied' THEN 'Client signed before applying'
                WHEN 'docs_submitted_before_applied' THEN 'Client submitted docs before applying'
                WHEN 'rejected_before_applied' THEN 'Client was rejected before applying'
                WHEN 'churned_before_signed' THEN 'Client churned before signing'
                ELSE 'Unknown sequence violation'
            END as description,
            violation_type,
            first_applied_date,
            first_signed_date,
            first_docs_date,
            first_rejected_date,
            first_churned_date
        FROM violations
        WHERE violation_type IS NOT NULL
        ORDER BY client_id
        """
        
        result_df = pd.read_sql_query(query, self.conn)
        print(f"Found {len(result_df)} sequence violations")
        return result_df
    
    def analyze_event_type_distribution(self) -> pd.DataFrame:
        """
        Analyze the distribution of event types to understand data patterns.
        
        Returns:
            pd.DataFrame: Event type counts and patterns
        """
        print("Analyzing event type distribution...")
        
        query = """
        SELECT 
            event_type,
            COUNT(*) as event_count,
            COUNT(DISTINCT client_id) as unique_clients,
            MIN(event_date) as earliest_date,
            MAX(event_date) as latest_date,
            ROUND(AVG(CAST(sales_rep_id AS FLOAT)), 2) as avg_sales_rep_id,
            GROUP_CONCAT(DISTINCT plan) as plans_involved,
            GROUP_CONCAT(DISTINCT region) as regions_involved
        FROM f_staging_events
        GROUP BY event_type
        ORDER BY event_count DESC
        """
        
        result_df = pd.read_sql_query(query, self.conn)
        print(f"Event type analysis completed. Found {len(result_df)} event types")
        return result_df
    
    def analyze_docs_submitted_pattern(self) -> pd.DataFrame:
        """
        Specifically analyze why docs_submitted events are rare.
        
        Returns:
            pd.DataFrame: Analysis of docs_submitted patterns
        """
        print("Analyzing docs_submitted event patterns...")
        
        # First check overall docs_submitted events
        docs_query = """
        SELECT 
            client_id,
            event_date as docs_submitted_date,
            plan,
            region,
            marketing_channel,
            sales_rep_id,
            source_system
        FROM f_staging_events
        WHERE event_type = 'docs_submitted'
        ORDER BY client_id
        """
        
        docs_df = pd.read_sql_query(docs_query, self.conn)
        
        # Check what happened to clients who should have submitted docs
        analysis_query = """
        WITH client_progressions AS (
            SELECT 
                client_id,
                COUNT(CASE WHEN event_type = 'applied' THEN 1 END) as applied_count,
                COUNT(CASE WHEN event_type = 'docs_submitted' THEN 1 END) as docs_count,
                COUNT(CASE WHEN event_type = 'signed' THEN 1 END) as signed_count,
                COUNT(CASE WHEN event_type = 'rejected' THEN 1 END) as rejected_count,
                MIN(CASE WHEN event_type = 'applied' THEN event_date END) as first_applied,
                MIN(CASE WHEN event_type = 'signed' THEN event_date END) as first_signed
            FROM f_staging_events
            GROUP BY client_id
        )
        SELECT 
            client_id,
            'docs_submitted_analysis' as inconsistency_type,
            CASE 
                WHEN applied_count > 0 AND docs_count = 0 AND signed_count > 0 THEN 'Applied and signed without docs submission'
                WHEN applied_count > 0 AND docs_count = 0 AND signed_count = 0 AND rejected_count = 0 THEN 'Applied but no docs submission (still pending?)'
                WHEN docs_count > 0 THEN 'Has docs submission event'
                ELSE 'Other pattern'
            END as description,
            applied_count,
            docs_count,
            signed_count,
            rejected_count,
            first_applied,
            first_signed
        FROM client_progressions
        WHERE applied_count > 0
        ORDER BY 
            CASE 
                WHEN docs_count > 0 THEN 1
                WHEN signed_count > 0 AND docs_count = 0 THEN 2
                ELSE 3
            END,
            client_id
        """
        
        analysis_df = pd.read_sql_query(analysis_query, self.conn)
        print(f"Docs submission analysis: {len(docs_df)} docs_submitted events, {len(analysis_df)} client patterns analyzed")
        
        return analysis_df
    
    def analyze_plan_inconsistencies(self) -> pd.DataFrame:
        """
        Analyze plan changes and inconsistencies within the same client.
        
        Returns:
            pd.DataFrame: Clients with plan inconsistencies
        """
        print("Analyzing plan inconsistencies...")
        
        query = """
        WITH client_plans AS (
            SELECT 
                client_id,
                COUNT(DISTINCT plan) as unique_plans,
                GROUP_CONCAT(DISTINCT plan) as all_plans,
                MIN(event_date) as first_event,
                MAX(event_date) as last_event
            FROM f_staging_events
            GROUP BY client_id
        )
        SELECT 
            client_id,
            'plan_inconsistency' as inconsistency_type,
            'Client has multiple different plans across events' as description,
            unique_plans,
            all_plans,
            first_event,
            last_event
        FROM client_plans
        WHERE unique_plans > 1
        ORDER BY unique_plans DESC, client_id
        """
        
        result_df = pd.read_sql_query(query, self.conn)
        print(f"Found {len(result_df)} clients with plan inconsistencies")
        return result_df

    def analyze_multiple_applications(self) -> pd.DataFrame:
        """
        Q1: Find clients with multiple application events.
        
        Returns:
            pd.DataFrame: Clients with multiple applied events and their details
        """
        print("Analyzing Q1: Clients with multiple applications...")
        
        # First get clients with multiple applications
        multiple_apps_query = """
        SELECT 
            client_id,
            COUNT(*) as application_count
        FROM f_staging_events
        WHERE event_type = 'applied'
        GROUP BY client_id
        HAVING COUNT(*) > 1
        """
        
        multiple_apps_df = pd.read_sql_query(multiple_apps_query, self.conn)
        
        if len(multiple_apps_df) == 0:
            print("No clients with multiple applications found")
            return pd.DataFrame(columns=['client_id', 'inconsistency_type', 'description', 
                                       'relevant_date', 'application_count', 'date_range_days'])
        
        # Get detailed information for these clients
        client_ids = multiple_apps_df['client_id'].tolist()
        client_ids_str = ','.join(map(str, client_ids))
        
        detail_query = f"""
        WITH app_details AS (
            SELECT 
                client_id,
                COUNT(*) as application_count,
                MIN(event_date) as first_application,
                MAX(event_date) as last_application,
                julianday(MAX(event_date)) - julianday(MIN(event_date)) as date_range_days
            FROM f_staging_events
            WHERE event_type = 'applied' AND client_id IN ({client_ids_str})
            GROUP BY client_id
        )
        SELECT 
            client_id,
            'Q1_multiple_applications' as inconsistency_type,
            'Client has multiple application events' as description,
            first_application as relevant_date,
            application_count,
            CAST(date_range_days AS INTEGER) as date_range_days
        FROM app_details
        """
        
        result_df = pd.read_sql_query(detail_query, self.conn)
        print(f"Found {len(result_df)} clients with multiple applications")
        return result_df
    
    def get_client_event_details(self, client_ids: List[int]) -> pd.DataFrame:
        """
        Get detailed event information for specific clients.
        
        Args:
            client_ids: List of client IDs to get details for
            
        Returns:
            pd.DataFrame: Detailed events for the specified clients
        """
        if not client_ids:
            return pd.DataFrame()
            
        client_ids_str = ','.join(map(str, client_ids))
        
        query = f"""
        SELECT 
            client_id,
            record_id,
            event_type,
            event_date,
            plan,
            region,
            marketing_channel,
            sales_rep_id,
            source_system,
            event_rank
        FROM f_staging_events
        WHERE client_id IN ({client_ids_str})
        ORDER BY client_id, event_date
        """
        
        return pd.read_sql_query(query, self.conn)
    
    def create_inconsistencies_summary(self) -> pd.DataFrame:
        """
        Create a comprehensive inconsistencies summary table.
        
        Returns:
            pd.DataFrame: Combined inconsistencies analysis
        """
        print("Creating comprehensive inconsistencies summary...")
        
        # Analyze all inconsistency types
        q3_results = self.analyze_churned_without_signed()
        q6_results = self.analyze_long_inactive_unsigned()
        q2_results = self.analyze_signed_without_applied()
        q1_results = self.analyze_multiple_applications()
        
        # New enhanced analyses
        unknown_values_results = self.analyze_unknown_values()
        sequence_violations_results = self.analyze_event_sequence_violations()
        event_distribution_results = self.analyze_event_type_distribution()
        docs_pattern_results = self.analyze_docs_submitted_pattern()
        plan_inconsistencies_results = self.analyze_plan_inconsistencies()
        
        # Combine all results
        all_inconsistencies = []
        
        for df in [q3_results, q6_results, q2_results, q1_results, 
                  unknown_values_results, sequence_violations_results, 
                  docs_pattern_results, plan_inconsistencies_results]:
            if not df.empty:
                all_inconsistencies.append(df)
        
        if all_inconsistencies:
            combined_df = pd.concat(all_inconsistencies, ignore_index=True, sort=False)
            
            # Add summary statistics
            summary_stats = combined_df['inconsistency_type'].value_counts().reset_index()
            summary_stats.columns = ['inconsistency_type', 'count']
            
            print(f"\nInconsistencies Summary:")
            for _, row in summary_stats.iterrows():
                print(f"  {row['inconsistency_type']}: {row['count']} cases")
            
            return combined_df
        else:
            print("No inconsistencies found")
            return pd.DataFrame()
    
    def export_to_csv(self, inconsistencies_df: pd.DataFrame) -> str:
        """
        Export inconsistencies analysis to CSV.
        
        Args:
            inconsistencies_df: The inconsistencies dataframe to export
            
        Returns:
            str: Path to the exported CSV file
        """
        print("Exporting inconsistencies data to CSV...")
        
        # Create output directory and write to CSV
        os.makedirs(self.output_dir, exist_ok=True)
        output_path = os.path.join(self.output_dir, 'f_inconsistencies.csv')
        inconsistencies_df.to_csv(output_path, index=False)
        
        print(f"Inconsistencies data written to '{output_path}'")
        return output_path
    
    def export_client_details(self, inconsistencies_df: pd.DataFrame) -> str:
        """
        Export detailed client events for inconsistent clients.
        
        Args:
            inconsistencies_df: The inconsistencies dataframe
            
        Returns:
            str: Path to the exported detailed CSV file
        """
        if inconsistencies_df.empty:
            print("No inconsistencies to export details for")
            return ""
            
        print("Exporting detailed client events for inconsistent clients...")
        
        # Get unique client IDs from inconsistencies
        client_ids = inconsistencies_df['client_id'].unique().tolist()
        
        # Get detailed events for these clients
        details_df = self.get_client_event_details(client_ids)
        
        # Create output path
        os.makedirs(self.output_dir, exist_ok=True)
        details_output_path = os.path.join(self.output_dir, 'f_inconsistencies_client_details.csv')
        details_df.to_csv(details_output_path, index=False)
        
        print(f"Client details written to '{details_output_path}'")
        return details_output_path
    
    def process_inconsistencies_analysis(self) -> tuple[str, str]:
        """
        Execute the complete inconsistencies analysis process.
        
        Returns:
            tuple: (Path to inconsistencies CSV, Path to client details CSV)
        """
        try:
            # Execute all steps in sequence
            self.load_staging_data()
            self.prepare_database()
            inconsistencies_df = self.create_inconsistencies_summary()
            
            summary_path = self.export_to_csv(inconsistencies_df)
            details_path = self.export_client_details(inconsistencies_df)
            
            # Export event distribution analysis separately (different structure)
            event_distribution_results = self.analyze_event_type_distribution()
            event_dist_path = ""
            if not event_distribution_results.empty:
                event_dist_path = os.path.join(self.output_dir, 'f_event_distribution_analysis.csv')
                event_distribution_results.to_csv(event_dist_path, index=False)
                print(f"Event distribution analysis saved to: {event_dist_path}")
            
            print("\nInconsistencies analysis completed successfully!")
            return summary_path, details_path
            
        except Exception as e:
            print(f"Error during inconsistencies analysis: {str(e)}")
            raise
        finally:
            if self.conn:
                self.conn.close()
    
    @staticmethod
    def get_business_questions() -> Dict[str, str]:
        """
        Get the business questions that drive this analysis.
        
        Returns:
            dict: Mapping of question codes to descriptions
        """
        return {
            "Q1": "Why do some clients have multiple 'applied' events? Are re-applications valid or system noise?",
            "Q2": "Can clients sign without applying first? (e.g., 1009 case)",
            "Q3": "Do some churned clients never signed? If so, what does 'churn' mean in that context?",
            "Q6": "Can we classify users with no 'churned' event but long inactivity as 'at risk'?",
            "NEW1": "What fields have unknown/missing values and how prevalent are they?",
            "NEW2": "Are there logical sequence violations (e.g., signed before applied)?",
            "NEW3": "Why is docs_submitted so rare? Do clients sign without submitting docs?",
            "NEW4": "Do clients change plans during their journey? Are plan fields consistent?",
            "NEW5": "What is the overall distribution of event types and patterns?"
        }


# Main execution
if __name__ == "__main__":
    processor = InconsistenciesProcessor()
    summary_file, details_file = processor.process_inconsistencies_analysis()
    
    # Print business questions for reference
    print("\nBusiness Questions Addressed:")
    for code, question in processor.get_business_questions().items():
        print(f"   {code}: {question}")
    
    print(f"\nFiles generated:")
    print(f"   Summary: {summary_file}")
    print(f"   Details: {details_file}")
