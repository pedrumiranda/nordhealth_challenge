import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.offline as pyo
import sys
import os
from datetime import datetime
import numpy as np

# Add the parent directories to the path to import our processors
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from c_features.f_inconsistencies import InconsistenciesProcessor


class InconsistenciesDashboard:
    """
    A specialized dashboard class for data inconsistencies analysis.
    Focuses on four key scenarios:
    1. Fields with unknown values
    2. Coherence between dates (sequence violations)
    3. Only one document signed event
    4. Multiple applied events
    """
    
    def __init__(self):
        """Initialize the inconsistencies dashboard."""
        self.inconsistencies_data = None
        self.client_details = None
        self.event_distribution = None
        
        # Set up plotly template
        self.template = "plotly_white"
        
        # Data paths
        self.features_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'c_features', 'data_output')
        self.output_dir = os.path.join(os.path.dirname(__file__), 'dashboards')
        
    def load_data(self):
        """Load inconsistencies data from features layer."""
        print("Loading inconsistencies data from features layer...")
        
        # Load inconsistencies summary
        inconsistencies_path = os.path.join(self.features_dir, 'f_inconsistencies.csv')
        self.inconsistencies_data = pd.read_csv(inconsistencies_path)
        
        # Load client details
        client_details_path = os.path.join(self.features_dir, 'f_inconsistencies_client_details.csv')
        self.client_details = pd.read_csv(client_details_path)
        
        # Load event distribution
        event_dist_path = os.path.join(self.features_dir, 'f_event_distribution_analysis.csv')
        self.event_distribution = pd.read_csv(event_dist_path)
        
        # Convert date columns
        date_columns = ['event_date', 'relevant_date', 'first_applied_date', 'first_signed_date', 
                       'first_docs_date', 'first_rejected_date', 'first_churned_date']
        
        for df in [self.inconsistencies_data, self.client_details]:
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
        
        print("Inconsistencies data loaded successfully!")
    
    def create_unknown_values_analysis(self):
        """Analyze fields with unknown values - Scenario 1."""
        print("Creating unknown values analysis...")
        
        # Filter for unknown values
        unknown_data = self.inconsistencies_data[
            self.inconsistencies_data['inconsistency_type'] == 'unknown_values'
        ].copy()
        
        if unknown_data.empty:
            # Create empty chart
            fig = go.Figure()
            fig.add_annotation(text="No unknown values found", 
                             xref="paper", yref="paper", x=0.5, y=0.5, 
                             showarrow=False, font=dict(size=16))
            fig.update_layout(title="Unknown Values Analysis", template=self.template, height=400)
            return fig
        
        # Count by description type
        unknown_counts = unknown_data['description'].value_counts()
        
        # Create bar chart
        fig = go.Figure(go.Bar(
            x=unknown_counts.index,
            y=unknown_counts.values,
            marker=dict(color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7']),
            text=unknown_counts.values,
            textposition='outside',
            hovertemplate='<b>%{x}</b><br>Count: %{y}<extra></extra>'
        ))
        
        fig.update_layout(
            title='Fields with Unknown Values',
            xaxis_title='Type of Unknown Value',
            yaxis_title='Number of Records',
            template=self.template,
            height=400,
            xaxis_tickangle=-45
        )
        
        return fig
    
    def create_sequence_violations_analysis(self):
        """Analyze date coherence/sequence violations - Scenario 2."""
        print("Creating sequence violations analysis...")
        
        # Filter for sequence violations
        sequence_data = self.inconsistencies_data[
            self.inconsistencies_data['inconsistency_type'] == 'sequence_violation'
        ].copy()
        
        if sequence_data.empty:
            # Create empty chart
            fig = go.Figure()
            fig.add_annotation(text="No sequence violations found", 
                             xref="paper", yref="paper", x=0.5, y=0.5, 
                             showarrow=False, font=dict(size=16))
            fig.update_layout(title="Date Sequence Violations", template=self.template, height=400)
            return fig
        
        # Create timeline visualization for sequence violations
        fig = go.Figure()
        
        colors = {'signed_before_applied': '#FF6B6B', 'docs_submitted_before_applied': '#4ECDC4',
                 'rejected_before_applied': '#45B7D1', 'churned_before_signed': '#96CEB4'}
        
        for _, row in sequence_data.iterrows():
            client_id = row['client_id']
            violation_type = row['violation_type']
            
            # Add events to timeline
            if pd.notna(row['first_applied_date']):
                fig.add_scatter(x=[row['first_applied_date']], y=[client_id], 
                               mode='markers', marker=dict(size=12, color='blue'),
                               name='Applied', showlegend=False,
                               hovertemplate=f'Client {client_id}: Applied<br>%{{x}}<extra></extra>')
            
            if pd.notna(row['first_signed_date']):
                fig.add_scatter(x=[row['first_signed_date']], y=[client_id], 
                               mode='markers', marker=dict(size=12, color=colors.get(violation_type, 'red')),
                               name='Signed', showlegend=False,
                               hovertemplate=f'Client {client_id}: Signed<br>%{{x}}<extra></extra>')
        
        fig.update_layout(
            title='Date Sequence Violations (Events Timeline)',
            xaxis_title='Date',
            yaxis_title='Client ID',
            template=self.template,
            height=400
        )
        
        return fig
    
    def create_docs_submitted_analysis(self):
        """Analyze the rare docs_submitted events - Scenario 3."""
        print("Creating docs_submitted analysis...")
        
        # Filter for docs submission analysis
        docs_data = self.inconsistencies_data[
            self.inconsistencies_data['inconsistency_type'] == 'docs_submitted_analysis'
        ].copy()
        
        if docs_data.empty:
            # Create empty chart
            fig = go.Figure()
            fig.add_annotation(text="No docs submission data found", 
                             xref="paper", yref="paper", x=0.5, y=0.5, 
                             showarrow=False, font=dict(size=16))
            fig.update_layout(title="Document Submission Analysis", template=self.template, height=400)
            return fig
        
        # Count by pattern
        docs_counts = docs_data['description'].value_counts()
        
        # Create pie chart
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']
        
        fig = go.Figure(go.Pie(
            labels=docs_counts.index,
            values=docs_counts.values,
            marker=dict(colors=colors),
            hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
        ))
        
        fig.update_layout(
            title='Document Submission Patterns',
            template=self.template,
            height=400
        )
        
        return fig
    
    def create_multiple_applications_analysis(self):
        """Analyze multiple application events - Scenario 4."""
        print("Creating multiple applications analysis...")
        
        # Filter for multiple applications
        apps_data = self.inconsistencies_data[
            self.inconsistencies_data['inconsistency_type'] == 'Q1_multiple_applications'
        ].copy()
        
        if apps_data.empty:
            # Create empty chart
            fig = go.Figure()
            fig.add_annotation(text="No multiple applications found", 
                             xref="paper", yref="paper", x=0.5, y=0.5, 
                             showarrow=False, font=dict(size=16))
            fig.update_layout(title="Multiple Applications Analysis", template=self.template, height=400)
            return fig
        
        # Create scatter plot of application count vs date range
        fig = go.Figure(go.Scatter(
            x=apps_data['application_count'],
            y=apps_data['date_range_days'],
            mode='markers+text',
            marker=dict(size=15, color='#FF6B6B', opacity=0.7),
            text=apps_data['client_id'],
            textposition='top center',
            hovertemplate='<b>Client %{text}</b><br>' +
                         'Applications: %{x}<br>' +
                         'Date Range: %{y} days<extra></extra>'
        ))
        
        fig.update_layout(
            title='Multiple Applications: Count vs Time Spread',
            xaxis_title='Number of Applications',
            yaxis_title='Date Range (Days)',
            template=self.template,
            height=400
        )
        
        return fig
    
    def create_event_distribution_summary(self):
        """Create event distribution summary showing why docs_submitted is rare."""
        print("Creating event distribution summary...")
        
        if self.event_distribution is None or self.event_distribution.empty:
            fig = go.Figure()
            fig.add_annotation(text="No event distribution data available", 
                             xref="paper", yref="paper", x=0.5, y=0.5, 
                             showarrow=False, font=dict(size=16))
            fig.update_layout(title="Event Distribution", template=self.template, height=400)
            return fig
        
        # Create bar chart of event counts
        fig = go.Figure(go.Bar(
            x=self.event_distribution['event_type'],
            y=self.event_distribution['event_count'],
            marker=dict(color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7']),
            text=self.event_distribution['event_count'],
            textposition='outside',
            hovertemplate='<b>%{x}</b><br>' +
                         'Count: %{y}<br>' +
                         'Unique Clients: %{customdata}<extra></extra>',
            customdata=self.event_distribution['unique_clients']
        ))
        
        fig.update_layout(
            title='Event Type Distribution (Highlighting docs_submitted Rarity)',
            xaxis_title='Event Type',
            yaxis_title='Number of Events',
            template=self.template,
            height=400
        )
        
        return fig
    
    def create_scenario_summary_table(self):
        """Create a comprehensive summary table of all four scenarios."""
        print("Creating scenario summary table...")
        
        # Prepare summary data
        scenarios_data = []
        
        # Scenario 1: Unknown Values
        unknown_count = len(self.inconsistencies_data[
            self.inconsistencies_data['inconsistency_type'] == 'unknown_values'
        ]) if not self.inconsistencies_data.empty else 0
        
        # Scenario 2: Sequence Violations
        sequence_count = len(self.inconsistencies_data[
            self.inconsistencies_data['inconsistency_type'] == 'sequence_violation'
        ]) if not self.inconsistencies_data.empty else 0
        
        # Scenario 3: Documents Submission
        docs_analysis = self.inconsistencies_data[
            self.inconsistencies_data['inconsistency_type'] == 'docs_submitted_analysis'
        ] if not self.inconsistencies_data.empty else pd.DataFrame()
        
        docs_has_submission = len(docs_analysis[docs_analysis['description'] == 'Has docs submission event']) if not docs_analysis.empty else 0
        docs_without_submission = len(docs_analysis[docs_analysis['description'] == 'Applied and signed without docs submission']) if not docs_analysis.empty else 0
        
        # Scenario 4: Multiple Applications
        multi_apps_count = len(self.inconsistencies_data[
            self.inconsistencies_data['inconsistency_type'] == 'Q1_multiple_applications'
        ]) if not self.inconsistencies_data.empty else 0
        
        # Build table data
        scenarios_data = [
            ['Scenario 1: Unknown Values', str(unknown_count), 'Records with missing/unknown field values'],
            ['Scenario 2: Date Sequence Violations', str(sequence_count), 'Events in wrong chronological order'],
            ['Scenario 3a: Has Document Submission', str(docs_has_submission), 'Clients who submitted documents'],
            ['Scenario 3b: Missing Document Submission', str(docs_without_submission), 'Clients signed without docs submission'],
            ['Scenario 4: Multiple Applications', str(multi_apps_count), 'Clients with multiple application events']
        ]
        
        # Create table
        fig = go.Figure(data=[go.Table(
            header=dict(
                values=['<b>Scenario</b>', '<b>Count</b>', '<b>Description</b>'],
                fill_color='#E6F3FF',
                align='left',
                font=dict(size=12, family='Arial'),
                height=40
            ),
            cells=dict(
                values=list(zip(*scenarios_data)),
                fill_color=[['#F8F9FA', '#FFFFFF'] * 3],
                align='left',
                font=dict(size=11, family='Arial'),
                height=35
            )
        )])
        
        fig.update_layout(
            title='Data Inconsistencies Summary - Four Key Scenarios',
            template=self.template,
            height=350,
            margin=dict(t=50, b=20, l=20, r=20)
        )
        
        return fig
    
    def create_problematic_events_table(self):
        """Create a detailed table showing the actual events with issues."""
        print("Creating problematic events table...")
        
        if self.inconsistencies_data.empty:
            fig = go.Figure()
            fig.add_annotation(text="No problematic events found", 
                             xref="paper", yref="paper", x=0.5, y=0.5, 
                             showarrow=False, font=dict(size=16))
            fig.update_layout(title="Problematic Events Details", template=self.template, height=400)
            return fig
        
        # Prepare events data for display
        events_data = []
        
        # Process each inconsistency type
        for _, row in self.inconsistencies_data.iterrows():
            client_id = row['client_id']
            issue_type = row['inconsistency_type']
            description = row['description']
            
            # Get relevant event details based on issue type
            if issue_type == 'unknown_values':
                # Show the specific event with unknown values
                event_date = row.get('event_date', '-')
                event_type = row.get('event_type', '-')
                plan = row.get('plan', '-')
                sales_rep_id = row.get('sales_rep_id', '-')
                
                events_data.append([
                    str(client_id),
                    'Unknown Values',
                    event_type,
                    str(event_date)[:10] if pd.notna(event_date) else '-',
                    str(plan),
                    str(sales_rep_id),
                    description
                ])
                
            elif issue_type == 'sequence_violation':
                # Show the violating events
                applied_date = row.get('first_applied_date', '-')
                signed_date = row.get('first_signed_date', '-')
                violation_type = row.get('violation_type', '-')
                
                events_data.append([
                    str(client_id),
                    'Sequence Violation',
                    violation_type.replace('_', ' ').title(),
                    f"Applied: {str(applied_date)[:10] if pd.notna(applied_date) else '-'}, Signed: {str(signed_date)[:10] if pd.notna(signed_date) else '-'}",
                    '-',
                    '-',
                    description
                ])
                
            elif issue_type == 'Q1_multiple_applications':
                # Show multiple applications info
                app_count = row.get('application_count', '-')
                date_range = row.get('date_range_days', '-')
                relevant_date = row.get('relevant_date', '-')
                
                events_data.append([
                    str(client_id),
                    'Multiple Applications',
                    f"{app_count} applications",
                    str(relevant_date)[:10] if pd.notna(relevant_date) else '-',
                    f"{date_range} days" if pd.notna(date_range) else '-',
                    '-',
                    description
                ])
                
            elif issue_type == 'docs_submitted_analysis':
                # Show docs submission patterns
                applied_count = row.get('applied_count', '-')
                docs_count = row.get('docs_count', '-')
                signed_count = row.get('signed_count', '-')
                first_applied = row.get('first_applied', '-')
                first_signed = row.get('first_signed', '-')
                
                events_data.append([
                    str(client_id),
                    'Document Pattern',
                    f"Applied: {applied_count}, Docs: {docs_count}, Signed: {signed_count}",
                    f"Applied: {str(first_applied)[:10] if pd.notna(first_applied) else '-'}",
                    f"Signed: {str(first_signed)[:10] if pd.notna(first_signed) else '-'}",
                    '-',
                    description
                ])
        
        if not events_data:
            # No data to display
            fig = go.Figure()
            fig.add_annotation(text="No events data available for display", 
                             xref="paper", yref="paper", x=0.5, y=0.5, 
                             showarrow=False, font=dict(size=16))
            fig.update_layout(title="Problematic Events Details", template=self.template, height=400)
            return fig
        
        # Sort by issue type and client ID
        events_data.sort(key=lambda x: (x[1], int(x[0])))
        
        # Define headers
        headers = [
            '<b>Client ID</b>',
            '<b>Issue Type</b>',
            '<b>Event Details</b>',
            '<b>Date Info</b>',
            '<b>Additional Info</b>',
            '<b>Sales Rep</b>',
            '<b>Description</b>'
        ]
        
        # Prepare data for table (transpose for plotly table format)
        table_data = list(zip(*events_data)) if events_data else [[] for _ in headers]
        
        # Create colors for rows based on issue type
        def get_row_color(issue_type):
            if 'Unknown Values' in issue_type:
                return '#FFEBEE'  # Light red
            elif 'Sequence Violation' in issue_type:
                return '#FFF3E0'  # Light orange
            elif 'Multiple Applications' in issue_type:
                return '#E3F2FD'  # Light blue
            elif 'Document Pattern' in issue_type:
                return '#F3E5F5'  # Light purple
            else:
                return '#F5F5F5'  # Light gray
        
        row_colors = [get_row_color(events_data[i][1]) for i in range(len(events_data))]
        
        fig = go.Figure(data=[go.Table(
            header=dict(
                values=headers,
                fill_color='#E6F3FF',
                align='center',
                font=dict(size=11, family='Arial'),
                height=35
            ),
            cells=dict(
                values=table_data,
                fill_color=[row_colors for _ in range(len(headers))],
                align='left',
                font=dict(size=10, family='Arial'),
                height=30
            )
        )])
        
        fig.update_layout(
            title='Problematic Events Details - Identified Issues',
            template=self.template,
            height=500,
            margin=dict(t=50, b=20, l=20, r=20)
        )
        
        return fig
        
    def create_inconsistencies_overview(self):
        """Create overview of inconsistencies by category."""
        if not self.inconsistencies_data.empty:
            category_counts = self.inconsistencies_data['inconsistency_type'].value_counts()
            
            colors = ['#E74C3C', '#F39C12', '#3498DB', '#2ECC71']
            
            fig = go.Figure(go.Pie(
                labels=category_counts.index,
                values=category_counts.values,
                marker=dict(colors=colors[:len(category_counts)]),
                hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
            ))
            
            fig.update_layout(
                title='Data Inconsistencies Overview',
                template=self.template,
                height=400
            )
            
            return fig
        else:
            fig = go.Figure()
            fig.add_annotation(
                text="No inconsistencies found",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16)
            )
            fig.update_layout(
                title='Data Inconsistencies Overview',
                template=self.template,
                height=400
            )
            return fig
    
    def create_business_questions_mapping(self):
        """Create mapping of business questions to inconsistencies."""
        if not self.inconsistencies_data.empty:
            # Map inconsistency types to business questions
            question_mapping = {
                'multiple_applications': 'Q1: Multiple applied events',
                'signed_without_applied': 'Q2: Signed without applying',
                'churned_without_signed': 'Q3: Churned without signing',
                'long_inactive_unsigned': 'Q6: Long inactive unsigned'
            }
            
            # Count inconsistencies by type
            type_counts = self.inconsistencies_data['inconsistency_type'].value_counts()
            
            # Prepare data for table
            table_data = []
            for inconsistency_type, count in type_counts.items():
                question = question_mapping.get(inconsistency_type, f"Unknown: {inconsistency_type}")
                table_data.append([question, str(count)])
            
            if table_data:
                header_color = '#E8F4FD'
                row_colors = ['#F2F9FF', '#FFFFFF']
                
                fig = go.Figure(data=[go.Table(
                    header=dict(
                        values=['<b>Business Question</b>', '<b>Count</b>'],
                        fill_color=header_color,
                        align='left',
                        font=dict(size=12)
                    ),
                    cells=dict(
                        values=[[row[0] for row in table_data], [row[1] for row in table_data]],
                        fill_color=[row_colors[i % 2] for i in range(len(table_data))],
                        align='left',
                        font=dict(size=11)
                    )
                )])
                
                fig.update_layout(
                    title='Business Questions Mapping',
                    template=self.template,
                    height=400
                )
                
                return fig
            else:
                fig = go.Figure()
                fig.add_annotation(
                    text="No business questions mapping available",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5, showarrow=False,
                    font=dict(size=16)
                )
                fig.update_layout(
                    title='Business Questions Mapping',
                    template=self.template,
                    height=400
                )
                return fig
        else:
            fig = go.Figure()
            fig.add_annotation(
                text="No inconsistencies to map",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16)
            )
            fig.update_layout(
                title='Business Questions Mapping',
                template=self.template,
                height=400
            )
            return fig
    
    def create_client_distribution(self):
        """Create distribution of clients with inconsistencies."""
        if not self.inconsistencies_data.empty:
            client_counts = self.inconsistencies_data['inconsistency_type'].value_counts()
            
            colors = ['#E74C3C', '#F39C12', '#3498DB', '#2ECC71']
            
            fig = go.Figure(go.Bar(
                x=client_counts.index,
                y=client_counts.values,
                marker=dict(color=colors[:len(client_counts)]),
                text=client_counts.values,
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>Count: %{y}<extra></extra>'
            ))
            
            fig.update_layout(
                title='Client Distribution by Inconsistency Type',
                xaxis_title='Inconsistency Type',
                yaxis_title='Number of Cases',
                template=self.template,
                height=400
            )
            
            return fig
        else:
            fig = go.Figure()
            fig.add_annotation(
                text="No client distribution data available",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16)
            )
            fig.update_layout(
                title='Client Distribution by Inconsistency Type',
                template=self.template,
                height=400
            )
            return fig
    
    def create_timeline_analysis(self):
        """Create timeline analysis of inconsistencies."""
        if not self.client_details.empty and 'event_date' in self.client_details.columns:
            # Convert event dates to datetime
            event_dates = pd.to_datetime(self.client_details['event_date'], errors='coerce')
            event_dates = event_dates.dropna()
            
            if len(event_dates) > 0:
                # Group by month and count
                monthly_counts = event_dates.dt.to_period('M').value_counts().sort_index()
                
                fig = go.Figure(go.Scatter(
                    x=[str(period) for period in monthly_counts.index],
                    y=monthly_counts.values,
                    mode='lines+markers',
                    marker=dict(size=8, color='#E74C3C'),
                    line=dict(width=2, color='#E74C3C'),
                    hovertemplate='Period: %{x}<br>Events: %{y}<extra></extra>'
                ))
                
                fig.update_layout(
                    title='Timeline of Inconsistent Events',
                    xaxis_title='Time Period',
                    yaxis_title='Number of Inconsistent Events',
                    template=self.template,
                    height=400
                )
                
                return fig
            else:
                fig = go.Figure()
                fig.add_annotation(
                    text="No valid date data for timeline analysis",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5, showarrow=False,
                    font=dict(size=16)
                )
                fig.update_layout(
                    title='Timeline of Inconsistent Events',
                    template=self.template,
                    height=400
                )
                return fig
        else:
            fig = go.Figure()
            fig.add_annotation(
                text="No date data available for timeline",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16)
            )
            fig.update_layout(
                title='Timeline of Inconsistent Events',
                template=self.template,
                height=400
            )
            return fig
    
    def create_severity_assessment(self):
        """Create severity assessment of inconsistencies."""
        if not self.inconsistencies_data.empty:
            # Define severity levels based on inconsistency type
            severity_mapping = {
                'multiple_applications': 'Medium',
                'signed_without_applied': 'High',
                'churned_without_signed': 'High',
                'long_inactive_unsigned': 'Low'
            }
            
            # Apply severity mapping
            self.inconsistencies_data['severity'] = self.inconsistencies_data['inconsistency_type'].map(severity_mapping)
            severity_counts = self.inconsistencies_data['severity'].value_counts()
            
            colors = {'High': '#E74C3C', 'Medium': '#F39C12', 'Low': '#2ECC71'}
            plot_colors = [colors.get(sev, '#95A5A6') for sev in severity_counts.index]
            
            fig = go.Figure(go.Bar(
                x=severity_counts.index,
                y=severity_counts.values,
                marker=dict(color=plot_colors),
                text=severity_counts.values,
                textposition='outside',
                hovertemplate='<b>%{x} Severity</b><br>Count: %{y}<extra></extra>'
            ))
            
            fig.update_layout(
                title='Severity Assessment of Inconsistencies',
                xaxis_title='Severity Level',
                yaxis_title='Number of Cases',
                template=self.template,
                height=400
            )
            
            return fig
        else:
            fig = go.Figure()
            fig.add_annotation(
                text="No severity data available",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16)
            )
            fig.update_layout(
                title='Severity Assessment of Inconsistencies',
                template=self.template,
                height=400
            )
            return fig
    
    def create_affected_clients_sample(self):
        """Create sample of affected clients."""
        if not self.client_details.empty:
            # Get a sample of affected clients
            sample_size = min(10, len(self.client_details))
            sample_clients = self.client_details.head(sample_size)
            
            # Prepare data for table
            display_columns = ['client_id', 'event_type', 'event_date', 'inconsistency_type']
            available_columns = [col for col in display_columns if col in sample_clients.columns]
            
            if available_columns:
                table_data = []
                for _, row in sample_clients.iterrows():
                    table_row = []
                    for col in available_columns:
                        value = row[col]
                        if pd.isna(value):
                            table_row.append('-')
                        else:
                            # Format dates nicely
                            if col == 'event_date' and pd.notna(value):
                                try:
                                    date_val = pd.to_datetime(value)
                                    table_row.append(date_val.strftime('%Y-%m-%d'))
                                except:
                                    table_row.append(str(value))
                            else:
                                table_row.append(str(value))
                    table_data.append(table_row)
                
                # Transpose data for plotly table format
                transposed_data = list(zip(*table_data)) if table_data else [[] for _ in available_columns]
                
                header_color = '#E6F3FF'
                row_colors = ['#F8F9FA', '#FFFFFF']
                
                fig = go.Figure(data=[go.Table(
                    header=dict(
                        values=[f'<b>{col.replace("_", " ").title()}</b>' for col in available_columns],
                        fill_color=header_color,
                        align='center',
                        font=dict(size=11)
                    ),
                    cells=dict(
                        values=transposed_data,
                        fill_color=[row_colors[i % 2] for i in range(len(table_data))],
                        align='center',
                        font=dict(size=10)
                    )
                )])
                
                fig.update_layout(
                    title='Sample of Affected Clients',
                    template=self.template,
                    height=400
                )
                
                return fig
            else:
                fig = go.Figure()
                fig.add_annotation(
                    text="No client details available for display",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5, showarrow=False,
                    font=dict(size=16)
                )
                fig.update_layout(
                    title='Sample of Affected Clients',
                    template=self.template,
                    height=400
                )
                return fig
        else:
            fig = go.Figure()
            fig.add_annotation(
                text="No affected clients data available",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16)
            )
            fig.update_layout(
                title='Sample of Affected Clients',
                template=self.template,
                height=400
            )
            return fig
    
    def generate_dashboard(self, save_path=None):
        """Generate the complete inconsistencies dashboard as HTML - Focused on 4 Key Scenarios."""
        print("Generating data inconsistencies dashboard (4 key scenarios)...")
        
        # Create individual charts for the 4 scenarios
        fig0 = self.create_problematic_events_table()  # New events table
        fig1 = self.create_scenario_summary_table()
        fig2 = self.create_unknown_values_analysis()
        fig3 = self.create_sequence_violations_analysis()
        fig4 = self.create_docs_submitted_analysis()
        fig5 = self.create_multiple_applications_analysis()
        fig6 = self.create_event_distribution_summary()
        
        # Create subplot layout (4x2 grid with events table at top spanning full width)
        subplot_fig = make_subplots(
            rows=4, cols=2,
            subplot_titles=(
                'Problematic Events Details - Identified Issues', '',
                'Summary: Four Key Scenarios', 'Scenario 1: Unknown Values',
                'Scenario 2: Date Sequence Violations', 'Scenario 3: Document Submission Patterns',
                'Scenario 4: Multiple Applications', 'Event Distribution (Context)'
            ),
            specs=[
                [{"type": "table", "colspan": 2}, None],  # Events table spans full width
                [{"type": "table"}, {"type": "bar"}],
                [{"type": "scatter"}, {"type": "pie"}],
                [{"type": "scatter"}, {"type": "bar"}]
            ],
            vertical_spacing=0.08,
            horizontal_spacing=0.08
        )
        
        # Add traces from individual figures
        # Problematic Events Table (Row 1, spanning full width)
        for trace in fig0.data:
            subplot_fig.add_trace(trace, row=1, col=1)
        
        # Summary Table (Row 2, Col 1)
        for trace in fig1.data:
            subplot_fig.add_trace(trace, row=2, col=1)
        
        # Unknown Values Analysis (Row 2, Col 2)
        for trace in fig2.data:
            subplot_fig.add_trace(trace, row=2, col=2)
        
        # Sequence Violations Analysis (Row 3, Col 1)
        for trace in fig3.data:
            subplot_fig.add_trace(trace, row=3, col=1)
        
        # Document Submission Analysis (Row 3, Col 2)
        for trace in fig4.data:
            subplot_fig.add_trace(trace, row=3, col=2)
        
        # Multiple Applications Analysis (Row 4, Col 1)
        for trace in fig5.data:
            subplot_fig.add_trace(trace, row=4, col=1)
        
        # Event Distribution Summary (Row 4, Col 2)
        for trace in fig6.data:
            subplot_fig.add_trace(trace, row=4, col=2)
        
        # Update layout
        subplot_fig.update_layout(
            title_text='<b>Data Inconsistencies Analysis - Four Key Scenarios</b>',
            title_x=0.5,
            title_font_size=24,
            height=1600,  # Increased height for 4 rows
            showlegend=False,
            template=self.template
        )
        
        # Add scenario-focused insights as annotation
        insights_text = self._generate_scenario_insights_text()
        subplot_fig.add_annotation(
            text=f"<b>Key Findings:</b><br>{insights_text.replace('• ', '• <br>')}",
            xref="paper", yref="paper",
            x=0.02, y=0.02,
            showarrow=False,
            align="left",
            bgcolor="rgba(255, 230, 230, 0.8)",
            bordercolor="rgba(0, 0, 0, 0.1)",
            borderwidth=1,
            font=dict(size=10)
        )
        
        # Save or show
        if save_path:
            pyo.plot(subplot_fig, filename=save_path, auto_open=False)
            print(f"Inconsistencies dashboard saved to: {save_path}")
        else:
            pyo.plot(subplot_fig, auto_open=True)
        
        return subplot_fig
    
    def _generate_scenario_insights_text(self):
        """Generate scenario-specific insights text for the four key areas."""
        if self.inconsistencies_data.empty:
            return "• No inconsistencies data available for analysis"
        
        insights = []
        
        # Scenario 1: Unknown Values
        unknown_count = len(self.inconsistencies_data[
            self.inconsistencies_data['inconsistency_type'] == 'unknown_values'
        ])
        if unknown_count > 0:
            insights.append(f"• {unknown_count} records have unknown/missing values (plan='Unknown', sales_rep_id=-1)")
        
        # Scenario 2: Sequence Violations  
        sequence_count = len(self.inconsistencies_data[
            self.inconsistencies_data['inconsistency_type'] == 'sequence_violation'
        ])
        if sequence_count > 0:
            insights.append(f"• {sequence_count} clients have events in wrong chronological order")
        
        # Scenario 3: Document Submission Gap
        docs_analysis = self.inconsistencies_data[
            self.inconsistencies_data['inconsistency_type'] == 'docs_submitted_analysis'
        ]
        if not docs_analysis.empty:
            docs_without = len(docs_analysis[docs_analysis['description'] == 'Applied and signed without docs submission'])
            docs_with = len(docs_analysis[docs_analysis['description'] == 'Has docs submission event'])
            insights.append(f"• Only {docs_with} client submitted documents, {docs_without} signed without docs")
        
        # Scenario 4: Multiple Applications
        multi_apps_count = len(self.inconsistencies_data[
            self.inconsistencies_data['inconsistency_type'] == 'Q1_multiple_applications'
        ])
        if multi_apps_count > 0:
            insights.append(f"• {multi_apps_count} clients have multiple application events")
        
        # Event Distribution Context
        if self.event_distribution is not None and not self.event_distribution.empty:
            docs_events = self.event_distribution[self.event_distribution['event_type'] == 'docs_submitted']['event_count'].iloc[0] if len(self.event_distribution[self.event_distribution['event_type'] == 'docs_submitted']) > 0 else 0
            total_applied = self.event_distribution[self.event_distribution['event_type'] == 'applied']['event_count'].iloc[0] if len(self.event_distribution[self.event_distribution['event_type'] == 'applied']) > 0 else 0
            insights.append(f"• Document submission rate: {docs_events}/{total_applied} applications submitted docs")
        
        if not insights:
            insights = ["• Data quality appears good across all four scenarios"]
        
        return '\n'.join(insights)
    
    def run_dashboard(self, save_path=None):
        """Run the complete dashboard generation process."""
        self.load_data()
        return self.generate_dashboard(save_path)


if __name__ == "__main__":
    # Create dashboard instance
    dashboard = InconsistenciesDashboard()
    
    # Create dashboards directory if it doesn't exist
    dashboards_dir = os.path.join(os.path.dirname(__file__), 'dashboards')
    os.makedirs(dashboards_dir, exist_ok=True)
    
    # Generate dashboard and save to dashboards folder
    output_path = os.path.join(dashboards_dir, 'inconsistencies_analysis_dashboard.html')
    dashboard.run_dashboard(save_path=output_path)
    
    print(f"\nInconsistencies dashboard saved to: {output_path}")
    print("Inconsistencies dashboard generation complete!")