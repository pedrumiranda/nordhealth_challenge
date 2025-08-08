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

from c_features.f_funnel_data import FunnelDataProcessor


class FunnelDashboard:
    """
    A specialized dashboard class for funnel analysis and conversion insights.
    """
    
    def __init__(self):
        """Initialize the funnel dashboard."""
        self.funnel_processor = FunnelDataProcessor()
        self.funnel_data = None
        self.funnel_metrics = None
        
        # Set up plotly template
        self.template = "plotly_white"
        
    def load_data(self):
        """Load funnel data."""
        print("Loading funnel data...")
        
        # Process funnel data
        _, self.funnel_metrics = self.funnel_processor.process_funnel_analysis()
        funnel_path = os.path.join(self.funnel_processor.output_dir, 'f_funnel_data.csv')
        self.funnel_data = pd.read_csv(funnel_path)
        
        # Convert date columns
        date_columns = ['applied_date', 'docs_submitted_date', 'rejected_date', 'signed_date', 'churned_date']
        for col in date_columns:
            if col in self.funnel_data.columns:
                self.funnel_data[col] = pd.to_datetime(self.funnel_data[col])
        
        print("Funnel data loaded successfully!")
        
    def create_funnel_overview(self):
        """Create comprehensive funnel overview visualization with all event types."""
        stages = ['Applied', 'Docs Submitted', 'Rejected', 'Signed', 'Churned', 'Active']
        counts = [
            self.funnel_metrics['applied_clients'],
            self.funnel_metrics['docs_submitted_clients'],
            self.funnel_metrics['rejected_clients'],
            self.funnel_metrics['signed_clients'],
            self.funnel_metrics['churned_clients'],
            self.funnel_metrics['active_clients']
        ]
        
        # Create funnel chart with different colors for different outcomes
        colors = ['#2E86C1', '#F39C12', '#E74C3C', '#28B463', '#8E44AD', '#27AE60']
        
        fig = go.Figure(go.Bar(
            y=stages,
            x=counts,
            orientation='h',
            marker=dict(color=colors),
            text=counts,
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>Count: %{x}<extra></extra>'
        ))
        
        fig.update_layout(
            title='Comprehensive Customer Funnel Overview',
            xaxis_title='Number of Clients',
            yaxis_title='Funnel Stage',
            template=self.template,
            height=400
        )
        
        return fig
        
    def create_conversion_rates(self):
        """Create comprehensive conversion rates visualization."""
        rates_data = {
            'Application Rate': self.funnel_metrics['application_rate'],
            'Docs Submission Rate': self.funnel_metrics['docs_submission_rate'],
            'Rejection Rate': self.funnel_metrics['rejection_rate'],
            'Conversion Rate': self.funnel_metrics['conversion_rate'],
            'Churn Rate': self.funnel_metrics['churn_rate']
        }
        
        stages = list(rates_data.keys())
        values = [r * 100 for r in rates_data.values()]
        colors = ['#3498DB', '#F39C12', '#E74C3C', '#2ECC71', '#8E44AD']
        
        fig = go.Figure(go.Bar(
            x=stages,
            y=values,
            marker=dict(color=colors),
            text=[f'{r:.1%}' for r in rates_data.values()],
            textposition='outside',
            hovertemplate='<b>%{x}</b><br>Rate: %{text}<extra></extra>'
        ))
        
        fig.update_layout(
            title='Comprehensive Conversion Rates',
            xaxis_title='Rate Type',
            yaxis_title='Percentage (%)',
            template=self.template,
            height=400
        )
        
        return fig
    
    def create_funnel_metrics_summary(self):
        """Create comprehensive funnel metrics summary table."""
        # Prepare comprehensive summary statistics
        metrics_data = [
            ['Total Clients', str(self.funnel_metrics['total_clients'])],
            ['Applied Clients', f"{self.funnel_metrics['applied_clients']} ({self.funnel_metrics['application_rate']:.1%})"],
            ['Docs Submitted', f"{self.funnel_metrics['docs_submitted_clients']} ({self.funnel_metrics['docs_submission_rate']:.1%})"],
            ['Rejected Clients', f"{self.funnel_metrics['rejected_clients']} ({self.funnel_metrics['rejection_rate']:.1%})"],
            ['Signed Clients', f"{self.funnel_metrics['signed_clients']} ({self.funnel_metrics['conversion_rate']:.1%})"],
            ['Churned Clients', f"{self.funnel_metrics['churned_clients']} ({self.funnel_metrics['churn_rate']:.1%})"],
            ['Active Clients', str(self.funnel_metrics['active_clients'])],
            ['', ''],
            ['Success Rate: Applied → Signed', f"{self.funnel_metrics['conversion_rate']:.1%}"],
            ['Retention Rate: Signed → Active', f"{(1-self.funnel_metrics['churn_rate']):.1%}"],
        ]
        
        header_color = '#E6F3FF'
        row_colors = ['#F8F9FA', '#FFFFFF']
        
        fig = go.Figure(data=[go.Table(
            header=dict(
                values=['<b>Funnel Metric</b>', '<b>Value</b>'],
                fill_color=header_color,
                align='left',
                font=dict(size=12)
            ),
            cells=dict(
                values=[[row[0] for row in metrics_data], [row[1] for row in metrics_data]],
                fill_color=[row_colors[i % 2] for i in range(len(metrics_data))],
                align='left',
                font=dict(size=11)
            )
        )])
        
        fig.update_layout(
            title='Funnel Metrics Summary',
            template=self.template,
            height=400
        )
        
        return fig
    
    def create_funnel_progression_timeline(self):
        """Create comprehensive timeline analysis of all funnel progression stages."""
        # Calculate time differences between all stages
        progression_data = []
        
        for _, row in self.funnel_data.iterrows():
            # Applied → Docs Submitted
            if pd.notna(row['applied_date']) and pd.notna(row['docs_submitted_date']):
                days = (row['docs_submitted_date'] - row['applied_date']).days
                progression_data.append({
                    'client_id': row['client_id'],
                    'stage': 'Applied → Docs',
                    'days': days
                })
            
            # Applied → Rejected
            if pd.notna(row['applied_date']) and pd.notna(row['rejected_date']):
                days = (row['rejected_date'] - row['applied_date']).days
                progression_data.append({
                    'client_id': row['client_id'],
                    'stage': 'Applied → Rejected',
                    'days': days
                })
            
            # Applied → Signed (direct)
            if pd.notna(row['applied_date']) and pd.notna(row['signed_date']):
                days = (row['signed_date'] - row['applied_date']).days
                progression_data.append({
                    'client_id': row['client_id'],
                    'stage': 'Applied → Signed',
                    'days': days
                })
            
            # Signed → Churned
            if pd.notna(row['signed_date']) and pd.notna(row['churned_date']):
                days = (row['churned_date'] - row['signed_date']).days
                progression_data.append({
                    'client_id': row['client_id'],
                    'stage': 'Signed → Churned',
                    'days': days
                })
        
        if progression_data:
            prog_df = pd.DataFrame(progression_data)
            
            fig = go.Figure()
            
            stages = prog_df['stage'].unique()
            colors = ['#F39C12', '#E74C3C', '#3498DB', '#8E44AD']
            
            for i, stage in enumerate(stages):
                stage_data = prog_df[prog_df['stage'] == stage]['days']
                fig.add_trace(go.Box(
                    y=stage_data,
                    name=stage,
                    marker_color=colors[i % len(colors)],
                    boxpoints='outliers'
                ))
            
            fig.update_layout(
                title='Time Between All Funnel Stages',
                yaxis_title='Days',
                xaxis_title='Stage Transition',
                template=self.template,
                height=400
            )
            
            return fig
        else:
            # Return empty figure with message
            fig = go.Figure()
            fig.add_annotation(
                text="No progression data available",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16)
            )
            fig.update_layout(
                title='Time Between All Funnel Stages',
                template=self.template,
                height=400
            )
            return fig
    
    def create_client_journey_analysis(self):
        """Create comprehensive analysis of different client journey patterns."""
        # Categorize clients by their comprehensive journey
        journey_types = []
        
        for _, row in self.funnel_data.iterrows():
            if pd.notna(row['rejected_date']):
                journey_types.append('Rejected<br>(Applied → Rejected)')
            elif pd.notna(row['churned_date']):
                if pd.notna(row['docs_submitted_date']):
                    journey_types.append('Full Journey with Docs<br>(Applied → Docs → Signed → Churned)')
                else:
                    journey_types.append('Full Journey<br>(Applied → Signed → Churned)')
            elif pd.notna(row['signed_date']):
                if pd.notna(row['docs_submitted_date']):
                    journey_types.append('Active with Docs<br>(Applied → Docs → Signed)')
                else:
                    journey_types.append('Active Client<br>(Applied → Signed)')
            elif pd.notna(row['docs_submitted_date']):
                journey_types.append('Docs Submitted<br>(Applied → Docs)')
            elif pd.notna(row['applied_date']):
                journey_types.append('Applied Only<br>(No Progress)')
            else:
                journey_types.append('Other')
        
        # Count journey types
        journey_counts = pd.Series(journey_types).value_counts()
        
        # Create pie chart with more colors
        colors = ['#E74C3C', '#2ECC71', '#F39C12', '#3498DB', '#9B59B6', '#1ABC9C', '#95A5A6']
        
        fig = go.Figure(go.Pie(
            labels=journey_counts.index,
            values=journey_counts.values,
            marker=dict(colors=colors[:len(journey_counts)]),
            hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
        ))
        
        fig.update_layout(
            title='Comprehensive Client Journey Patterns',
            template=self.template,
            height=500
        )
        
        return fig
    
    def create_conversion_funnel_waterfall(self):
        """Create comprehensive waterfall chart showing all funnel stages."""
        # Calculate comprehensive funnel stages
        total = self.funnel_metrics['total_clients']
        applied = self.funnel_metrics['applied_clients']
        docs_submitted = self.funnel_metrics['docs_submitted_clients']
        rejected = self.funnel_metrics['rejected_clients']
        signed = self.funnel_metrics['signed_clients']
        churned = self.funnel_metrics['churned_clients']
        active = self.funnel_metrics['active_clients']
        
        stages = ['Total<br>Clients', 'Applied', 'Docs<br>Submitted', 'Rejected', 'Signed', 'Churned', 'Active']
        values = [total, applied, docs_submitted, rejected, signed, churned, active]
        
        # Create bar chart with different colors for different outcomes
        colors = ['#3498DB', '#2ECC71', '#F39C12', '#E74C3C', '#27AE60', '#8E44AD', '#1ABC9C']
        
        fig = go.Figure(go.Bar(
            x=stages,
            y=values,
            marker=dict(color=colors),
            text=values,
            textposition='outside',
            hovertemplate='<b>%{x}</b><br>Count: %{y}<extra></extra>'
        ))
        
        fig.update_layout(
            title='Comprehensive Funnel Progression',
            xaxis_title='Funnel Stage',
            yaxis_title='Number of Clients',
            template=self.template,
            height=400
        )
        
        return fig
    
    def create_events_details_table(self):
        """Create detailed events table showing earliest application times for each event type."""
        # Prepare events details data
        events_data = []
        
        for _, row in self.funnel_data.iterrows():
            client_id = row['client_id']
            events_row = [str(client_id)]
            
            # Add each event date (convert to string for display)
            event_columns = ['applied_date', 'docs_submitted_date', 'rejected_date', 'signed_date', 'churned_date']
            for col in event_columns:
                if pd.notna(row[col]):
                    # Format date as string for display
                    date_str = row[col].strftime('%Y-%m-%d') if pd.notna(row[col]) else '-'
                    events_row.append(date_str)
                else:
                    events_row.append('-')
            
            # Calculate days between key events
            days_to_sign = '-'
            days_to_churn = '-'
            
            if pd.notna(row['applied_date']) and pd.notna(row['signed_date']):
                days_to_sign = str((row['signed_date'] - row['applied_date']).days)
            
            if pd.notna(row['signed_date']) and pd.notna(row['churned_date']):
                days_to_churn = str((row['churned_date'] - row['signed_date']).days)
            
            events_row.extend([days_to_sign, days_to_churn])
            events_data.append(events_row)
        
        # Sort by applied_date (earliest first)
        events_data.sort(key=lambda x: x[1] if x[1] != '-' else '9999-12-31')
        
        # Define headers
        headers = [
            '<b>Client ID</b>',
            '<b>Applied Date</b>',
            '<b>Docs Submitted</b>',
            '<b>Rejected Date</b>',
            '<b>Signed Date</b>',
            '<b>Churned Date</b>',
            '<b>Days to Sign</b>',
            '<b>Days to Churn</b>'
        ]
        
        # Prepare data for table (transpose for plotly table format)
        table_data = list(zip(*events_data)) if events_data else [[] for _ in headers]
        
        # Create colors for alternating rows
        num_rows = len(events_data)
        row_colors = ['#F8F9FA' if i % 2 == 0 else '#FFFFFF' for i in range(num_rows)]
        
        fig = go.Figure(data=[go.Table(
            header=dict(
                values=headers,
                fill_color='#E6F3FF',
                align='center',
                font=dict(size=11, family='Arial'),
                height=30
            ),
            cells=dict(
                values=table_data,
                fill_color=[row_colors for _ in range(len(headers))],
                align='center',
                font=dict(size=10, family='Arial'),
                height=25
            )
        )])
        
        fig.update_layout(
            title='Detailed Client Events Timeline (Earliest Events)',
            template=self.template,
            height=600,
            margin=dict(t=50, b=20, l=20, r=20)
        )
        
        return fig
    
    def create_monthly_trend_analysis(self, ax):
        """Create monthly trend analysis if date data is available."""
        # Check if we have date data for trend analysis
        dated_data = self.funnel_data[self.funnel_data['applied_date'].notna()].copy()
        
        if len(dated_data) > 0:
            # Extract month from applied_date
            dated_data['month'] = dated_data['applied_date'].dt.to_period('M')
            
            # Count applications by month
            monthly_applied = dated_data.groupby('month').size()
            
            # Count conversions by month (based on signed_date month)
            signed_data = dated_data[dated_data['signed_date'].notna()].copy()
            if len(signed_data) > 0:
                signed_data['signed_month'] = signed_data['signed_date'].dt.to_period('M')
                monthly_signed = signed_data.groupby('signed_month').size()
                
                # Plot both trends
                ax.plot(range(len(monthly_applied)), monthly_applied.values, 
                       marker='o', label='Applications', linewidth=2, color='#3498DB')
                
                if len(monthly_signed) > 0:
                    ax.plot(range(len(monthly_signed)), monthly_signed.values, 
                           marker='s', label='Conversions', linewidth=2, color='#2ECC71')
                
                ax.set_xlabel('Time Period')
                ax.set_ylabel('Count')
                ax.set_title('Application & Conversion Trends', fontsize=14, fontweight='bold')
                ax.legend()
                ax.grid(True, alpha=0.3)
            else:
                ax.bar(range(len(monthly_applied)), monthly_applied.values, 
                      color='#3498DB', alpha=0.7)
                ax.set_xlabel('Time Period')
                ax.set_ylabel('Applications')
                ax.set_title('Application Trends', fontsize=14, fontweight='bold')
                ax.grid(True, alpha=0.3)
        else:
            ax.text(0.5, 0.5, 'No date data available for trend analysis', 
                   ha='center', va='center', transform=ax.transAxes, fontsize=12)
            ax.set_title('Trend Analysis', fontsize=14, fontweight='bold')
    
    def generate_dashboard(self, save_path=None):
        """Generate the complete funnel dashboard as HTML."""
        print("Generating funnel analysis dashboard...")
        
        # Create individual charts
        fig1 = self.create_funnel_overview()
        fig2 = self.create_conversion_rates()
        fig3 = self.create_funnel_metrics_summary()
        fig4 = self.create_client_journey_analysis()
        fig5 = self.create_funnel_progression_timeline()
        fig6 = self.create_conversion_funnel_waterfall()
        fig7 = self.create_events_details_table()
        
        # Create subplot layout (4x2 grid with events table spanning full width)
        subplot_fig = make_subplots(
            rows=4, cols=2,
            subplot_titles=(
                'Funnel Overview', 'Conversion Rates',
                'Journey Patterns', 'Timeline Analysis',
                'Waterfall Analysis', 'Metrics Summary',
                'Detailed Client Events Timeline', ''
            ),
            specs=[
                [{"type": "bar"}, {"type": "bar"}],
                [{"type": "pie"}, {"type": "box"}],
                [{"type": "bar"}, {"type": "table"}],
                [{"type": "table", "colspan": 2}, None]
            ],
            vertical_spacing=0.06,
            horizontal_spacing=0.08,
            row_heights=[0.2, 0.2, 0.2, 0.4]
        )
        
        # Add traces from individual figures
        # Funnel Overview
        for trace in fig1.data:
            subplot_fig.add_trace(trace, row=1, col=1)
        
        # Conversion Rates
        for trace in fig2.data:
            subplot_fig.add_trace(trace, row=1, col=2)
        
        # Journey Patterns
        for trace in fig4.data:
            subplot_fig.add_trace(trace, row=2, col=1)
        
        # Timeline Analysis
        for trace in fig5.data:
            subplot_fig.add_trace(trace, row=2, col=2)
        
        # Waterfall Analysis
        for trace in fig6.data:
            subplot_fig.add_trace(trace, row=3, col=1)
        
        # Metrics Summary
        for trace in fig3.data:
            subplot_fig.add_trace(trace, row=3, col=2)
        
        # Events Details Table (spanning full width)
        for trace in fig7.data:
            subplot_fig.add_trace(trace, row=4, col=1)
        
        # Update layout
        subplot_fig.update_layout(
            title_text='<b>Customer Funnel Analysis Dashboard</b>',
            title_x=0.5,
            title_font_size=24,
            height=1600,
            showlegend=False,
            template=self.template
        )
        
        # Add insights as annotation
        insights_text = self._generate_funnel_insights_text()
        subplot_fig.add_annotation(
            text=f"<b>Key Insights:</b><br>{insights_text.replace('• ', '• <br>')}",
            xref="paper", yref="paper",
            x=0.02, y=0.02,
            showarrow=False,
            align="left",
            bgcolor="rgba(230, 243, 255, 0.8)",
            bordercolor="rgba(0, 0, 0, 0.1)",
            borderwidth=1,
            font=dict(size=10)
        )
        
        # Save or show
        if save_path:
            pyo.plot(subplot_fig, filename=save_path, auto_open=False)
            print(f"Funnel dashboard saved to: {save_path}")
        else:
            pyo.plot(subplot_fig, auto_open=True)
        
        return subplot_fig
    
    def _generate_funnel_insights_text(self):
        """Generate funnel-specific insights text."""
        conversion_rate = self.funnel_metrics['conversion_rate']
        churn_rate = self.funnel_metrics['churn_rate']
        drop_off_rate = 1 - conversion_rate
        retention_rate = 1 - churn_rate
        
        insights = f"""
FUNNEL ANALYSIS INSIGHTS:
• Conversion Rate: {conversion_rate:.1%} of applicants successfully sign
• Drop-off Rate: {drop_off_rate:.1%} of applicants do not convert
• Churn Rate: {churn_rate:.1%} of signed clients have churned
• Retention Rate: {retention_rate:.1%} of signed clients remain active
• Journey Analysis: Review pie chart for different client paths
        """.strip()
        
        return insights
    
    def run_dashboard(self, save_path=None):
        """Run the complete funnel dashboard generation process."""
        try:
            self.load_data()
            dashboard_fig = self.generate_dashboard(save_path)
            print("Funnel dashboard generation completed successfully!")
            return dashboard_fig
            
        except Exception as e:
            print(f"Error generating funnel dashboard: {str(e)}")
            raise


# Main execution
if __name__ == "__main__":
    dashboard = FunnelDashboard()
    
    # Create dashboards directory if it doesn't exist
    dashboards_dir = os.path.join(os.path.dirname(__file__), 'dashboards')
    os.makedirs(dashboards_dir, exist_ok=True)
    
    # Generate dashboard and save to dashboards folder
    output_path = os.path.join(dashboards_dir, 'funnel_analysis_dashboard.html')
    dashboard.run_dashboard(save_path=output_path)
    
    print(f"\nFunnel dashboard saved to: {output_path}")
    print("Funnel dashboard generation complete!")
