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

from c_features.f_churn_data import ChurnDataProcessor


class ChurnDashboard:
    """
    A specialized dashboard class for churn analysis and insights.
    """
    
    def __init__(self):
        """Initialize the churn dashboard."""
        self.churn_processor = ChurnDataProcessor()
        self.churn_data = None
        
        # Set up plotly template
        self.template = "plotly_white"
        
    def load_data(self):
        """Load churn data."""
        print("Loading churn data...")
        
        # Process churn data
        self.churn_processor.process_churn_analysis()
        churn_path = os.path.join(self.churn_processor.output_dir, 'f_churn_data.csv')
        self.churn_data = pd.read_csv(churn_path)
        
        # Convert date columns
        date_columns = ['last_event_date', 'applied_date', 'signed_date', 'churned_date']
        for col in date_columns:
            if col in self.churn_data.columns:
                self.churn_data[col] = pd.to_datetime(self.churn_data[col])
        
        print("Churn data loaded successfully!")
        print(f"Churn data columns: {list(self.churn_data.columns)}")
        print(f"Churn data shape: {self.churn_data.shape}")
        
    def create_churn_summary_stats(self):
        """Create churn summary statistics."""
        # Calculate churn statistics
        total_clients = len(self.churn_data)
        clients_with_signed = self.churn_data['signed_date'].notna().sum()
        avg_days_last_event = self.churn_data['days_since_last_event'].mean()
        avg_days_since_signed = self.churn_data['days_since_signed'].mean()
        
        # Risk categorization based on churn status and days since last event
        def categorize_risk(row):
            # If client is already churned, mark as churned
            if 'is_churned' in row and row['is_churned'] == 1:
                return 'Already Churned'
            
            days = row['days_since_last_event']
            if pd.isna(days):
                return 'Unknown'
            elif days <= 30:
                return 'Low Risk'
            elif days <= 60:
                return 'Medium Risk'
            else:
                return 'High Risk'
        
        # Apply risk categorization considering churn status
        if 'is_churned' in self.churn_data.columns:
            self.churn_data['risk_category'] = self.churn_data.apply(categorize_risk, axis=1)
        else:
            # Fallback if is_churned column doesn't exist yet
            self.churn_data['risk_category'] = self.churn_data['days_since_last_event'].apply(
                lambda days: 'Unknown' if pd.isna(days) else 
                            'Low Risk' if days <= 30 else 
                            'Medium Risk' if days <= 60 else 
                            'High Risk'
            )
        
        # Count metrics for different risk categories
        churned_count = (self.churn_data['risk_category'] == 'Already Churned').sum()
        high_risk_count = (self.churn_data['risk_category'] == 'High Risk').sum()
        medium_risk_count = (self.churn_data['risk_category'] == 'Medium Risk').sum()
        low_risk_count = (self.churn_data['risk_category'] == 'Low Risk').sum()
        unknown_risk_count = (self.churn_data['risk_category'] == 'Unknown').sum()
        
        # Calculate averages for active clients only (excluding churned)
        active_clients = self.churn_data[self.churn_data['risk_category'] != 'Already Churned']
        avg_days_last_event = self.churn_data['days_since_last_event'].mean()
        avg_days_last_event_active = active_clients['days_since_last_event'].mean() if len(active_clients) > 0 else 0
        avg_days_since_signed = self.churn_data['days_since_signed'].mean()
        
        # Calculate clients at risk (excluding churned)
        at_risk_count = high_risk_count + medium_risk_count
        
        # Prepare summary statistics
        stats_data = [
            ['Total Clients', str(total_clients)],
            ['Clients Churned', str(churned_count)],
            ['Clients in Risk of Churn', str(at_risk_count)],
            ['', ''],
            ['High Risk (>60 days)', str(high_risk_count)],
            ['Medium Risk (31-60 days)', str(medium_risk_count)],
            ['Low Risk (≤30 days)', str(low_risk_count)],
            ['Unknown Risk', str(unknown_risk_count)],
            ['', ''],
            ['Avg Days Since Last Event (Active)', f"{avg_days_last_event_active:.1f}"],
            ['Churn Rate', f"{(churned_count/total_clients*100):.1f}%"],
        ]
        
        header_color = '#FFE6E6'
        row_colors = ['#FFF2F2', '#FFFFFF']
        
        fig = go.Figure(data=[go.Table(
            header=dict(
                values=['<b>Churn Metric</b>', '<b>Value</b>'],
                fill_color=header_color,
                align='left',
                font=dict(size=12)
            ),
            cells=dict(
                values=[[row[0] for row in stats_data], [row[1] for row in stats_data]],
                fill_color=[row_colors[i % 2] for i in range(len(stats_data))],
                align='left',
                font=dict(size=11)
            )
        )])
        
        fig.update_layout(
            title='Churn Summary Statistics',
            template=self.template,
            height=400
        )
        
        return fig
        
    def create_churn_distribution(self):
        """Create churn risk distribution chart (excluding already churned)."""
        # Only show risk distribution for active clients (excluding churned)
        active_clients = self.churn_data[self.churn_data['risk_category'] != 'Already Churned']
        risk_counts = active_clients['risk_category'].value_counts()
        
        # Define colors for risk levels
        color_mapping = {
            'Low Risk': '#2ECC71',      # Green
            'Medium Risk': '#F39C12',   # Orange  
            'High Risk': '#E74C3C',     # Red
            'Unknown': '#95A5A6'        # Gray
        }
        colors = [color_mapping.get(risk, '#95A5A6') for risk in risk_counts.index]
        
        fig = go.Figure(go.Pie(
            labels=risk_counts.index,
            values=risk_counts.values,
            marker=dict(colors=colors),
            hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
        ))
        
        fig.update_layout(
            title='Risk Distribution (Active Clients)',
            template=self.template,
            height=400
        )
        
        return fig
    
    def create_days_since_analysis(self):
        """Create distribution of days since last event (active clients only)."""
        # Only show distribution for active clients (excluding churned)
        active_clients = self.churn_data[self.churn_data['risk_category'] != 'Already Churned']
        
        fig = go.Figure(go.Histogram(
            x=active_clients['days_since_last_event'],
            nbinsx=30,
            marker_color='#3498DB',
            opacity=0.7,
            hovertemplate='Days: %{x}<br>Count: %{y}<extra></extra>'
        ))
        
        fig.update_layout(
            title='Days Since Last Event (Active Clients)',
            xaxis_title='Days Since Last Event',
            yaxis_title='Number of Active Clients',
            template=self.template,
            height=400
        )
        
        return fig
    
    def create_days_since_signed_distribution(self):
        """Create distribution of days since signed."""
        signed_data = self.churn_data[self.churn_data['days_since_signed'].notna()]
        
        if len(signed_data) > 0:
            fig = go.Figure(go.Histogram(
                x=signed_data['days_since_signed'],
                nbinsx=25,
                marker_color='#2ECC71',
                opacity=0.7,
                hovertemplate='Days: %{x}<br>Count: %{y}<extra></extra>'
            ))
            
            fig.update_layout(
                title='Days Since Signed Distribution',
                xaxis_title='Days Since Signed',
                yaxis_title='Number of Clients',
                template=self.template,
                height=400
            )
            
            return fig
        else:
            fig = go.Figure()
            fig.add_annotation(
                text="No signed clients data available",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16)
            )
            fig.update_layout(
                title='Days Since Signed Distribution',
                template=self.template,
                height=400
            )
            return fig
    
    def create_churned_vs_at_risk_comparison(self):
        """Create comparison between churned clients and those at risk."""
        churned_count = (self.churn_data['risk_category'] == 'Already Churned').sum()
        high_risk_count = (self.churn_data['risk_category'] == 'High Risk').sum()
        medium_risk_count = (self.churn_data['risk_category'] == 'Medium Risk').sum()
        at_risk_count = high_risk_count + medium_risk_count
        
        categories = ['Clients Churned', 'Clients in Risk of Churn']
        counts = [churned_count, at_risk_count]
        colors = ['#8E44AD', '#E74C3C']  # Purple for churned, Red for at risk
        
        fig = go.Figure(go.Bar(
            x=categories,
            y=counts,
            marker=dict(color=colors),
            text=counts,
            textposition='outside',
            hovertemplate='<b>%{x}</b><br>Count: %{y}<extra></extra>'
        ))
        
        fig.update_layout(
            title='Churned vs At Risk Clients',
            xaxis_title='Client Status',
            yaxis_title='Number of Clients',
            template=self.template,
            height=400
        )
        
        return fig
    
    def create_churn_events_details_table(self):
        """Create detailed churn events table showing client progression."""
        # Prepare events details data
        events_data = []
        
        for _, row in self.churn_data.iterrows():
            client_id = row['client_id']
            events_row = [str(client_id)]
            
            # Add key dates
            date_columns = ['applied_date', 'signed_date', 'last_event_date', 'churned_date']
            for col in date_columns:
                if col in row and pd.notna(row[col]):
                    # Format date as string for display
                    if pd.api.types.is_datetime64_any_dtype(self.churn_data[col]):
                        date_str = row[col].strftime('%Y-%m-%d')
                    else:
                        date_str = str(row[col])
                    events_row.append(date_str)
                else:
                    events_row.append('-')
            
            # Add metrics and event details
            days_since_last = row.get('days_since_last_event', '-')
            days_since_signed = row.get('days_since_signed', '-')
            last_event_type = row.get('last_event_type', 'Unknown')
            risk_category = row.get('risk_category', 'Unknown')
            
            events_row.extend([
                str(int(days_since_last)) if pd.notna(days_since_last) else '-',
                str(int(days_since_signed)) if pd.notna(days_since_signed) else '-',
                last_event_type,
                risk_category
            ])
            
            events_data.append(events_row)
        
        # Sort by risk category and days since last event
        def sort_key(x):
            risk_order = {'High Risk': 0, 'Medium Risk': 1, 'Low Risk': 2, 'Already Churned': 3, 'Unknown': 4}
            risk_priority = risk_order.get(x[8], 5)  # Risk category is now at index 8
            days = float(x[5]) if x[5] != '-' else 999  # Days since last event at index 5
            return (risk_priority, -days)  # Negative days to sort highest days first within same risk
        
        events_data.sort(key=sort_key)
        
        # Define headers
        headers = [
            '<b>Client ID</b>',
            '<b>Applied Date</b>',
            '<b>Signed Date</b>',
            '<b>Last Event Date</b>',
            '<b>Churned Date</b>',
            '<b>Days Since Last Event</b>',
            '<b>Days Since Signed</b>',
            '<b>Last Event Type</b>',
            '<b>Risk Category</b>'
        ]
        
        # Prepare data for table (transpose for plotly table format)
        table_data = list(zip(*events_data)) if events_data else [[] for _ in headers]
        
                        # Create colors for rows based on risk category
        def get_row_color(risk_category):
            if risk_category == 'High Risk':
                return '#FFEBEE'  # Light red
            elif risk_category == 'Medium Risk':
                return '#FFF8E1'  # Light orange
            elif risk_category == 'Low Risk':
                return '#E8F5E8'  # Light green
            elif risk_category == 'Already Churned':
                return '#F3E5F5'  # Light purple
            elif risk_category == 'Unknown':
                return '#F5F5F5'  # Light gray
            else:
                return '#FFFFFF'  # White
        
        row_colors = [get_row_color(events_data[i][8]) for i in range(len(events_data))]
        
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
            title='Detailed Client Churn Events (Sorted by Risk)',
            template=self.template,
            height=600,
            margin=dict(t=50, b=20, l=20, r=20)
        )
        
        return fig
    
    def create_churn_timeline_analysis(self):
        """Create churn timeline if date data is available."""
        # Check if we have any date columns
        date_columns = [col for col in self.churn_data.columns if 'date' in col.lower()]
        
        if date_columns and 'last_event_date' in self.churn_data.columns:
            # Convert to datetime if not already
            last_event_dates = pd.to_datetime(self.churn_data['last_event_date'], errors='coerce')
            last_event_dates = last_event_dates.dropna()
            
            if len(last_event_dates) > 0:
                # Group by month and count
                monthly_counts = last_event_dates.dt.to_period('M').value_counts().sort_index()
                
                fig = go.Figure(go.Scatter(
                    x=[str(period) for period in monthly_counts.index],
                    y=monthly_counts.values,
                    mode='lines+markers',
                    marker=dict(size=8, color='#E74C3C'),
                    line=dict(width=2, color='#E74C3C'),
                    hovertemplate='Period: %{x}<br>Events: %{y}<extra></extra>'
                ))
                
                fig.update_layout(
                    title='Last Event Timeline Analysis',
                    xaxis_title='Time Period',
                    yaxis_title='Number of Last Events',
                    template=self.template,
                    height=400
                )
                
                return fig
            else:
                fig = go.Figure()
                fig.add_annotation(
                    text="No valid date data available",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5, showarrow=False,
                    font=dict(size=16)
                )
                fig.update_layout(
                    title='Last Event Timeline Analysis',
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
                title='Last Event Timeline Analysis',
                template=self.template,
                height=400
            )
            return fig
    
    def create_risk_by_client_segment(self):
        """Create risk distribution by client segment."""
        # Create risk level analysis by available segments
        if 'region' in self.churn_data.columns:
            risk_by_region = pd.crosstab(self.churn_data['region'], self.churn_data['risk_category'])
            
            fig = go.Figure()
            
            colors = ['#2ECC71', '#F39C12', '#E74C3C', '#95A5A6']  # Green, Orange, Red, Gray
            risk_levels = risk_by_region.columns
            
            for i, risk_level in enumerate(risk_levels):
                fig.add_trace(go.Bar(
                    name=risk_level,
                    x=risk_by_region.index,
                    y=risk_by_region[risk_level],
                    marker_color=colors[i % len(colors)],
                    hovertemplate=f'<b>{risk_level}</b><br>Region: %{{x}}<br>Count: %{{y}}<extra></extra>'
                ))
            
            fig.update_layout(
                title='Risk Level by Client Segment',
                xaxis_title='Region',
                yaxis_title='Number of Clients',
                template=self.template,
                height=400,
                barmode='group'
            )
            
            return fig
        else:
            fig = go.Figure()
            fig.add_annotation(
                text="No region data available for segmentation",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16)
            )
            fig.update_layout(
                title='Risk Level by Client Segment',
                template=self.template,
                height=400
            )
            return fig
    
    def generate_dashboard(self, save_path=None):
        """Generate the complete churn dashboard as HTML."""
        print("Generating churn analysis dashboard...")
        
        # Create individual charts
        fig1 = self.create_churn_summary_stats()
        fig2 = self.create_churn_distribution()
        fig3 = self.create_days_since_analysis()
        fig4 = self.create_churned_vs_at_risk_comparison()
        fig5 = self.create_churn_timeline_analysis()
        fig6 = self.create_risk_by_client_segment()
        fig7 = self.create_churn_events_details_table()
        
        # Create subplot layout (4x2 grid with events table spanning full width)
        subplot_fig = make_subplots(
            rows=4, cols=2,
            subplot_titles=(
                'Summary Statistics', 'Risk Distribution (Active)',
                'Days Since Last Event (Active)', 'Churned vs At Risk',
                'Timeline Analysis', 'Risk by Segment',
                'Detailed Client Events (Sorted by Risk)', ''
            ),
            specs=[
                [{"type": "table"}, {"type": "pie"}],
                [{"type": "histogram"}, {"type": "bar"}],
                [{"type": "scatter"}, {"type": "bar"}],
                [{"type": "table", "colspan": 2}, None]
            ],
            vertical_spacing=0.06,
            horizontal_spacing=0.08,
            row_heights=[0.2, 0.2, 0.2, 0.4]
        )
        
        # Add traces from individual figures
        # Summary Statistics
        for trace in fig1.data:
            subplot_fig.add_trace(trace, row=1, col=1)
        
        # Risk Distribution
        for trace in fig2.data:
            subplot_fig.add_trace(trace, row=1, col=2)
        
        # Days Since Last Event (Active)
        for trace in fig3.data:
            subplot_fig.add_trace(trace, row=2, col=1)
        
        # Churned vs At Risk Comparison
        for trace in fig4.data:
            subplot_fig.add_trace(trace, row=2, col=2)
        
        # Timeline Analysis
        for trace in fig5.data:
            subplot_fig.add_trace(trace, row=3, col=1)
        
        # Risk by Segment
        for trace in fig6.data:
            subplot_fig.add_trace(trace, row=3, col=2)
        
        # Churn Events Details Table (spanning full width)
        for trace in fig7.data:
            subplot_fig.add_trace(trace, row=4, col=1)
        
        # Update layout
        subplot_fig.update_layout(
            title_text='<b>Customer Churn Analysis Dashboard</b>',
            title_x=0.5,
            title_font_size=24,
            height=1600,
            showlegend=False,
            template=self.template
        )
        
        # Add insights as annotation
        insights_text = self._generate_churn_insights_text()
        subplot_fig.add_annotation(
            text=f"<b>Key Insights:</b><br>{insights_text.replace('• ', '• <br>')}",
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
            print(f"Churn dashboard saved to: {save_path}")
        else:
            pyo.plot(subplot_fig, auto_open=True)
        
        return subplot_fig
    
    def _generate_churn_insights_text(self):
        """Generate churn-specific insights text."""
        churned_count = (self.churn_data['risk_category'] == 'Already Churned').sum()
        high_risk_count = (self.churn_data['risk_category'] == 'High Risk').sum()
        medium_risk_count = (self.churn_data['risk_category'] == 'Medium Risk').sum()
        total_clients = len(self.churn_data)
        
        churn_rate = (churned_count / total_clients * 100) if total_clients > 0 else 0
        high_risk_percentage = (high_risk_count / total_clients * 100) if total_clients > 0 else 0
        at_risk_count = high_risk_count + medium_risk_count
        at_risk_percentage = (at_risk_count / total_clients * 100) if total_clients > 0 else 0
        
        # Calculate average for active clients only
        active_clients = self.churn_data[self.churn_data['risk_category'] != 'Already Churned']
        avg_days_last_event_active = active_clients['days_since_last_event'].mean() if len(active_clients) > 0 else 0
        
        insights = [
            f"• {churned_count} clients churned",
            f"• {at_risk_count} clients in risk of churn",
            f"• {high_risk_count} clients at high risk (>60 days inactive)",
            f"• Average days since last activity (active clients): {avg_days_last_event_active:.1f} days"
        ]
        
        return '\n'.join(insights)
    
    def run_dashboard(self, save_path=None):
        """Run the complete dashboard generation process."""
        self.load_data()
        return self.generate_dashboard(save_path)


if __name__ == "__main__":
    # Create dashboard instance
    dashboard = ChurnDashboard()
    
    # Create dashboards directory if it doesn't exist
    dashboards_dir = os.path.join(os.path.dirname(__file__), 'dashboards')
    os.makedirs(dashboards_dir, exist_ok=True)
    
    # Generate dashboard and save to dashboards folder
    output_path = os.path.join(dashboards_dir, 'churn_analysis_dashboard.html')
    dashboard.run_dashboard(save_path=output_path)
    
    print(f"\nChurn dashboard saved to: {output_path}")
    print("Churn dashboard generation complete!")