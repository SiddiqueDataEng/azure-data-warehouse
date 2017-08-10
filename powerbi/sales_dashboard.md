# Sales Analytics Dashboard

## Overview
Power BI dashboard connecting to Azure SQL Data Warehouse for real-time sales analytics.

## Data Sources
- Azure SQL Data Warehouse
- Connection: DirectQuery mode for real-time data

## Key Visualizations

### 1. Sales Overview
- Total Revenue (Card)
- Total Orders (Card)
- Average Order Value (Card)
- Profit Margin % (Card)

### 2. Sales Trends
- Revenue by Month (Line Chart)
- Orders by Day of Week (Column Chart)
- YoY Growth % (KPI)

### 3. Customer Analysis
- Top 10 Customers by Revenue (Bar Chart)
- Customer Segmentation (Pie Chart)
- Customer Lifetime Value (Scatter Plot)

### 4. Product Performance
- Top Products by Revenue (Table)
- Category Performance (Treemap)
- Product Profitability (Matrix)

### 5. Geographic Analysis
- Sales by Region (Map)
- State-wise Revenue (Filled Map)

## Filters
- Date Range Slicer
- Product Category
- Customer Segment
- Sales Region

## Refresh Schedule
- DirectQuery: Real-time
- Import Mode: Daily at 6 AM

## Row-Level Security
- Sales managers see only their region
- Executives see all data
