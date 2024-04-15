#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from datetime import timedelta

# Assuming 'merged_data.csv' is the correct path, and the CSV has headers.
df = pd.read_csv('merged_data_2.csv', low_memory=False, parse_dates=['Timestamp'])


# In[6]:


df = df[df['score'] != '{\n    "Sentiment": 3.7\n}']
df["score"] = df["score"].astype(float)


# In[7]:


type(df['score'][0])


# In[8]:


df.set_index('Timestamp', inplace=True)

app = dash.Dash(__name__)

# Prepare dropdown options, ensuring data type consistency
dropdown_options = [{'label': str(ticker), 'value': str(ticker)} for ticker in df['Ticker'].dropna().unique()]

app.layout = html.Div([
    html.H1('Real-Time Stock Monitor',style={"font-weight": "bold","text-align":"center"},),
    html.Div('---Group 11'),
    dcc.Dropdown(
        id='ticker-selector',
        options=dropdown_options,
        value=dropdown_options[0]['value'] if dropdown_options else None,  # Default value to first ticker if available
        clearable=False,
        style={'width': '50%'}
    ),
    dcc.Graph(id='live-update-graph'),
    dcc.Interval(
        id='interval-component',
        interval=1*1000,  # 5 seconds
        n_intervals=0
    )
])

@app.callback(
    Output('live-update-graph', 'figure'),
    [Input('interval-component', 'n_intervals'),
     Input('ticker-selector', 'value')]
)
def update_graph_live(n, selected_ticker):
    # Filter data for the selected ticker
    filtered_df = df[df['Ticker'] == selected_ticker]
    
    # Calculate the current time window
    start_time = filtered_df.index.min() + timedelta(minutes=n)
    end_time = start_time + timedelta(minutes=200)

    # Further filter data based on the time window
 
    mask = (filtered_df.index >= start_time) & (filtered_df.index <= end_time)
    filtered_df = filtered_df.loc[mask, ['Close','close_MA','score','UpperBand','LowerBand']]


    
    
    
    
    
    figure = go.Figure()
    figure.add_trace(go.Scatter(x=filtered_df.index, y=filtered_df['Close'], mode='lines', name=f'{selected_ticker} Close Rate'))
    figure.add_trace(go.Scatter(x=filtered_df.index, y=filtered_df['UpperBand'], mode='lines', name=f'{selected_ticker} Upper Band'))
    figure.add_trace(go.Scatter(x=filtered_df.index, y=filtered_df['LowerBand'], mode='lines', name=f'{selected_ticker} Lower Band'))
    figure.add_trace(go.Scatter(x=filtered_df.index, y=filtered_df['close_MA'], mode='lines', name=f'Moving Average'))
    
    
    
    if 'score' in filtered_df.columns and filtered_df['score'].notna().any():
        figure.add_trace(go.Scatter(
            x=filtered_df.index, 
            y=filtered_df['score'], 
            mode='markers',  # Change to markers for spots
            name='Score',
            marker=dict(color='red',
                       
                       ),
            yaxis='y2'  # Assign to secondary y-axis
        ))
    
        figure.update_layout(
        title=f'{selected_ticker} Close',
        xaxis_title='Date',
        xaxis_range=[start_time, end_time],
        yaxis=dict(
            title='Price',
            side='left',  # Primary y-axis on the left
        ),
       yaxis2=dict(
            title='Score',
            overlaying='y',  # Overlay y2 on the same x-axis as y
            side='right',  # Secondary y-axis on the right
            showgrid=True,  # Optional: turn off grid for secondary y-axis
            range = [1,5]
        ),
        plot_bgcolor="white"
        )
    return figure

if __name__ == '__main__':
    app.run_server(debug=True, port = 8061)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




