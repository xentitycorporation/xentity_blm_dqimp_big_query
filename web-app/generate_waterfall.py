import pandas as pd
import plotly.graph_objects as go
import os

# 1. Load your updated data
# Using a relative path because the script and CSV are in the same repo
csv_path = 'web-app/BLM_Waterfall_Counts_Flattened.csv'
df = pd.read_csv(csv_path)
df = df.sort_values('Sort')

# 2. Define Waterfall Logic
df['measure'] = df['Type'].apply(lambda x: 'total' if 'Snapshot Total' in x else 'relative')

# 3. Format Labels
def get_label(row):
    if row['Type'] == 'Snapshot Total':
        return str(row['Month'])
    elif row['Type'] == 'Fixed Cases':
        return 'Fixes'
    else:
        return 'Newly Out-of-Sync'

df['display_label'] = df.apply(get_label, axis=1)

# 4. Build the Chart
fig = go.Figure(go.Waterfall(
    measure = df['measure'],
    x = df['display_label'],
    textposition = "outside",
    text = df['Value'],
    y = df['Value'],
    connector = {"line":{"color":"rgb(63, 63, 63)"}},
    increasing = {"marker":{"color": "#FF4136"}}, 
    decreasing = {"marker":{"color": "#2ECC40"}}, 
    totals = {"marker":{"color": "#0074D9"}}
))

fig.update_layout(
    title = "Waterfall of CSE_DISP Sync Errors Month to Month",
    showlegend = False,
    plot_bgcolor='rgba(240, 242, 248, 1)',
    yaxis_title="Cases Out of Sync",
    template="plotly_white"
)

# 5. Save the output
# We save it to the web-app folder so GitHub Pages can find it
fig.write_html("web-app/index.html")
