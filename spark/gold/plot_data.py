import os
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
from plotly.offline import plot


ts = 1678335199  # int(time.time())
gold_q1_input = f"./case/gold/uploaded_at={ts}/q1"
gold_q2_input = f"./case/gold/uploaded_at={ts}/q2"
gold_q3_input = f"./case/gold/uploaded_at={ts}/q3"

plots_output = f"./case/gold/uploaded_at={ts}/plots"
os.makedirs(os.path.dirname(plots_output), exist_ok=True)

df_q1 = pd.read_parquet(gold_q1_input)
df_q2 = pd.read_parquet(gold_q2_input)
df_q3 = pd.read_parquet(gold_q3_input)


q1a_plot = px.bar(df_q1, x='character_name',
                  y=['comics_available', 'events_available', 'stories_available', 'series_available'],
                  title='Top 10 Characters Appearances'
                  )
q1b_plot = px.bar(df_q1, x='character_name', y='total_event_days', title='Top 10 Characters Total Event Days')

q2_plot = px.line(df_q2, x='event_year', y='distinct_characters', title='Distinct Characters per Year')

q3_plot = px.density_heatmap(df_q3, x="event_year", y="character_name", z='distinct_events', text_auto=True)

figures = [
    (q1a_plot, 1, 1),
    (q1b_plot, 1, 2),
    (q2_plot, 2, 1),
    (q3_plot, 2, 2)
]
fig = make_subplots(
    rows=2,
    cols=2,
    subplot_titles=['Top 10 Characters Appearances', 'Top 10 Characters Total Event Days',
                    'Distinct Characters per Year', 'Heatmap of Characters and Years']
)

for tup in figures:
    (figure, r, c) = tup
    for trace in range(len(figure["data"])):
        fig.append_trace(figure["data"][trace], row=r, col=c)
        # fig.

fig.write_image(f"{plots_output}/dash.jpeg")
plot(fig, filename=f"{plots_output}/dash2.jpeg")
plot(fig)
# fig.show()


fig.show()
# plotly.offline.plot(fig, filename='C:/plotlyplots/canada_offline.html')

