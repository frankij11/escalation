import pandas as pd
import panel as pn
import param
import hvplot.pandas
import plotly.express as px

pd.options.plotting.backend='plotly'
pn.extension('plotly','tabulator','perspective', sizing_mode = 'stretch_width')

from fred import Escalation

esc=Escalation()

def add_indices(event):
    df = esc.make_index_from_search(search.value, limit=limit.value)
    print(df.columns)
    print(indices.object.columns)
    df_widget.stream(df.rename({i: str(i) for i in range(10)}, axis=1))
    indices.stream (df.rename({i: str(i) for i in range(10)}, axis=1))
    search.value=''



series = esc.make_index_from_search( ["Gross Domestic Product Deflator", "CPI", "PPI"],limit=1).drop_duplicates()

df_widget = pn.widgets.Tabulator(series.rename({i: str(i) for i in range(10)}, axis=1))

indices = pn.pane.Perspective(series.rename({i: str(i) for i in range(10)}, axis=1),toggle_config=False)
series_ids = pn.widgets.MultiChoice(options=series.id.unique().tolist())
base_year=pn.widgets.Select(name="Base Year",value=2023,options=list(range(1970, 2060)))
outlays = pn.widgets.Tabulator(name="Outlay Profile",value=pd.DataFrame(columns=['sum']+ list(range(10)), data=[[1,.75,.25]+[0.0]*8]))
fys = pn.widgets.IntRangeSlider(start=1970,end=2060)

search = pn.widgets.TextInput()
limit = pn.widgets.IntInput(start=1, value=1,end=5)
search_btn = pn.widgets.Button(name="Search For Indices")
search_btn.on_click(add_indices)

@pn.depends(series_ids=series_ids, outlays=outlays, base_year=base_year)
def update_indices(series_ids, outlays,base_year):
    print(base_year)
    df_widget.object=esc.make_index(series_ids, outlays = outlays, base_year=base_year)
    indices.patch(df_widget.object)


@pn.depends(df_widget=df_widget)    
def graph_indices(df_widget):
    df = df_widget
    rate = df.plot(x='FY', y='rate', color='series_id')
    rate.layout.autosize=True
    
    raw = df.plot(x='FY', y='raw', color='series_id')
    raw.layout.autosize=True
    return pn.Row(
        pn.Card(pn.pane.Plotly(rate, config={'responsive':True})),
        pn.Card(pn.pane.Plotly(raw, config={'responsive':True}))
    )



template = pn.template.FastListTemplate(
    title='Escalation Analysis: Search for actual data and create indices', 
        sidebar=[pn.Column(search, limit,search_btn)],
    main=[pn.Column(outlays,base_year,fys, indices, graph_indices)],
    accent_base_color="#88d8b0",
    header_background="#88d8b0",
)
template.servable()   
