# Die Erstellung dieser Datei orientiert sich an:
# Quelle:
#       https://github.com/Coding-with-Adam/Dash-by-Plotly/blob/master/Dash_Interactive_Graphs/Scatter_mapbox/recycling.py
#       https://github.com/Coding-with-Adam/Dash-by-Plotly/blob/master/Bootstrap/bootstrap_modal.py

import dash
from dash import html, dcc, dash_table
from dash_extensions.enrich import Output, DashProxy, Input, State, MultiplexerTransform
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import urllib.request
import pandas as pd
import urllib.request
import json
import ssl
import pgeocode
import re
from pathlib import Path
# https://py-tutorial-de.readthedocs.io/de/latest/modules.html
from import_history import *
from charts import *

# Konfiguration des Parsers
config = configparser.ConfigParser()
config.read('application.ini')

# HTTPS Request ermöglichen
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Land definieren
nomi = pgeocode.Nominatim('de')
count = 0

# Alte Bilder werden entfernt.
# Quelle: https://linuxize.com/post/python-delete-files-and-directories/
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = BASE_DIR + '/assets'
for f in Path(BASE_DIR).glob('*.png'):
    try:
        f.unlink()
    except OSError as e:
        print("Error: %s : %s" % (f, e.strerror))

# Token für Mapbox
mapbox_access_token = config['apikeys']['mapskey']

# App definieren. Hierbei wird MultiplexerTransform benutzt, um ein Output bei mehreren Callbacks zu ermöglichen
app = DashProxy(__name__, prevent_initial_callbacks=False, transforms=[MultiplexerTransform()],
                external_stylesheets=[dbc.themes.BOOTSTRAP])

# Alerts für die Pop Up/Validierung
alert_radius = dbc.Alert("Bitte geben Sie eine Zahl zwischen 1 und 25 ein.", color='danger', dismissable=True)
alert_plz = dbc.Alert("Bitte geben Sie eine fünfstellige Postleitzahl ein.", color='danger', dismissable=True)
alert_plz_nan = dbc.Alert("Die eingegebene Postleitzahl ist uns leider nicht bekannt. "
                          "Versuchen Sie mit einer anderen PLZ.", color='danger', dismissable=True)

# Pop Up Fenster für Statistiken
# Dabei widr in einem Modal eine Zeile mit zwei Columns sowie jeweils zwei Cards in jeder Column erstellt
modal = html.Div(
    [
        dbc.Modal([
            dbc.ModalHeader('Statistiken'),
            dbc.ModalBody(
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                dbc.Card([
                                    dbc.CardImg(top=True, id='laystday'),
                                    dbc.CardBody([
                                        html.H5('Preisverlauf des letzten Tages', className='card-subtitle')
                                    ])
                                ]),
                                html.Hr(),
                                dbc.Card([
                                    dbc.CardImg(top=True, id='last14days'),
                                    dbc.CardBody([
                                        html.H5('Preisverlauf der letzten 14 Tage', className='card-subtitle')
                                    ])
                                ])
                            ], id='firstcol'
                        ),
                        dbc.Col(
                            [
                                dbc.Card([
                                    dbc.CardImg(top=True, id='last7days'),
                                    dbc.CardBody([
                                        html.H5('Preisverlauf der letzten 7 Tage', className='card-subtitle')
                                    ])
                                ]),
                                html.Hr(),
                                dbc.Card([
                                    dbc.CardImg(top=True, id='besttime'),
                                    dbc.CardBody([
                                        html.H5('Beste Tankzeitpunkte der letzten 6 Wochen ', className='card-subtitle')
                                    ])
                                ])
                            ], id='secondcol')
                    ], id='modalrow')
            )
        ],
            id='modal',
            is_open=False,
            size='xl',
            backdrop=True,
            scrollable=True,
            centered=True,
            fade=True
        )
    ]
)

# Weboberfläche für die Dateneingaeb,, Dropdown Menü, Tabelle sowie Button für Statistiken
main_card = dbc.Card(
    [
        dbc.CardBody(
            [
                html.H5("PLZ und Radius eingeben:", className='card-subtitle'),
                # Die Eingabe für Postleitzahl und Radius.
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                dbc.Label('Postleitzahl', className='mr-2'),
                                dbc.Input(id='input-plz', type='text', value='58636', placeholder="PLZ eingeben")
                            ], width=8, className='mr-3'
                        ),
                        dbc.Col(
                            [
                                dbc.Label('Radius', className='mr-2'),
                                dbc.Input(id='input-radius', type='number', value='5', placeholder="Radius eingeben")
                            ], width=4, className='mr-3'
                        )
                    ], className='g-3'
                ),
                # Die Alerts werden ausgegeben, falls die Eingabe falsch ist
                html.Div(id='alert-radius', children=[]),
                html.Div(id='alert-plz', children=[]),
                html.Br(),
                dbc.Button('Suchen', id='submit-val', color='primary', className='me-1', size='lg'),
                html.Hr(),

                # Dropdown mit der Tankstellenliste, mit der niedrigsten Summe aller Preisen
                html.H5("Tankstellen mit den niedrigsten Preisen in der Umgebung", className='card-subtitle'),
                html.P(),
                dcc.Dropdown(id='dropdown',
                             placeholder='Bitte eine Tankstelle auswählen...',
                             optionHeight=35
                             ),
                html.Hr(),

                # Tankstelle mit der niedrigsten Summe aller Preisen in einer Tabelle
                html.Div([dash_table.DataTable(id='table', style_data={'whiteSpace': 'normal', 'height': 'auto'})]),
                html.Hr(),
                # Für die "Beste Zeit" Statistik ist der Sprittyp nötig.
                # Dieser wird mit Hilfe eines Dropdowns vom User abgefragt
                html.Div([
                    "Bitte einen Sprittyp für die Statistiken auswählen",
                    dcc.Dropdown(id='statistiken_dd',
                                 options=[
                                     {'label': 'E5', 'value': 'E5'},
                                     {'label': 'E10', 'value': 'E10'},
                                     {'label': 'Diesel', 'value': 'Diesel'}
                                 ],
                                 value='E5', clearable=False, multi=False, searchable=False
                                 )
                ]),
                html.P(),
                dbc.Button('Statistiken', id='statistiken', color='primary', className='me-1', size='lg'),
                modal
            ])
    ], color='light'
)

# Die Map wird in einem Graph erstellt
graph_card = dbc.Card(
    [
        dbc.CardBody(
            [
                html.H1("Liste der Tankstellen in der Umgebung!", className='card-title',
                        style={'textAlign': 'center'}),
                dcc.Graph(id='graph', figure={}, config={'displayModeBar': False}, style={'height': '90vh'})
            ]
        )
    ], color='light'
)

app.layout = html.Div([
    dbc.Row([dbc.Col(main_card, width=3), dbc.Col(graph_card, width=8)], justify='around', className='row g-1'),

    # Zwischenspeicher für die Data Table
    dcc.Store(id='intermediate-value', storage_type='session')
])


def get_data_column(df_table):
    # Die Daten der ausgewählten Tankstelle werden in der Tabelle angezeigt
    data_table = [{'name': i,
                   'infos': j}
                  for i, j in [('Name', df_table['Namen']),
                               ('Adresse', df_table['Adresse']),
                               ('E5', df_table['E5']), ('E10', df_table['E10']),
                               ('Diesel', df_table['Diesel'])
                               ]
                  ]

    columns = [
        {'name': '', 'id': 'name'},
        {'name': 'Infos', 'id': 'infos'}
    ]
    return data_table, columns


# ---------------------------------------------------------------
# Output für Graph, Zwischenspeicher, Alerts, Dropdownliste, Table
@app.callback(
    [Output('graph', 'figure'), Output('intermediate-value', 'data'),
     Output('alert-radius', 'children'), Output('alert-plz', 'children'),
     Output('dropdown', 'options'), Output('dropdown', 'value'),
     Output('table', 'data'), Output('table', 'columns')],
    Input('submit-val', 'n_clicks'),
    [State('input-plz', 'value'), State('input-radius', 'value')]
)
def update_figure(n, plz, rad):
    # Die Eingabe wird validiert
    tmp_rad = int(rad)
    if tmp_rad < 1 or tmp_rad > 25:
        return dash.no_update, dash.no_update, alert_radius, dash.no_update, \
               dash.no_update, dash.no_update, dash.no_update, dash.no_update
    elif not re.fullmatch(r'\d{5}', plz):
        return dash.no_update, dash.no_update, dash.no_update, alert_plz, \
               dash.no_update, dash.no_update, dash.no_update, dash.no_update

    # Der Request wird zusammengebaut
    home = config['url']['tankerkoenigurl']
    func = "list.php?"
    lat_plz = nomi.query_postal_code(plz).get('latitude')
    lon_plz = nomi.query_postal_code(plz).get('longitude')
    # Falls es NaN zurückgegeben wird, dann wird ein Alert angezeigt
    if np.isnan(lat_plz) or np.isnan(lon_plz):
        return dash.no_update, dash.no_update, dash.no_update, alert_plz_nan, \
               dash.no_update, dash.no_update, dash.no_update
    local = f"lat={lat_plz}&lng={lon_plz}&rad={rad}"
    opti = "&sort=price&type=diesel&apikey="
    apikey = config['apikeys']['tankerkoenigkey']
    assert apikey != ''

    # Der Request wir gefeuert
    url = home + func + local + opti + apikey
    html_req = urllib.request.urlopen(url, context=ctx).read()
    data = json.loads(html_req)
    # Die Listen Latitude, Longitude sowie IDs werden mit Daten befüllt
    ids = list()
    longitude = list()
    latitude = list()

    for ts in data['stations']:
        ids.append(ts['id'])
        latitude.append(ts['lat'])
        longitude.append(ts['lng'])

    # Die Preise für jede Tankstelle abrufen
    # Diese müssen stufenweise geholt werden, da die API nur 10 Responses liefert
    n = 10
    test = [ids[i:i + n] for i in range(0, len(ids), n)]
    prices = dict()
    func = "prices.php?"
    for i in range(len(test)):
        ids_price = ','.join(test[i])
        if ids_price[-1] == ',':
            ids_price = ids_price[:-1]
        url = home + func + "ids=" + ids_price + "&apikey=" + apikey
        html_price = urllib.request.urlopen(url, context=ctx).read()
        details = json.loads(html_price)
        prices.update(details['prices'])

    # Die Listen werden mit Daten befüllt
    tankst_infos = list()
    e5 = list()
    e10 = list()
    diesel = list()
    namen = list()
    adress = list()
    # Die if Bedingung dient zur Daten Validierung. Unvolständige Daten werden nicht berücksichtigt
    for ts in data['stations']:
        if (ts['id'] in prices) and (prices[ts['id']]['status'] != 'closed') \
                and (False not in prices[ts['id']].values()):
            namen.append(ts['name'])
            adress.append(ts['street'] + ' ' + ts['houseNumber'])
            tankst_infos.append('Name: ' + ts['name'] + '<br>' +
                                'Adresse: ' + ts['street'] + ' ' + ts['houseNumber'] + '<br>' +
                                'Preise: E5 - ' + str(prices[ts['id']]['e5']) + '<br>' +
                                '   E10 - ' + str(prices[ts['id']]['e10']) + '<br>' +
                                '   Diesel - ' + str(prices[ts['id']]['diesel']))
            e5.append(prices[ts['id']]['e5'])
            e10.append(prices[ts['id']]['e10'])
            diesel.append(prices[ts['id']]['diesel'])

    # Die Daten aller Listen werden in einem Pandas DataFrame gespeichert
    df = pd.DataFrame(data=list(zip(ids, latitude, longitude, namen, adress, tankst_infos, e5, e10, diesel)),
                      columns=['ID', 'Latitude', 'Longitude', 'Namen', 'Adresse', 'Infos', 'E5', 'E10', 'Diesel'])
    df_sub = df

    # Die Preise aller Tankstellen mit der niedrigsten Summe
    df['Price_Summe'] = df['E5'] + df['E10'] + df['Diesel']
    df_table = df.loc[df['Price_Summe'] == df['Price_Summe'].min(), :].copy(deep=True)
    # Spalte für die Dropdownliste wird erstellt
    df_table['Dropdown'] = df_table['Namen'] + ', ' + df_table['Adresse']
    options = [{'label': i, 'value': i} for i in df_table['Dropdown']]
    value_dd = options[0]['value']
    value_dd1 = [value_dd]
    df_get_data_columns = df_table[df_table['Dropdown'].isin(value_dd1)]
    # Bei jeder Suche wird die Tabelle mit dem ersten Eintrag aus der Liste befüllt
    data_table, columns = get_data_column(df_get_data_columns)
    # Create figure
    location = [go.Scattermapbox(
        lat=df_sub['Latitude'],
        lon=df_sub['Longitude'],
        mode='markers',
        marker={'color': 'green', 'size': 10},
        hoverinfo='text',
        hovertext=df_sub['Infos']
    )]

    # Return figure
    return {
               'data': location,
               'layout': go.Layout(
                   hovermode='closest',
                   autosize=True,
                   mapbox=dict(
                       accesstoken=mapbox_access_token,
                       bearing=0,
                       style='light',
                       center=dict(
                           lat=lat_plz,
                           lon=lon_plz
                       ),
                       pitch=0,
                       zoom=12
                   ),
               )
           }, df_table.to_dict('records'), dash.no_update, dash.no_update, options, value_dd, data_table, columns


# Das Fenster mit den Statistiken wird angezeigt
@app.callback(
    [Output('modal', 'is_open'), Output('laystday', 'src'),
     Output('last7days', 'src'), Output('last14days', 'src'),
     Output('besttime', 'src')],
    Input('statistiken', 'n_clicks'),
    State('modal', 'is_open')
)
def toggle_modal(n, is_open):
    global count
    laystday = 'ChartLastDay' + str(count-1) + '.png'
    last7days = 'ChartLast7Days' + str(count-1) + '.png'
    last14days = 'ChartLast14Days' + str(count-1) + '.png'
    besttime = 'ChartBestTime' + str(count-1) + '.png'
    if n:
        return not is_open, app.get_asset_url(laystday), app.get_asset_url(last7days), app.get_asset_url(last14days), app.get_asset_url(besttime)
    return is_open, app.get_asset_url(laystday), app.get_asset_url(last7days), app.get_asset_url(last14days), app.get_asset_url(besttime)


# Die Tabelle wird mit Daten befüllt
@app.callback(
    [Output('table', 'data'), Output('table', 'columns')],
    [Input('intermediate-value', 'data'), Input('dropdown', 'value'),
     Input('statistiken_dd', 'value')]
)
def update_table(data, value, value_sdd):
    global count
    df_table = pd.DataFrame(data)
    value = [value]
    df_table = df_table[df_table['Dropdown'].isin(value)]
    data_table, columns = get_data_column(df_table)

    # Die Charts Funktionen werden aufgerufen
    id_diagram = list(df_table['ID'])
    id = str(id_diagram[0])
    datum = get_max_date(id)
    createChart_lastDay(id, datum, count)
    createChart_last7Days(id, datum, count)
    createChart_last14Days(id, datum, count)
    createChart_BestTime(id, datum, value_sdd, count)
    count += 1
    return data_table, columns


if __name__ == '__main__':
    app.run_server(debug=False)
