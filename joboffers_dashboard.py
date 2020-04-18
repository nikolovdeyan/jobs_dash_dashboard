import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State

import os
import psycopg2
import pandas as pd


def make_header():
    return html.Div(
        id='title',
        children=[
            html.H1('Jobs Dashboard')
        ])


def make_tabs():
    return html.Div(
        id='app-main',
        children=[
            dcc.Tabs(
                id='tabs-container',
                value='hometab',
                children=[
                    dcc.Tab(
                        id='home-tab',
                        label='Home',
                        value='hometab',
                    ),
                    dcc.Tab(
                        id='boards-tab',
                        label='Boards',
                        value='boardtab',
                    ),
                    dcc.Tab(
                        id='jobs-tab',
                        label='Jobs',
                        value='jobtab',
                    ),
                    dcc.Tab(
                        id='companies-tab',
                        label='Companies',
                        value='companiestab',
                ),
            ]),
            html.Div(
                id='tab-content',
            ),
        ])


def make_home_tab():
    return html.Div(
        children=[
            html.Div(
                className='row flex-display',
                children=[html.H3('Home Tab Content')],
            )]
    )


def make_boards_tab():
    # The whole tab element
    return html.Div(
        children=[
            # The first row (filters and stats)
            html.Div(
                className='row flex-display',
                children=[
                    # The filter options
                    html.Div(
                        className='pretty_container four columns',
                        children=[
                            dcc.Dropdown(
                                id='boards-selection-dropdown',
                                options=[
                                    {'label': 'data jobs 2 weeks',
                                     'value': 'v_jobboard_data_offers_2w'},
                                    {'label': 'extended targets 2 weeks',
                                     'value': 'v_jobboard_extended_targets_2w'},
                                    {'label': 'sofia all 1 week',
                                     'value': 'v_jobboard_sofia_all_1w'},
                                ],
                                value='v_jobboard_data_offers_2w',
                            ),
                            html.Button(
                                id='boards-selection-btn',
                                n_clicks=0,
                                children='Run'),
                            dcc.Loading(
                                id='boards-selection-loading',
                                type='default',
                                children=html.Div(id='boards-query-output'))
                        ]),
                    # The info container
                    html.Div(
                        id='boards-info-container',
                        className='eight columns'),
                ]),
            html.Div(
                className='row flex-display',
                children=[
                    html.Div(
                        className='pretty_container twelve columns',
                        children=[
                            dcc.Loading(
                                type='default',
                                fullscreen=False,
                                children=html.Div(id='boards-query-table')
                            )]
                    )
                ])
        ])


def make_jobs_tab():
    # The whole tab element
    return html.Div(
        children=[
            # The first row (filters and stats)
            html.Div(
                className='row flex-display',
                children=[
                    # The filter options
                    html.Div(
                        className='pretty_container four columns',
                        children=[
                            html.P('Use PostgreSQL full-text search to filter job titles.'),
                            dcc.Input(id='jobs-tsv-query', type='text'),
                            html.Button(id='jobs-tsv-btn', n_clicks=0, children='Submit'),
                        ]),
                    # The info container
                    html.Div(
                        className='eight columns',
                        children=[
                            dcc.Loading(
                                type='default',
                                children=html.Div(id='jobs-info-container')
                        )]
                    ),
                ]),
            html.Div(
                className='row flex-display',
                children=[
                    html.Div(
                        className='pretty_container twelve columns',
                        children=dcc.Loading(html.Div(id='jobs-query-table'))
                    ),
                ])
        ])


def make_companies_tab():
    return html.Div(
        children=[
            html.Div(
                children=[
                    html.Div(
                        id='companies-filter',
                        children=[
                            html.P('Use PostgreSQL full-text search to filter company titles.'),
                            dcc.Input(id='companies-tsv-query', type='text'),
                            html.Button(id='companies-tsv-btn', n_clicks=0, children='Submit'),
                            dcc.Loading(id='loading-companies', type='default', children=html.Div(id='companies-query-output'))
                    ]),
                    html.Div(id='companies-query-output'),
                ]),
        ])


def make_jobs_stats_row(jobs_df):
    salary_only_df = jobs_df[jobs_df['norm_salary'].notnull()]
    num_results = len(jobs_df)
    num_results_salary = len(salary_only_df)
    avg_results_salary = round(salary_only_df['norm_salary'].mean(), 2)
    return [
        html.Div(
            className='row container-display',
            children=[
                html.Div(
                    id='total-results',
                    className='mini_container',
                    children=[
                        html.H6(id='total-results-text'),
                        html.P(f'Total results: {num_results}')
                    ],
                ),
                html.Div(
                    id='salary-results',
                    className='mini_container',
                    children=[
                        html.H6(id='salary-results-text'),
                        html.P(f'Results with salary: {num_results_salary}')
                    ],
                ),
                html.Div(
                    id='avg-salary',
                    className='mini_container',
                    children=[
                        html.H6(id='avgsalary-text'),
                        html.P(f'Average salary: {avg_results_salary}')
                    ],
                ),
                html.Div(
                    id='salary-level',
                    className="mini_container",
                    children=[
                        html.H6(id="salary-level-text"),
                        html.P(f'All average: '),
                    ],
                ),
            ]),
        html.Div(
            id='countGraphContainer',
            className='pretty_container',
            children=[
                '<chart placeholder>'
                #dcc.Graph(id='count_graph')
            ])
    ]


def make_jobs_table_row(jobs_df, max_rows=100):
    jobs_df = jobs_df.reset_index()
    jobs_df['subm_date'] = jobs_df['subm_date'].dt.strftime('%Y-%m-%d')
    table_columns = ['subm_date', 'job_id', 'job_title', 'company',
                     'job_location', 'salary', 'norm_salary']
    jobs_df = jobs_df[table_columns]
    return html.Div(
        children=[
            html.Table([
                html.Thead(
                    html.Tr([html.Th(col) for col in jobs_df.columns])
                ),
                html.Tbody([
                    html.Tr([
                        html.Td(jobs_df.iloc[i][col]) for col in jobs_df.columns
                    ]) for i in range (min(len(jobs_df), max_rows))
                ])
            ])
        ])


def df_to_table(dataframe, max_rows=100):
    return html.Div(
        children=[
            html.Table([
                html.Thead(
                    html.Tr([html.Th(col) for col in dataframe.columns])
                ),
                html.Tbody([
                    html.Tr([
                        html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
                    ]) for i in range (min(len(dataframe), max_rows))
                ])
            ])
        ])



db_creds = {
    'dbname': os.getenv('DBNAME'),
    'user': os.getenv('USERNAME'),
    'password': os.getenv('PASSWORD'),
    'host': os.getenv('DBHOST'),
    'port': os.getenv('DBPORT'),
}

app = dash.Dash(__name__)
app.config['suppress_callback_exceptions']=True
app.layout = html.Div(
    id='app-container',
    children=[
        make_header(),
        make_tabs(),
    ])


@app.callback(Output('boards-query-table', 'children'),
              [Input('boards-selection-btn', 'n_clicks')],
              [State('boards-selection-dropdown', 'value')])
def on_boards_query_submit(n_clicks, board_selection):
    with psycopg2.connect(f'dbname={db_creds["dbname"]}') as conn:
        board_df = pd.read_sql_query(
            f'select * from {board_selection}',
            conn, index_col='subm_date')
        board_df.index = pd.to_datetime(board_df.index)
    return df_to_table(board_df)



@app.callback([Output('jobs-info-container', 'children'),
               Output('jobs-query-table', 'children')],
              [Input('jobs-tsv-btn', 'n_clicks')],
              [State('jobs-tsv-query', 'value')])
def on_jobs_query_submit(n_clicks, query_str):
    with psycopg2.connect(f'dbname={db_creds["dbname"]}') as conn:
        jobs_df = pd.read_sql_query(
            f"select * from f_offers_by_title_kw('{query_str}')",
            conn, index_col='subm_date')
    jobs_df.index = pd.to_datetime(jobs_df.index)
    # Content for statistics elements:
    jobs_stats = make_jobs_stats_row(jobs_df)
    # Content for table element:
    jobs_table = make_jobs_table_row(jobs_df)
    return [jobs_stats, jobs_table]



@app.callback(Output('companies-query-output', 'children'),
              [Input('companies-tsv-btn', 'n_clicks')],
              [State('companies-tsv-query', 'value')])
def on_companies_query_submit(n_clicks, query_str):
    with psycopg2.connect(f'dbname={db_creds["dbname"]}') as conn:
        companies_df = pd.read_sql_query(
            f"select * from f_offers_by_company_kw('{query_str}')",
            conn, index_col='subm_date')
        companies_df.index = pd.to_datetime(companies_df.index)
    return df_to_table(companies_df)



@app.callback(Output('tab-content', 'children'),
              [Input('tabs-container', 'value'),])
def on_tabs_tab_selecion(tab):
    if tab == 'hometab':
        return make_home_tab()
    elif tab == 'boardtab':
        return make_boards_tab()
    elif tab == 'jobtab':
        return make_jobs_tab()
    elif tab == 'companiestab':
        return make_companies_tab()



if __name__ == '__main__':
    app.run_server(debug=True, dev_tools_hot_reload=False)
