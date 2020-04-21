import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State

import plotly.graph_objs as go
import os
import psycopg2
import pandas as pd


def make_header():
    return html.Div(id="title", children=[html.H1("Jobs Dashboard")])


def make_tabs():
    return html.Div(
        id="app-main",
        children=[
            dcc.Tabs(
                id="tabs-container",
                value="hometab",
                children=[
                    dcc.Tab(id="home-tab", label="Home", value="hometab",),
                    dcc.Tab(id="boards-tab", label="Boards", value="boardtab",),
                    dcc.Tab(id="jobs-tab", label="Jobs", value="jobtab",),
                    dcc.Tab(
                        id="companies-tab", label="Companies", value="companiestab",
                    ),
                ],
            ),
            html.Div(id="tab-content",),
        ],
    )


def make_home_tab():
    # The whole tab element
    return html.Div(
        children=[
            # The first row
            html.Div(
                className="row flex-display",
                children=[
                    html.Div(className="pretty_container eight columns", children=[]),
                    html.Div(className="pretty_container eight columns", children=[
                        html.Div(id="home-linechart-container"),
                    ]),
                ],
            ),
            # The second row
            html.Div(
                className="row flex-display",
                children=[
                    html.Div(className="pretty_container twelve columns", children=[]),
                ],
            ),
            # The third row
            html.Div(
                className="row flex-display",
                children=[
                    html.Div(className="pretty_container eight columns", children=[]),
                    html.Div(className="pretty_container eight columns", children=[]),
                ],
            ),
        ]
    )


def make_boards_tab():
    # The whole tab element
    return html.Div(
        children=[
            # The first row (filters and stats)
            html.Div(
                className="row flex-display",
                children=[
                    # The filter options
                    html.Div(
                        className="pretty_container four columns",
                        children=[
                            dcc.Dropdown(
                                id="boards-selection-dropdown",
                                options=[
                                    {
                                        "label": "data jobs 2 weeks",
                                        "value": "v_jobboard_data_offers_2w",
                                    },
                                    {
                                        "label": "extended targets 2 weeks",
                                        "value": "v_jobboard_extended_targets_2w",
                                    },
                                    {
                                        "label": "sofia all 1 week",
                                        "value": "v_jobboard_sofia_all_1w",
                                    },
                                ],
                                value="v_jobboard_data_offers_2w",
                            ),
                            html.Button(
                                id="boards-selection-btn", n_clicks=0, children="Run"
                            ),
                            dcc.Loading(
                                id="boards-selection-loading",
                                type="default",
                                children=html.Div(id="boards-query-output"),
                            ),
                        ],
                    ),
                    # The info container
                    html.Div(id="boards-info-container", className="eight columns"),
                ],
            ),
            html.Div(
                className="row flex-display",
                children=[
                    html.Div(
                        className="pretty_container twelve columns",
                        children=[
                            dcc.Loading(
                                type="default",
                                fullscreen=False,
                                children=html.Div(id="boards-query-table"),
                            )
                        ],
                    )
                ],
            ),
        ]
    )


def make_jobs_tab():
    # The whole tab element
    return html.Div(
        children=[
            # The first row (filters and stats)
            html.Div(
                className="row flex-display",
                children=[
                    # The filter options
                    html.Div(
                        className="pretty_container four columns",
                        children=[
                            html.P(
                                "Use PostgreSQL full-text search:"
                            ),
                            dcc.Input(id="jobs-tsv-query", type="text"),
                            html.Button(
                                id="jobs-tsv-btn", n_clicks=0, children="Submit"
                            ),
                        ],
                    ),
                    # The info container
                    html.Div(
                        className="eight columns",
                        children=[
                            html.Div(id="jobs-info-container"),
                            html.Div(
                                className="pretty_container",
                                children=[html.Div(id="jobs-chart-container")],
                            ),
                        ],
                    ),
                ],
            ),
            html.Div(
                className="row flex-display",
                children=[
                    html.Div(
                        className="pretty_container twelve columns",
                        children=dcc.Loading(html.Div(id="jobs-query-table")),
                    ),
                ],
            ),
        ]
    )


def make_companies_tab():
    return html.Div(
        children=[
            html.Div(
                children=[
                    html.Div(
                        id="companies-filter",
                        children=[
                            html.P(
                                "Use PostgreSQL full-text search:"
                            ),
                            dcc.Input(id="companies-tsv-query", type="text"),
                            html.Button(
                                id="companies-tsv-btn", n_clicks=0, children="Submit"
                            ),
                            dcc.Loading(
                                id="loading-companies",
                                type="default",
                                children=html.Div(id="companies-query-output"),
                            ),
                        ],
                    ),
                    html.Div(id="companies-query-output"),
                ]
            ),
        ]
    )


def make_jobs_stats_row(jobs_df):
    salary_only_df = jobs_df[jobs_df["norm_salary"].notnull()]
    num_results = len(jobs_df)
    num_results_salary = len(salary_only_df)
    avg_results_salary = round(salary_only_df["norm_salary"].mean(), 2)
    return [
        html.Div(
            className="row container-display",
            children=[
                html.Div(
                    id="total-results",
                    className="mini_container",
                    children=[html.H6(f"Total results: {num_results}")],
                ),
                html.Div(
                    id="salary-results",
                    className="mini_container",
                    children=[html.H6(f"Results with salary: {num_results_salary}")],
                ),
                html.Div(
                    id="avg-salary",
                    className="mini_container",
                    children=[html.H6(f"Average salary: {avg_results_salary}")],
                ),
                html.Div(
                    id="salary-level",
                    className="mini_container",
                    children=[html.H6(f"All average: ")],
                ),
            ],
        ),
    ]


def make_jobs_table(jobs_df, max_rows=100):
    jobs_df = jobs_df.reset_index()
    jobs_df["subm_date"] = jobs_df["subm_date"].dt.strftime("%Y-%m-%d")
    table_columns = [
        "subm_date",
        "job_id",
        "job_title",
        "company",
        "job_location",
        "salary",
        "norm_salary",
    ]
    jobs_df = jobs_df[table_columns].head(max_rows)
    return html.Div(
        children=[
            html.Table(
                [
                    html.Thead(html.Tr([html.Th(col) for col in jobs_df.columns])),
                    html.Tbody(
                        [
                            # make_jobs_table_row(row) for row in jobs_df
                            html.Tr(
                                [
                                    html.Td(
                                        html.A(
                                            href=f"https://www.jobs.bg/job/{jobs_df.iloc[i][col]}",
                                            children=f"{jobs_df.iloc[i][col]}",
                                            target='_blank'
                                        )
                                    )
                                    if col == 'job_id'
                                    else html.Td(jobs_df.iloc[i][col])
                                    for col in jobs_df.columns
                                ]
                            )
                            for i in range(min(len(jobs_df), max_rows))
                        ]
                    ),
                ]
            )
        ]
    )


def make_boards_table_row(boards_df, max_rows=100):
    boards_df = boards_df.reset_index()
    boards_df["subm_date"] = boards_df["subm_date"].dt.strftime("%Y-%m-%d")
    table_columns = [
        "subm_date",
        "job_id",
        "job_title",
        "company_name",
        "job_location",
        "text_salary",
        "norm_salary",
    ]
    boards_df = boards_df[table_columns]
    return html.Div(
        children=[
            html.Table(
                [
                    html.Thead(html.Tr([html.Th(col) for col in boards_df.columns])),
                    html.Tbody(
                        [
                            html.Tr(
                                [
                                    html.Td(
                                        html.A(
                                            href=f"https://www.jobs.bg/job/{boards_df.iloc[i][col]}",
                                            children=f"{boards_df.iloc[i][col]}",
                                            target='_blank'
                                        )
                                    )
                                    if col == 'job_id'
                                    else html.Td(boards_df.iloc[i][col])
                                    for col in boards_df.columns
                                ]
                            )
                            for i in range(min(len(boards_df), max_rows))
                        ]
                    ),
                ]
            )
        ]
    )


def df_to_table(dataframe, max_rows=100):
    return html.Div(
        children=[
            html.Table(
                [
                    html.Thead(html.Tr([html.Th(col) for col in dataframe.columns])),
                    html.Tbody(
                        [
                            html.Tr(
                                [
                                    html.Td(dataframe.iloc[i][col])
                                    for col in dataframe.columns
                                ]
                            )
                            for i in range(min(len(dataframe), max_rows))
                        ]
                    ),
                ]
            )
        ]
    )


def make_jobs_chart(jobs_df):
    monthly_count_series = jobs_df.resample("1MS")["job_id"].count()
    monthly_count_series = monthly_count_series[1:-1]
    monthly_count_df = monthly_count_series.to_frame(name="subm_count")
    monthly_count_df = monthly_count_df.sort_index(ascending=False)
    monthly_count_df.rename_axis("month_ts", axis="index", inplace=True)

    jobs_submissions_bar = go.Bar(
        x=[month for month in monthly_count_df.index],
        y=[value for value in monthly_count_df.subm_count],
        hoverinfo="x+y",
        showlegend=False,
        marker=dict(line=dict(width=1,), opacity=0.8,),
    )
    data = [jobs_submissions_bar]
    layout = go.Layout(
        autosize=True,
        showlegend=False,
        hidesources=True,
        margin=dict(l=30, r=30, b=20, t=20), # NOQA
        plot_bgcolor="#F9F9F9",
        paper_bgcolor="#F9F9F9",
        xaxis=dict(
            type="date",
            fixedrange=True,
            hoverformat="%m, %Y",
            ticks="outside",
            tickmode="auto",
        ),
        yaxis=dict(fixedrange=True, ticks="outside", tickwidth=1,),
        bargap=0.2,
        barmode="group",
    )
    fig = go.Figure(data=data, layout=layout)
    return dcc.Graph(figure=fig)



db_creds = {
    "dbname": os.getenv("DBNAME"),
    "user": os.getenv("USERNAME"),
    "password": os.getenv("PASSWORD"),
    "host": os.getenv("DBHOST"),
    "port": os.getenv("DBPORT"),
}

app = dash.Dash(__name__)
app.config["suppress_callback_exceptions"] = True
app.layout = html.Div(id="app-container", children=[make_header(), make_tabs()])


@app.callback(
    Output("boards-query-table", "children"),
    [Input("boards-selection-btn", "n_clicks")],
    [State("boards-selection-dropdown", "value")],
)
def on_boards_query_submit(n_clicks, board_selection):
    with psycopg2.connect(f'dbname={db_creds["dbname"]}') as conn:
        board_df = pd.read_sql_query(
            f"select * from {board_selection}", conn, index_col="subm_date"
        )
        board_df.index = pd.to_datetime(board_df.index)
    return make_boards_table_row(board_df)


@app.callback(
    [
        Output("jobs-info-container", "children"),
        Output("jobs-chart-container", "children"),
        Output("jobs-query-table", "children"),
    ],
    [Input("jobs-tsv-btn", "n_clicks")],
    [State("jobs-tsv-query", "value")],
)
def on_jobs_query_submit(n_clicks, query_str):
    with psycopg2.connect(f'dbname={db_creds["dbname"]}') as conn:
        jobs_df = pd.read_sql_query(
            f"select * from f_offers_by_title_kw('{query_str}')",
            conn,
            index_col="subm_date",
        )
    jobs_df.index = pd.to_datetime(jobs_df.index)
    # Content for statistics elements:
    jobs_stats = make_jobs_stats_row(jobs_df)
    # Content for chart element:
    jobs_chart = make_jobs_chart(jobs_df)
    # Content for table element:
    jobs_table = make_jobs_table(jobs_df)
    return [jobs_stats, jobs_chart, jobs_table]


@app.callback(
    Output("companies-query-output", "children"),
    [Input("companies-tsv-btn", "n_clicks")],
    [State("companies-tsv-query", "value")],
)
def on_companies_query_submit(n_clicks, query_str):
    with psycopg2.connect(f'dbname={db_creds["dbname"]}') as conn:
        companies_df = pd.read_sql_query(
            f"select * from f_offers_by_company_kw('{query_str}')",
            conn,
            index_col="subm_date",
        )
        companies_df.index = pd.to_datetime(companies_df.index)
    return df_to_table(companies_df)




@app.callback(Output("tab-content", "children"), [Input("tabs-container", "value")])
def on_tabs_tab_selecion(tab):
    if tab == "hometab":
        return make_home_tab()
    elif tab == "boardtab":
        return make_boards_tab()
    elif tab == "jobtab":
        return make_jobs_tab()
    elif tab == "companiestab":
        return make_companies_tab()


if __name__ == "__main__":
    app.run_server(debug=True, dev_tools_hot_reload=False)
