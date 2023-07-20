from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

import pandas as pd
import gspread_pandas as gpd
import os
os.environ['GSPREAD_PANDAS_CONFIG_DIR'] = 'secrets'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "secrets/google_secret.json"

from datetime import timedelta, date, datetime

MAIN_SHEET_ID = #"Insert your Google Sheet ID here"
GA_PROPERTY_ID = #"Insert your Google Analytics Property ID here"

START_DATE = (datetime.now() - timedelta(days=365)).date().strftime("%Y-%m-%d")
END_DATE = date.today().strftime("%Y-%m-%d")

CONV_METRICS = ("conversions", "totalUsers")
CONV_DIMENSIONS = ("sessionSource", "sessionMedium", "campaignName")

DEFAULT_DATE_RANGE = ((START_DATE,END_DATE),)

def google_analytics_data_to_df(
    property_id : str,
    dimensions : tuple[str],
    metrics : tuple[str],
    date_ranges : tuple[str],
):
    dimensions = [Dimension(name=dim) for dim in dimensions]
    metrics = [Metric(name=metric) for metric in metrics]
    date_ranges = [DateRange(start_date=dr[0], end_date=dr[1]) for dr in date_ranges]

    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=dimensions,
        metrics=metrics,
        date_ranges=date_ranges,
    )
    response = client.run_report(request)

    headers = [*[d.name for d in dimensions], *[metric.name for metric in metrics]]
    rows = []
    for row in response.rows:
        r = [v.value for v in row.dimension_values]
        r.extend([x.value for x in row.metric_values])
        rows.append(r)

    df = pd.DataFrame(columns=headers)

    for row in rows:
        df.loc[len(df)] = row

    return df

def drop_to_sheets(spread, sheet_name, campaigns_df):
    spread.open_sheet(sheet_name, create = True)
    spread.sheet.clear()
    spread.df_to_sheet(campaigns_df, replace = False)


analytics_df = google_analytics_data_to_df(GA_PROPERTY_ID, CONV_DIMENSIONS, CONV_METRICS, DEFAULT_DATE_RANGE)

spread = gpd.Spread(MAIN_SHEET_ID)

drop_to_sheets(spread, 'Analytics Data', analytics_df)