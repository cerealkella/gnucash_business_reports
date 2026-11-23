import pandas as pd
import plotly.express as px
import streamlit as st
from gnucash_business_reports.builder import GnuCash_Data_Analysis


gda = GnuCash_Data_Analysis()
gda.year = 2026
years_to_go_back = 5


def trend_years():
    years_to_go_back = 5
    starting_year = gda.year - years_to_go_back
    label_string = ""
    for x in range(years_to_go_back):
        print(x)
        year_to_add = starting_year + x + 1
        label_string += str(year_to_add) + " "
    return label_string.rstrip()

data = gda.get_df_with_trend_data(gda.get_summary_by_account, years_to_go_back, ["Account"], "Amount")
data["Trend"] = data["Trend"].apply(lambda x: [float(i) for i in str(x).split()] if pd.notna(x) else [])

# bins = st.slider("Number of bins", min_value=3, max_value=10, value=5)

# fig = px.histogram(x=data, nbins=bins, labels={"x": "Value", "count": "Frequency"})
# st.plotly_chart(fig, use_container_width=True)


st.title("Yearly Trend")


st.bar_chart(data["Trend"].iloc[0])