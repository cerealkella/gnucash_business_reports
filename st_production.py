import pandas as pd
import streamlit as st
from gnucash_business_reports.builder import GnuCash_Data_Analysis


gda = GnuCash_Data_Analysis()
gda.year = 2026

st.set_page_config(layout="wide")
st.title("Production")

df = gda.get_production(include_operation_id=False, rename_columns=True)

crop = df.index.get_level_values('Crop').unique().to_list()
selected_crop = st.radio("Crop", crop)
filtered_df = df[df.index.get_level_values('Crop') == selected_crop]

st.subheader(f"{selected_crop} Production")
st.dataframe(filtered_df, hide_index=False)

st.subheader("Production by Field")
st.bar_chart(filtered_df.reset_index().set_index("Farm")["Bu/Acre"])