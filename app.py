import pandas as pd
import streamlit as st
from gnucash_business_reports.builder import GnuCash_Data_Analysis


gda = GnuCash_Data_Analysis()
gda.year = 2026
st.set_page_config(layout="wide")
st.title("Financials")


# df_original = gda.get_farm_cash_transactions() #.groupby(["account_type"]).sum(numeric_only=True)

df_original = gda.filter_by_year(gda.get_farm_cash_transactions(), column="post_date",)
df_details = df_original.set_index("account_code")
df_details = df_details[[    "post_date",
                            "src_code",
                            "account_name",
                            "account_desc",
                            "memo",
                            "quantity",
                            "amt",
                            ]]

df = df_original.groupby(["account_type"])\
        .sum(numeric_only=True) \
        .style.format({"amt": "${:,.2f}"})

event = st.dataframe(
    df,
    on_select="rerun",
    selection_mode="single-row",  # or "single-row"
    hide_index=False,
)

if event.selection.rows:
    index_name = df.index.name
    selected_labels = df.index[event.selection.rows].tolist()

    # st.write(f"Index Name: **{index_name}**")
    # st.write(f"Selected {index_name}s:", selected_labels)

    dff = gda.get_summary(groupby=["account_type", "account_code", "account_name"]) \
        .set_index(["account_type", "account_code"])
    dff = dff[dff.index.get_level_values('account_type') == selected_labels[0]]

    st.subheader("Selected Rows")
    account_event = st.dataframe(dff.style.format({"amt": "${:,.2f}"}),
                    selection_mode="single-row",
                    on_select="rerun",
                    hide_index=False,)

    if account_event.selection.rows:
        account_code = dff.index.names[1]
        # st.write(account_event.selection.rows)
        selected_account_labels = dff.index[account_event.selection.rows].tolist()

        # st.write(f"Index Name: **{account_code}**")
        # st.write(f"Selected {account_code}s:", selected_account_labels)

        df_details = df_details[df_details.index.get_level_values('account_code') == selected_account_labels[0][1]]

        st.dataframe(df_details)

        df_chart = df_details[["post_date", "amt"]]
        df_chart["month"] = df_chart["post_date"].dt.month_name()

        st.bar_chart(df_chart, x="month", y="amt", x_label="Date", y_label="$")

