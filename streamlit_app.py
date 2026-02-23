import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")
st.write("Orders that need to be filled.")

session = get_active_session()

# Puxa pedidos pendentes (garanta que ORDER_UID vem junto!)
sp_df = (
    session.table("SMOOTHIES.PUBLIC.ORDERS")
    .select("ORDER_UID", "INGREDIENTS", "NAME_ON_ORDER", "ORDER_FILLED")
    .filter(col("ORDER_FILLED") == False)
)

# Se não tem pedidos pendentes
if sp_df.count() == 0:
    st.success("There are no pending orders right now", icon="👍")
else:
    # Converte para Pandas para editar no Streamlit
    editable_df = sp_df.to_pandas()

    # Opcional: deixar só ORDER_FILLED editável (recomendado)
    edited_df = st.data_editor(
        editable_df,
        num_rows="fixed",
        disabled=["ORDER_UID", "INGREDIENTS", "NAME_ON_ORDER"],  # só marca filled
    )

    submitted = st.button("Submit")

    if submitted:
        # Garante boolean no pandas (às vezes vem como object)
        edited_df["ORDER_FILLED"] = edited_df["ORDER_FILLED"].astype(bool)

        og_dataset = session.table("SMOOTHIES.PUBLIC.ORDERS")

        # Cria dataframe Snowpark com as alterações
        edited_dataset = session.create_dataframe(
            edited_df[["ORDER_UID", "ORDER_FILLED"]]
        )

        try:
            og_dataset.merge(
                edited_dataset,
                og_dataset["ORDER_UID"] == edited_dataset["ORDER_UID"],
                [
                    when_matched().update(
                        {"ORDER_FILLED": edited_dataset["ORDER_FILLED"]}
                    )
                ],
            )
            st.success("Order(s) Updated!", icon="👍")
        except Exception as e:
            st.error("Something went wrong while updating.")
            st.write(e)


