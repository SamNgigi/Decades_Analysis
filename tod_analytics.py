import streamlit as st
import pandas as pd

from tasks import get_df


st.title("Turn Of Decade Analysis")
st.header("""When do the most notable events in history occur?""")
st.text("Is it around the Turn of a decade,(xxx0s +- 2) years or mid decade (xxx3-xxx7)?")
st.text("This is a simple research to try answer that question.")

start, stop = st.beta_columns(2)

begin = start.number_input("Start Decade. eg 1900", value=1900)
end = stop.number_input("End Decade. eg 2000", value=2021)

@st.cache(suppress_st_warning=True)
def fetch(begin, end):
    if(str(begin)[-1] != "0"):
        st.write("Start Decade must end with a 0")
    else:
        st.write("Start Decade", begin)
        st.write("End Decade", end)
        return get_df(begin, end)

df = fetch(begin, end)
st.dataframe(df)







