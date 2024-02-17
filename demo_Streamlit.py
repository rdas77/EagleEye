import pandas as pd
import numpy as np
import streamlit as st

df = pd.DataFrame(
np.random.randn(50, 20),
columns=('col %d' % i for i in range(20)))
print(df)
st.dataframe(df)
