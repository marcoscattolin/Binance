# %%
2+2

# %%
import pandas as pd

df = pd.DataFrame({
    'a' : [1,2,3],
    'b' : ['pippo', 'pluto', 'paperino'],
})

# %%
import matplotlib.pyplot as plt

plt.plot([1,2,3], [1,2,4])


# %%
import plotly.io as pio
import plotly.express as px

pio.renderers.default = 'notebook_connected'

px.bar(df, x = 'b', y = 'a')
# %%
from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as func
from pyspark.sql import Window

spark = (
    SparkSession
    .Builder()
    .appName('')
    .getOrCreate()
    )
sqlContext = SQLContext(spark)
# %%
