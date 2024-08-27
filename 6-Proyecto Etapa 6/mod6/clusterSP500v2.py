import streamlit as st
import seaborn as sns
import pandas as pd
from pandas import read_csv
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import pairwise_distances_argmin_min

import altair as alt
from sklearn.decomposition import PCA
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
import numpy as np


# Cargar datos del SP500
df = pd.read_csv("./stock_prices.csv")
print('Datos originales:')
print(df.head())

# Verificar valores nulos
print("\nValores nulos en cada columna:")
print(df.isnull().sum())
 
# Eliminar filas con valores nulos
data_cleaned = df.dropna()
 
# Eliminar duplicados
data_cleaned = data_cleaned.drop_duplicates()
 
# Convertir tipos de datos si es necesario (ejemplo: convertir una columna a tipo numérico)
# Reemplaza 'nombre_columna' con el nombre real de tu columna
# data_cleaned['nombre_columna'] = pd.to_numeric(data_cleaned['nombre_columna'], errors='coerce')
 
# Mostrar las primeras filas del DataFrame limpio
print("\nDatos limpios:")
print(data_cleaned.head())
 
# Guardar el DataFrame limpio en un nuevo archivo CSV (opcional)
# data_cleaned.to_csv('datos_limpios.csv', index=False)
# dataframe = pd.read_csv("C:\01-PJ_Analitica-AVDV288\Proyecto\6-Proyecto Etapa 6\mod6\stock_prices.csv")
# dataframe.head()

# Setting the id column as the index
# dataframe = pd.read_csv("C:\01-PJ_Analitica-AVDV288\Proyecto\6-Proyecto Etapa 6\mod6\stock_prices.csv", index_col="id")
# dataframe = pd.read_csv("C:\01-PJ_Analitica-AVDV288\Proyecto\6-Proyecto Etapa 6\mod6\stock_prices.csv", index_col=0)
 
# Preview first 5 rows
# dataframe.head()

st.title('Clusterización con KMeans - Dataset del SP500')
st.write('Datos del SP500:')

st.write(df.head())

df1 = df[['Date', 'Ticker', 'Close']].dropna()

# Escalado de los datos


scaler = StandardScaler()
df_scaled = scaler.fit_transform(df1)

st.write('Datos Preprocesados:')
st.write(pd.DataFrame(df_scaled, columns=['Date', 'Ticker', 'Close']).head())

# Selección del número de clusters
n_clusters = st.slider('Selecciona el número de clusters', 2, 10, 4)

# Aplicar KMeans
kmeans = KMeans(n_clusters=n_clusters, random_state=42)
clusters = kmeans.fit_predict(df_scaled)

df['Cluster'] = clusters

st.write(f'Clusters generados con KMeans para {n_clusters} clusters.')
st.write(df.head())

# Visualización interactiva con Altair
chart = alt.Chart(df).mark_circle(size=60).encode(
    x='Date',
    y='Ticker',
    color='Cluster:N',
    tooltip=['Date', 'Ticker', 'Close', 'Cluster']
).interactive()

st.altair_chart(chart, use_container_width=True)

st.subheader('Métricas de Clusterización')
st.write(f'Inercia del modelo: {kmeans.inertia_:.2f}')

st.write('Cambia el número de clusters con el slider para observar cómo se modifican los clusters y la inercia.')

# Visualización de los clusters en 3D
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')

scatter = ax.scatter(df['Date'], df['Ticker'], df['Close'], c=df['Cluster'], cmap='viridis')

ax.set_xlabel('Date')
ax.set_ylabel('Ticker')
ax.set_zlabel('Close')
ax.set_title(f'Clusters con {n_clusters} Clusters (Gráfico 3D)')

# Mostrar el gráfico en Streamlit
st.pyplot(fig)

# Aplicar PCA para reducir a 2 dimensiones
pca = PCA(n_components=2)
df_pca = pca.fit_transform(df_scaled)

# Convertir a DataFrame para facilitar el manejo
df_pca = pd.DataFrame(df_pca, columns=['PCA1', 'PCA2'])
df_pca['Cluster'] = clusters

st.write('Datos después de aplicar PCA:')
st.write(df_pca.head())

# Visualización interactiva con Altair tras PCA
chart_pca = alt.Chart(df_pca).mark_circle(size=60).encode(
    x='PCA1',
    y='PCA2',
    color='Cluster:N',
    tooltip=['PCA1', 'PCA2', 'Cluster']
).interactive()

st.altair_chart(chart_pca, use_container_width=True)
