import streamlit as st
import seaborn as sns
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import altair as alt
from sklearn.decomposition import PCA
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt

# Cargar datos del SP500
sp500 = sns.load_dataset('sp500')

st.title('Clusterización con KMeans - Dataset del SP500')
st.write('Datos del SP500:')
st.write(sp500.head())

df = sp500[['Date', 'Symbol', 'Close']].dropna()

# Escalado de los datos

scaler = StandardScaler()
df_scaled = scaler.fit_transform(df)

st.write('Datos Preprocesados:')
st.write(pd.DataFrame(df_scaled, columns=['Date', 'Symbol', 'Close']).head())

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
    y='Symbol',
    color='Cluster:N',
    tooltip=['Date', 'Symbol', 'Close', 'Cluster']
).interactive()

st.altair_chart(chart, use_container_width=True)

st.subheader('Métricas de Clusterización')
st.write(f'Inercia del modelo: {kmeans.inertia_:.2f}')

st.write('Cambia el número de clusters con el slider para observar cómo se modifican los clusters y la inercia.')

# Visualización de los clusters en 3D
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')

scatter = ax.scatter(df['Date'], df['Symbol'], df['Close'], c=df['Cluster'], cmap='viridis')

ax.set_xlabel('Date')
ax.set_ylabel('Symbol')
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