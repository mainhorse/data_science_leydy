from fastapi import FastAPI
import pandas as pd
app = FastAPI()


df = pd.read_csv(".\\Data\\movies_dataset_ETL.csv", delimiter=',', header=0)
#df2 = pd.read_csv(".\\Data\\credits_ETL.csv", delimiter=',', header=0)
director = pd.read_csv(".\\Data\\directors_dataset.csv", delimiter=',',header=0)
pre_recommendations = pd.read_csv(".\\Data\\recomendaciones_peliculas.csv", delimiter=',',header=0)

'''1. Se ingresa un idioma (como están escritos en el dataset, 
no hay que traducirlos!). Debe devolver la cantidad de películas producidas en ese idioma.'''

@app.get('/idioma')
async def peliculas_idioma(idioma: str):
    count = df[df['original_language'] == idioma].shape[0]
    return {f"{count} películas fueron estrenadas en {idioma}"}

'''2. Se ingresa una pelicula. Debe devolver la duracion y el año.'''

@app.get('/duracion')
async def peliculas_duracion(Pelicula: str):
    pelicula = df[df['title'] == Pelicula]
    if pelicula.empty:
        return {f"No se encotró la película {pelicula}"}
    duracion = pelicula['runtime'].values[0]
    duracion = int(duracion)
    anio = pelicula['release_year'].values[0]
    return {f"La película {Pelicula} tiene una duración de {duracion} minutos y fue estrenada el año {anio}"}

'''3. Se ingresa la franquicia, retornando la cantidad de peliculas, ganancia total y promedio'''

@app.get('/franquicia')
async def franquicia(Franquicia: str):
    peliculas_franquicia = df[df['name_btc']== Franquicia]
    cantidad_peliculas = peliculas_franquicia.shape[0]
    if cantidad_peliculas == 0:
        return {f"No se encontraron franquicias por el nombre {Franquicia}"}
    ganancia_total = (peliculas_franquicia['revenue']).sum()
    return f"La franquicia {Franquicia} posee {cantidad_peliculas} películas, una ganancia total de {ganancia_total} USD y una ganancia promedio de {ganancia_total/cantidad_peliculas} USD."

'''4. Se ingresa un país (como están escritos en el dataset, no hay que traducirlos!), 
retornando la cantidad de peliculas producidas en el mismo.'''

@app.get('/pais')
async def peliculas_pais(Pais: str):
    pel_pais = df[df['country_name']== Pais].shape[0]
    return f"Se produjeron {pel_pais} películas en el país {Pais}"

'''5. productoras_exitosas( Productora: str ): Se ingresa la productora, 
entregandote el revenue total y la cantidad de peliculas que realizó.'''

@app.get('/productora')
async def productoras_exitosas(Productora: str):
    peliculas_productoras = df[df['ption_companies_name']== Productora]
    if peliculas_productoras.empty:
        return f"No se encontraron productoras por el nombre {Productora}"
    cantidad_peliculas = peliculas_productoras.shape[0]
    revenue = peliculas_productoras['revenue'].sum()
    return f"La productora {Productora} ha realizado {cantidad_peliculas} películas y ha tenido un total de recaudación de {revenue} dólares"

'''6. get_director(nombre_director): Se ingresa el nombre de un director que se encuentre dentro de un dataset
debiendo devolver el éxito del mismo medido a través del retorno. Además, deberá devolver el nombre de cada 
película con la fecha de lanzamiento, retorno individual, costo y ganancia de la misma, en formato lista.'''


@app.get('/director')
def get_director( nombre_director):
    movies_list = []
    return_var = 0
    movie_name_exit = ''
    total_retorno = 0
    movies = director[director['Nombre_Director'] == nombre_director]
    if movies.shape[0] >= 1:
        for index, movie in movies.iterrows():  
            if return_var < movie['Retorno_Inversion'] :
                movie_name_exit = movie['Nombre_Pelicula']
                return_var = movie['Retorno_Inversion'] 
            movie_dic = {
                'name_movie': movie['Nombre_Pelicula'],
                'release_day': movie['Fecha_Estreno_Pelicula'],
                'return': movie['Retorno_Inversion'],
                'budget': movie['Presupuesto'],
                'revenue': movie['Recaudacion_Total']
            }
            total_retorno += movie['Retorno_Inversion']
            movies_list.append(movie_dic)
            
        return {"Película_más_exitosa": movie_name_exit, " Total_retorno": total_retorno,"Películas" : movies_list}
    return f"No se encontro al director {nombre_director}"

@app.get('/recomendacion')
def recomendacion( titulo ):
    recommendations = pre_recommendations[pre_recommendations['movie_title'] == titulo]
    if not recommendations.empty:
        recommended_titles = recommendations.iloc[0]['recommended_movies']
        return recommended_titles.split(', ')
    else:
        return ["Película no encontrada en la Base de Datos."]
    