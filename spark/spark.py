from pyspark.sql import SparkSession
from pyspark.sql.functions import split
import pandas as pd
import requests
import json
import re
import unidecode
import sys

def main():

    def snake_case(df):
        new_cols=(unidecode.unidecode(column.replace(' ', '_').lower()) for column in df.columns)
        df_csv2 = df.toDF(*new_cols)
        return df_csv2

    def snake_case2(df):
    
        new_cols_json=(re.sub(r'(?<!^)(?=[A-Z])', '_', column).lower() for column in df.columns)
        df_json2 = df.toDF(*new_cols_json)
        new_cols_json = (unidecode.unidecode(column.replace('i_c_a_o', 'icao').lower()) for column in df_json2.columns)
        df_json2 = df_json2.toDF(*new_cols_json)
        return df_json2

    def faz_request(url, headers):

        dic = {"data":[]}
        for icao in list_aerodromos:

            try:
                querystring = {"icao":icao}
                response = requests.request("GET", url, headers=headers, params=querystring)

                if 'error' not in json.loads(response.text):
                    dic['data'].append(json.loads(response.text))
                else:
                    pass
            except:
                print(sys.exc_info())
                raise ValueError('Erro na requisição')
        return dic
    
    def export_json(df, dest):
        df.coalesce(1).write.mode("overwrite").json(dest)

    spark = SparkSession.builder.getOrCreate()

    # -------------------------- crio a camada raw ------------------------------
    df_csv = spark.read.csv('./AIR_CIA/*.csv',sep=';',
                         inferSchema=True, header=True)
    df_json = spark.read.json('./VRA/*.json')

    export_json(df_csv, './raw/')
    export_json(df_json, './raw/')
    
    df_csv2 =  snake_case(df_csv)
    df_companhia = df_csv2.withColumn('icao_empresa', split(df_csv2['icao_iata'], ' ').getItem(0)) \
        .withColumn('iata', split(df_csv2['icao_iata'], ' ').getItem(1))

    
    df_json2 = snake_case2(df_json)

    list_aerodromos_origem = [item[0] for item in df_json2.select('icao_aerodromo_origem').distinct().collect()]
    list_aerodromos_destino = [item[0] for item in df_json2.select('icao_aerodromo_destino').distinct().collect()]
    list_intesection_aerodromos = set(list_aerodromos_origem).intersection(set(list_aerodromos_destino))
    list_dif_aerodromos = set(list_aerodromos_origem).symmetric_difference(set(list_aerodromos_destino))
    list_aerodromos = set(list_dif_aerodromos).union(list_intesection_aerodromos)

    url = "https://airport-info.p.rapidapi.com/airport"

    headers = {
        "X-RapidAPI-Key": "a8488c52d4msha8cee3a4df7faf1p1964bejsnb04f94ec608f",
        "X-RapidAPI-Host": "airport-info.p.rapidapi.com"
    }

    dic = faz_request(url, headers)


    schema = list(dic['data'][0].keys())
    df_aerodromos = pd.DataFrame(dic['data']) # como são 300 registros, faz sentido usar pandas
    df_aerodromos2 = spark.createDataFrame(df_aerodromos)
    export_json(df_aerodromos2, './raw/')

    #----------- Crio a camada trusted -------------------------------#
    ## Criando as Tmepviews
    df_companhia.createOrReplaceTempView("companhia")
    df_aerodromos2.createOrReplaceTempView("aerodromos")
    df_json2.createOrReplaceTempView("vra")

    query = """ WITH temp_rota as (SELECT *, CONCAT(icao_aerodromo_origem,'-',icao_aerodromo_destino) AS rota 
                FROM vra),
                temp_comp_area as (select v.rota, c.razao_social, c.icao_empresa, v.icao_aerodromo_origem, v.icao_aerodromo_destino FROM companhia as c LEFT JOIN temp_rota as v ON c.icao_empresa = v.icao_empresa_aerea),
                temp_comp_area2 as (select *, a.name as name_origem  
                                    FROM temp_comp_area as tc 
                                    LEFT JOIN aerodromos as a on tc.icao_aerodromo_origem = a.icao 
                                    ),
                temp_comp_area3 as (select tc2.rota, tc2.razao_social, tc2.icao_empresa, tc2.icao_aerodromo_origem, tc2.icao_aerodromo_destino, tc2.icao, tc2.name_origem , tc2.state as estado_origem, a.name as name_destino, a.state as estado_destino
                                    FROM temp_comp_area2 as tc2 
                                    LEFT JOIN aerodromos as a on tc2.icao_aerodromo_destino = a.icao 
                                    )
                SELECT * FROM temp_comp_area3
                """
    df_trusted = spark.sql(query)

    export_json(df_trusted, './trusted/')

    

    # ------------------------- crio a camada refined ---------------------------------------

    query2 = """ with questao1 as (SELECT razao_social,icao_empresa,name_origem,name_destino,icao_aerodromo_origem,icao_aerodromo_destino,estado_origem,estado_destino,rota, count(rota), rank () over(PARTITION BY razao_social order by count(rota) DESC) as rank 
             FROM trusted
             GROUP BY razao_social,icao_empresa,name_origem,name_destino,icao_aerodromo_origem,icao_aerodromo_destino,estado_origem,estado_destino,rota)
            select * from questao1
            where rank = 1
             """
    df_trusted.createOrReplaceTempView("trusted")

    
    df_q1 = spark.sql(query2)

    # O que devo fazer na query:

    query3 = """ WITH temp_rota as (SELECT icao_aerodromo_origem,icao_aerodromo_destino,icao_empresa_aerea,situacao_voo,partida_real,chegada_real, CONCAT(icao_aerodromo_origem,'-',icao_aerodromo_destino) AS rota 
                                    FROM vra),
                temp_comp_area as (SELECT c.razao_social,c.icao_empresa, t.icao_aerodromo_origem,t.icao_aerodromo_destino,t.icao_empresa_aerea,t.situacao_voo,t.rota, t.partida_real,t.chegada_real 
                                FROM temp_rota as t 
                                LEFT JOIN companhia as c 
                                ON t.icao_empresa_aerea = c.icao_empresa),
                temp_aerodromo_origem as (SELECT a.name as name_aeroporto_origem, a.icao as icao_aeroporto_origem, t.razao_social,t.icao_empresa, t.icao_aerodromo_origem,t.icao_empresa_aerea,t.situacao_voo,t.rota,t.partida_real
                                FROM aerodromos as a 
                                LEFT JOIN temp_comp_area AS t
                                ON a.icao = t.icao_aerodromo_origem
                                
                ),
                temp_aerodromo_destino as (SELECT a.name as name_aeroporto_destino, a.icao as icao_aeroporto_destino, t.razao_social,t.icao_empresa, t.icao_aerodromo_destino,t.icao_empresa_aerea,t.situacao_voo,t.rota, t.chegada_real
                                FROM aerodromos as a 
                                LEFT JOIN temp_comp_area AS t
                                ON a.icao = t.icao_aerodromo_destino
                                
                ),

                temp_origem as (SELECT name_aeroporto_origem, icao_aerodromo_origem, razao_social,situacao_voo, count(distinct rota) as rotas_origem, count(partida_real) as n_partidas
                FROM temp_aerodromo_origem
                WHERE situacao_voo = 'REALIZADO'
                GROUP BY icao_aerodromo_origem,name_aeroporto_origem,razao_social,situacao_voo
                ORDER BY rotas_origem desc),

                temp_destino as (SELECT name_aeroporto_destino, icao_aeroporto_destino,razao_social,situacao_voo, count(distinct rota) as rotas_destino, count(chegada_real) as n_chegadas
                FROM temp_aerodromo_destino
                WHERE situacao_voo = 'REALIZADO'
                GROUP BY icao_aeroporto_destino, name_aeroporto_destino,razao_social,situacao_voo
                ORDER BY rotas_destino desc)

                select to.name_aeroporto_origem, td.name_aeroporto_destino, to.icao_aerodromo_origem,td.icao_aeroporto_destino, to.razao_social,to.rotas_origem,td.rotas_destino, to.n_partidas, td.n_chegadas
                from temp_origem as to 
                LEFT JOIN temp_destino as td
                ON to.icao_aerodromo_origem = td.icao_aeroporto_destino
                """
    df_q2 = spark.sql(query3)

    export_json(df_q1, './refined/')
    export_json(df_q2, './refined/')

if __name__ == '__main__':
    main()


