import io
from fastapi import APIRouter, Response, status, File, UploadFile
from enum import Enum
from typing import Optional
import pandas as pd
import numpy as np
import networkx as nx
from networkx.readwrite import json_graph
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import json

router = APIRouter(
    prefix='/networkx',
    tags=['networkx']
)

@router.post(
  '',
  summary='Upload file to networkx',
  description='Upload file to networkx',
  response_description="json file"
  )
async def get_graph(file: UploadFile, source: str, target: str):
    df = pd.read_csv(file.file)
    file.file.close()

    edge_attrs = []
    for series_name, series in df.items():
        if (series_name != 'Sender' and series_name != 'Receiver'):
            edge_attrs.append(series_name)

    G = nx.from_pandas_edgelist(df, source, target, edge_attrs, create_using=nx.MultiDiGraph())
    #   with open('networkdata1.json', 'w') as outfile1:
    #       outfile1.write(json.dumps(json_graph.node_link_data(G)))
    centrality = nx.eigenvector_centrality_numpy(G, max_iter=1000000)
    data = pd.DataFrame(centrality.items(), columns=["name", "eigenvector"])
    data = data.sort_values(by=['eigenvector'], ascending=False)
    
    nx.set_node_attributes(G, dict((k, format(v, '.6f')) for k,v in centrality.items()), "eigenvector")
    
    degrees = dict((x,y) for x,y in G.degree())
    nx.set_node_attributes(G, degrees, "degree")

    AG = nx.from_pandas_edgelist(df, 'Sender', 'Receiver', edge_attrs)

    def CalculateNeighbors(n):
        return len(AG[n].keys())

    neighbors = dict(zip(list(AG.nodes), map(CalculateNeighbors, AG.nodes)))
    nx.set_node_attributes(G, neighbors, "neighbors")

    betweenness = nx.betweenness_centrality(G)
    nx.set_node_attributes(G, betweenness, "betweenness")

    edf = pd.DataFrame(centrality.items(), columns=['id', 'eigenvector']).sort_values(by='eigenvector', ascending=False).head(10)
    edf['eigenvector'] = edf['eigenvector'].map(lambda x: '%.8f' % x)
    bdf = pd.DataFrame(betweenness.items(), columns=['id', 'betweenness']).sort_values(by='betweenness', ascending=False).head(10)
    bdf['betweenness'] = bdf['betweenness'].map(lambda x: '%.8f' % x)
    ddf = pd.DataFrame(degrees.items(), columns=['id', 'degrees']).sort_values(by='degrees', ascending=False).head(10)
    ndf = pd.DataFrame(neighbors.items(), columns=['id', 'neighbors']).sort_values(by='neighbors', ascending=False).head(10)

    mostCentral = {}

    i = 0
    for index, row in ndf.iterrows():
        mostCentral[row['id']] = 10-i
        i += 1

    i = 0
    for index, row in ddf.iterrows():
        if row['id'] not in mostCentral.keys():
            mostCentral[row['id']] = 10-i
        else:
            mostCentral[row['id']] = mostCentral[row['id']] + 10 - i
        i += 1


    i = 0
    for index, row in edf.iterrows():
        if row['id'] not in mostCentral.keys():
            mostCentral[row['id']] = 10-i
        else:
            mostCentral[row['id']] = mostCentral[row['id']] + 10 - i
        i += 1


    i = 0
    for index, row in bdf.iterrows():
        if row['id'] not in mostCentral.keys():
            mostCentral[row['id']] = 10-i
        else:
            mostCentral[row['id']] = mostCentral[row['id']] + 10 - i
        i += 1

    mostCentral = pd.DataFrame(mostCentral.items(), columns=['id', 'score']).sort_values(by='score', ascending=False).head(10)
    mostCentral['rank'] = np.arange( start=1, stop=len(mostCentral) + 1)
    rankings = {'MostCentral': mostCentral.to_dict('records'), 'MostConnected': ndf.to_dict('records'), 'MostEvent':ddf.to_dict('records'), 'HiddenInfluencer': edf.to_dict('records'), 'TopFacilitator': bdf.to_dict('records')} 
    rankings = pd.DataFrame(rankings).to_dict('records')

    result = json_graph.node_link_data(G)
    result["rankings"] = rankings
    json_compatible_item_data = jsonable_encoder(json.dumps(result))
    return JSONResponse(content=json_compatible_item_data)
