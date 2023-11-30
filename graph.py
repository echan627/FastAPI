import io
from fastapi import APIRouter, Response, status, File, UploadFile
from enum import Enum
from typing import Optional

import graphistry
import pandas as pd

router = APIRouter(
    prefix='/graphistry',
    tags=['graphistry']
)

# @app.get('/all')
# def get_all_blogs():
#   return {'message': 'All blogs provided'}

def emerging_leader(row):
    return 1 - row['eigenvector_centrality'] + row['degree_ratio']


def gate_keeper(row):
    return 1 - row['degree_ratio'] + row['eigenvector_centrality']


def boundary_spanner(row):
    return 1 - row['degree_ratio'] + row['top_facilitator']

@router.post(
  '',
  summary='Upload file to Graphistry',
  description='Upload file to Graphistry',
  response_description="datasetId and rankibgs"
  )
async def get_dataSetId(file: UploadFile, source: str, target: str):
  graphistry.register(api=3, username='elchan2', password='LP&YQ0p02^1mJYGA', protocol='https', server='madison.graphistry.com')
  data = pd.read_csv(file.file)
  file.file.close()


  graph = graphistry.edges(data).bind(source="Sender", destination="Receiver").materialize_nodes()
  graph = graph.compute_igraph('betweenness', out_col='top_facilitator')
  graph = graph.compute_igraph('eigenvector_centrality')
  graph = graph.get_degrees()
  graph = graph.compute_igraph('k_core')
  graph._nodes['k_core'] = graph._nodes['k_core'].astype('string')

  ndf = graph._nodes
  edf = graph._edges

  # Create a dictionary to store the results
  results = {}

  # Iterate over each node in ndf
  for node_id in ndf['id']:
      # Filter edf to include only edges where the node_id is the source
      
      df1 = edf[edf['Sender'] == node_id]
      df2 = edf[edf['Receiver'] == node_id]
      rdf2 = pd.DataFrame({'Sender': df2['Receiver'], 'Receiver': df2['Sender']})
      
      frames = [df1, rdf2]
      node_edges = pd.concat(frames)
      
      # Calculate the number of unique outgoing target nodes
      unique_target_nodes_count = node_edges['Receiver'].nunique()
      
      # Store the result in the dictionary
      results[node_id] = unique_target_nodes_count

  # Convert the results dictionary to a DataFrame and add it as a new column in ndf
  result_df = pd.DataFrame(results.items(), columns=['id', 'most_connected'])
  ndf = ndf.merge(result_df, on='id', how='left')

  # Fill NaN values with 0 if necessary
  ndf['most_connected'] = ndf['most_connected'].fillna(0)
    
  mostConnecteddf = ndf.sort_values(by='most_connected', ascending=False).head(10)
  mostConnecteds = mostConnecteddf['id'].values.tolist()
  highestD = mostConnecteddf.iloc[0,7]

  print(highestD)


  mostEventdf = graph._nodes.sort_values(by='degree', ascending=False).head(10)
  mostEvents = mostEventdf['id'].values.tolist()

  hiddenInfluencerdf = graph._nodes.sort_values(by='eigenvector_centrality', ascending=False).head(10)
  hiddenInfluencers = hiddenInfluencerdf['id'].values.tolist()

  topFacilitatordf = graph._nodes.sort_values(by='top_facilitator', ascending=False).head(10)
  topFacilitators = topFacilitatordf['id'].values.tolist()
  highestB = topFacilitatordf.iloc[0,1]


  mostCentral = {}

  for i in range(0, len(mostConnecteds)-1):
      
      if mostConnecteds[i] not in mostCentral.keys():
          mostCentral[mostConnecteds[i]] = 10-i
      else:
          mostCentral[mostConnecteds[i]] = mostCentral[mostConnecteds[i]] + 10 - i

      if mostEvents[i] not in mostCentral.keys():
          mostCentral[mostEvents[i]] = 10-i
      else:
          mostCentral[mostEvents[i]] = mostCentral[mostEvents[i]] + 10 - i
          
      if hiddenInfluencers[i] not in mostCentral.keys():
          mostCentral[hiddenInfluencers[i]] = 10-i
      else:
          mostCentral[hiddenInfluencers[i]] = mostCentral[hiddenInfluencers[i]] + 10 - i

      if topFacilitators[i] not in mostCentral.keys():
          mostCentral[topFacilitators[i]] = 10-i
      else:
          mostCentral[topFacilitators[i]] = mostCentral[topFacilitators[i]] + 10 - i
  
  mostCentral = pd.DataFrame(mostCentral.items(),columns=['id','score']).sort_values(by='score', ascending=False).head(10)['id'].values.tolist()
  highLeaders_left = hiddenInfluencerdf.merge(topFacilitatordf, on='id', how='left', suffixes=('', '_y'))
  highLeaders_right = hiddenInfluencerdf.merge(topFacilitatordf, on='id', how='right', suffixes=('_y', ''))
  frames = [highLeaders_left, highLeaders_right]
  highLeaders = pd.concat(frames).drop_duplicates()

  highLeaders['top_facilitator'] = highLeaders['top_facilitator'].apply(lambda x: x/highestB)
  highLeaders['high_leader'] = highLeaders[['top_facilitator', 'eigenvector_centrality']].sum(axis=1)
  highLeaders = highLeaders.sort_values(by='high_leader', ascending=False).head(10)['id'].values.tolist()

  ndf_copy = ndf.copy()
  ndf_copy['top_facilitator'] = ndf_copy['top_facilitator'].apply(lambda x: x/highestB)
  ndf_copy['degree_ratio'] = ndf_copy['most_connected'].apply(lambda x: x/highestD)


  ndf_copy['emerging_leader'] = ndf_copy.apply(emerging_leader, axis=1)
  emergingLeader = ndf_copy.sort_values(by='emerging_leader', ascending=False).head(10)['id'].values.tolist()

  ndf_copy['gate_keeper'] = ndf_copy.apply(gate_keeper, axis=1)
  gateKeeper = ndf_copy.sort_values(by='gate_keeper', ascending=False).head(10)['id'].values.tolist()

  ndf_copy['boundary_spanner'] = ndf_copy.apply(boundary_spanner, axis=1)
  boundarySpanner = ndf_copy.sort_values(by='boundary_spanner', ascending=False).head(10)['id'].values.tolist()
  print(boundarySpanner)

  dict = {'Most Central': mostCentral, 'Most Connected': mostConnecteds, 'Most Event':mostEvents, 'Hidden Influencer': hiddenInfluencers, 'Top Facilitator': topFacilitators,\
           'High Leader': highLeaders, 'Emerging Leader': emergingLeader, 'GateKeeper-Facilitator': gateKeeper, 'Boundary Spanner': boundarySpanner} 
  rankings = pd.DataFrame(dict).to_dict('records')
  graphInfo = (graph.nodes(ndf).bind(node='id').plot(render=False))

  dataset = "dataset="
  idx1: int = graphInfo.find(dataset) + len(dataset)
  idx2: int = graphInfo.find("&", idx1)
  return {'dataSetId': graphInfo[idx1:idx2],'rankings': rankings}
