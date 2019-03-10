from networkx.algorithms.community import greedy_modularity_communities
#from networkx.algorithms.community.quality import performance
import pandas as pd

def user_community_generator(campus, resolution):
    for G in campus.UserGraph_generator(resolution):
        communities_lst = list(greedy_modularity_communities(G))
        # print(performance(G, communities_lst))
        dic = {}
        for i,community in enumerate(communities_lst,0):
            for user_id in community:
                dic[user_id] = i
        yield pd.Series(dic, name='clusters_by_modularity')

def user_community(G):
    communities_lst = list(greedy_modularity_communities(G))
    # print(performance(G, communities_lst))
    dic = {}
    for i, community in enumerate(communities_lst, 0):
        for user_id in community:
            dic[user_id] = i
    return pd.Series(dic, name='clusters_by_modularity')