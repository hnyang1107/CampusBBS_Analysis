from sklearn import preprocessing
from sklearn.cluster import KMeans
from community import  user_community_generator
from sklearn.metrics.cluster import adjusted_rand_score
import pandas as pd

def user_feature_generator(campus, resolution):
    for df in campus.df_with_resolution_generator(resolution):
        # 数据预处理
        min_max_scaler = preprocessing.MinMaxScaler()
        user_feature_post = df['post_id'].groupby([df['post_id'],df['board_name_cn']]).count().unstack().fillna(0)
        user_feature_reply = df['reply_id'].groupby([df['reply_id'], df['board_name_cn']]).count().unstack().fillna(0)
        user_feature = user_feature_post.combine_first(user_feature_reply) # TODO 弄清楚原理
        user_feature[::] = min_max_scaler.fit_transform(user_feature)
        yield  user_feature

def user_feature_kmeans(user_feature, feature):
    """

    :param user_feature: 用户特征矩阵
    :param feature: 某一特征
    :return: 用户根据特征聚类结果
    """
    data = user_feature[feature].values.reshape(-1,1)
    km = KMeans()
    km.fit(data)
    print("n_clusters: %d, inertia: %f" % (km.n_clusters, km.inertia_))
    return pd.Series(km.labels_,index=user_feature.index, name='clusters_by_kmeans')

def sorted_feature_generator(campus, resolution): # TODO 修改变量名
    user_feature_list = list(user_feature_generator(campus, resolution))
    user_community_list = list(user_community_generator(campus, resolution))

    for (c,c_) in zip(user_community_list, user_feature_list):
        c = c.sort_index()
        dic = {}
        for feature in c_.columns:
            x =  user_feature_kmeans(c_, feature).sort_index()
            dic[feature] = adjusted_rand_score(c.values,x.values)
        yield pd.Series(dic, name='feature_importance').sort_values(ascending=False)