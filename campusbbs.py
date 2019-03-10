
"""
TODO List:
-----------
 + 研究一下 G.node 别人是怎么写的



"""

import pandas as pd
import networkx as nx
from datetime import timedelta


class CampusBBS(object):

    def __init__(self, campus_name, *time_args, from_pickle=True): # TODO 若不存在 df 应该是返回空的dataframe而不是报错
        """ 从数据库文件中读取数据构建 CampusBBS 类型
        ----
        为了避免由于版本迭代而造成 pickle 文件无法读取，在 ./csv 路径下存储了对应高校的 csv 文件，
        默认从 ./pickle 路径下读取 pickle 文件（form_pickle=True）可以提高读取效率.
        
        -----
        Args:
            campus_name: 高校名称英文缩写，不区分大小写
            from_pickle: 是否从 pickle 文件读取，False 则读取 csv 文件
            *time_args: 起止时间参数，若给出必须同时指定起始和终止时间戳
            
        -----
        return:
            CampusBBS 类型数据
            
        -----
        example:
            shu = CampusBBS('shu', 2011) # 读取 2011 年 shu 实例
            shu = CampussBBS('shu', '2011/2/4','2011/2/9') # 读取 2011/2/4 - 2011/2/9(包括)时间内 shu 实例
            
        """
        self.name = str.upper(campus_name)
        try:
            # 原始数据已经删掉 post_id 为空的行
            if from_pickle:
                df = pd.read_pickle('./pickle/data_' + str.lower(campus_name) +'.pkl')
            else:
                df = pd.read_csv('./csv/data_'+ str.lower(campus_name) +'.csv', index_col='post_time', parse_dates=['post_time'], infer_datetime_format=True)
        except:
            raise KeyError("高校 %s 在数据库中不存在!" % campus_name)
            
        if time_args:
            if len(time_args)==1:
                self.df = df[str(time_args[0])].copy()
            elif len(time_args)==2:
                self.df = df[str(time_args[0]):str(time_args[1])].copy()
            else:
                raise KeyError("时间参数错误，必须指定一个或两个时间参数！")
        else:
            self.df = df   
            
        self.start_time = self.df.index.min()
        self.end_time = self.df.index.max()


    def number_of_users(self): 
        """统计某一时段论坛内出现的不同的 ID 数目 
        """
        post_id_set = set(self.df.post_id.unique())
        target_id_set = set(self.df.target.dropna().unique())
        return len(post_id_set.union(target_id_set))
    
    def number_of_boards(self):
        """统计某一时段论坛版块总数
        """
        try:
            return self.df.board_name_en.nunique()
        except:
            return self.df.board_name_cn.nunique()

    def number_of_threads(self):
        """统计某一时段论坛帖子总数
        """
        return self.df.thread_uid.nunique()

    
    def describe(self):
        """打印当前时间段内的论坛信息
        """
        print("当前高校： %s" % self.name)
        print("当前时段起始日期： %s" % self.start_time)
        print("当前时段结束日期： %s" % self.end_time)
        print("当前时段内总用户数：%d" % self.number_of_users())
        print("当前时段内总版块数：%d" % self.number_of_boards())
        print("当前时段内总帖子数：%d" % self.number_of_threads())
    
    
    def _user2user_graph(self, weight=True, self_loop=False): # TODO 使用 for 效率太低
        """用户和用户间构建的简单无向图
        
        允许存在用户和用户自身的连边，两用户之间只存在一条边，默认为加权网络，权重为交互
        次数，若设置为 False， 则返回简单无向无权网络
        
        Args:
            weight：是否为有权网络.
        return:
            networkx.Graph 类型的简单无向网络. 
            连边属性：
                weight: 用户间交互次数
                time: 用户交互时间戳
        """
        G = nx.Graph()
        for post_time, post in self.df.iterrows():
            n0 = post.post_id
            n1 = post.target
            if n0!=n1 or self_loop: # 只有同时为 False 才不进入连边
                if G.has_edge(n0, n1):
                    G[n0][n1]['weight'] += 1
                else:
                    G.add_edge(n0,n1, weight=1)
                    G[n0][n1]['time'] = post_time
        return G
        
    
    def user2user_graph(self, weight=True, self_loop=False): # TODO 结构过于复杂，应删掉子函数，两列变一列的映射
        """用户和用户间构建的简单无向图
        
        允许存在用户和用户自身的连边，两用户之间只存在一条边，默认为加权网络，权重为交互
        次数，若设置为 False， 则返回简单无向无权网络
        
        Args:
            weight：是否为有权网络.
        return:
            networkx.Graph 类型的简单无向网络. 
            连边属性：
                weight: 用户间交互次数
                time: 用户交互时间戳
        """ 
        df = self.df.dropna(subset=['target']).copy()
        
        if not self_loop: # 删除自己回复自己的
            df = df[df.post_id != df.target].copy()
        def _to_edge_tuple(x):
            if x['post_id'] > x['target']:
                return (x['target'], x['post_id'])
            else:
                return (x['post_id'], x['target'])
        df['edge'] = df.apply(_to_edge_tuple, axis=1) #生成无序边元组
        weight_series =  df.groupby('edge').size()
        df = pd.merge(df, weight_series.to_frame('weight').reset_index())
        G = nx.from_pandas_edgelist(df,'post_id','target',edge_attr=['weight'])
        
        return G
    
    def user2board_bigraph(self): # TODO
        pass
    
    
    
    
    
    
    
    
    
    
    
    
    
    def df_user(self):
        return self.df_all.set_index(['post_id', self.df_all.index])

    def df_with_resolution_generator(self, resolution):
        """
        最小分辨率为'D'（天）
        :param resolution:
        :return:
        """
        for period in self.df_all.resample(resolution, kind='period').size().index:
            start_timestamp = period.to_timestamp(how='start')
            end_timestamp = period.to_timestamp(how='end')
            df = self.df_all.loc[start_timestamp: end_timestamp + timedelta(days=1)]
            if resolution=='Y':
                df.name = str(start_timestamp.year)
            if resolution=='M':
                df.name = str(start_timestamp.year) + '-' + str(start_timestamp.month)
            if resolution=='W':
                df.name = str(start_timestamp.year) + '-W' +str(start_timestamp.week)
            if resolution=='D':
                df.name = str(start_timestamp.date())
            yield df

    def UserGraph_generator(self, resolution, graph_type='undirected_graph'):
        """
        不同时间分辨率下切分 df_all 得到的网络
        :param resolution:
        :param user_feature:
        :param create_using: 网络类型
        :return:
        """
        for df in self.df_with_resolution_generator(resolution):
           # df = df.dropna(subset=['reply_id']).copy() #删除没有回复的孤立点
            name = df.name
            def to_edge_tuple(x):
                if x['post_id'] > x['reply_id']:
                    return (x['reply_id'], x['post_id'])
                else:
                    return (x['post_id'], x['reply_id'])

            if graph_type == 'undirected_graph':
                df['edge'] = df.apply(to_edge_tuple, axis=1) #生成无序边元组
            if graph_type == 'directed_graph':
                df['edge'] = df[['post_id', 'reply_id']].apply(tuple, axis=1) # 生成有序边元祖
            weight_series =  df.groupby('edge').size()
            df = pd.merge(df, weight_series.to_frame('weight').reset_index())

            if graph_type == 'undirected_graph':
                G = nx.from_pandas_edgelist(df,'post_id','reply_id',edge_attr=['weight'])
            if graph_type == 'directed_graph':
                G = nx.from_pandas_edgelist(df, 'post_id', 'reply_id', edge_attr=['weight'],create_using=nx.DiGraph())
            G.name = name
            yield G

    def query_board(self, board_name_cn, period=None): # TODO 检查
        if not period:
            return self.df_all[self.df_all.board_name_cn == board_name_cn]
        else:
            return self.df_all[period][self.df_all[period].board_name_cn == board_name_cn]

    def query_user_post(self, post_id, period='all'): # TODO 检查
        if not period:
            return self.df_all[self.df_all.post_id == post_id]
        else:
            return self.df_all[period][self.df_all[period].post_id == post_id]