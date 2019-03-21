#!/usr/bin/env python
# coding: utf-8

# In[34]:


# %load facebook_conversion_gentic_algorithm.py
#!/usr/bin/env python

# In[1]:


import numpy as np
# from GAIndividual import GAIndividual
import random
import copy
import matplotlib.pyplot as plt
import pandas as pd
import index_collector_leadgen_facebook
sizepop, vardim, MAXGEN, params = 200, 7, 3, [0.9, 0.1, 0.5]


class GeneticAlgorithm(object):
    '''
    The class for genetic algorithm
    '''

    def __init__(self, sizepop, vardim, bound, MAXGEN, params):
        '''
        sizepop: population sizepop 種群數量
        vardim: dimension of variables 變量維度
        bound: boundaries of variables 變量邊界 -10 10 (最佳化權重上下限)
        MAXGEN: termination condition  迭代次數  1000 (子代代數)
        param: 交叉率, 變異率, alpha = [0.9, 0.1, 0.5]
        '''
        self.sizepop = sizepop
        self.MAXGEN = MAXGEN
        self.vardim = vardim
        self.bound = bound
        self.population = []
        self.fitness = np.zeros((self.sizepop, 1))
        self.trace = np.zeros((self.MAXGEN, 2))
        self.params = params

    def initialize(self):
        '''
        initialize the population
        '''
        for i in range(0, self.sizepop):
            ind = GAIndividual(self.vardim, self.bound)
            ind.generate()
            self.population.append(ind)

    def evaluate(self):
        '''
        evaluation of the population fitnesses
        '''
        for i in range(0, self.sizepop):
            self.population[i].calculate_fitness()
            self.fitness[i] = self.population[i].fitness

    def solve(self):
        '''
        evolution process of genetic algorithm
        '''
        self.t = 0
        self.initialize()
        self.evaluate()
        best = np.max(self.fitness)
        best_index = np.argmax(self.fitness)
        self.best = copy.deepcopy(self.population[best_index])
        self.avefitness = np.mean(self.fitness)
        self.trace[self.t, 0] = (1 - self.best.fitness) / self.best.fitness
        self.trace[self.t, 1] = (1 - self.avefitness) / self.avefitness
        print("Generation %d: optimal function value is: %f; average function value is %f" % (
            self.t, self.trace[self.t, 0], self.trace[self.t, 1]))
        while (self.t < self.MAXGEN - 1):
            self.t += 1
            self.selection_operation()
            self.crossover_operation()
            self.mutation_operation()
            self.evaluate()
            best = np.max(self.fitness)
            best_index = np.argmax(self.fitness)
            if best > self.best.fitness:
                self.best = copy.deepcopy(self.population[best_index])
            self.avefitness = np.mean(self.fitness)
#             self.trace[self.t, 0] = (1 - self.best.fitness) / self.best.fitness
#             self.trace[self.t, 1] = (1 - self.avefitness) / self.avefitness
            self.trace[self.t, 0] = self.best.fitness
            self.trace[self.t, 1] = self.avefitness
            print("Generation %d: optimal function value is: %f; average function value is %f" % (
                self.t, self.trace[self.t, 0], self.trace[self.t, 1]))

        print("Optimal function value is: %f; " %
              self.trace[self.t, 0])
        print("Optimal solution is:")
        print("m1--m2--m3--m4--m5--mspend--mbid")
        print(self.best.chrom)
        self.print_result()
        return self.best.chrom

    def selection_operation(self):
        '''
        selection operation for Genetic Algorithm
        '''
        newpop = []
        total_fitness = np.sum(self.fitness)
        accu_fitness = np.zeros((self.sizepop, 1))

        sum1 = 0.
        for i in range(0, self.sizepop):
            accu_fitness[i] = sum1 + self.fitness[i] / total_fitness
            sum1 = accu_fitness[i]

        for i in range(0, self.sizepop):
            r = random.random()
            idx = 0
            for j in range(0, self.sizepop - 1):
                if j == 0 and r < accu_fitness[j]:
                    idx = 0
                    break
                elif r >= accu_fitness[j] and r < accu_fitness[j + 1]:
                    idx = j + 1
                    break
            newpop.append(self.population[idx])
        self.population = newpop

    def crossover_operation(self):
        '''
        crossover operation for genetic algorithm
        '''
        newpop = []
        for i in range(0, self.sizepop, 2):
            idx1 = random.randint(0, self.sizepop - 1)
            idx2 = random.randint(0, self.sizepop - 1)
            while idx2 == idx1:
                idx2 = random.randint(0, self.sizepop - 1)
            newpop.append(copy.deepcopy(self.population[idx1]))
            newpop.append(copy.deepcopy(self.population[idx2]))
            r = random.random()
            if r < self.params[0]:
                cross_pos = random.randint(1, self.vardim - 1)
                for j in range(cross_pos, self.vardim):
                    newpop[i].chrom[j] = newpop[i].chrom[j] * self.params[2] +                     (1 - self.params[2]) * newpop[i + 1].chrom[j]
                    newpop[i + 1].chrom[j] = newpop[i + 1].chrom[j] *  self.params[2] +                     (1 - self.params[2]) * newpop[i].chrom[j]
        self.population = newpop

    def mutation_operation(self):
        '''
        mutation operation for genetic algorithm
        '''
        newpop = []
        for i in range(0, self.sizepop):
            newpop.append(copy.deepcopy(self.population[i]))
            r = random.random()
            if r < self.params[1]:
                mutate_pos = random.randint(0, self.vardim - 1)
                theta = random.random()
                if theta > 0.5:
                    newpop[i].chrom[mutate_pos] = newpop[i].chrom[
                        mutate_pos] - (newpop[i].chrom[mutate_pos] - self.bound[0, mutate_pos]) * (1 - random.random() ** (1 - self.t / self.MAXGEN))
                else:
                    newpop[i].chrom[mutate_pos] = newpop[i].chrom[
                        mutate_pos] + (self.bound[1, mutate_pos] - newpop[i].chrom[mutate_pos]) * (1 - random.random() ** (1 - self.t / self.MAXGEN))
        self.population = newpop

    def print_result(self):
        '''
        plot the result of the genetic algorithm
        '''
        x = np.arange(0, self.MAXGEN)
        y1 = self.trace[:, 0]
        y2 = self.trace[:, 1]
        plt.plot(x, y1, 'r', label='optimal value')
        plt.plot(x, y2, 'g', label='average value')
        plt.xlabel("Iteration")
        plt.ylabel("function value")
        plt.title("Genetic algorithm for function optimization")
        plt.legend()
        plt.show()


class GAIndividual(object):
    '''
    individual of genetic algorithm
    '''

    def __init__(self,  vardim, bound):
        '''
        vardim: dimension of variables
        bound: boundaries of variables
        '''
        self.vardim = vardim
        self.bound = bound
        self.fitness = 0.

    def generate(self):
        '''
        generate a random chromsome for genetic algorithm
        '''
        len = self.vardim
        rnd = np.random.random(size=len)
        self.chrom = np.zeros(len)
        for i in range(0, len):
            self.chrom[i] = self.bound[0, i] +                 (self.bound[1, i] - self.bound[0, i]) * rnd[i]

    def calculate_fitness(self):
        '''
        calculate the fitness of the chromsome
        '''
        self.fitness = ObjectiveFunc.fitness_function(self.chrom, df)
#         print(self.fitness)

class ObjectiveFunc(object):
    '''
    objective function of genetic algorithm
    '''

    def __init__(self, campaign_id=None):
        self.mydb = facebook_leadgen_index_collector.connectDB(
            "dev_facebook_test")

    def fitness_function(optimal_weight, df):        
        
        m1 = df['fb_pixel_lead'] / df['fb_pixel_complete_registration']
        m2 = df['fb_pixel_complete_registration'] / df['fb_pixel_view_content']
        m3 = df['fb_pixel_view_content'] / df['landing_page_view']
        m4 = df['landing_page_view'] / df['link_click']
        m5 = df['link_click'] / df['impressions']
        m_spend = -(df['daily_budget'] - df['spend']) / df['daily_budget']
        m_bid = (df['campaign_bid'] - df['cost_per_fb_pixel_lead']) / df['campaign_bid']

        status = np.array([m1, m2, m3, m4, m5, m_spend, m_bid])
        r = np.dot(optimal_weight, status)
        return r

    def adset_fitness(optimal_weight, df):
        
        m1 = df['fb_pixel_lead'] / df['fb_pixel_complete_registration']
        m2 = df['fb_pixel_complete_registration'] / df['fb_pixel_view_content']
        m3 = df['fb_pixel_view_content'] / df['landing_page_view']
        m4 = df['landing_page_view'] / df['link_click']
        m5 = df['link_click'] / df['impressions']
        m_spend = -(df['daily_budget'] - df['spend']) / df['daily_budget']
        m_bid = (df['bid_amount'] - df['cost_per_fb_pixel_lead']) / df['bid_amount']
        
        status = np.array([m1, m2, m3, m4, m5, m_spend, m_bid])
        status = np.nan_to_num(status)
        r = np.dot(optimal_weight, status)
        return r

    def campaign_status(self, campaign_id):
        df_camp = pd.read_sql(
            "SELECT * FROM campaign_target WHERE campaign_id={}" .format(campaign_id), con=self.mydb)
        df_metrics = pd.read_sql(
            "SELECT * FROM campaign_leads_metrics WHERE campaign_id={}" .format(campaign_id), con=self.mydb)
        df_camp['campaign_bid'] = df_camp['spend_cap']/df_camp['target']
        self.charge_type = df_camp['charge_type'].iloc[0]
        spend = df_camp['spend'].iloc[0]
        campaign_cost_per_target = df_camp['cost_per_target'].iloc[0]
        campaign_target = df_camp['target'].iloc[0]
        impressions = df_camp['impressions'].iloc[0]
        df = pd.DataFrame(
            {
                'campaign_id': [campaign_id],
                'campaign_cost_per_target': [campaign_cost_per_target],
                'campaign_target': [campaign_target],
                'campaign_bid': [df_camp['campaign_bid'].iloc[0]],
                'daily_budget': [df_camp['daily_budget'].iloc[0]]
            }
        )
        df = pd.merge(df, df_metrics, on=['campaign_id'])
        df = df.convert_objects(convert_numeric=True)
        df.fillna(value=0, inplace=True)
        return df

    def adset_status(self, adset_id):
        df = pd.DataFrame({'adset_id': [], 'target': [],
                           'impressions': [], 'bid_amount': []})

        df_adset = pd.read_sql(
            "SELECT * FROM adset_leads_metrics WHERE adset_id={} ORDER BY request_time DESC LIMIT 1".format(adset_id), con=self.mydb)
        df_adset.fillna(value=0, inplace=True)
        return df_adset


def ga_optimal_weight(campaign_id, df_weight):
    request_time = datetime.datetime.now().date()
    mydb = facebook_leadgen_index_collector.connectDB("dev_facebook_test")
#     df_weight = pd.read_sql("SELECT * FROM conversion_optimal_weight WHERE campaign_id={}".format(campaign_id), con=mydb)
    df_camp = pd.read_sql(
        "SELECT * FROM campaign_target WHERE campaign_id={}".format(campaign_id), con=mydb)
    charge_type = df_camp['charge_type'].iloc[0]
    adset_list = facebook_leadgen_index_collector.Campaigns(campaign_id, charge_type).get_adsets()
    for adset_id in adset_list:
        df = ObjectiveFunc().adset_status(adset_id)
        r = ObjectiveFunc.adset_fitness(df_weight, df)
        print(r)
        df_final = pd.DataFrame({'campaign_id': campaign_id, 'adset_id': adset_id,
                                 'score': r[0], 'request_time': request_time}, index=[0])

        facebook_leadgen_index_collector.insertion("adset_score", df_final)


# In[35]:



if __name__ == "__main__":
    import datetime
    starttime = datetime.datetime.now()
    campaign_list = facebook_leadgen_index_collector.get_campaign_target()['campaign_id'].unique()
    for camp_id in campaign_list:
        print('campaign_id:', camp_id)
        global df
        df = ObjectiveFunc().campaign_status(camp_id)
        bound = np.tile([[0], [10]], vardim)
        ga = GeneticAlgorithm(sizepop, vardim, bound, MAXGEN, params)
        optimal = ga.solve()
        score = ObjectiveFunc.fitness_function(optimal, df)
        weight_columns = ['w1', 'w2', 'w3', 'w4',
                          'w5', 'w_spend', 'w_bid']
        df_weight = pd.DataFrame(
            data=[optimal], columns=weight_columns, index=[0])

        df_final = pd.DataFrame({'campaign_id': camp_id, 'score': score}, columns=[
                                'campaign_id', 'score'], index=[0])
        df_final = pd.concat([df_weight, df_final], axis=1,
                             sort=True, ignore_index=False)
        facebook_leadgen_index_collector.check_optimal_weight(camp_id, df_final)
        ga_optimal_weight(camp_id, df_weight)
        print('optimal_weight:', optimal)
        print(datetime.datetime.now()-starttime)
    print(datetime.datetime.now()-starttime)


# In[37]:


# import datetime
# print('campaign_id:', 23843003561380761)
# global df
# df = ObjectiveFunc().campaign_status(23843003561380761)
# bound = np.tile([[0], [1]], vardim)
# ga = GeneticAlgorithm(sizepop, vardim, bound, MAXGEN, params)
# optimal = ga.solve()
# score = ObjectiveFunc.fitness_function(optimal, df)
# # print(score)
# weight_columns = ['w1', 'w2', 'w3', 'w4',
#                   'w5', 'w_spend', 'w_bid']
# df_weight = pd.DataFrame(
#     data=[optimal], columns=weight_columns, index=[0])

# df_final = pd.DataFrame({'campaign_id': 23843003561380761, 'score': score}, columns=[
#                         'campaign_id', 'score'], index=[0])
# df_final = pd.concat([df_weight, df_final], axis=1,
#                      sort=True, ignore_index=False)
# facebook_leadgen_index_collector.check_optimal_weight(23843003561380761, df_final)
# ga_optimal_weight(23843003561380761, df_weight)
# print('optimal_weight:', optimal)


# In[1]:


#get_ipython().system('jupyter nbconvert --to script genetic_algorithm_leadgen_facebook.ipynb')


# In[ ]:




