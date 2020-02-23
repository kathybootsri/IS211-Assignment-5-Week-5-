# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 19:19:54 2020

@author: kathybootsri
"""

from pythonds.basic import Queue

class Server:
    def __init__(self):
        self.currentTask = None
        self.timeRemaining = 0

    def tick(self):
        if self.currentTask != None:
            self.timeRemaining = self.timeRemaining - 1
            if self.timeRemaining <= 0:
                self.currentTask = None

    def busy(self):
        if self.currentTask != None:
            return True
        else:
            return False

    def startNext(self,newtask):
        self.currentTask = newtask

class Task:
    def __init__(self,time):
        self.timestamp = time

    def getStamp(self):
        return self.timestamp
    
    def waitTime(self, finishttime, submissiontime):
        return finishttime - submissiontime


"""PART I"""
        
def simulateOneServer(file_name):

    import pandas as pd
    
    df = pd.read_csv(file_name, index_col=0, names = ['currentSecond', 'task', 'Time'])
    
    df2 = df.reset_index()

    server = Server()
    requestQueue = Queue()
    waitingtimes = [] 
    starttime = df2['currentSecond'][0]

    for x in range(len(df2)):
        task = Task(df2['Time'][x])
        requestQueue.enqueue(task)
    
    for x in range(len(df2)):
        endtime = starttime + df2['Time'][x]
        starttime = endtime
        nexttask = requestQueue.dequeue()         
        waitingtimes.append(nexttask.waitTime(starttime, df2['currentSecond'][x]))

    if (not server.busy()) and (not requestQueue.isEmpty()):
        server.startNext(nexttask)
    
    server.tick()
      
    averageWait=sum(waitingtimes)/len(waitingtimes)
    print("Average Wait %6.2f secs for a single server."%(averageWait))

file_name = 'http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv'
  
simulateOneServer(file_name)  

"""PART II"""

def simulateManyServers(file_name, servers):

    import pandas as pd
    
    df = pd.read_csv(file_name, index_col=0, names = ['currentSecond', 'task', 'Time'])
    
    df2 = df.reset_index()
    
    df2['RoundRobin'] = ''

    networks = servers
    orig_networks = servers

    for x in range(len(df2)):
        df2.at[x, 'RoundRobin'] = networks
        networks -= 1
        if networks == 0:
            networks = orig_networks
    
    for x in range(1, orig_networks + 1):
        df3 = df2[df2['RoundRobin']==x] 
        df3.reset_index(inplace=True)
            
        server = Server()
        requestQueue = Queue()
        waitingtimes = [] 
        starttime = df3['currentSecond'][0]
    
        for x in range(len(df3)):
            task = Task(df3['Time'][x])
            requestQueue.enqueue(task)
        
        for x in range(len(df3)):
            endtime = starttime + df3['Time'][x]
            if df3['currentSecond'][x] > endtime:
                starttime = df3['currentSecond'][x]
            else:
                starttime = endtime
            nexttask = requestQueue.dequeue()         
            waitingtimes.append(nexttask.waitTime(starttime, df3['currentSecond'][x]))
                    
                
            if (not server.busy()) and (not requestQueue.isEmpty()):
                server.startNext(nexttask)
        
            
#            all_waitingtimes.append(averagesubWait)
            
            server.tick()
              
    averageWait=sum(waitingtimes)/len(waitingtimes)
    print("Average Wait {} secs for {} servers.".format("{:.2f}".format(round(averageWait,2)), servers))
    


file_name = 'http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv'
  
simulateManyServers(file_name, 2)  
simulateManyServers(file_name, 3)  

"""PART 3"""
print("When multiple servers are provided, the calculation dramatically decreases to a more reasonable processing time.")