# fig = plt.figure()
# ax = plt.axes(projection='3d')
# ax.scatter3D(testx[:,0]/(sum(testx[:,0])), testx[:,1]/(sum(testx[:,1])), testx[:,2]/(sum(testx[:,2])), c=testY, marker='o')



#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This is an example implementation of PageRank. For more conventional use,
Please refer to PageRank implementation provided by graphx

Example Usage:
bin/spark-submit examples/src/main/python/pagerank.py data/mllib/pagerank_data.txt 10
"""
from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession
from pyspark import SparkContext

import string
import os.path
import os

def computeAuth(urls, hub):
    """Calculates URL contributions to the rank of other URLs."""
    #num_urls = len(urls)
    
    for url in urls:
        yield (url, hub)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

def parseNeighborsVer2(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    for elep in parts:
        #print(elep)
        yield (elep)



def allHubComp(toNode,cenHub):
    
    for ele in toNode:
        yield (ele,cenHub)

    


if __name__ == "__main__":


    direc = sys.argv[3]
    
    if not os.path.exists(direc):
        os.makedirs(direc)
    
    outputAuthoFileName="authority.txt"
    outputHubFileName="hub.txt"

    


    outputAuthoFile=open(direc+'//'+outputAuthoFileName,'a')
    outputHubFile=open(direc+'//'+outputHubFileName,'a')


    
    
    print("WARN: This is a naive implementation of PageRank and is given as an example!\n" +
          "Please refer to PageRank implementation provided by graphx",
          file=sys.stderr)

    # Initialize the spark context.
 
    sc = SparkContext(appName="HITSalgorithm")

    # Loads in input file. It should be in format of:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...
    #lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    lines = sc.textFile(sys.argv[1])


    
    # Loads all URLs from input file and initialize their neighbors.
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    setNode=lines.map(lambda urls: parseNeighbors(urls)).distinct().collect()
    realsetNode=[element for tupl in setNode for element in tupl]
    realsetNode=sorted(list(set(realsetNode)))
    realsetNode=sc.parallelize(realsetNode)

    realsetNodeList=realsetNode.collect()

    
    #setNode=lines.map(lambda urls: parseNeighborsVer2(urls)).distinct()

    #n=links.count()
    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    # hubs = links.map(lambda url_neighbors: (url_neighbors[0],1.0))
    # hubs = realsetNode.map(lambda x:(x,1.0))
    hubs = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    
    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(int(sys.argv[2])):
        # Calculates authority.
        authPiece = links.join(hubs).flatMap(
            lambda url_urls_rank: computeAuth(url_urls_rank[1][0], url_urls_rank[1][1]))

        auth =  authPiece.reduceByKey(add).sortByKey()
        maxTuple=auth.max(key=lambda x:x[1])
        maxAuthVal=maxTuple[1]
        auth = auth.map(lambda x:(x[0],x[1]/maxAuthVal))
        

        
        inversedlinks=links.map(lambda url_neighbors: (list(url_neighbors[1]),url_neighbors[0]))
        
        hubPiece=inversedlinks.flatMap(lambda x: allHubComp(x[0],x[1]))
        hubPieceT=hubPiece.join(auth).map(
            lambda url_urls_rank: (url_urls_rank[1][0], url_urls_rank[1][1]))

        hubs=hubPieceT.reduceByKey(add).sortByKey()
        maxTupleH=hubs.max(key=lambda x:x[1])
        maxHubVal=maxTupleH[1]

        hubs = hubs.map(lambda x:(x[0],x[1]/maxHubVal))



    authList=auth.collect()
    hubsList=hubs.collect()
    existAuthNode=[]
    existHubNode=[]
    nonExistAuthNodeAndZero=[]
    nonExistHubNodeAndZero=[]
    
    
    if len(authList)<len(realsetNodeList):
        for aInd in range(len(authList)):
            existAuthNodeInd=realsetNodeList.index(authList[aInd][0])
            existAuthNode.append(realsetNodeList[existAuthNodeInd])
                          
    
    
        nonExistAuthNode=list(set(realsetNodeList).difference(set(existAuthNode)))
    
        for nEAin in range(len(nonExistAuthNode)):
            nonExistAuthNodeAndZero.append((nonExistAuthNode[nEAin],0.0))


        for aEAZIn in range(len(nonExistAuthNodeAndZero)):
            authList.append(nonExistAuthNodeAndZero[aEAZIn])
    
              
    if len(hubsList)<len(realsetNodeList):
        for hInd in range(len(hubsList)):
            existHubNodeInd=realsetNodeList.index(hubsList[hInd][0])
            existHubNode.append(realsetNodeList[existHubNodeInd])
    
        nonExistHubNode=list(set(realsetNodeList).difference(set(existHubNode)))
    
        for nEHin in range(len(nonExistHubNode)):
            nonExistHubNodeAndZero.append((nonExistHubNode[nEHin],0.0))

        for nEHZIn in range(len(nonExistHubNodeAndZero)):
            hubsList.append(nonExistHubNodeAndZero[nEHZIn])
    
    # check who is zero auth or hubs
    
    
    # float("{0:.2f}".format(auth[1])
    # print(str(authList))
    # print(str(hubsList))

    authListInt=[]
    hubsListInt=[]
    for AlistInd in range(len(authList)):
        nodeInt=int(authList[AlistInd][0])
        authListInt.append((nodeInt,authList[AlistInd][1]))

    authListInt=sorted(authListInt)    

    for HlistInd in range(len(hubsList)):
        nodeInt=int(hubsList[HlistInd][0])
        hubsListInt.append((nodeInt,hubsList[HlistInd][1]))
    hubsListInt=sorted(hubsListInt)
    
    for AlistInd in range(len(authListInt)):

        if authListInt[AlistInd][1]!=0.0:
    
            strA=str(authListInt[AlistInd][0])+','+str("{0:.5f}".format(float(authListInt[AlistInd][1])))+'\n'
            
            
            outputAuthoFile.write(strA)
        
    for HlistInd in range(len(hubsListInt)):

        if hubsListInt[HlistInd][1]!=0.0:
            
            strB=str(hubsListInt[HlistInd][0])+','+str("{0:.5f}".format(float(hubsListInt[HlistInd][1])))+'\n'
            outputHubFile.write(strB)
                
        #outputAuthoFile.write(str(authList)+'\n')
        #outputHubFile.write(str(hubsList)+'\n')
        
        
    # Collects all URL ranks and dump them to console.
    #for (link, rank) in ranks.collect():
    #    print("%s has rank: %s." % (link, rank))

    outputAuthoFile.close()
    outputHubFile.close()
    sc.stop()
