import sys
import numpy as np
import heapq
from heapq import heappush, heappop
import collections
import networkx as nx
from numpy import linalg as LA

def flatten(x):
    if isinstance(x, collections.Iterable):
        return [a for i in x for a in flatten(i)]
    else:
        return [x]        
def newEdges(subedges,subrealOneOfCluster,subG):
    
    
    subrealOneOfCluster=flatten(subrealOneOfCluster)
    
    mheap=[]
    heappush(mheap, subedges)
    mheap=mheap[0]
    for inH in mheap:
        for inSOC in subrealOneOfCluster:
            if inSOC in inH:
                subG.remove_edge(inH[0],inH[1])
        
    return subG

def newEdgesVer2(edgesS,willBeBrokenclusterS):
    
    newEdges=[]
    
#     print(1)
    
    for eleEdges in edgesS:
        
        if (eleEdges[0] in willBeBrokenclusterS) and (eleEdges[1] in willBeBrokenclusterS):
            newEdges.append(eleEdges)
    
    return newEdges

def biggestCluster(SrealOneOfCluster):
    
    
    current=0
    for inS in SrealOneOfCluster:
        if len(inS)>current:
            current=len(inS)
            bigClu=inS
        
    return bigClu


  
    
if __name__ == "__main__":
     
    sampleData=sys.argv[1]
#              
    iter = int(sys.argv[2])-1
#     k = int(sys.argv[3])
#     n = int(sys.argv[4])
#     p = float(sys.argv[5]) 
    output = sys.argv[3]

#     sampleData='sample_data.txt'
    
#    sampleData='out.ego-facebook'
#     sampleData='toyData3P69.txt'
#    output = 'HW5Task2Output.txt'
#    iter=3

    nodes=[]
    edges=[]
    clusterF=[]
    clusterT=[] 
    
    realOneOfCluster=[]
    
    with open (sampleData, "r") as sample:
        for line in sample:
            line=line.replace("\n", "")
            pairElement=line.split(' ')
            
            for ele in pairElement:
                nodes.append(int(ele))
                
            
            edges.append((int(pairElement[0]),int(pairElement[1])))


    nodes=sorted(set(nodes))
    edges=sorted(set(edges))
    
    G=nx.Graph()
    G.add_nodes_from(nodes)
    G.add_edges_from(edges)
    
#     adjacentMatrix=nx.to_numpy_matrix(G)
    i=0
    while i<iter: 
        
        clusterF=[]
        clusterT=[] 
    
        L=nx.laplacian_matrix(G)
        #array type of L
        aL=L.toarray()
        
        eig=LA.eig(aL)
        
        sortedeigV=sorted(eig[0])
        

        
        secondSmallestEigenValueIndex=list(eig[0]).index(sortedeigV[1])
        fiddlerVector = eig[1][:,secondSmallestEigenValueIndex]
        
        clusterResult=fiddlerVector>=0
        for indexC in range(len(clusterResult)):
            if i==0:
                if clusterResult[indexC]==False:
                    clusterF.append(nodes[indexC])
            
                if clusterResult[indexC]==True:
                    clusterT.append(nodes[indexC])
            
            if i!=0:
                if clusterResult[indexC]==False:
                    clusterF.append(nodesWB[indexC])
            
                if clusterResult[indexC]==True:
                    clusterT.append(nodesWB[indexC])
        
        
        if len(clusterF)>len(clusterT):
#             willBeBrokencluster=clusterF
#             realOneOfCluster.append(clusterT)
            
            realOneOfCluster.append(clusterF)
            realOneOfCluster.append(clusterT)
            willBeBrokencluster=biggestCluster(realOneOfCluster)
            removedIn=realOneOfCluster.index(willBeBrokencluster)
            realOneOfCluster.pop(removedIn)
            

            
        if len(clusterF)<len(clusterT):
#             willBeBrokencluster=clusterT
#             realOneOfCluster.append(clusterF)
            
            realOneOfCluster.append(clusterF)
            realOneOfCluster.append(clusterT)
            willBeBrokencluster=biggestCluster(realOneOfCluster)
            removedIn=realOneOfCluster.index(willBeBrokencluster)
            realOneOfCluster.pop(removedIn)
            
        if len(clusterF)==len(clusterT):
            realOneOfCluster.append(clusterF)
            realOneOfCluster.append(clusterT)
            i=iter+1
        
        #if len(clusterF)==len(clusterT)
    
        
        
#         G=newEdges(edges,realOneOfCluster,G)
        edgesWB=newEdgesVer2(edges,willBeBrokencluster)
        nodesWB=willBeBrokencluster
        
        G=nx.Graph()
        G.add_nodes_from(nodesWB)
        G.add_edges_from(edgesWB)
    
        i=i+1
    if len(willBeBrokencluster)!=0 and ((i-iter)!=2):
        realOneOfCluster.append(willBeBrokencluster)
    
    
    
    
    FullTotalPoint=[]
    
    totalAssign=realOneOfCluster
#     print('')

    temClusSeTemp=[]
    with open(output, "w") as text_file:
        for indTAS in totalAssign: 
            
            clu=flatten(indTAS)
            clu=str(clu).replace(" ", "")
            clu=clu.replace('[', "")
            clu=clu.replace(']', "")
#             text_file.write("%f,%f,%d\n" %(totalAssign[indTAS][0][0],totalAssign[indTAS][0][1],totalAssign[indTAS][1]))
            text_file.write("%s\n" %(clu))
            
            


    
    
    
    
