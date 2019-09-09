# Link Analysis & Social Networks

I implemented Hyperlink-induced topic search (HITS) and Spectral Clustering.




## Reference
1 [Link Analysis](http://infolab.stanford.edu/~ullman/mmds/ch5.pdf)

2 [Mining Social-Network Graphs](http://infolab.stanford.edu/~ullman/mmds/ch10.pdf
)

## How to run

bin/spark-submit HyunJun_Choi_hits3.py Wiki-Vote.txt 5 wiki-output

python HyunJun_Choi_spectral3.py out.ego-facebook 3 clusters.txt