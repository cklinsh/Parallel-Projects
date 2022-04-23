# KNN with MASS

MASS (Multi Agent Spacial Simulation) is a parallelization library created by Dr. Munehiro Fukuda of UW Bothell and collaborators. 

As the name suggests, MASS uses mobile agents as the primary means of parallelization; however, computation of _places_ is also parallelized. 

In this project, no agents are used. Instead, data elements are partitioned among places. Then, places compute in parallel their own distance to the target point (i.e. the point for which we are trying to calculate the nearest neighbors). Finally, the K nearest points to the target are retreived and returned.

Several different strategies (for example, building a tree of neighbors) were used, but none were as fast as the methods used in this code.
