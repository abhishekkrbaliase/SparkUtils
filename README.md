# SparkUtils
This gives an example of how to query cassandra from spark and delete the partition iff 
1. In a partition, all possible values of a non partition column are only present. E.g. it should contain all {x, y, a, b}
   and only {x, y, a, b}
2. The count of one of the possible values, say x, is greater than minimum count, i.e count(x) > minCount
