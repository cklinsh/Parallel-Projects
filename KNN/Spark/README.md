# KNN with Spark

This code implements a K-Nearest Neighbors algorithm with Java Spark. KNN is quite easy to conceptualize in Spark. First, read in the file via JavaSparkContext. Then, map each non-target point into the form "1/distance-to-target point". Afterwards, use top(K) to get the closest K points. The normal behavior of top( ) is to return the largest rather than the smallest values, so 1/distance was used to reverse the order of points while keeping the actual distance from each point to the target quickly and easily accessible for display to the user.

Note that data is stored as Strings rather than Tuples; this was to improve ease of data manipulation and provide a more general sense for how Spark actually sped up computation, as KNN is often used with non-numeric data.
