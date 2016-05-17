# schedule_and_reuse_in_spark
该仓库用于存放和研究生毕业设计相关资料和代码
该仓库用于存放和研究生毕业设计相关资料和代码

Hello, my good friend, welcome to visit my repository.

This repository's source code(spark version: spark_1.5.0) in branch of spark_core has achieve this goal: reusing result of spark rdd between applications. But it only support some easy transformations using in applications, these transformations are textFile, objectFile, map, flatMap, join, union, reduceByKey, groupByKey. Though the number of transformations we support is very small, it's enough to describe the principle of reusing rdd's result between applications. And you can extend it to all of the transformations easily.

The design of this system concludes three components: DAGMacher & Rewriter, Cacher, CacheManager, the functions of these components describe below. 1. DAGMacher & Rewriter: this component compare the input dag to the cache dags and rewrite it when the input dag has the same part with a dag in cache.(NOTE: during the comparing, I haven't considered the compute function of rdd.) 2. Cacher: cache the sub-dag of input dag into disk when there are some sub-dags has the value to cache. 3. CacheManager: this component design to replace cache when there're not enough capacity to store a new cache, and matatain the consistency of cache. This system's source code is in the folder of src/main/scala/rddShare/core in branch of spark_core.

Before you use this source code, there are three conf files(default.conf, transformation and rddShare.sql) you need to configure. You can get these conf files in branch of spark_core/rddShare, and you need copy this folder into $SPARK_HOME/conf directory.

After that, you should set your own configure using default.conf file. And you can set the transformation priority in transformation file. As I use mysql to store the metadata of cache, so you need to create the database and table in mysql, you can use the rddShare.sql to create.

OK, you can run the example in $SPARK_HOME/core/src/main/scala/rddShare/test/testWordCount.scala. If you want to run your own example, please use the support transformations.
