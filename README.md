Spark Scala Anchor Extract from WARC files
==========================================

Compile with [sbt][1]:

    sbt assembly

Run with:

    spark-submit target/scala-2.10/AnchorExtract-assembly-1.0.jar <in> <out>
    
Tip: This config helped me a lot for running really large datasets:

    --conf spark.yarn.executor.memoryOverhead=2048

[1]: http://www.scala-sbt.org/ "sbt - The interactive build tool"
