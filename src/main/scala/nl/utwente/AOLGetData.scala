package nl.utwente

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.io.LongWritable

import java.util.Random

object AOLGetData {

  val random = new Random(); 

  def selectByDate(train: Boolean, date: String): Boolean = {
    val cutoff = "2006-05-09" // train/test before/after 8 May 2006
    (train && date < cutoff) || (!train && date > cutoff)
  }


  def sampleByCount(train: Boolean, count: Integer): Boolean = {
    train || random.nextInt(4000) < count
  }


  def main(args: Array[String]) {
    val appName = this.getClass.getName
    if (args.length != 3) {
      throw new IllegalArgumentException(s"Usage: $appName [train|test] <in> <out>");
    }
    val train   = (args(0) == "train")
    val inDir   = args(1)
    val outDir  = args(2)
    val conf    = new SparkConf().setAppName(s"$appName $inDir $outDir")
    val sc      = new SparkContext(conf)

    val aol = sc.textFile(inDir)
    val late = aol.map{line => line.split("\t")}.filter{log => selectByDate(train, log(2))} // After 8 May 2006 
    val queries = late.filter{log => log.length == 3 && log(1).length() > 1}.map{log => (log(1), 1)} // no clicks, query length > 1
    val counts = queries.reduceByKey{(a, b) => a + b}.sortBy(c => c._2, false) // counting
    val sample = counts.filter{c => sampleByCount(train, c._2)} 
    val output = sample.map{r => r._1 + "\t" + r._2.toString}
    output.saveAsTextFile(outDir)
  }
}
