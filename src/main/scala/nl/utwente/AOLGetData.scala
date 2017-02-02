package nl.utwente

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.io.LongWritable

import java.util.Random

object AOLGetData {

  val random = new Random(); 

  def selectByDateAndId(train: Boolean, annonId: String, date: String): Boolean = {
    try {
      val cutoff = "2006-05-09" // train/test before/after 8 May 2006
      val inTrain =  train && (annonId.toInt % 100 != 99)
      val inTest  = !train && (annonId.toInt % 100 == 99) // every 100th user is in test
      return (inTrain && date < cutoff) || (inTest && date > cutoff)
    } catch {
      case e: Exception => println("WARN: " + e);
    }
    return false
  }


  def sampleByCount(train: Boolean, count: Integer): Boolean = {
    train || random.nextInt(25) < count
  }


  def filterLog(query: String): Boolean = {
    query.length > 1 && !query.matches(".*\\.[a-z].*") && query.matches(".*[a-z].*") // no urls, one letter
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
    val late = aol.map{line => line.split("\t")}.filter{log => selectByDateAndId(train, log(0), log(2))}
    val queries = late.filter{log => log.length == 3 && filterLog(log(1))}.map{log => (log(1), 1)} // no clicks
    val counts = queries.reduceByKey{(a, b) => a + b}.sortBy(c => c._2, false) // counting
    val sample = counts.filter{c => sampleByCount(train, c._2)}
    val output = sample.map{r => r._2.toString + "\t" + r._1}
    output.saveAsTextFile(outDir)
  }
}
