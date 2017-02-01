package nl.utwente

import java.io.ByteArrayOutputStream
import java.io.InputStreamReader

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.io.LongWritable
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.StringEscapeUtils

import org.htmlcleaner.HtmlCleaner
import org.jwat.warc.WarcRecord
import nl.surfsara.warcutils.WarcInputFormat
import com.mydatafactory.ean.ExtractEan

object EanExtract {

  def getContent(record: WarcRecord): String = {
    val content = new ByteArrayOutputStream()
    try {
      val cLen = record.header.contentLength.toInt
      val cStream = record.getPayload.getInputStream()
      val buf = new Array[Byte](cLen)
      var nRead = cStream.read(buf)
      while (nRead != -1) {
        content.write(buf, 0, nRead)
        nRead = cStream.read(buf)
      }
      cStream.close()
    } catch {
      case e: Exception => println("WARN: " + e);
    }
    content.toString("UTF-8")
  }


  def cleanPage(page: String): String = {
    page.replaceAll("[\\r\\n\\t]", " ")
  }


  def main(args: Array[String]) {
    val appName = this.getClass.getName
    if (args.length != 2) {
      throw new IllegalArgumentException(s"Usage: $appName <in> <out>")
    }
    val inDir   = args(0)
    val outDir  = args(1)
    val conf    = new SparkConf().setAppName(s"$appName $inDir $outDir")
    val sc      = new SparkContext(conf)

    val warcf = sc.newAPIHadoopFile(
      inDir,
      classOf[WarcInputFormat],  // Input
      classOf[LongWritable],     // OutputKey
      classOf[WarcRecord]        // OutputValue
    )
    val html  = warcf.map(w => (w._2.header.warcTargetUriStr, getContent(w._2))).cache() // TODO: also contains WARC header
    val found = html.map(w => (w._1, ExtractEan.findEan(w._2), cleanPage(w._2)))
    val pages = found.filter(w => w._1 != null && w._2.size > 0).map(w => w.productIterator.mkString("\t"))
    pages.saveAsTextFile(outDir, classOf[GzipCodec])
  }
}
