package nl.utwente

import java.io.ByteArrayOutputStream
import java.io.InputStreamReader

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.io.LongWritable
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.StringEscapeUtils

import org.htmlcleaner.HtmlCleaner
import org.jwat.warc.WarcRecord
import nl.surfsara.warcutils.WarcInputFormat

object AnchorExtract {

  def getContent(record: WarcRecord): String = {
    val content = new ByteArrayOutputStream();
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
    content.toString("UTF-8");
  }


  def scrapeAnchors(html: String): List[String] = {
    var anchors = new scala.collection.mutable.ListBuffer[String]
    val cleaner = new HtmlCleaner()
    try {
      val rootNode = cleaner.clean(html)
      if (rootNode != null) {
        val titleNode = rootNode.findElementByName("title", true)
        if (titleNode != null) {
          anchors += titleNode.getText.toString
        }
        val elements = rootNode.getElementsByName("a", true)
        for (elem <- elements) {
          val rel  = elem.getAttributeByName("rel")  // no nofollow
          val href = elem.getAttributeByName("href") // must be a link
          if (href != null && (rel == null || !rel.equalsIgnoreCase("nofollow"))) {
            val text = StringEscapeUtils.unescapeHtml(elem.getText.toString)
            val texts = text.split(": ")
            anchors ++= texts
          }
        }
      }
    } catch {
      case e: Exception => println("WARN: " + e);
    }
    return anchors.toList
  }


  def cleanAnchors(text: String): String = {
    val noBraces = text.replaceAll("\\([^\\)]+\\)|\\[[^\\]]+\\]|\\{[^\\}]+\\}", " ")
    val lower    = noBraces.toLowerCase.replaceAll("[\\(\\)\\[\\]\\{\\}]", " ");
    val ellipsis = lower.replaceAll("[0-9a-z]+\\.\\.\\.", " ").replaceAll("\\.\\.\\.[0-9a-z]*", " ")
    val urls     = ellipsis.replaceAll("https?:\\/\\/[^ ]*", " ")
    val emails   = urls.replaceAll("@[a-z0-9\\-\\.]+", " ")
    val words    = emails.replaceAll(" [^a-z0-9]+ ", " ").replaceAll("[\\r\\n\\t ]+", " ")
    val trailing = words.replaceAll("^[^0-9a-z]+|[^0-9a-z]+$", "")
    return trailing
  }


  def main(args: Array[String]) {
    val appName = this.getClass.getName
    if (args.length != 2) {
      throw new IllegalArgumentException(s"Usage: $appName <in> <out>");
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
    val html = warcf.map{w => (w._2.header.warcTargetUriStr, getContent(w._2))}.cache() // TODO: also contains WARC header
    val anchors = html.flatMap{w => scrapeAnchors(w._2)}
    val texts  = anchors.map{w => (cleanAnchors(w), 1)}
    val best = texts.reduceByKey((a, b) => a + b).filter(w => w._2 > 2)
    val output =  best.sortBy(c => c._2, false).map(w => w._2 + "\t" + w._1)
    output.saveAsTextFile(outDir)
  }
}
