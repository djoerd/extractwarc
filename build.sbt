name		:= "EanExtract"
version		:= "1.0"
scalaVersion	:= "2.10.5"

packAutoSettings

val sparkV	= "1.6.3"
val hadoopV	= "2.6.0"
val jwatV	= "1.0.2"

resolvers += "nl.surfsara" at "http://wwwhome.cs.utwente.nl/~hiemstra/repository/"
resolvers += "com.mydatafactory" at "http://wwwhome.cs.utwente.nl/~hiemstra/repository/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkV % "provided",
  "org.apache.hadoop" % "hadoop-client" % hadoopV % "provided",
  "org.jwat"          % "jwat-warc"      % jwatV,
  "net.sourceforge.htmlcleaner"   % "htmlcleaner" % "2.18",
  "nl.surfsara"       % "warcutils"      % "1.4",
  "com.mydatafactory" % "ean"            % "0.2"
)
