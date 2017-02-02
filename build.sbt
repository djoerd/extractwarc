name		:= "AnchorExtract"
version		:= "1.0"
scalaVersion	:= "2.10.5"

packAutoSettings

val sparkV	= "1.6.1"
val hadoopV	= "2.7.1"
val jwatV	= "1.0.2"

resolvers += "nl.surfsara" at "http://wwwhome.cs.utwente.nl/~hiemstra/repository/"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"    % sparkV  % "provided",
  "org.apache.hadoop" %  "hadoop-client" % hadoopV % "provided",
  "org.jwat"          %  "jwat-warc"     % jwatV,
  "org.jsoup"         %  "jsoup"         % "1.10.2",
  "nl.surfsara"       %  "warcutils"     % "1.4"
)
