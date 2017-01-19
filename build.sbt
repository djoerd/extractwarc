name		:= "AnchorExtract"
version		:= "1.0"
scalaVersion	:= "2.10.5"

packAutoSettings

val sparkV	= "1.6.1"
val hadoopV	= "2.7.1"
val jwatV	= "1.0.0"

resolvers += "nl.surfsara" at "http://beehub.nl/surfsara-repo/releases"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkV % "provided",
  "org.apache.hadoop" %  "hadoop-client" % hadoopV % "provided",
  "org.jwat"          % "jwat-common"    % jwatV,
  "org.jwat"          % "jwat-warc"      % jwatV,
  "org.jwat"          % "jwat-gzip"      % jwatV,
  "net.sourceforge.htmlcleaner"   % "htmlcleaner" % "2.18",
  "nl.surfsara"       % "warcutils"      % "1.3"
)
