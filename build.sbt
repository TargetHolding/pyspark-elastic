name := "pyspark-elastic"

version := "0.1.5"

organization := "TargetHolding"

scalaVersion := "2.10.4"

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0") 

libraryDependencies ++= Seq(
	"org.elasticsearch" %% "elasticsearch-spark" % "2.1.0.Beta4"
)

spName := "TargetHolding/pyspark-elastic"

sparkVersion := "1.3.1"

sparkComponents += "streaming"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
