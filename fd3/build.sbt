name := "LabClick"

version := "0.1"

scalaVersion := "2.11.12"

sparkVersion := "2.3.2"

sparkComponents ++= Seq("sql", "streaming")


//spDependencies += "datastax/spark-cassandra-connector:2.0.1-s_2.11"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.2"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.1"

libraryDependencies += "org.scala-lang" % "scala-library-all" % "2.11.12"

assemblyJarName in assembly := s"${name.value.replace(' ','-')}-${version.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
