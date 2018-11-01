name := "LabClick"

version := "0.1"

scalaVersion := "2.11.12"

sparkVersion := "2.1.0"

sparkComponents ++= Seq("sql", "streaming")


spDependencies += "datastax/spark-cassandra-connector:2.0.1-s_2.11"

//libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.1.0"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.1"

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


/*
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
*/