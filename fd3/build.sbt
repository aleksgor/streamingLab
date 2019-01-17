name := "LabClick"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.2"


resolvers += "spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.1"

libraryDependencies += "org.scala-lang" % "scala-library-all" % "2.11.12"

assemblyJarName in assembly := s"${name.value.replace(' ','-')}-${version.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

lazy val kafkaClients = "org.apache.kafka" % "kafka-clients" % "0.10.0.1" excludeAll(excludeJpountz) // add more exclusions here

libraryDependencies += "net.manub" %% "scalatest-embedded-kafka" % "0.15.1"// % Test
// unit tests
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += "org.mockito" % "mockito-core" % "1.9.5" % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.8.0" % Test

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
