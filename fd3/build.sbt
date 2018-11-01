name := "fd2"

version := "0.1"

scalaVersion := "2.11.12"

name := "fraudDetection2"

version := "0.1"

scalaVersion := "2.11.12"
val dseVersion = "6.0.4"



addSbtPlugin("com.fhuertas" % "cassandra-sink" % "1.0.0")


resolvers += Resolver.mavenLocal // for testing
resolvers += "DataStax Repo" at "https://repo.datastax.com/public-repos/"


resolvers += "oss sonatype" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.2"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"

libraryDependencies += "com.fhuertas" %% "cassandra_sink_2.2.1" % "1.0.0"

//libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.2"

//libraryDependencies += "com.datastax.dse" % "dse-spark-dependencies" % dseVersion exclude(
//  "org.slf4j", "log4j-over-slf4j")


lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")


lazy val kafkaClients = "org.apache.kafka" % "kafka-clients" % "0.10.0. 1" excludeAll(excludeJpountz)