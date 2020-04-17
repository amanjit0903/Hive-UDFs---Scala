name:="UDF_test"
version:="1.0"
scalaVersion:="2.11.8"
resolvers += "Hive Exec" at "https://mvnrepository.com/artifact/org.apache.hadoop.hive/hive-exec"
libraryDependencies += "org.apache.hive" % "hive-exec" % "2.0.0"
libraryDependencies += "org.pentaho" % "pentaho-aggdesigner-algorithm" % "5.1.5-jhyde" % Test
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
