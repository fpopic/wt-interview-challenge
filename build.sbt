lazy val root = (project in file(".")).settings(
  name  := "WaytationChallenge",
  version := "1.0",
  scalaVersion := "2.11.11",
  mainClass in Compile := Some("com.waytation.Main"),
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  parallelExecution in Test := true,
  test in assembly := {} // remove if you want to start tests
)

val sparkVersion = "2.2.0"

lazy val unprovidedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe" % "config" % "1.3.1",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % Test
)

libraryDependencies ++= unprovidedDependencies

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case _ => MergeStrategy.first
}

resolvers ++= Seq(
  "Cloudera Maven repository" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Artima Maven Repository" at "http://repo.artima.com/releases"
)