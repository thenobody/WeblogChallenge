name := "WeblogChallenge"

version := "1.0"

scalaVersion := "2.10.6"

parallelExecution in Test := false

libraryDependencies ++= {
  val sparkVersion = "1.6.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "com.github.nscala-time" %% "nscala-time" % "2.8.0",
    "org.scalatest" %% "scalatest" % "2.2.6" % "test"
  )
}
    