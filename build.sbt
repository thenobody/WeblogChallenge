name := "WeblogChallenge"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= {
  Seq(
    "org.apache.spark" %% "spark-core" % "1.6.0",
    "com.github.nscala-time" %% "nscala-time" % "2.8.0",
    "org.scalatest" %% "scalatest" % "2.2.6" % "test"
  )
}
    