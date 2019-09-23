lazy val root = (project in file("."))
  .settings(
    organization := "com.martinbrosenberg",
    name := "data-engineer-project",
    version := "0.0.1",
    scalaVersion := "2.12.10",
    libraryDependencies ++= Seq(
//      "org.json4s"       %% "json4s-jackson" % "3.6.7",
      "org.apache.spark" %% "spark-core"     % "2.4.4",
      "org.apache.spark" %% "spark-sql"      % "2.4.4",
      "org.scalatest"    %% "scalatest"      % "3.0.8" % "test",
    ),
  )
