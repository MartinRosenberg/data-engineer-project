lazy val root = (project in file("."))
  .settings(
    organization := "com.martinbrosenberg",
    name := "data-engineer-project",
    version := "0.0.1",
    scalaVersion := "2.12.10",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.8" % "test",
    ),
  )
