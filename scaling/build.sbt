name := "m3_334036"
version := "1.0"

libraryDependencies += "org.rogach" %% "scallop" % "4.0.2"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.10"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2" % "provided"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2"
libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.13.2"
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.3.0"

scalaVersion in ThisBuild := "2.11.12"
