name := "tempdsread"

version := "0.1"

scalaVersion := "2.11.12"

ThisBuild / useCoursier := false

libraryDependencies += "ot.dispatcher" % "dispatcher_2.11" % "1.5.2"  % Compile
libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.11" % "1.1.0"  % Compile
libraryDependencies += "org.json" % "org.json" % "chargebee-1.0"
libraryDependencies += "com.cronutils" % "cron-utils" % "9.1.0"

scalacOptions ++= Seq("-target:jvm-1.8")