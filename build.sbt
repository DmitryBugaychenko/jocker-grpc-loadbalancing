name := "jocker-grpc-loadbalancer"

version := "0.1"

scalaVersion := "2.12.15"

idePackagePrefix := Some("org.jocker.grpc.loadbalancer")

val grpcVersion = "1.41.0"
libraryDependencies ++= Seq(
  "io.grpc" % "grpc-protobuf" % grpcVersion,
  "io.grpc" % "grpc-stub" % grpcVersion,
  "io.grpc" % "grpc-services" % grpcVersion,
  "io.grpc" % "grpc-netty-shaded" % grpcVersion % Test,
  "io.grpc" % "grpc-testing-proto" % grpcVersion % Test
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.6"
