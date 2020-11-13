addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.34")
addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.8.0")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")

libraryDependencies += "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.4.0"