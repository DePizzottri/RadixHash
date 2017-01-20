fork in run := true

javaOptions in run ++= Seq("-Xmx6G")

javaOptions in run ++= Seq("-Xss1G")

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
