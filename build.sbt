fork in run := true

javaOptions in run ++= Seq("-Xmx10G")

javaOptions in run ++= Seq("-Xss1G")

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "com.jcraft" % "jzlib" % "1.1.3"

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

parallelExecution in Test := true
