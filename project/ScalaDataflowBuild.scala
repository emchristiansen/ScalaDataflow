import sbt._
import Keys._

object GCETestBuild extends Build {
  def extraResolvers = Seq(
    resolvers ++= Seq(
      "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
      "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
//      "repo.codahale.com" at "http://repo.codahale.com",
      //"spray-io" at "http://repo.spray.io/",
      "typesafe-releases" at "http://repo.typesafe.com/typesafe/repo"
      //"Local Maven Repository" at Path.userHome.asFile.toURI.toURL + "/.m2/repository"
    )
  )

  val scalaVersionString = "2.11.5"

  def extraLibraryDependencies = Seq(
    libraryDependencies ++= Seq(
      //"opencv" % "opencv" % "2.4.9",
      //"nebula" %% "nebula" % "0.1-SNAPSHOT",
      //"billy" %% "billy" % "0.1-SNAPSHOT",
      //"skunkworks" %% "skunkworks" % "0.1-SNAPSHOT",
      //"org.expecty" % "expecty" % "0.9",
      //"commons-lang" % "commons-lang" % "2.6",
      //"org.scala-lang" % "scala-reflect" % scalaVersionString,
      //"org.scala-lang" % "scala-compiler" % scalaVersionString,
      //"org.apache.commons" % "commons-math3" % "3.1.1",
      //"commons-io" % "commons-io" % "2.4",
      "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "0.3.150109",
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "org.scalacheck" %% "scalacheck" % "1.12.1" % "test"
      //"org.scala-stm" %% "scala-stm" % "0.7",
      //"com.chuusai" %% "shapeless" % "1.2.4",
      //"org.clapper" %% "grizzled-scala" % "1.1.3",
      //"org.scalanlp" %% "breeze-math" % "0.2-SNAPSHOT",
      //"org.spire-math" %% "spire" % "0.3.0",
      //"org.scalaz" %% "scalaz-core" % "7.0-SNAPSHOT",
      //"io.spray" %%  "spray-json" % "1.2.3",
      //"org.rogach" %% "scallop" % "0.8.1",
      //"junit" % "junit" % "4.11" % "test",
      //"org.imgscalr" % "imgscalr-lib" % "4.2"
    )
  )

  def updateOnDependencyChange = Seq(
    watchSources <++= (managedClasspath in Test) map { cp => cp.files })

  def scalaSettings = Seq(
    scalaVersion := scalaVersionString,
    scalacOptions ++= Seq(
      "-optimize",
      "-unchecked",
      "-deprecation",
      "-feature"
      //"-language:implicitConversions",
      // "-language:reflectiveCalls",
      //"-language:postfixOps"
    )
  )

  def moreSettings =
    Project.defaultSettings ++
    extraResolvers ++
    extraLibraryDependencies ++
    scalaSettings ++
    //assemblySettings ++
    //SbtStartScript.startScriptForJarSettings ++
    updateOnDependencyChange

  val projectName = "GCETest"
  lazy val root = {
    val settings = moreSettings ++ Seq(name := projectName, fork := true)
    Project(id = projectName, base = file("."), settings = settings)
  }
}
