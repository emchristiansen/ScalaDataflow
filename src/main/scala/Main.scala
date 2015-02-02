import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.{ PipelineOptions, PipelineOptionsFactory }
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner
import com.google.cloud.dataflow.sdk.transforms.Create
import com.google.cloud.dataflow.sdk.transforms.{ Count, DoFn, ParDo }
import com.google.cloud.dataflow.sdk.values.KV
import scala.sys.process._
import java.nio.file.{ Paths, Files }

object Main extends App {
  val baseDirectory = "gs://theia"

  // It appears we must explicity feed our command-line arguments to the
  // Pipeline constructor.
  val options = PipelineOptionsFactory.fromArgs(args).as(classOf[PipelineOptions])

  println(options)

  println("Starting Main")

  // Create the Pipeline with default options.
  val p = Pipeline.create(options)

  val ints = p.apply(Create.of("0", "1", "2", "3")).setCoder(StringUtf8Coder.of())

  val render = new DoFn[String, String]() {

    var dockerPath = ""
    override def startBundle(c: DoFn[String, String]#Context) {
      // assert(0 == "sudo apt-get update".!)
      // assert(0 == "curl -sSL https://get.docker.com/ubuntu/ | sudo sh".!)
      // assert(0 == "sudo apt-get install -y docker.io".!)
      // assert(0 == "sudo docker pull emchristiansen/mitsubadocker".!)

      // "curl -sSL https://get.docker.com/ubuntu/ | sudo sh".!

      // dockerPath = "which docker".!!
      // "touch hi_eric".!

      "sudo apt-get update && sudo apt-get install -y wget".!
      "wget http://www.mitsuba-renderer.org/releases/current/wheezy/collada-dom_2.4.0-1_amd64.deb".!
      "sudo dpkg -i collada-dom_2.4.0-1_amd64.deb; sudo apt-get -yf install".!
      "wget http://www.mitsuba-renderer.org/releases/current/wheezy/mitsuba_0.5.0-1_amd64.deb".!
      "sudo dpkg -i mitsuba_0.5.0-1_amd64.deb; sudo apt-get -yf install".!

      dockerPath = "which mitsuba".!!

      // val homeDirectory = System.getProperty("user.home")
      // val dockerSharedDirectory = Paths.get(homeDirectory, "docker_shared_directory")

      // if (!Files.exists(dockerSharedDirectory)) {
        // Files.createDirectories(
      // }

      // "rm -rf ~/theia" !!

      // "mkdir ~/theia" !!

      // "gsutil -m cp -r gs://theia/testdata ~/theia/data" !!
    }

    override def processElement(c: DoFn[String, String]#ProcessContext) {
      c.output("processed_cloud " ++ dockerPath ++ c.element())
      // val sceneFile = c.element()

      // "sudo docker run -t -v /tmp/theia/data:/opt/data emchristiansen/mitsubadocker mitsuba /opt/share/cbox/cbox.xml -o /opt/share/cbox.png" !!
    }
  }

  // val provision = new DoFn[String, String]() {
  // override def startBundle(c: DoFn[String, String]#Context) {
  // }

  // override def processElement(c: DoFn[String, String]#ProcessContext) {
  // }
  // }

  ints.apply(ParDo.of(render))
    .apply(TextIO.Write.to("gs://theia/provision.txt"))

  p.run()

  println("Main done")
}
