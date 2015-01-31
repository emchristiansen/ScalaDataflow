import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner
import com.google.cloud.dataflow.sdk.transforms.Create
import com.google.cloud.dataflow.sdk.transforms.{Count, DoFn, ParDo}
import com.google.cloud.dataflow.sdk.values.KV
import scala.sys.process._

object Main extends App {
  val baseDirectory = "gs://theia"

  // It appears we must explicity feed our command-line arguments to the
  // Pipeline constructor.
  val options = PipelineOptionsFactory.fromArgs(args).as(classOf[PipelineOptions])

  println("Starting Main")

  // Create the Pipeline with default options.
  val p = Pipeline.create(options)

  val ints = p.apply(Create.of("0", "1", "2", "3")).setCoder(StringUtf8Coder.of())

  val render = new DoFn[String, String]() {
    override def startBundle(c: DoFn[String, String]#Context) {
      "sudo docker pull emchristiansen/mitsubadocker" !!

      "rm -rf ~/theia" !!

      "mkdir ~/theia" !!

      "gsutil -m cp -r gs://theia/testdata ~/theia/data" !!
    }

    override def processElement(c: DoFn[String, String]#ProcessContext) {
      val sceneFile = c.element()

      "sudo docker run -t -v /tmp/theia/data:/opt/data emchristiansen/mitsubadocker mitsuba /opt/share/cbox/cbox.xml -o /opt/share/cbox.png" !!
    }
  }

  val provision = new DoFn[String, String]() {
    override def startBundle(c: DoFn[String, String]#Context) {
    }

    override def processElement(c: DoFn[String, String]#ProcessContext) {
    }
  }

  ints.apply(ParDo.of(provision))
    .apply(TextIO.Write.to("gs://theia/provision.txt"))

  p.run()

  println("Main done")
}
