import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner
import com.google.cloud.dataflow.sdk.transforms.{Count, DoFn, ParDo}
import com.google.cloud.dataflow.sdk.values.KV

object Main extends App {
  // It appears we must explicity feed our command-line arguments to the
  // Pipeline constructor.
  val options = PipelineOptionsFactory.fromArgs(args).as(classOf[PipelineOptions])

  println("Starting Main")

  // Create the Pipeline with default options.
  val p = Pipeline.create(options)

  val splitIntoWords = new DoFn[String, String]() {
    override def processElement(c: DoFn[String, String]#ProcessContext) {
      val words = c.element().split("[^a-zA-Z']+")
      for (word <- words) {
        if (!word.isEmpty()) {
          c.output(word)
        }
      }
    }
  }

  val formatResults = new DoFn[KV[String, java.lang.Long], String]() {
    override def processElement(
      c: DoFn[KV[String, java.lang.Long], String]#ProcessContext
    ) {
      c.output(c.element().getKey() + ": " + c.element().getValue())
    }
  }

  p.apply(TextIO.Read.from("gs://dataflow-samples/shakespeare/kinglear.txt"))
    .apply(ParDo.of(splitIntoWords))
    .apply(Count.perElement[String]())
    .apply(ParDo.of(formatResults))
    // You will need to change this to a file you can write to.
    .apply(TextIO.Write.to("gs://theia/counts.txt"))

  p.run()

  println("Main done")
}
