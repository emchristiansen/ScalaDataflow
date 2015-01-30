import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner
import com.google.cloud.dataflow.sdk.transforms.{Count, DoFn, ParDo}
import com.google.cloud.dataflow.sdk.values.KV

object Main extends App {
  //  val options = PipelineOptionsFactory.create()
  val options = PipelineOptionsFactory.fromArgs(args).as(classOf[PipelineOptions])
  //  options.setProject("Theia")
  //  options.setStagingLocation("gs://theia/staging")
  //  options.setTempLocation("gs://theia/tmp")
  // options.setRunner(classOf[DataflowPipelineRunner])
  options.setRunner(classOf[BlockingDataflowPipelineRunner])

  println("Here")
  println(args.toList.toString)
  println(options)

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
      // Thread.sleep(10000)
      c.output(c.element().getKey() + ": " + c.element().getValue())
    }
  }

  // Apply a root transform, a text file read, to the pipeline.
  p.apply(TextIO.Read.from("gs://dataflow-samples/shakespeare/kinglear.txt"))
    // Apply a ParDo transform to the PCollection resulting from the text file read
    .apply(ParDo.of(splitIntoWords))
    // Apply the Count.PerElement transform to the PCollection of text strings resulting from the ParDo
    .apply(Count.perElement[String]())
    // Apply a ParDo transform to format the PCollection of word counts from Count() for output
    .apply(ParDo.of(formatResults))
    // Apply a text file write transform to the PCollection of formatted word counts
    .apply(TextIO.Write.to("gs://theia/counts.txt"))

  p.run()

  println("Hello from GCETest")
}
