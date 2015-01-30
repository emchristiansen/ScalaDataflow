This is a simple port to Scala of the [Word Count](https://cloud.google.com/dataflow/java-sdk/wordcount-example) example for [Google Cloud Dataflow](https://cloud.google.com/dataflow/).
There's nothing clever here, but there are a few gotchas in the switch, and this might save you a few minutes of set up time.

Run `sh run.sh` to launch a BlockingDataflowPipelineRunner.
Note you will need to change a few values to run your own project (currently it tries to run one of mine).