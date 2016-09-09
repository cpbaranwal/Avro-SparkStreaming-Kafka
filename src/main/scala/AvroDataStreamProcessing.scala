package main.scala


import com.typesafe.config.ConfigFactory
import main.utils.{SparkUtilsJava, SparkUtilsScala}
import org.apache.log4j.Logger
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}


object AvroDataStreamProcessing {

  private val conf = ConfigFactory.load()
  val sparkConf = new SparkConf()
  val logger = Logger.getLogger(AvroDataStreamProcessing.getClass)


  def main(args: Array[String]) {
    if (args.length < 2) {
      System.exit(1)
    }

    //  setting spark conf parameters
    setSparkConfigParams()

    //  setting up streaming context
    val Array(brokers, topics) = args
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topicsSet = topics.split(",").toSet
    logger.info("main : kafkaParams = " + kafkaParams + "   topicsSet= " + topicsSet)

    //val ssc = StreamingContext.getOrCreate(checkpointDir, setupSsc(topicsSet, kafkaParams, checkpointDir, memConInfo2,msc) _)
    val ssc = setupSsc(topicsSet, kafkaParams)

    /* Start the spark streaming   */
    ssc.start()
    ssc.awaitTermination()
  } //main() ends


  /* set spark config params from application.conf */
  def setSparkConfigParams() = {
    sparkConf.setAppName(conf.getString("application.spark-streaming-app"))
    sparkConf.set("spark.driver.extraJavaOptions", conf.getString("application.driver-options"))
    sparkConf.set("spark.executor.extraJavaOptions", conf.getString("application.executor-options"))
  }


  /* this method sets up streaming context and defines the steps which every dstream will undergo.
     All the code processing logic which will be executed under each partition is part of this method.
   */
  def setupSsc(
                topicsSet: Set[String],
                kafkaParams: Map[String, String])(): StreamingContext = {


    //create spark and memsql context
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(conf.getInt("application.sparkbatchinterval")))

    processAvroRequestData(ssc, kafkaParams, topicsSet)

    ssc
  } //setUp(ssc) ends


  /*
     processing flow for avro format data
     */
  def processAvroRequestData(ssc: StreamingContext, kafkaParams: Map[String, String], topicsSet: Set[String]) = {
    val kafkaOffsetZookeeperNode = conf.getString("application.kafka_offset_zookeeper_node")
    val zookeeper_host = conf.getString("application.zookeeper_host")

    /** create custom avro direct kafka stream **/
    val messages = SparkUtilsScala.createCustomDirectKafkaStreamAvro(ssc, kafkaParams, zookeeper_host, kafkaOffsetZookeeperNode, topicsSet)

    /* processing of dstream starts from here */
    val requestLines = messages.map(_._2)
    requestLines.foreachRDD((rdd, time: Time) => {
      val timeSec0 = System.currentTimeMillis() / 1000
      logger.info("foreachRDD  STARTS   batchTime=" + time + "      timeSec0=" + timeSec0)
      rdd.foreachPartition { partitionOfRecords => {
        if (partitionOfRecords.isEmpty) {
          logger.info(" rdd.foreachPartition batchTime=" + time + "partitionOfRecords FOUND EMPTY ,IGNORING THIS PARTITION")
        }
        else {
          //TODO  right now per partition: can be done better
          // note : if multiple topics, every topic should adhere to same avro schema (topicsSet.last)
          val recordInjection = SparkUtilsJava.getRecordInjection(topicsSet.last)
          for (avroLine <- partitionOfRecords) {
            try {
              val record = recordInjection.invert(avroLine).get
              val field1Value = record.get("field1")
              val field2Value = record.get("field2")
              val field3Value = record.get("field3")

              /**
                * PROCESSING LOGIC AFTER WE EXTRACT DATA FIELDS FROM AVRO RECORD LINES which is i
                */
            }
            catch {
              case ex: Exception =>
                logger.error("request Exception in add batch. ex=" + ex)
            }
          }
        }
      } //else loop ends
      } //partition ends
    })
  }


}