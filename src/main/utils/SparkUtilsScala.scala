package main.utils

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

/**
  * Created by chandan on 09/09/16.
  * contains spark/kafka utility methods common to processing
  */
object SparkUtilsScala {
  val logger = Logger.getLogger(SparkUtilsScala.getClass)


  /*
  1. method to create direct kafka stream using kafka offset from zookeeper node
  if available else from latest kafka offset from kafka brokers
  2. it is overloaded version of default createDirectStream() method of Direct kafka
      as we do not need checkpointing of state but we do need need kafka offsets
      for fault tolerance and upgradation.
  */

  def createCustomDirectKafkaStreamAvro(ssc: StreamingContext, kafkaParams: Map[String, String], zkHosts: String, zkPath: String,
                                        topics: Set[String]): InputDStream[(String, Array[Byte])] = {
    val zkClient = new ZkClient(zkHosts, Constants.ZOOKEEPER_SESSION_TIMEOUT, Constants.ZOOKEEPER_CONNECTION_TIMEOUT)
    //only one time during start up, try to read offset from zookeeper
    val storedOffsets = readOffsets(zkClient, zkHosts, zkPath)
    val kafkaStream = storedOffsets match {
      case None =>
        // start from the latest offsets if not found in zookeeper
        KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topics)
      case Some(fromOffsets) =>
        // start from previously saved offsets if found in zookeeper
        val messageHandler = (mmd: MessageAndMetadata[String, Array[Byte]]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder, (String, Array[Byte])](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    // save the offsets before processing for each partition of each kafka topic
    //on restart,only this offset range will be re-processed with atleast once, rest others exactly once
    kafkaStream.foreachRDD(rdd => saveOffsets(zkClient, zkHosts, zkPath, rdd))
    kafkaStream
  }

  /*
  Read the previously saved offsets of kafka topic partiions from Zookeeper
  */
  def readOffsets(zkClient: ZkClient, zkHosts: String, zkPath: String): Option[Map[TopicAndPartition, Long]] = {
    logger.info("readOffsets: Reading offsets from Zookeeper")
    val stopwatch = new Stopwatch()
    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)
    offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
        logger.info(s"readOffsets: Read offset ranges: $offsetsRangesStr")
        val offsets = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map { case Array(topic, partitionStr, offsetStr) => TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong }
          .toMap
        logger.info("readOffsets: Done reading offsets from Zookeeper. Took " + stopwatch)
        Some(offsets)
      case None =>
        logger.warn("readOffsets: No offsets found in Zookeeper. Took " + stopwatch)
        None
    }
  }


  /*
     save offsets of each kakfa partition of each kafka topic to zookeeper
   */
  def saveOffsets(zkClient: ZkClient, zkHosts: String, zkPath: String, rdd: RDD[_]): Unit = {
    logger.info("saveOffsets: Saving offsets to Zookeeper")
    val stopwatch = new Stopwatch()
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //offsetsRanges.foreach(offsetRange => logger.info(s"saveOffsets: chandan : Using offsetRange = ${offsetRange}"))
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.topic}:${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    logger.info("saveOffsets: Writing offsets to Zookeeper zkClient=" + zkClient + "  zkHosts=" + zkHosts + "zkPath=" + zkPath + "  offsetsRangesStr:" + offsetsRangesStr)
    ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)
    //new ZkUtils(zkClient,new ZkConnection(zkHosts),true).updatePersistentPath( zkPath, offsetsRangesStr)
    logger.info("saveOffsets: updating offsets in Zookeeper. Took " + stopwatch)
  }

  class Stopwatch {
    private val start = System.currentTimeMillis()

    override def toString = (System.currentTimeMillis - start) + " ms"
  }



}
