import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import realTimeInsert.ImproveData
import tools.{ContantsSchema, DBUtils, DataUtils, GlobalConfigUtils}

object App {

  val KUDU_MASTER = GlobalConfigUtils.kuduMaster
  def main(args: Array[String]): Unit = {
    @transient //防止序列化
    val sparkConf = new SparkConf()
      .setAppName("APP")
      .setMaster("local[6]")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true") //开启wal预写日志，保存数据源的可靠性
      .set("spark.worker.timeout", GlobalConfigUtils.sparkWorkTimeout)
      .set("spark.core.max", GlobalConfigUtils.sparkMaxCores)
      .set("spark.rpc.askTimeout", GlobalConfigUtils.sparkRpcTimeout)
      .set("spark.task.mackFailure", GlobalConfigUtils.sparkTaskMaxFailures)
      .set("spark.speculation", GlobalConfigUtils.sparkSpeculation)
      .set("spark.driver.allowMutilpleContext", GlobalConfigUtils.sparkDriverAllowMutilpleContext)
      .set("spark.driver.allowMutilpleContext", GlobalConfigUtils.sparkSerializer)
      .set("spark.buffer.pageSize", GlobalConfigUtils.sparkBufferPageSize)
    val sc = new SparkContext(sparkConf)
    val sqlContext = SparkSession.builder().config(sparkConf).getOrCreate().sqlContext
    val ssc = new StreamingContext(sc, Seconds(2))
    //创建kudu客户端
    val kuduContext = new KuduContext(KUDU_MASTER, sqlContext.sparkContext)
    //设置checkpoint
    ssc.checkpoint("./Kafka_Receiver")
    //kafka相关
    val topics = Map(GlobalConfigUtils.kafkaTopic -> 3)
    //创建Dstream 消费kafka数据
    val receiverDstream = (1 to 3).map(x => {
      val stream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, GlobalConfigUtils.kafkaServer, GlobalConfigUtils.groupId, topics)
      stream
    }
    )
    val unionDStream: DStream[(String, String)] = ssc.union(receiverDstream)

    val Dstrem2: DStream[String] = unionDStream.map(_._2)


    Dstrem2.foreachRDD(x => {
      if (!x.isEmpty()) {
        val dataFrame: DataFrame = sqlContext.read.json(x)

        val test = dataFrame.select("ip", "sessionid", "advertisersid", "adorderid", "adcreativeid", "adplatformproviderid", "sdkversion", "adplatformkey")
        // ImproveData.process(sqlContext, sc, kuduContext, dataFrame)
        //调用方法将数据落地
        DBUtils.process(kuduContext, test, GlobalConfigUtils.odsPrefix + DataUtils.NowDate() + "7", KUDU_MASTER, ContantsSchema.odsSchema, "ip")
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
