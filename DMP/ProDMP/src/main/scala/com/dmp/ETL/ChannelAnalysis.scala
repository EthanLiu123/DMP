package com.dmp.ETL

import com.dmp.`trait`.ProcessData
import com.dmp.tools._
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.kudu.spark.kudu._
/**
  * Created by angel
  */
object ChannelAnalysis extends ProcessData{
  val KUDU_MASTER = GlobalConfigUtils.kuduMaster
  val SOURCE_TABLE = GlobalConfigUtils.ODS_PREFIX + DataUtils.NowDate()
  val SINK_TABLE = GlobalConfigUtils.ChannelAnalysis
  val kuduOptions:Map[String , String] = Map(
    "kudu.master" -> KUDU_MASTER ,
    "kudu.table" -> SOURCE_TABLE
  )
  override def process(sQLContext: SQLContext, sparkContext: SparkContext, kuduContext: KuduContext): Unit = {
    //1:获取数据
    val data = sQLContext.read.options(kuduOptions).kudu
    data.registerTempTable("ods")
    //2:报表
    val tmp = sQLContext.sql(ContantsSQL.ChannelAnalysis_tmp)
    tmp.registerTempTable("temp_table")
    val result = sQLContext.sql(ContantsSQL.ChannelAnalysis)

    //3：数据落地
    val schema:Schema = ContantsSchema.ChannelAnalysis
    val partitionID = "channelid"
    DBUtils.process(kuduContext , result , SINK_TABLE , KUDU_MASTER , schema , partitionID)
  }
}
