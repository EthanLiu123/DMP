package com.dmp.tags.mkTags

import com.dmp.`trait`.Tags
import org.apache.spark.sql.Row

/**
  * Created by angel
  */
object Tags_adType extends Tags{
  /**
    * 打标签的方法
    *
    * @param args 传入的标签（标签个数不确定）
    **/
  override def makeTags(args: Any*): Map[String, Double] = {
    var map = Map[String, Double]()
    if(args.length>0){
      val row: Row = args(0).asInstanceOf[Row]
      //adspacetype       1 banner 2 插屏   3 全屏
      val adspacetype = row.getAs[Long]("adspacetype").toInt
      if(adspacetype != "" || adspacetype != null){
        adspacetype match{
          case x if x == 1 => map += ("LC"+x -> 1)
          case x if x == 2 => map += ("LC"+x -> 1)
          case x if x == 3 => map += ("LC"+x -> 1)
        }
      }
    }
    map
  }
}
