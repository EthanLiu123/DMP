package com.dmp.trading

import java.util

import com.dmp.tools.{GlobalConfigUtils, ParseJson}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod

/**
  * Created by angel
  */
object ExtractTrading {

  def getArea(location:String):String = {
    //https://restapi.amap.com/v3/geocode/regeo?&location=116.310003,39.991957&key=6d5bd7e435a61f48ebc572e4f4945799
    //代码发送-->url -->response -->商圈
    val client = new HttpClient
    val url = "https://restapi.amap.com/v3/geocode/regeo?&location="+location+"&key="+GlobalConfigUtils.getKey
    val method = new GetMethod(url)
    val code: Int = client.executeMethod(method)
    var temp = ""
    if(code == 200){
      val response = method.getResponseBodyAsString
      val trade: String = ParseJson.parse(response)
      temp = trade
    }
    temp
  }
}
