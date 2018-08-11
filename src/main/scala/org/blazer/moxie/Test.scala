package org.blazer.moxie

import com.alibaba.fastjson.{JSON, JSONObject}
//import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
//import scala.collection.mutable.ListBuffer

object Test {

//    def main(args: Array[String]): Unit = {
//        val spark = SparkSession.builder().getOrCreate
//        val moxie = spark.read.format("orc").load("/user/deploy/warehouse/moxie")
//        moxie.createOrReplaceTempView("moxie")
//        //val result = spark.sql("select * from moxie").count
//        val result = spark.sql("select * from moxie limit 1")
//        val field_elimiter = ","
//        val result2 = result.map(row => {
//            val json = JSON.parseArray(row.toString).getJSONObject(0)
//            val _modify_time = getStr(json.getString("last_modify_time"))
//            val _mobile = getStr(json.getString("mobile"))
//            val _code = getStr(json.getString("code"))
//            val _message = getStr(json.getString("message"))
//            val _open_time = getStr(json.getString("open_time"))
//            val _name = getStr(json.getString("name"))
//            val _idcard = getStr(json.getString("idcard"))
//            val _carrier = getStr(json.getString("carrier"))
//            val _province = getStr(json.getString("province"))
//            val _city = getStr(json.getString("city"))
//            val _level = getStr(json.getString("level"))
//            val _state = getStr(json.getString("state"))
//            val _package_name = getStr(json.getString("package_name"))
//            val _available_balance = getStr(json.getString("available_balance"))
//            val rst = _modify_time + field_elimiter + _mobile + field_elimiter + _code + field_elimiter + _message + field_elimiter + _open_time + field_elimiter + _name + field_elimiter + _idcard + field_elimiter + _carrier + field_elimiter + _province + field_elimiter + _city + field_elimiter + _level + field_elimiter + _state + field_elimiter + _package_name + field_elimiter + _available_balance
//            rst
//        })
//        result2.foreach(a => {
//            println(a)
//        })
//    }
//
//    def getStr(str: String): String = {
//        if (str == null || str.trim.isEmpty) {
//            return "null"
//        }
//        return str
//    }
//
//    def getInt(str: String): Integer = {
//        if (str == null || str.trim.isEmpty) {
//            return 0
//        }
//        try {
//            return str.toInt
//        } catch {
//            // case ex : Exception
//            case Exception => {
//                return 0
//            }
//        }
//        return 0
//    }

}
