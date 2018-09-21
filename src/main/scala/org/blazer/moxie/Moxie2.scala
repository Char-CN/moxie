package org.blazer.moxie

import java.io._
import java.nio.charset.StandardCharsets

import scala.io.Source
import scala.util.control.Breaks._

import com.alibaba.fastjson.{JSON, JSONObject}

object Moxie2 {

  def main(args: Array[String]): Unit = {
    var json_dir_path = "/Users/hyy/test.json"
    var output_path = "/Users/hyy/test/"

    json_dir_path = "/Users/hyy/za/moxie/test/data/"
    output_path = "/Users/hyy/za/moxie/test"

    if (args.length < 2) {
      println(s"args error : Usage : java -jar xxx.jar {json_dir_path} {output_path}")
      System.exit(-1)
    }
    json_dir_path = args(0)
    output_path = args(1)

    output_path = output_path + File.separator

    // append 追加文件
    val file_moxie_base_info = getBufferedWriter(output_path + "moxie_base_info.csv")
    val file_moxie_packages_info = getBufferedWriter(output_path + "moxie_packages_info.csv")
    val file_moxie_families_info = getBufferedWriter(output_path + "moxie_families_info.csv")
    val file_moxie_recharges_info = getBufferedWriter(output_path + "moxie_recharges_info.csv")
    val file_moxie_bills_info = getBufferedWriter(output_path + "moxie_bills_info.csv")
    val file_moxie_calls_info = getBufferedWriter(output_path + "moxie_calls_info.csv")
    val file_moxie_smses_info = getBufferedWriter(output_path + "moxie_smses_info.csv")
    val file_moxie_nets_info = getBufferedWriter(output_path + "moxie_nets_info.csv")
    val field_delimiter = "\t"
    var success_count = 0

    var allow_write_file_index = "0,1,2,3,4,5,6,7"
    if (args.length >= 3) {
      allow_write_file_index = args(2)
    }
    allow_write_file_index = "," + allow_write_file_index + ","

    val dir = new java.io.File(json_dir_path, "")
    println("exists: " + dir.exists())
    println("canRead: " + dir.canRead)
    println("canWrite: " + dir.canWrite)
    println("isDirectory: " + dir.isDirectory)
    println("dir length: " + dir.length())
    println("listFiles length: " + dir.listFiles().length)
    dir.listFiles.foreach { file =>
      try {
        val json_path = file.getAbsolutePath
        val json_name = file.getName
        var jsonStr = ""
        for (line <- Source.fromFile(json_path, "UTF-8").getLines) {
          jsonStr += line
        }
        val json = JSON.parseObject(jsonStr)
        breakable {
          if (json == null) {
            println(s"parse json [$json_name}] is null.")
            break
          } else if (getStr(json.getString("mobile")) == "null") {
            println(s"parse mobile [" + json.getString("mobile") + "] is null.")
            break
          } else {
            // moxie_base_info
            val _modify_time = getStr(json.getString("last_modify_time"))
            val _mobile = getStr(json.getString("mobile"))
            val _code = getStr(json.getString("code"))
            val _message = getStr(json.getString("message"))
            val _open_time = getStr(json.getString("open_time"))
            val _name = getStr(json.getString("name"))
            val _idcard = getStr(json.getString("idcard"))
            val _carrier = getStr(json.getString("carrier"))
            val _province = getStr(json.getString("province"))
            val _city = getStr(json.getString("city"))
            val _level = getStr(json.getString("level"))
            val _state = getStr(json.getString("state"))
            val _package_name = getStr(json.getString("package_name"))
            val _available_balance = getStr(json.getString("available_balance"))

            if (allow_write_file_index.contains(",0,")) {
              // moxie_base_info
              output(file_moxie_base_info, _modify_time + field_delimiter + _mobile + field_delimiter + _code + field_delimiter + _message + field_delimiter + _open_time + field_delimiter + _name + field_delimiter + _idcard + field_delimiter + _carrier + field_delimiter + _province + field_delimiter + _city + field_delimiter + _level + field_delimiter + _state + field_delimiter + _package_name + field_delimiter + _available_balance)
              file_moxie_base_info.flush()
            }

            if (allow_write_file_index.contains(",1,")) {
              // moxie_packages_info
              val packages = json.getJSONArray("packages")

              if (packages != null && packages.size() > 0) {
                for (i <- 0 until packages.size()) {
                  val pkg = packages.get(i).asInstanceOf[JSONObject]
                  val items = pkg.getJSONArray("items")

                  val _bill_month = getStr(pkg.getString("bill_start_date"))
                  if (items != null && items.size() > 0) {
                    for (j <- 0 until items.size()) {
                      val obj = items.get(j).asInstanceOf[JSONObject]
                      val _packages_item = getStr(obj.getString("item"))
                      val _packages_unit = getStr(obj.getString("unit"))
                      val _packages_total = getStr(obj.getString("total"))
                      val _packages_used = getStr(obj.getString("used"))
                      output(file_moxie_packages_info, _modify_time + field_delimiter + _mobile + field_delimiter + _bill_month + field_delimiter + _packages_item + field_delimiter + _packages_unit + field_delimiter + _packages_total + field_delimiter + _packages_used)
                    }
                    file_moxie_packages_info.flush()
                  }
                }
              }
            }

            if (allow_write_file_index.contains(",2,")) {
              // moxie_families_info
              val families = json.getJSONArray("families")
              if (families != null && families.size() > 0) {
                for (i <- 0 until families.size()) {
                  val pkg = families.get(i).asInstanceOf[JSONObject]
                  val items = pkg.getJSONArray("items")

                  val _family_num = getStr(pkg.getString("family_num"))
                  if (items != null && items.size() > 0) {
                    for (j <- 0 until items.size()) {
                      val obj = items.get(j).asInstanceOf[JSONObject]
                      val _long_number = getStr(obj.getString("long_number"))
                      val _short_number = getStr(obj.getString("short_number"))
                      val _member_type = getStr(obj.getString("member_type"))
                      val _join_date = getStr(obj.getString("join_date"))
                      val _expire_date = getStr(obj.getString("expire_date"))
                      var _is_master = 0
                      if (_long_number == _mobile) {
                        _is_master = 1
                      }
                      output(file_moxie_families_info, _modify_time + field_delimiter + _mobile + field_delimiter + _family_num + field_delimiter + _is_master + field_delimiter + _long_number + field_delimiter + _short_number + field_delimiter + _member_type + field_delimiter + _join_date + field_delimiter + _expire_date)
                    }
                    file_moxie_families_info.flush()
                  }
                }
              }
            }

            if (allow_write_file_index.contains(",3,")) {
              // moxie_recharges_info
              val recharges = json.getJSONArray("recharges")
              if (recharges != null && recharges.size() > 0) {
                for (i <- 0 until recharges.size()) {
                  val pkg = recharges.get(i).asInstanceOf[JSONObject]
                  val _recharges_id = getStr(pkg.getString("details_id"))
                  var _amount = getInt(pkg.getString("amount"))
                  var _recharge_type = ""

                  if (_amount >= 0) {
                    _recharge_type = "充值"
                  } else {
                    _recharge_type = "退款"
                    _amount = _amount * -1
                  }

                  val _type = getStr(pkg.getString("type"))
                  val _recharge_date = getStr(pkg.getString("recharge_date"))
                  val _recharge_time = getStr(pkg.getString("recharge_time"))
                  output(file_moxie_recharges_info, _modify_time + field_delimiter + _mobile + field_delimiter + _recharges_id + field_delimiter + _recharge_type + field_delimiter + _amount + field_delimiter + _type + field_delimiter + _recharge_date + field_delimiter + _recharge_time)
                }
                file_moxie_recharges_info.flush()
              }
            }

            if (allow_write_file_index.contains(",4,")) {
              // moxie_bills_info
              val bills = json.getJSONArray("bills")
              if (bills != null && bills.size() > 0) {
                for (i <- 0 until bills.size()) {
                  val pkg = bills.get(i).asInstanceOf[JSONObject]

                  val _bill_month = getStr(pkg.getString("bill_month"))
                  val _base_fee = getStr(pkg.getString("base_fee"))
                  val _extra_service_fee = getStr(pkg.getString("extra_service_fee"))
                  val _voice_fee = getStr(pkg.getString("voice_fee"))
                  val _sms_fee = getStr(pkg.getString("sms_fee"))
                  val _web_fee = getStr(pkg.getString("web_fee"))
                  val _extra_fee = getStr(pkg.getString("extra_fee"))
                  val _total_fee = getStr(pkg.getString("total_fee"))
                  val _discount = getStr(pkg.getString("discount"))
                  val _extra_discount = getStr(pkg.getString("extra_discount"))
                  val _actual_fee = getStr(pkg.getString("actualFee"))
                  val _paid_fee = getStr(pkg.getString("paid_fee"))
                  val _unpaid_fee = getStr(pkg.getString("unpaid_fee"))
                  val _point = getStr(pkg.getString("point"))
                  val _last_point = getStr(pkg.getString("last_point"))
                  val _related_mobiles = getStr(pkg.getString("related_mobiles"))
                  val _notes = getStr(pkg.getString("notes"))
                  output(file_moxie_bills_info, _modify_time + field_delimiter + _mobile + field_delimiter + _bill_month + field_delimiter + _base_fee + field_delimiter + _extra_service_fee + field_delimiter + _voice_fee + field_delimiter + _sms_fee + field_delimiter + _web_fee + field_delimiter + _extra_fee + field_delimiter + _total_fee + field_delimiter + _discount + field_delimiter + _extra_discount + field_delimiter + _actual_fee + field_delimiter + _paid_fee + field_delimiter + _unpaid_fee + field_delimiter + _point + field_delimiter + _last_point + field_delimiter + _related_mobiles + field_delimiter + _notes)
                }
                file_moxie_bills_info.flush()
              }
            }

            if (allow_write_file_index.contains(",5,")) {
              // moxie_calls_info
              val calls = json.getJSONArray("calls")
              if (calls != null && calls.size() > 0) {
                for (i <- 0 until calls.size()) {
                  val pkg = calls.get(i).asInstanceOf[JSONObject]
                  val items = pkg.getJSONArray("items")

                  val _bill_month = getStr(pkg.getString("bill_month"))
                  val _total_calls_num = getStr(pkg.getString("total_size"))

                  if (items != null && items.size() > 0) {
                    for (j <- 0 until items.size()) {
                      val obj = items.get(j).asInstanceOf[JSONObject]
                      val _call_id = getStr(obj.getString("details_id"))
                      val _call_date = getStr(obj.getString("time"))
                      val _call_time = getStr(obj.getString("time"))
                      val _location = getStr(obj.getString("location"))
                      val _location_type = getStr(obj.getString("location_type"))
                      val _dial_tpye = getStr(obj.getString("dial_type"))
                      val _peer_number = getStr(obj.getString("peer_number"))
                      val _call_duration = getStr(obj.getString("duration"))
                      val _fee = getStr(obj.getString("fee"))
                      output(file_moxie_calls_info, _modify_time + field_delimiter + _mobile + field_delimiter + _bill_month + field_delimiter + _total_calls_num + field_delimiter + _call_id + field_delimiter + _call_date + field_delimiter + _call_time + field_delimiter + _location + field_delimiter + _location_type + field_delimiter + _dial_tpye + field_delimiter + _peer_number + field_delimiter + _call_duration + field_delimiter + _fee)
                    }
                    file_moxie_calls_info.flush()
                  }
                }
              }
            }


            if (allow_write_file_index.contains(",6,")) {
              // moxie_smses_info
              val smses = json.getJSONArray("smses")
              if (smses != null && smses.size() > 0) {
                for (i <- 0 until smses.size()) {
                  val pkg = smses.get(i).asInstanceOf[JSONObject]
                  val items = pkg.getJSONArray("items")

                  val _bill_month = getStr(pkg.getString("bill_month"))
                  val _total_smses_num = getStr(pkg.getString("total_size"))

                  if (items != null && items.size() > 0) {
                    for (j <- 0 until items.size()) {
                      val obj = items.get(j).asInstanceOf[JSONObject]
                      val _smses_id = getStr(obj.getString("details_id"))
                      val _smses_date = getStr(obj.getString("time"))
                      val _smses_time = getStr(obj.getString("time"))
                      val _location = getStr(obj.getString("location"))
                      val _msg_type = getStr(obj.getString("msg_type"))
                      val _send_tpye = getStr(obj.getString("send_tpye"))
                      val _service_name = getStr(obj.getString("service_name"))
                      val _peer_number = getStr(obj.getString("peer_number"))
                      val _fee = getStr(obj.getString("fee"))
                      output(file_moxie_smses_info, _modify_time + field_delimiter + _mobile + field_delimiter + _bill_month + field_delimiter + _total_smses_num + field_delimiter + _smses_id + field_delimiter + _smses_date + field_delimiter + _smses_time + field_delimiter + _location + field_delimiter + _msg_type + field_delimiter + _send_tpye + field_delimiter + _service_name + field_delimiter + _peer_number + field_delimiter + _fee)
                    }
                    file_moxie_smses_info.flush()
                  }
                }
              }
            }

            if (allow_write_file_index.contains(",7,")) {
              // moxie_nets_info
              val nets = json.getJSONArray("nets")
              if (nets != null && nets.size() > 0) {
                for (i <- 0 until nets.size()) {
                  val pkg = nets.get(i).asInstanceOf[JSONObject]
                  val items = pkg.getJSONArray("items")

                  val _bill_month = getStr(pkg.getString("bill_month"))
                  val _total_smses_num = getStr(pkg.getString("total_size"))

                  if (items != null && items.size() > 0) {
                    for (j <- 0 until items.size()) {
                      val obj = items.get(j).asInstanceOf[JSONObject]
                      val _nets_id = getStr(obj.getString("details_id"))
                      val _nets_date = getStr(obj.getString("time"))
                      val _nets_time = getStr(obj.getString("time"))
                      val _location = getStr(obj.getString("location"))
                      val _net_type = getStr(obj.getString("net_type"))
                      val _service_name = getStr(obj.getString("service_name"))
                      val _nets_duration = getStr(obj.getString("duration"))
                      val _nets_subflow = getStr(obj.getString("subflow"))
                      val _fee = getStr(obj.getString("fee"))
                      output(file_moxie_nets_info, _modify_time + field_delimiter + _mobile + field_delimiter + _bill_month + field_delimiter + _total_smses_num + field_delimiter + _nets_id + field_delimiter + _nets_date + field_delimiter + _nets_time + field_delimiter + _location + field_delimiter + _net_type + field_delimiter + _service_name + field_delimiter + _nets_duration + field_delimiter + _nets_subflow + field_delimiter + _fee)
                    }
                    file_moxie_nets_info.flush()
                  }
                }
              }
            }

            success_count = success_count + 1
            println(s"parse [$success_count] [$json_name] [${_mobile}] successful.")
          }
        }
      } catch {
        case e: Exception => println(file.getName + "||||||||" + e.getMessage)
      }
    }
  }

  def getStr(str: String): String = {
    if (str == null || str.trim.isEmpty) "null" else str
  }

  def getInt(str: String): Integer = {
    if (str == null || str.trim.isEmpty) return 0
    try {
      str.toInt
    } catch {
      case _: Exception => 0
    }
    0
  }

  def getBufferedWriter(path: String, append: Boolean = false): BufferedWriter = {
    new BufferedWriter(
      new OutputStreamWriter(new FileOutputStream(path, append), StandardCharsets.UTF_8))
  }

  def output(bufferedWriter: BufferedWriter, str: String): Unit = {
    bufferedWriter.write(str + "\n")
  }
}
