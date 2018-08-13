package org.blazer.moxie

import java.io._
import scala.io.Source
import com.alibaba.fastjson.{JSON, JSONObject}

object Moxie {

    def main(args: Array[String]): Unit = {
        var json_path = "/Users/hyy/test.json"
        var output_path = "/Users/hyy/test/"

        json_path = "/Users/hyy/Downloads/download_zoc/hb_carrier_1182433_1180485_103497881_201711161510816476126.json"
        output_path = "/Users/hyy/Downloads/download_zoc"

        if (args.length < 2) {
            println(s"args error : Usage : java -jar xxx.jar {json_path} {output_path}")
            System.exit(-1)
        }
        json_path = args(0)
        output_path = args(1)

        output_path = output_path + File.separator

        val file_moxie_base_info = new FileWriter(output_path + "moxie_base_info.csv", true)
        val file_moxie_packages_info = new FileWriter(output_path + "moxie_packages_info.csv", true)
        val file_moxie_families_info = new FileWriter(output_path + "moxie_families_info.csv", true)
        val file_moxie_recharges_info = new FileWriter(output_path + "moxie_recharges_info.csv", true)
        val file_moxie_bills_info = new FileWriter(output_path + "moxie_bills_info.csv", true)
        val file_moxie_calls_info = new FileWriter(output_path + "moxie_calls_info.csv", true)
        val file_moxie_smses_info = new FileWriter(output_path + "moxie_smses_info.csv", true)
        val file_moxie_nets_info = new FileWriter(output_path + "moxie_nets_info.csv", true)
        val field_elimiter = "\t"

        var jsonStr = ""
        for (line <- Source.fromFile(json_path).getLines) {
            jsonStr += line
        }

        val json = JSON.parseObject(jsonStr)

        if (json == null) {
            println(s"parse json is null. [$json_path]")
            return
        }

        // moxie_base_info
        val _modify_time = json.getString("last_modify_time")
        val _mobile = json.getString("mobile")
        val _code = json.getString("code")
        val _message = json.getString("message")
        val _open_time = json.getString("open_time")
        val _name = json.getString("name")
        val _idcard = json.getString("idcard")
        val _carrier = json.getString("carrier")
        val _province = json.getString("province")
        val _city = json.getString("city")
        val _level = json.getString("level")
        val _state = json.getString("state")
        val _package_name = json.getString("package_name")
        val _available_balance = json.getString("available_balance")

        if (_mobile == null) {
            println(s"parse mobile is null. [${_mobile}]")
            return
        }

        // moxie_base_info
        output(file_moxie_base_info, _modify_time + field_elimiter + _mobile + field_elimiter + _code + field_elimiter + _message + field_elimiter + _open_time + field_elimiter + _name + field_elimiter + _idcard + field_elimiter + _carrier + field_elimiter + _province + field_elimiter + _city + field_elimiter + _level + field_elimiter + _state + field_elimiter + _package_name + field_elimiter + _available_balance)
        file_moxie_base_info.flush()

        // moxie_packages_info
        val packages = json.getJSONArray("packages")
        if (packages != null && packages.size() > 0) {
            for (i <- 0 until packages.size()) {
                val pkg = packages.get(i).asInstanceOf[JSONObject]
                val items = pkg.getJSONArray("items")

                val _bill_month = pkg.get("bill_start_date")
                if (items != null && items.size() > 0) {
                    for (j <- 0 until items.size()) {
                        val obj = items.get(j).asInstanceOf[JSONObject]
                        val _packages_item = obj.get("item")
                        val _packages_unit = obj.get("unit")
                        val _packages_total = obj.get("total")
                        val _packages_used = obj.get("used")
                        output(file_moxie_packages_info, _modify_time + field_elimiter + _mobile + field_elimiter + _bill_month + field_elimiter + _packages_item + field_elimiter + _packages_unit + field_elimiter + _packages_total + field_elimiter + _packages_used)
                    }
                    file_moxie_packages_info.flush()
                }
            }
        }

        // moxie_families_info
        val families = json.getJSONArray("families")
        if (families != null && families.size() > 0) {
            for (i <- 0 until families.size()) {
                val pkg = families.get(i).asInstanceOf[JSONObject]
                val items = pkg.getJSONArray("items")

                val _family_num = pkg.get("family_num")
                val _is_master = 1
                if (items != null && items.size() > 0) {
                    for (j <- 0 until items.size()) {
                        val obj = items.get(j).asInstanceOf[JSONObject]
                        val _long_number = obj.get("long_number")
                        val _short_number = obj.get("short_number")
                        val _member_type = obj.get("member_type")
                        val _join_date = obj.get("join_date")
                        val _expire_date = obj.get("expire_date")
                        output(file_moxie_families_info, _modify_time + field_elimiter + _mobile + field_elimiter + _family_num + field_elimiter + _is_master + field_elimiter + _long_number + field_elimiter + _short_number + field_elimiter + _member_type + field_elimiter + _join_date + field_elimiter + _expire_date)
                    }
                    file_moxie_families_info.flush()
                }
            }
        }

        // moxie_recharges_info
        val recharges = json.getJSONArray("recharges")
        if (recharges != null && recharges.size() > 0) {
            for (i <- 0 until recharges.size()) {
                val pkg = recharges.get(i).asInstanceOf[JSONObject]
                val _recharges_id = pkg.get("details_id")
                var _amount = pkg.get("amount").toString.toInt
                var _recharge_type = ""

                if (_amount >= 0) {
                    _recharge_type = "充值"
                } else {
                    _recharge_type = "退款"
                    _amount = _amount * -1
                }

                val _type = pkg.get("type")
                val _recharge_date = pkg.get("recharge_date")
                val _recharge_time = pkg.get("recharge_time")
                output(file_moxie_recharges_info, _modify_time + field_elimiter + _mobile + field_elimiter + _recharges_id + field_elimiter + _recharge_type + field_elimiter + _amount + field_elimiter + _type + field_elimiter + _recharge_date + field_elimiter + _recharge_time)
            }
            file_moxie_recharges_info.flush()
        }

        // moxie_bills_info
        val bills = json.getJSONArray("bills")
        if (bills != null && bills.size() > 0) {
            for (i <- 0 until bills.size()) {
                val pkg = bills.get(i).asInstanceOf[JSONObject]

                val _bill_month = pkg.get("bill_month")
                val _base_fee = pkg.get("base_fee")
                val _extra_service_fee = pkg.get("extra_service_fee")
                val _voice_fee = pkg.get("voice_fee")
                val _sms_fee = pkg.get("sms_fee")
                val _web_fee = pkg.get("web_fee")
                val _extra_fee = pkg.get("extra_fee")
                val _total_fee = pkg.get("total_fee")
                val _discount = pkg.get("discount")
                val _extra_discount = pkg.get("extra_discount")
                val _actual_fee = pkg.get("actualFee")
                val _paid_fee = pkg.get("paid_fee")
                val _unpaid_fee = pkg.get("unpaid_fee")
                val _point = pkg.get("point")
                val _last_point = pkg.get("last_point")
                val _related_mobiles = pkg.get("related_mobiles")
                val _notes = pkg.get("notes")
                output(file_moxie_bills_info, _modify_time + field_elimiter + _mobile + field_elimiter + _bill_month + field_elimiter + _base_fee + field_elimiter + _extra_service_fee + field_elimiter + _voice_fee + field_elimiter + _sms_fee + field_elimiter + _web_fee + field_elimiter + _extra_fee + field_elimiter + _total_fee + field_elimiter + _discount + field_elimiter + _extra_discount + field_elimiter + _actual_fee + field_elimiter + _paid_fee + field_elimiter + _unpaid_fee + field_elimiter + _point + field_elimiter + _last_point + field_elimiter + _related_mobiles + field_elimiter + _notes)
            }
            file_moxie_bills_info.flush()
        }

        // moxie_calls_info
        val calls = json.getJSONArray("calls")
        if (calls != null && calls.size() > 0) {
            for (i <- 0 until calls.size()) {
                val pkg = calls.get(i).asInstanceOf[JSONObject]
                val items = pkg.getJSONArray("items")

                val _bill_month = pkg.get("bill_month")
                val _total_calls_num = pkg.get("total_size")

                if (items != null && items.size() > 0) {
                    for (j <- 0 until items.size()) {
                        val obj = items.get(j).asInstanceOf[JSONObject]
                        val _call_id = obj.get("details_id")
                        val _call_date = obj.get("time")
                        val _call_time = obj.get("time")
                        val _location = obj.get("location")
                        val _location_type = obj.get("location_type")
                        val _dial_tpye = obj.get("dial_type")
                        val _peer_number = obj.get("peer_number")
                        val _call_duration = obj.get("duration")
                        val _fee = obj.get("fee")
                        output(file_moxie_calls_info, _modify_time + field_elimiter + _mobile + field_elimiter + _bill_month + field_elimiter + _total_calls_num + field_elimiter + _call_id + field_elimiter + _call_date + field_elimiter + _call_time + field_elimiter + _location + field_elimiter + _location_type + field_elimiter + _dial_tpye + field_elimiter + _peer_number + field_elimiter + _call_duration + field_elimiter + _fee)
                    }
                    file_moxie_calls_info.flush()
                }
            }
        }


        // moxie_smses_info
        val smses = json.getJSONArray("smses")
        if (smses != null && smses.size() > 0) {
            for (i <- 0 until smses.size()) {
                val pkg = smses.get(i).asInstanceOf[JSONObject]
                val items = pkg.getJSONArray("items")

                val _bill_month = pkg.get("bill_month")
                val _total_smses_num = pkg.get("total_size")

                if (items != null && items.size() > 0) {
                    for (j <- 0 until items.size()) {
                        val obj = items.get(j).asInstanceOf[JSONObject]
                        val _smses_id = obj.get("details_id")
                        val _smses_date = obj.get("time")
                        val _smses_time = obj.get("time")
                        val _location = obj.get("location")
                        val _msg_type = obj.get("msg_type")
                        val _send_tpye = obj.get("send_tpye")
                        val _service_name = obj.get("service_name")
                        val _peer_number = obj.get("peer_number")
                        val _fee = obj.get("fee")
                        output(file_moxie_smses_info, _modify_time + field_elimiter + _mobile + field_elimiter + _bill_month + field_elimiter + _total_smses_num + field_elimiter + _smses_id + field_elimiter + _smses_date + field_elimiter + _smses_time + field_elimiter + _location + field_elimiter + _msg_type + field_elimiter + _send_tpye + field_elimiter + _service_name + field_elimiter + _peer_number + field_elimiter + _fee)
                    }
                    file_moxie_smses_info.flush()
                }
            }
        }

        // moxie_nets_info
        val nets = json.getJSONArray("nets")
        if (nets != null && nets.size() > 0) {
            for (i <- 0 until nets.size()) {
                val pkg = nets.get(i).asInstanceOf[JSONObject]
                val items = pkg.getJSONArray("items")

                val _bill_month = pkg.get("bill_month")
                val _total_smses_num = pkg.get("total_size")

                if (items != null && items.size() > 0) {
                    for (j <- 0 until items.size()) {
                        val obj = items.get(j).asInstanceOf[JSONObject]
                        val _nets_id = obj.get("details_id")
                        val _nets_date = obj.get("time")
                        val _nets_time = obj.get("time")
                        val _location = obj.get("location")
                        val _net_type = obj.get("net_type")
                        val _service_name = obj.get("service_name")
                        val _nets_duration = obj.get("duration")
                        val _nets_subflow = obj.get("subflow")
                        val _fee = obj.get("fee")
                        output(file_moxie_nets_info, _modify_time + field_elimiter + _mobile + field_elimiter + _bill_month + field_elimiter + _total_smses_num + field_elimiter + _nets_id + field_elimiter + _nets_date + field_elimiter + _nets_time + field_elimiter + _location + field_elimiter + _net_type + field_elimiter + _service_name + field_elimiter + _nets_duration + field_elimiter + _nets_subflow + field_elimiter + _fee)
                    }
                    file_moxie_nets_info.flush()
                }
            }
        }

        println(s"parse [$json_path] successful.")
    }

    def output(fileWriter: FileWriter, str: String): Unit = {
        fileWriter.write(str + "\n")
    }

}