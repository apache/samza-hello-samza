package com.magnetic.streaming.common

import com.magnetic.streaming.common.ParseHelpers._

object AdEventsParser {

  val MAGNETIC_NETWORK_ID = 1
  
  val IMPRESSION  = "imp"
  val CLICK       = "clk"
  val CONVERSION  = "conv"
  val CONVCOUNT   = "ccount"
  val RETARGETING = "rpx"
  val RETARCOUNT  = "rcount"
  val ERROR       = "error"
  val SKIP        = "skip"
  val BID         = "bid"
    
  val ACLK_PATH_QS = "/aclk/(\\w+=.*)/qe=".r
  val VALID_AUCTION_ID_PATTERN = "[\\-\\.\\w\\d]{6,50}".r

  def parse_querystring(fields: Map[String, String]): Map[String, String] = {
    fields.get("l") match {
      case Some(ACLK_PATH_QS(aclk)) => aclk.split('/').map(split_one(_)).toMap
      case _ =>
        parse_qs(fields.getOrElse("q", "").stripSuffix("/"))
    }
  }
  
  def parse_common(fields: Map[String, String]): (String, Long, String, Map[String, String], String) = {
    
    val timestamp = parse_timestamp(fields.getOrElse("t", ""))
    val querystring = parse_querystring(fields)
    val cookie = {
      val c = fields.getOrElse("c", "").split('|')(0)
      if (c.length() == 32) c
      else null
    }
    
    val (event_type, pixel_id) = {
	    if (fields.getOrElse("rpt", "") == "ahtm") (IMPRESSION, null)
	    else if (fields.getOrElse("rpt", "") == "aclk") (CLICK, null)
	    else (SKIP, null)
    }

    (event_type, timestamp, cookie, querystring, pixel_id)
  }

  def parse_fields(line: String):Map[String, String] = {
    line.split("\t").withFilter(_.contains("=")).map(split_one(_)).toMap
  }

  def should_skip_impression(fields: Map[String, String], querystring: Map[String, String]): Boolean = {
    false //TODO implement
  }

  def parse_imp_meta(line: String): Map[String, Any] = {
    val fields = parse_fields(line)
    val (event_type, timestamp, cookie, querystring, pixel_id) = parse_common(fields)
    event_type match {
      case IMPRESSION =>
        if (should_skip_impression(fields, querystring))
          throw new RuntimeException("Should skip impression")
        else {
          val network_id = _int(querystring.getOrElse("mp", MAGNETIC_NETWORK_ID.toString))

          val auction_id = {
            if (network_id == MAGNETIC_NETWORK_ID)
              querystring("id")
            else
              VALID_AUCTION_ID_PATTERN.findFirstIn(querystring.getOrElse("id", "")) match {
                case Some(x) => querystring("id")
                case _ => s"fake-$cookie-$timestamp"
              }
          }
          val d = scala.collection.mutable.Map[String, Any]()
          d("event_type") = IMPRESSION
          d("auction_id") = auction_id
          d("log_timestamp") = timestamp
          d("imp_log_line") = line
          d.toMap
        }
      case _ =>
        throw new RuntimeException("Not impression: " + event_type)
    }
  }

  def parse_bid_meta(line: String): Map[String, Any] = {
    val fields = line.split("\t", 10)
    val auction_id = fields(8)
    val timestamp = fields(0).toLong
    val d = scala.collection.mutable.Map[String, Any]()
    d("event_type") = BID
    d("auction_id") = auction_id
    d("log_timestamp") = timestamp
    d("bid_log_line") = line
    d.toMap
  }
}
