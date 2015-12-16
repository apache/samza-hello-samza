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
    
  val ACLK_PATH_QS = "/aclk/(\\w+=.*)/qe=".r
  val VALID_AUCTION_ID_PATTERN = "[\\-\\.\\w\\d]{6,50}".r

  def parse_querystring(fields: Map[String, String]): Map[String, String] = {
    fields.get("l") match {
      case Some(ACLK_PATH_QS(aclk)) => aclk.split('/').map(split_one(_)).toMap
      case _ =>
        parse_qs(fields.getOrElse("q", "").stripSuffix("/"))
    }
  }
  
  def parse_common(fields: Map[String, String]): (String, String, String, Map[String, String], String) = {
    
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

  def extract_auction_id(querystring: Map[String, String]): Option[String] = {
    val network_id = _int(querystring.getOrElse("mp", MAGNETIC_NETWORK_ID.toString))

    if (network_id == MAGNETIC_NETWORK_ID)
      Some(querystring("id"))
    else
      VALID_AUCTION_ID_PATTERN.findFirstIn(querystring.getOrElse("id", ""))
  }

  def should_skip_impression(fields: Map[String, String], querystring: Map[String, String]): Boolean = {
    false //TODO implement
  }

  def extract_impression_auction_id(line: String): Option[String] = {
    val fields = parse_fields(line)
    val (event_type, timestamp, cookie, querystring, pixel_id) = parse_common(fields)
    event_type match {
      case IMPRESSION =>
        if (should_skip_impression(fields, querystring))
          None
        else
          extract_auction_id(querystring)
      case _ =>
        None
    }
  }

  def extract_bid_auction_id(line: String): Option[String] = {
    try {
      val auction_id = line.split("\t", 10)(8)
      Some(auction_id)
    }
    catch {
      case t: Throwable => None
    }
  }
}
