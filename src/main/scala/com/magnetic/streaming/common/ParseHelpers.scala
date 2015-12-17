package com.magnetic.streaming.common

import java.net.{URL, URLDecoder}
import java.text.SimpleDateFormat
import java.util.TimeZone

import scala.util.Try

object ParseHelpers {
  
  def _int(s: String): java.lang.Integer = {
    Try {s.toInt}.toOption match {
      case Some(i) => i
      case _ => null
    }
  }
  
  def split_one(s: String, delim: String = "="): (String, String) = {
    s.split(delim, 2).toList match {
      case k :: Nil => k -> ""
      case k :: v :: Nil => k -> v
      case _ => "" -> ""
    }
  }
  
  def parse_timestamp(raw_timestamp: String): Long = {
    Try {new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss Z]").parse(raw_timestamp)}.toOption match {
      case Some(t) => t.getTime/1000
      case _ => 0
    }
  }
  
  def parse_qs(q:String): Map[String,String] = {
    URLDecoder.decode(q).split("&").flatMap(_.split(";")).map(_.trim).withFilter(_.length > 0).map(split_one(_)).toMap
  }
}