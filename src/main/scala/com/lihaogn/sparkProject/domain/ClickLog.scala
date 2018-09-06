package com.lihaogn.sparkProject.domain

/**
  * 清洗后的日志格式
  *
  * @param ip
  * @param time
  * @param courseId
  * @param statusCode 日志访问状态码
  * @param referer
  */
case class ClickLog(ip: String, time: String, courseId: Int, statusCode: Int, referer: String)
