package com.lihaogn.sparkProject.domain

/**
  * 课程点击次数实体类
  *
  * @param day_course  对应HBase中的rowkey
  * @param click_count 访问次数
  */
case class CourseClickCount(day_course: String, click_count: Long)
