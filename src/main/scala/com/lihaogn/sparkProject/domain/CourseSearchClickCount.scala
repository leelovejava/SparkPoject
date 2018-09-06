package com.lihaogn.sparkProject.domain

/**
  * 从搜索引擎过来的课程点击数实体类
  * @param day_search_course
  * @param click_count
  */
case class CourseSearchClickCount(day_search_course: String, click_count: Long)
