package com.example

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @define Todo
 * @author ying.jiang
 * @date 2020-05-25-15:08:00
 */
object WordCountSparkSql {
  def main(args: Array[String]): Unit = {

    // 创建SparkSession对象，并设置属性
    val session = SparkSession.builder()
      .appName("SparkSqlWordCount")
      .master("local[*]")
      .getOrCreate()

    // 读取hdfs中文件
    val lines: Dataset[String] = session.read.textFile("hdfs://centos02:8020/input/words.txt")

    // 引入session隐式转换
    import session.implicits._

    // 将Dataset中数据按空格分割合并
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    // 修改默认列名
    var df: DataFrame = words.withColumnRenamed("value", "word")

    // 创建视图
    df.createTempView("v_words")

   // 执行sql
    val result = session.sql("select word, count(*) as count from v_words group by word order by count desc")

    // 打印查询结果
    result.show()

    // 关闭session
    session.close()
  }
}
