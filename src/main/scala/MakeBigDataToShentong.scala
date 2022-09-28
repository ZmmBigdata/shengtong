import com.github.javafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.LocalDate
import java.time.temporal.ChronoUnit.DAYS
import java.util.{Locale, Properties}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


//神通数据表初始化数据插入，大批量数据插入(不建议直接1个亿，1千万可以)
object MakeBigDataToShentong {
  private val logger: Logger = Logger.getLogger(MakeBigDataToShentong.getClass)
  private val max_records = 1000000 //一次制造的数据条数
  private val from = LocalDate.of(2001, 1, 1)
  private val to = LocalDate.of(2002, 1, 1)

  def random(from: LocalDate, to: LocalDate): LocalDate = {
    val diff = DAYS.between(from, to)
    val random = new Random(System.nanoTime) // 随时结果
    from.plusDays(random.nextInt(diff.toInt))
  }

  // define a method to make data
  def Creater(): ArrayBuffer[String] = {
    val faker = new Faker(Locale.CHINA)
    val arrayResult = new ArrayBuffer[String]()
    for (i <- 1 to max_records) {
      var logintime = random(from, to)
      val str = new String(faker.number().randomNumber() + "\u0001" + faker.name().username() + "\u0001" + faker.number().numberBetween(0, 90) + "\u0001" +
        faker.phoneNumber().phoneNumber() + "\u0001" + faker.number().randomDigit() + "\u0001" + logintime + "\u0001" + faker.address.cityName() + "\u0001" +
        faker.book.author + "\u0001" + faker.book.genre + "\u0001" + faker.book.publisher + "\u0001" + faker.book.title + "\u0001" +
        faker.beer.name + "\u0001" + faker.beer.hop + "\u0001" + faker.beer.style + "\u0001" + faker.animal.name + "\u0001" +
        faker.ancient.god + "\u0001" + faker.ancient.hero + "\u0001" + faker.ancient.primordial + "\u0001" + faker.ancient.titan +
        "\u0001" + faker.app.name + "\u0001" + faker.app.author + "\u0001" + faker.app.version)
      arrayResult += str
    }
    arrayResult
  }

  // send data to database
  def SendData(sparkSession: SparkSession, sparkContext: SparkContext, arrayBuffer: ArrayBuffer[String], viewName: String, properties: Properties): Unit = {
    //通过sparkContext 将数组集合转换为RDD
    val rdd = sparkContext.parallelize(arrayBuffer)
    val arrRDD: RDD[Array[String]] = rdd.map(x => x.split("\u0001"))

    //通过RDD，配合样例类，将我们的数据转换成样例类对象
    val personRDD: RDD[Person] = arrRDD.map(x => Person(x(0), x(1), x(2).toInt, x(3), x(4).toInt, x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21)))

    //导入sparkSession当中的隐式转换，将我们的样例类对象转换成DataFrame
    import sparkSession.implicits._
    val personDF: DataFrame = personRDD.toDF()

    //将DataFrame注册成为一张表模型
    personDF.createTempView(s"${viewName}")

    //获取表当中的数据
    val result: DataFrame = sparkSession.sql(s"select * from ${viewName}")
    sparkSession.sql(s"select count(1) as person_view_count from ${viewName}").show()
    logger.error("临时视图表：" + viewName + "已创建完成！")

    val url = "jdbc:oscar://192.168.0.87:2003/osrdb?useUnicode=true&amp;characterEncoding=utf-8;useOldAliasMetadataBehavior=true"
    val tableName = "TEST.shentong_person_01" //神通数据库 空间.表名
    val startTime: Long = System.currentTimeMillis()
    result.write.mode(SaveMode.Append).jdbc(url, tableName, properties)
    val endTime: Long = System.currentTimeMillis()
    logger.error(viewName + " 临时表数据插入成功,插入总耗时(单位:ms)：" + (endTime - startTime))

  }

  def main(args: Array[String]): Unit = {
    //    获取sparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("spark_Mysql").master("local[*]").getOrCreate()
    //    通过sparkSession得到sparkContext
    val sparkContext: SparkContext = sparkSession.sparkContext

    val properties = new Properties()
    properties.setProperty("driver", "com.oscar.Driver")
    properties.setProperty("user", "sysdba") //神通数据库用户名
    properties.setProperty("password", "szoscar55") //神通数据库密码

    val arrayBufferResult: ArrayBuffer[String] = Creater()
    SendData(sparkSession, sparkContext, arrayBufferResult, "baowu_person_bigdata", properties)
    logger.error("一共制造数据条数：" + (max_records))
    sparkContext.stop()
    sparkSession.close()
  }
}
