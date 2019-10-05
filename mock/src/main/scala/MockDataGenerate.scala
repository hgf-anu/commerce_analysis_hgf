/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 */

import java.util.UUID

import commons.model.{ProductInfo, UserInfo, UserVisitAction}
import commons.utils.{DateUtils, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


/**
 * 模拟的数据
 * date：是当前日期
 * age: 0 - 59
 * professionals: professional[0 - 59]
 * cities: 0 - 9
 * sex: 0 - 1
 * keywords: ("火锅", "蛋糕", "重庆辣子鸡", "重庆小面", "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
 * categoryIds: 0 - 99
 * ProductId: 0 - 99
 */
object MockDataGenerate{

	/**
	 * 模拟用户行为信息
	 *
	 * @return
	 */
	private def mockUserVisitActionData():Array[UserVisitAction] ={

		val searchKeywords = Array( "华为手机", "联想笔记本", "小龙虾", "卫生纸", "吸尘器", "Lamer", "机器学习", "苹果", "洗面奶", "保温杯" )
		// yyyy-MM-dd
		val date:String = DateUtils.getTodayDate()
		// 关注四个行为：搜索、点击、下单、支付
		val actions = Array( "search", "click", "order", "pay" )
		val random = new Random()
		val rows:ArrayBuffer[UserVisitAction] = ArrayBuffer[UserVisitAction]()

		// 一共100个用户（有重复）
		for( i <- 0 to 100 ) {
			val userid:Int = random.nextInt( 100 )
			// 每个用户产生10个session
			for( j <- 0 to 10 ) {
				// 不可变的，全局的，独一无二的128bit长度的标识符，用于标识一个session，体现一次会话产生的sessionId是独一无二的
				val sessionid:String = UUID.randomUUID().toString().replace( "-", "" )
				// 在yyyy-MM-dd后面添加一个随机的小时时间（0-23）
				val baseActionTime:String = date + " " + random.nextInt( 23 )
				// 每个(userid + sessionid)生成0-100条用户访问数据
				for( k <- 0 to random.nextInt( 100 ) ) {
					val pageid:Int = random.nextInt( 10 )
					// 在yyyy-MM-dd HH后面添加一个随机的分钟时间和秒时间
					val actionTime:String = baseActionTime + ":" + StringUtils.fulfuill( String.valueOf( random.nextInt(
						59 ) ) ) + ":" + StringUtils.fulfuill( String.valueOf( random.nextInt( 59 ) ) )
					var searchKeyword:String = null
					var clickCategoryId:Long = -1L
					var clickProductId:Long = -1L
					var orderCategoryIds:String = null
					var orderProductIds:String = null
					var payCategoryIds:String = null
					var payProductIds:String = null
					val cityid:Long = random.nextInt( 10 ).toLong
					// 随机确定用户在当前session中的行为
					val action:String = actions( random.nextInt( 4 ) )

					// 根据随机产生的用户行为action决定对应字段的值
					action match {
						case "search" => searchKeyword = searchKeywords( random.nextInt( 10 ) )
						case "click" => clickCategoryId = random.nextInt( 100 ).toLong
							clickProductId = String.valueOf( random.nextInt( 100 ) ).toLong
						case "order" => orderCategoryIds = random.nextInt( 100 ).toString
							orderProductIds = random.nextInt( 100 ).toString
						case "pay" => payCategoryIds = random.nextInt( 100 ).toString
							payProductIds = random.nextInt( 100 ).toString
					}

					rows += UserVisitAction( date,
					                         userid,
					                         sessionid,
					                         pageid,
					                         actionTime,
					                         searchKeyword,
					                         clickCategoryId,
					                         clickProductId,
					                         orderCategoryIds,
					                         orderProductIds,
					                         payCategoryIds,
					                         payProductIds,
					                         cityid )
				}
			}
		}
		rows.toArray
	}

	/**
	 * 模拟用户信息表
	 *
	 * @return
	 */
	private def mockUserInfo():Array[UserInfo] ={

		val rows:ArrayBuffer[UserInfo] = ArrayBuffer[UserInfo]()
		val sexes = Array( "male", "female" )
		val random = new Random()

		// 随机产生100个用户的个人信息
		for( i <- 0 to 100 ) {
			val userid:Int = i
			val username:String = "user" + i
			val name:String = "name" + i
			val age:Int = random.nextInt( 60 )
			val professional:String = "professional" + random.nextInt( 100 )
			val city:String = "city" + random.nextInt( 100 )
			val sex:String = sexes( random.nextInt( 2 ) )
			rows += UserInfo( userid, username, name, age, professional, city, sex )
		}
		rows.toArray
	}

	/**
	 * 模拟产品数据表
	 *
	 * @return
	 */
	private def mockProductInfo():Array[ProductInfo] ={

		val rows:ArrayBuffer[ProductInfo] = ArrayBuffer[ProductInfo]()
		val random = new Random()
		val productStatus = Array( 0, 1 )

		// 随机产生100个产品信息
		for( i <- 0 to 100 ) {
			val productId:Int = i
			val productName:String = "product" + i
			val extendInfo:String = "{\"product_status\": " + productStatus( random.nextInt( 2 ) ) + "}"

			rows += ProductInfo( productId, productName, extendInfo )
		}

		rows.toArray
	}

	/**
	 * 将DataFrame插入到Hive表中
	 *
	 * @param spark     SparkSQL客户端
	 * @param tableName 表名
	 * @param dataDF    DataFrame
	 */
	private def insertHive(spark:SparkSession, tableName:String, dataDF:DataFrame):Unit ={
		spark.sql( "DROP TABLE IF EXISTS " + tableName )
		dataDF.write.saveAsTable( tableName )
	}

	val USER_VISIT_ACTION_TABLE = "user_visit_action"
	val USER_INFO_TABLE = "user_info"
	val PRODUCT_INFO_TABLE = "product_info"

	/**
	 * 主入口方法
	 *
	 * @param args 启动参数
	 */
	def main(args:Array[String]):Unit ={

		// 创建Spark配置
		val sparkConf:SparkConf = new SparkConf().setAppName( "MockData" ).setMaster( "local[*]" )

		// 创建Spark SQL 客户端
		val spark:SparkSession = SparkSession.builder().config( sparkConf ).enableHiveSupport().getOrCreate()

		// 模拟数据,用户动作表userVisitActionData
		val userVisitActionData:Array[UserVisitAction] = this.mockUserVisitActionData()
		val userInfoData:Array[UserInfo] = this.mockUserInfo()
		val productInfoData:Array[ProductInfo] = this.mockProductInfo()

		// 将模拟数据装换为RDD
		val userVisitActionRdd:RDD[UserVisitAction] = spark.sparkContext.makeRDD( userVisitActionData )
		val userInfoRdd:RDD[UserInfo] = spark.sparkContext.makeRDD( userInfoData )
		val productInfoRdd:RDD[ProductInfo] = spark.sparkContext.makeRDD( productInfoData )

		// 加载SparkSQL的隐式转换支持
		import spark.implicits._

		// 将用户访问数据装换为DF保存到Hive表中
		val userVisitActionDF:DataFrame = userVisitActionRdd.toDF()
		insertHive( spark, USER_VISIT_ACTION_TABLE, userVisitActionDF )

		// 将用户信息数据转换为DF保存到Hive表中
		val userInfoDF:DataFrame = userInfoRdd.toDF()
		insertHive( spark, USER_INFO_TABLE, userInfoDF )

		// 将产品信息数据转换为DF保存到Hive表中
		val productInfoDF:DataFrame = productInfoRdd.toDF()
		insertHive( spark, PRODUCT_INFO_TABLE, productInfoDF )

		spark.close
	}

}
