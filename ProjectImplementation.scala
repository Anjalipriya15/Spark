package FinalProject

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}





case class mobileData(user_id:String,song_id:String,artist_id:String,recordtime:Long,start_ts:Long,
                      end_ts:Long,geo_cd:String,station_id:String,song_end_type:Int,likes:Int,dislike:Int)

object ProjectImplementation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("Project Implementation")
      .config("spark.some.config.option", "some-value").getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("WARN")
    println("Begining of the Project")

    //Station lookup data//////////////////////////////////////////////////////////////////////////////////////////////////////////////

    val station_geo_map = spark.read.option("inferSchema", true)
      .csv("F:\\Acadgild Big Data\\music\\Final Project\\lookupfiles\\stn-geocd.txt")

    val station_data = station_geo_map.withColumnRenamed("_c0", "StationId").withColumnRenamed("_c1", "geo_cd")

    station_data.createOrReplaceTempView("Station_geo_map")

    println("station look up table created")

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    ///Song artist lookup data///////////////////////////////////////////////////////////////////////////////////////////////////
    val myschema = StructType(Array(
      StructField("song_id", StringType),
      StructField("artist_id", StringType)
    ))

    val song_artist_map = spark.read.format("csv").schema(myschema)
      .load("F:\\Acadgild Big Data\\music\\Final Project\\lookupfiles\\song-artist.txt")

    song_artist_map.createOrReplaceTempView("Song_artist_map")

    println("song artist look up table created")
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /////////Subscribed user lookup table/////////////////////////////////////////////////////////////////////////////////////////


    val myschemasubs = StructType(Array(
      StructField("user_id", StringType),
      StructField("subscription_start_date", LongType),
      StructField("subscription_end_date", LongType)
    ))

    val sub_user = spark.read.format("csv").schema(myschemasubs)
      .load("F:\\Acadgild Big Data\\music\\Final Project\\lookupfiles\\user-subscn.txt")

    val subscribed_user = sub_user.select(col("user_id"), from_unixtime(col("subscription_start_date"), "yyyy-MM-dd hh:mm:ss").alias("sub_start_date"),
      from_unixtime(col("subscription_end_date"), "yyyy-MM-dd hh:mm:ss").alias("sub_end_date"))

    subscribed_user.createOrReplaceTempView("Subscribed_users")

    println("Subscribed user lookup table created")
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //user-artist map/////////////////////////////////////////////////////////////////////////////////////////////////////////////


    val myschemauser = StructType(Array(
      StructField("user_id", StringType),
      StructField("artist_id", StringType)
    ))


    val user_artist = spark.read.format("csv").schema(myschemauser)
      .load("F:\\Acadgild Big Data\\music\\Final Project\\lookupfiles\\user-artist.txt")

    val user_artist_map = user_artist.select(col("user_id"), split(col("artist_id"), "&").getItem(0).alias("aid1"),
      split(col("artist_id"), "&").getItem(1).alias("aid2"),
      split(col("artist_id"), "&").getItem(2).alias("aid3"))

   user_artist_map.show()




    user_artist_map.createOrReplaceTempView("User_Artist_map")

    println("User Artist Map lookup table created")

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    ///loading the log files coming from mobile application/////////////////////////////////////////////////////////////////////////

    import spark.implicits._

    val mob_data = sc.textFile("F:\\Acadgild Big Data\\music\\Final Project\\data\\mob\\file.txt")
      .map(x => x.split(","))
      .map(x => mobileData(x(0), x(1), x(2), x(3).toLong, x(4).toLong, x(5).toLong, x(6), x(7), x(8).toInt, x(9).toInt, x(10).toInt)).toDF()

    val mob_data1 = mob_data.select(col("user_id"), col("song_id"), col("artist_id"), from_unixtime(col("recordtime"), "yyyy-MM-dd hh:mm:ss").alias("recordtime"),
      from_unixtime(col("start_ts"), "yyyy-MM-dd hh:mm:ss").alias("start_ts"), from_unixtime(col("end_ts"), "yyyy-MM-dd hh:mm:ss").alias("end_ts"),
      col("geo_cd"), col("station_id"), col("song_end_type"), col("likes"), col("dislike"))

    mob_data1.createOrReplaceTempView("mobile_data")
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    val web_data = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "record")
      .load("F:\\Acadgild Big Data\\music\\Final Project\\data\\web\\file.xml")

    web_data.createOrReplaceTempView("web_data")
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    ///mobile and web combined data

    val combined_data = mob_data1.select(col("user_id"), col("song_id"), col("artist_id"), col("recordtime"),
      col("start_ts"), col("end_ts"),
      col("geo_cd"), col("station_id"), col("song_end_type"), col("likes"), col("dislike")).union(
      web_data.select(col("user_id"), col("song_id"), col("artist_id"), col("timestamp"),
        col("start_ts"), col("end_ts"),
        col("geo_cd"), col("station_id"), col("song_end_type"), col("like"), col("dislike"))
    )

    combined_data.createOrReplaceTempView("combined_data")
    /////////////Data Enrichment/////////////////////////////////////////////////////////////////////////////////////////////////

    val station=station_data.withColumnRenamed("geo_cd","Geo")
    val joinExpression=combined_data.col("station_id")===station.col("StationId")
    val station_lookup=combined_data.join(station,joinExpression,joinType = "left_outer")
    station_lookup.createOrReplaceTempView("station_lookup")

    val combined_data_enrich1=station_lookup.withColumn("Geos",when(isnull(col("geo_cd")).or(col("geo_cd")===""),col("Geo")).otherwise(col("geo_cd")))
      .select(col("user_id"),col("song_id"),col("artist_id"),col("recordtime"),col("start_ts"),
        col("end_ts"),col("Geos").alias("geo_cd"),col("station_id"),col("song_end_type"),
        col("likes"),col("dislike"))




    val artist=song_artist_map.withColumnRenamed("artist_id","ArtistId").withColumnRenamed("song_id","SongID")
    // artist.show
    val joinExpression1=combined_data_enrich1.col("song_id")===artist.col("SongID")
    val artist_lookup=combined_data_enrich1.join(artist,joinExpression1,joinType = "left_outer")
    val combined_data_enrich2=artist_lookup.withColumn("artists",when(isnull(col("artist_id")).or(col("artist_id")===""),col("ArtistId")).otherwise(col("artist_id")))
      .select(col("user_id"),col("song_id"),col("artists").alias("artist_id"),col("recordtime"),col("start_ts"),
        col("end_ts"),col("geo_cd"),col("station_id"),col("song_end_type"),
        col("likes"),col("dislike"))


    //intitial validation of data///////////////////////////////////////////////////////////////////////////////////////////////////////
val combined=combined_data_enrich2.na.fill("3",Seq("song_end_type"))



    val li = col("likes") === 1
    val dis = col("dislike") === 1
//val expre=spark.sql("select * from combined_data where user_id='' or user_id is null")
    val expre=(col("user_id")==="").or(col("artist_id")==="").or(col("song_id")==="")
      .or(col("recordtime")==="").or(col("start_ts")==="").or(col("end_ts")==="")
      .or(col("geo_cd")==="")

    val expre1=isnull(col("user_id")).or(isnull(col("artist_id"))).or(isnull(col("song_id")))
      .or(isnull(col("recordtime"))).or(isnull(col("start_ts"))).or(isnull(col("end_ts")))
      .or(isnull(col("geo_cd"))).or(col("start_ts")>col("end_ts"))

   val filtered_data = combined.withColumn("status",expre.or(li.and(dis)).or(expre1) )

    val invalid_record=filtered_data.where("status=true")

    //invalid_record.show()
    val valid_record=filtered_data.where("status=false")

    val valid_record_enrich=valid_record.withColumn("liking",when(isnull(col("likes")).or(col("likes")===""),0).otherwise(col("likes")))
      .withColumn("disliking",when(isnull(col("dislike")).or(col("dislike")===""),0).otherwise(col("dislike")))
      .select(col("user_id"),col("song_id"),col("artist_id"),col("recordtime"),col("start_ts"),
        col("end_ts"),col("geo_cd"),col("station_id"),col("song_end_type"),
        col("liking").alias("likes"),col("disliking").alias("dislike"),col("status"))



    //valid_record_enrich.show()

    valid_record_enrich.createOrReplaceTempView("valid_record")
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



////////////////////Data Analysis//////////////////////////////////////////////////////////////////////////////////////////////////////


    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //valid_record_enrich.show(100)
    val subscription=subscribed_user.withColumnRenamed("user_id","UserId")
    val joinExpression2=valid_record_enrich.col("user_id")===subscription.col("UserId")
    val res1=valid_record_enrich.join(subscription,joinExpression2,joinType = "left_outer")
    val res2=res1.withColumn("sub_status",when(isnull(col("UserId")).or(col("sub_end_date")<col("start_ts")),"unsubscribed_user").otherwise("subscribed_user"))
        .withColumn("duration",datediff(col("end_ts"),col("start_ts")))
    val duration_of_song_byuser=res2.groupBy("sub_status").agg(sum("duration").alias("duration"))
    duration_of_song_byuser.show()

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val user_artist_lookup=user_artist_map.withColumnRenamed("user_id","UserId")
    val joinExpression3=valid_record_enrich.col("user_id")===user_artist_lookup.col("UserId")
    val res3=valid_record_enrich.join(user_artist_lookup,joinExpression3,joinType = "inner")
    res3.show()

    val res4=res3.withColumn("stat",when((col("artist_id")===col("aid1")).or(col("artist_id")===col("aid2"))
    .or(col("artist_id")===col("aid3")),"follow").otherwise("not follow"))
      .where("stat='follow'").groupBy(col("artist_id")).agg(countDistinct("user_id").alias("count"))
      .orderBy(col("count").desc)
    res4.show(10)



    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    valid_record_enrich.show()
    valid_record_enrich.where("likes=1 or song_end_type=0")
      .groupBy(col("song_id")).agg(count(col("song_id")).alias("counts"))
      .orderBy(col("counts").desc).show()
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


res2.where("sub_status='unsubscribed_user'").groupBy(col("user_id")).agg(sum(col("duration")).alias("duration"))
      .orderBy(col("duration").desc).show(10)

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    valid_record_enrich.where("likes=1").groupBy("station_id")
      .agg(count(col("song_id")))
      .show()



  }

}