import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

import CONSTANTS._

object Angriff {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    import spark.implicits._

    //-----INPUT ARGUMENTS-----
    val date_start:String             ="2019-10-01"
    val date_finish:String            = "2019-11-01"
    val product_name                  = "null"
    val projectID:Long                = 2
    val target_numbers:Array[Long]    = Array(5)
    val source_platform:Array[String] = Array("adriver","bq")
    val flat_path:Array[String]       = Array("my_path")
    val output_path:String            = "/home/eva91/Documents/OMD/past_result"
    //-----INPUT ARGUMENTS-----

    //-----REGISTER-----

    case class CustomMap(clutch:String,hts:Long,conversion:String)

    def new_wave(arr:Seq[Map[String,Long]],date_start:Long,no_conversion:String):Seq[String] = {
      val arr_reverse = arr.reverse
      val (before,after) = arr_reverse.partition(elem =>elem(elem.keySet.head) < date_start)
      val before_sort = before.takeWhile(elem => elem.keySet.head.endsWith("_0"))
      val r = before_sort.reverse  ++ after.reverse
      r.map(_.keys.head)

    }
//
    val new_wave_udf = spark.udf.register("new_wave",new_wave(_:Seq[Map[String,Long]],_:Long,_:String):Seq[String])
    val path_creator_udf = spark.udf.register("path_creator",pathCreator(_:Seq[String],_:String):Array[String])
    //-----REGISTER-----


    val arg_value = ArgValue(
      date_start,
      date_finish,
      product_name,
      projectID,
      target_numbers,
      source_platform,
      flat_path,
      output_path
    )

    // Seq with date_start(Unix Hit) & date_finish(Unix)
    val date_range:Vector[Long] = Vector(
      arg_value.date_start,
      arg_value.date_finish).map(DateStrToUnix)

    //Check `date_finish` is greater than `date_start`
    val date_pure = date_range match {
      case Vector(a,b) if a < b  => date_range
      case Vector(a,b) if a >= b => throw new Exception("`date_start` is greater than `date_finish`")
      case _                     => throw new Exception("`date_range` must have type Vector[Long]")
    }

    val dd = date_range(0)

    val target_goal = arg_value.target_numbers :+ TRANSIT_ACTION //`TRANSIT_ACTION ` defined in `CONSTANTS` module

    val channel_creator_udf = spark.udf.register("channel_creator",channel_creator(_:String,_:String,_:String,_:String,_:String,_:String):String)


    val data_past = spark.read.format("parquet").option("mergeSchema","true").option("inferSchema","false").
      load("/home/eva91/Documents/OMD/Attribution_Window/*.parquet.gzip")

    val data_past_work = data_past.select(
      $"ProjectID".cast(sql.types.LongType),
      $"ClientID".cast(sql.types.StringType),
      $"HitTimeStamp".cast(sql.types.LongType),
      $"ga_sourcemedium".cast(sql.types.StringType),
      $"utm_source".cast(sql.types.StringType),
      $"utm_medium".cast(sql.types.StringType),
      $"adr_typenum".cast(sql.types.StringType),
      $"adr_profile".cast(sql.types.StringType),
      $"ga_location".cast(sql.types.StringType),
      $"goal".cast(sql.types.LongType),
      $"src".cast(sql.types.StringType)
    )

    val data_october = spark.read.format("parquet").option("inferSchema","false").
      load("/home/eva91/Documents/OMD/test_2019_10.parquet.gzip")

    val data_october_work = data_october.select(
      $"ProjectID".cast(sql.types.LongType),
      $"ClientID".cast(sql.types.StringType),
      $"HitTimeStamp".cast(sql.types.LongType),
      $"ga_sourcemedium".cast(sql.types.StringType),
      $"utm_source".cast(sql.types.StringType),
      $"utm_medium".cast(sql.types.StringType),
      $"adr_typenum".cast(sql.types.StringType),
      $"adr_profile".cast(sql.types.StringType),
      $"ga_location".cast(sql.types.StringType),
      $"goal".cast(sql.types.LongType),
      $"src".cast(sql.types.StringType)
    )

    val data_all = data_october_work.union(data_past_work)

    data_all.printSchema()


    val data_cache = data_all.cache()

    //Unique ClientID in October - 59173
    val octoberID = data_cache.
      filter($"HitTimeStamp" >= date_pure(0) && $"HitTimeStamp" < date_pure(1)).
      filter($"ProjectID"    === arg_value.projectID).
      filter($"goal".isin(target_numbers:_*)).
      filter($"src".isin(arg_value.source_platform:_*)).
      select($"ClientID").distinct.collect().map(_(0).toString).toArray

    println(octoberID.length)

    //DataFrame wih octoberID records (all periods)
    val data_interest = data_cache.
      filter($"ProjectID"  === arg_value.projectID).
      filter($"ClientID".isin(octoberID:_*)).
      filter($"src".isin(arg_value.source_platform:_*))


    val data_custom = data_interest.withColumn("channel", channel_creator_udf(
      $"src",
      $"ga_sourcemedium",
      $"utm_source",
      $"utm_medium",
      $"adr_typenum",
      $"adr_profile")).select(
      $"ClientID",
      $"HitTimeStamp",
      $"goal",
      $"channel"
    )

    val data_preprocess_0 = data_custom.
      withColumn("goal",when($"goal".isNull,TRANSIT_ACTION).otherwise($"goal")).
      filter($"goal".isin(target_goal:_*)).
      sort($"ClientID", $"HitTimeStamp".asc)

    val data_preprocess_1 = data_preprocess_0.
      withColumn("conversion",when($"goal" === TRANSIT_ACTION,NO_CONVERSION_SYMBOL).otherwise(CONVERSION_SYMBOL)).select(
      $"ClientID",
      $"channel",
      $"conversion",
      $"HitTimeStamp"
    )

    val data_clutch = data_preprocess_1.withColumn("clutch_new",concat($"channel",lit(GLUE_SYMBOL),$"conversion"))

    val data_seq = data_clutch.withColumn("my_class",map($"clutch_new",$"HitTimeStamp"))

    val data_group = data_seq.groupBy($"ClientID").agg(collect_list($"my_class").as("my_seq"))

    data_group.printSchema()

    val data_test = data_group.select($"ClientID",new_wave_udf($"my_seq",lit(dd),lit(NO_CONVERSION_SYMBOL)).as("clutch_arr"))///!!!!!

    val data_assembly_cache = data_test.cache()

    //Create user successful paths(chains)
    val data_path_success = data_assembly_cache.select(
      $"ClientID",
      path_creator_udf($"clutch_arr",lit("success")).as("paths")
    ).withColumn("status",lit(true))

    //Create user failed paths(chains)
    val data_path_fail = data_assembly_cache.select(
      $"ClientID",
      path_creator_udf($"clutch_arr",lit("fail")).as("paths")
    ).withColumn("status",lit(false))

    //Union all successfull and failed paths
    val data_path = data_path_success.union(data_path_fail)

    //Cache DataFrame. Cause we use it DataFrame twice
    val data_path_explode = data_path.select($"status",explode($"paths").as("paths")).cache()

    val total_conversion = data_path_explode.filter($"status" === true).count()

    //`status` column has Boolean type and consists of true and false values. We did pivot method to get `paths`|`true`|`false` columns
    val result = data_path_explode.
      groupBy($"paths").
      pivot($"status").
      agg(count($"status"))

    val result_sorted = try {
      result.sort($"true".desc)
    } catch {
      case impossible_to_sort : UnsupportedOperationException => result.withColumn("true",lit(null))
      case _                  : Throwable                     => result.withColumn("true",lit(null))
    }

    //Add `share` column . `share` column signs chain share(contribution) in  total conversions
    val result_withShare = result_sorted.withColumn("share", $"true" / lit(total_conversion))

    val output_data = result_withShare.select(
      $"paths",
      $"true",
      $"false",
      $"share"
    )

    output_data.coalesce(1).
      write.format("csv").
      option("header","true").
      mode("overwrite").
      save(output_path)

  }

}
