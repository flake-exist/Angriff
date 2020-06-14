object CONSTANTS {
  //CONSTANT
  val DATE_PATTERN = "[12]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01])".r // Pattern for RegEx . Check correct string `Date` or not
  val DATE_UNIX_TIME_STAMP = new java.text.SimpleDateFormat("yyyy-MM-dd") // Pattern to convert date(String) into Unix Time Stamp
  val TRANSIT_ACTION       : Long   = -1 //Indicate the action was not a conversion
  val GLUE_SYMBOL          : String = "::_" //Use it to concatenate `channel` columns with `conversion` columns
  val CONVERSION_SYMBOL    : String = "get@conv"
  val NO_CONVERSION_SYMBOL : String = "get@no_conv"
  val GLUE_SYMBOL_POS      : String = GLUE_SYMBOL + CONVERSION_SYMBOL //symbol denotes the contact with channel ended up with conversion
  val GLUE_SYMBOL_NEG      : String = GLUE_SYMBOL + NO_CONVERSION_SYMBOL //symbol denotes the contact with channel ended up without conversion
  val usage = "Usage : [--projectID] ] [--date_start] [--date_finish] [--target_numbers] [--product_name] [--source_platform] [--flat_path] [--output_path]"
  val necessary_args = Array(
    "projectID",
    "date_start",
    "date_finish",
    "target_numbers",
    "product_name",
    "source_platform",
    "flat_path",
    "output_path")
  //CONSTANT

  //UDF
  def pathCreator(arr         : Seq[String],
                  mode        : String,
                  contact_pos : String = GLUE_SYMBOL_POS,
                  contact_neg : String = GLUE_SYMBOL_NEG,
                  transit     : String = "=>"
                 )            : Array[String] = {
    //Create user paths

    val touchpoints = arr.mkString(transit)
    val paths = touchpoints.split(contact_pos).map(_.trim)
    val target_paths = mode match {
      case "success" => paths.filterNot(_.endsWith(contact_neg)).map(_.stripPrefix(transit))
      case "fail"    => paths.filter(_.endsWith(contact_neg)).map(_.stripPrefix(transit))
    }
//    val success_paths = paths.filterNot(_.endsWith(contact_neg)).map(_.stripPrefix(transit))
    val chains = target_paths.map(_.replace(contact_neg,""))
    chains
  }

  def channel_creator(
                       src:String,
                       ga_sourcemedium : String,
                       utm_source      : String,
                       utm_medium      : String,
                       adr_typenum     : String,
                       adr_profile     : String) = {

    val channel = src match {
      case "adriver" if adr_typenum == "0" => "view" + ":" + utm_source + " / " + utm_medium + ":" + adr_profile.toString
      case "adriver" if adr_typenum != "0" => "click" + ":" + utm_source + " / " + utm_medium + ":" + adr_profile.toString
      case "seizmik"                       => "seizmik_channel" //ALLERT NEED TO EDIT IN FUTURE!!!
      case "ga" | "bq"                     => ga_sourcemedium
      case _                               => throw new Exception("Unknown data source")
    }
    channel

  }


  def DateStrToUnix(date:String):Long = {
    val date_correct = DATE_PATTERN.findFirstIn(date) match {
      case Some(s) => DATE_UNIX_TIME_STAMP.parse(s).getTime()
      case _       => throw new Exception("Incorrect Date Format.Use YYYY-MM_dd format")}
    date_correct
    }
  //function check value if it equals null or is empty
  def isEmpty(x:String) = x == "null" || x.isEmpty || x == null

  //Parse input arguments from command line.Convert position arguments to named arguments
  def argsPars(args:Array[String],usage:String): collection.mutable.Map[String,String] = {

    if (args.length == 0) {
      throw new Exception(s"Empty argument Array. $usage")
    }

    val (options,_) = args.partition(_.startsWith("-")) //Collect [--arg=value] from iargs
    val optionsMap  = collection.mutable.Map[String,String]()
    options.map{elem =>
      val pair_untrust  = elem.split("=")
      val pair_trust = pair_untrust match {
        case Array(_,_) => pair_untrust
        case _          => throw new Exception(s"Can not parse $pair_untrust. $usage")
      }
      val opt_val  = pair_trust(0).split("-{1,2}")(1) -> pair_trust(1)
      optionsMap += opt_val //Add Map(option -> value)
    }
    optionsMap
  }

  }

//Class for parsing input json file. Used with input one argument - JSON file
case class Jvalue(date_start      : String,
                  date_finish     : String,
                  product_name    : String,
                  projectID       : Long,
                  target_numbers  : Array[Long],
                  source_platform : Array[String],
                  flat_path       : String,
                  output_path     : String
                 )

//Class for collecting input args and its values from command line
case class ArgValue(date_start    : String,
                  date_finish     : String,
                  product_name    : String,
                  projectID       : Long,
                  target_numbers  : Array[Long],
                  source_platform : Array[String],
                  flat_path       : Array[String],
                  output_path     : String
                 )



