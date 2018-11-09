
import java.text.SimpleDateFormat
import org.apache.spark.sql.types.{StringType, DateType, FloatType, IntegerType}

val rddRaw = sc.textFile("/user/spark/dw/2017.csv") 
val rddSplit = rddRaw.map(_.split(','))
def build_row_weather(doc : Array[String]): Row = {
    var station_identifier: String = doc(0)
    var observation_type : String = doc(2)
    var observation_value: String = doc(3)
  
    var precipitation: Float = 0f
    var max_temparature: Float = 0f
    var min_temparature: Float = 0f
    var snowfall: Int = 0
    var snowDepth: Int = 0
    var evaporation: Float = 0
    var water_equivalent_snow_depth: Float = 0f
    var water_equivalent_snow_fall: Float = 0f
    var sunshine: Float = 0f
    
    // transformation   
    val date_format = new SimpleDateFormat("yyyyMMdd")
    val java_date = date_format.parse(doc(1))
    val observation_date = new java.sql.Date(java_date.getTime());
  
    val value: Int = observation_value.toInt

    observation_type match {
        case "PRCP" => precipitation=value/10
        case "TMAX" => max_temparature=value/10
        case "TMIN" => min_temparature=value/10
        case "SNOW" => snowfall=value
        case "SNWD" => snowDepth=value
        case "EVAP" => evaporation=value/10
        case "WESD" => water_equivalent_snow_depth=value/10
        case "WESF" => water_equivalent_snow_fall=value/10
        case "PSUN" => sunshine=value/10
    }
  
    return Row(
        station_identifier,
        observation_date,
        precipitation,
        max_temparature,
        min_temparature,
        snowfall,
        snowDepth,
        evaporation,
        water_equivalent_snow_depth,
        water_equivalent_snow_fall,
        sunshine
    )
}

val schema_weather: StructType = StructType(Seq(
            StructField(name = "StationIdentifier", dataType = StringType, nullable = false),
            StructField(name = "ObservationDate", dataType = DateType, nullable = false),
            StructField(name = "Precipitation", dataType = FloatType, nullable = false),
            StructField(name = "MaxTemparature", dataType = FloatType, nullable = false),
            StructField(name = "MinTemparature", dataType = FloatType, nullable = false),
            StructField(name = "Snowfall", dataType = IntegerType, nullable = false),
            StructField(name = "SnowDepth", dataType = IntegerType, nullable = false),
            StructField(name = "Evaporation", dataType = FloatType, nullable = false),
            StructField(name = "WaterEquivalentSnowDepth", dataType = FloatType, nullable = false),
            StructField(name = "WaterEquivalentSnowFall", dataType = FloatType, nullable = false),
            StructField(name = "Sunshine", dataType = FloatType, nullable = false)
         )
        )

val row_rdd_weather = rddSplit.map(build_row_weather)
val dataframe_weather = spark.createDataFrame(row_rdd_weather, schema_weather)

dataframe_weather.createOrReplaceTempView("WeatherCurated")
