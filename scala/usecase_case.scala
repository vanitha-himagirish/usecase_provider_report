// Import 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

// Create SparkSession
val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Providers_Report")
      .getOrCreate()

// Reading Member Eligibility dataset
val dfMemEligibility = spark.read.format("csv")
.option("Header", "true") 
.option("inferSchema", "true") 
.load("dbfs:/FileStore/citi_data/member_eligibility.csv")
/***********************************************************/

// Reading Member Months dataset 
val dfMemMonths = spark.read.format("csv")
.option("Header", "true") 
.option("inferSchema", "true") 
.load("dbfs:/FileStore/citi_data/member_months.csv")

val dfMem =dfMemMonths.withColumnRenamed("member_id","memmonths_member_id")
dfMem.printSchema
/************************************************************************/

/********Case 1 ***********************************************************************************************************************************************/
////Given the two data datasets, calculate the total number of members months. The resulting set should contain the member's ID, full name, along with the number of member months.
// Output the report in json, partitioned by the membersID.
val dfMemEligibleMonths=dfMemEligibility.join(dfMem,dfMemEligibility("member_id") === dfMem("memmonths_member_id"),"left")
.withColumn("member_full_name", concat(col("first_name"), lit(" "), col("middle_name"), lit(" "),col("last_name")))
.select("member_id","member_full_name","eligibility_member_month")

val dfTotalMemMonths=dfMemEligibleMonths.groupBy("member_id","member_full_name").agg(count("eligibility_member_month").alias("total_mem_months")).select("member_id","member_full_name","total_mem_months").orderBy("member_id")

//Save to JSON File
dfTotalMemMonths.write.mode("overwrite")
  .partitionBy("member_id")
  .json("/FileStore/citi_usecase_provider_report/total_mem_months/")
/********End of Case1 *****************************************************************************************************************************/



/********Case 2***********************************************************************************************************************************************/
val dfMonthsPerYear=dfMemMonths
.withColumn("effective_member_year",year($"eligiblity_effective_date"))
.withColumn("member_month",month($"eligiblity_effective_date"))
.select("member_id","effective_member_year","eligibility_member_month")

//Aggregate Functions
val windowSpecAgg  = Window.partitionBy("member_id","effective_member_year").orderBy("member_id")

val dfTotalMonthsPerYear=dfMonthsPerYear
.withColumn("total_mem_months_per_year",count("eligibility_member_month").over(windowSpecAgg))

val finaldf = dfTotalMonthsPerYear.select("member_id","eligibility_member_month","total_mem_months_per_year")

//Save to JSON File
finaldf.write.mode("overwrite")
  .json("/FileStore/citi_usecase_provider_report/total_months_per_year/")
/********End of Case2 *****************************************************************************************************************************/

