//Validation of case 1
val dfcase1 = spark.read.option("multiline","true")
      .json("/FileStore/citi_usecase_provider_report/total_mem_months/")
dfcase1.where("member_id=57").show()

//Validation of case 1
val dfcase2 = spark.read
      .json("/FileStore/citi_usecase_provider_report/total_months_per_year/")
dfcase2.select("*").where("member_id=57").show()
