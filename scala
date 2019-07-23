var data = spark.read.format("csv").
    schema(customSchema).
    option("header", "true").
    load("/user/---.csv")
