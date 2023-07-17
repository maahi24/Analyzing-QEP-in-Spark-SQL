//
// Project 2, starter code
//
// 1. Loads Cities and Countries as dataframes and creates views
//    so that we can issue SQL queries on them.
// 2. Runs 2 example queries, shows their results, and explains
//    their query plans.
//

// note that we use the default `spark` session provided by spark-shell

val cities = (spark.read
        .format("csv")
        .option("header", "true") // first line in file has headers
        .load("./Cities.csv"));
cities.createOrReplaceTempView("Cities")

val countries = (spark.read
        .format("csv")
        .option("header", "true")
        .load("./Countries.csv"));
countries.createOrReplaceTempView("Countries")

// look at the schemas for Cities and Countries
cities.printSchema()
countries.printSchema()

// Example 1
var df = spark.sql("SELECT city FROM Cities")
df.show()  // display the results of the SQL query
df.explain(true)  // explain the query plan in detail:
                  // parsed, analyzed, optimized, and physical plans

// Example 2
df = spark.sql("""
    SELECT *
    FROM Cities
    WHERE temp < 5 OR true
""")
df.show()
df.explain(true)

//Problem 1
df = spark.sql("""
     SELECT country, EU
     FROM Countries
     WHERE coastline = "yes"
""")
df.show()
df.explain(true)

//Problem 2
df = spark.sql("""
     SELECT city
     FROM (
        SELECT city, temp
        FROM Cities 
     )
     WHERE temp < 4
""")
df.show()
df.explain(true)


//Problem 3
df = spark.sql("""
     SELECT *
     FROM Cities, Countries
     WHERE Cities.country = Countries.country
         AND Cities.temp < 4
         AND Countries.pop > 6
""")
df.show()
df.explain(true)


//Problem 4
df = spark.sql("""
     SELECT city, pop 
     FROM Cities, Countries
     WHERE Cities.country = Countries.country
         AND Countries.pop > 6
""")
df.show()
df.explain(true)


//Problem 5
df = spark.sql("""
     SELECT *
     FROM Countries
     WHERE country LIKE "%e%d"
""")
df.show()
df.explain(true)

//Problem 6
df = spark.sql("""
     SELECT *
     FROM Countries
     WHERE country LIKE "%ia"
""")
df.show()
df.explain(true)

//Problem 7
df = spark.sql("""
     SELECT t1 + 1 as t2
     FROM (
        SELECT cast(temp as int) + 1 as t1
        FROM Cities
     )
""")
df.show()
df.explain(true)


//Problem 8
df = spark.sql("""
     SELECT t1 + 1 as t2
     FROM (
        SELECT temp + 1 as t1
        FROM Cities
     )
""")
df.show()
df.explain(true)

//Problem 9
df = spark.sql("""
     SSELECT A.country, B.pop, C.city, C.temp
FROM Countries as A, Countries as B
WHERE A.country = B.country
Cities as C
ON
C.country = A.country
AND
C.country = B.country
""")
df.show()
df.explain(true)

//Problem 10
df = spark.sql("""
     SELECT Cities.country, MIN(Cities.temp) 
     FROM Cities, Countries
     WHERE Cities.country = Countries.country
""")
df.show()
df.explain(true)





