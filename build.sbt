name := "ApacheSpark-Assignment2"

version := "1.0"

scalaVersion :=  "2.11.4"

libraryDependencies  ++= {
                          Seq(
			        "org.apache.spark" %% "spark-core" % "2.0.0",
                            "org.apache.spark" %% "spark-sql" % "2.0.0",
                            "com.databricks" %% "spark-csv" % "1.4.0"
                             )
                        }
fork := true
