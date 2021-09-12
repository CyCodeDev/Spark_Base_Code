/*
// return numnber of cores per worker:
java.lang.Runtime.getRuntime.availableProcessors
sc.defaultParallelism // # workers

import com.microsoft

*/


import org.apache.spark.sql.Dataframe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import java.util.Properties


import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.config._
import com.microsoft.azure.sqldb.spark.connect._
import com.microsoft.azure.sqldb.spark.connect.DataFrameFunctions

import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy
//import com.microsoft.azure.sqldb.spark.bulkcopy._

import com.microsoft.sqlserver.jdbc.SQLServerDriver

import com.microsoft.azure.sqldb.spark.query._
//import com.microsoft.azure.sqldb.spark._

import com.microsoft.sqlserver.jdbc.spark._


spark.conf.set("spark.sql.session.timeZone". "UTC")



// BREAK


val sp_scope: String = "set_azure_security_scope"
val tenant_id = dbutils.secrets.get(scope = sp_scope, "tenant_id")
val client_id = dbutils.secrets.get(scope = sp_scope, "client_id")
val client_secret = dbutils.secrets.get(scope = sp_scope, key = "client_secret")

spark.conf.set("fs.adl.oauth2.access.token.provider.type", "CLientCredentials")
spark.conf.set("fs.adl.oauth2.refresh.url", s"https://login.microsoftonline.com/${tenant_id}/oauth2/token")
spark.conf.set("fs.adl.oauth2.client.id", client_id)
spark.conf.set("fs.adl.oauth2.credential", client_secret)


// MOUNTING DATA FROM ADLS
val configs = Map(
	"fs.adl.oauth2.acccess.token.provider.type" -> "ClientCredential",
	"fs.adl.oauth2.client.id", -> client_id,
	"fs.adl.oauth2.credential" -> client_secret,
	"fs.adl.oauth2.refresh.url" -> s"https://login.microsoftonline.com/${tenant_id}/oauth2/token")

dbutils.fs.mount(
	source = "adl://adls-address.azuredatalakestore.net/directory/in/adls",
	mountPoint = "/mnt/adls/sandbox/directory/initials",
	extraConfigs = configs
	)

// Begin spark entry point/session

val spark_session = SparkSession
	.builder()
	.appName("My App")
	.master("local[*]")
	.config("spark.sql.join.preferSorgMergeJoin", "true")
	.getOrCreate()

// Parse function
val split_string = (input: String, delim: String, i: Int_) => scala.util.Try(input.split(s"[${delim}]")(i-1)).getOrCreate(null)
spark_session.udf.register("split_string", split_string)



// Read parquet from adls mount location
spark_session.read
	.format("parquet")
	.option("inferSchema", "true")
	.option("recursiveFileLookup", "false")
	.option("ignoreLeadingWhiteSpace", "true")
	.option("ignoreTrailingWhiteSpace", "true")
	.option("mergeSchema", "true")
	.load("dbfs:/mnt/adls/sandbox/directory/initials")
	.createOrReplaceTempView("sandbox_temp_view")

// WRITE TO ADLS

val table_view = spark.session.sql(""" select * from sandbox_temp_view """).repartitoin(20, col("partition_column")).persist()
table_view.write.format("parquet").mode("overwrite").save("adl://adlsdirectory.azuredatalakestore.net/directory/to/choose")



// CONNECT TO DB

// CONNECTIONS CREDENTIALS
val sqldw_scope: String = "scope_name"
val sqldw_user: String = dbutils.secrets.get(scope = sqldw_scope, key = "username")
val sqldw_pwd: String = dbutils.secrets.get(scope = sqldw_scope, key = "password")

// CONNECTION DETAILS
val sqldw_hostname: String = "host_name"
val sqldw_database: String = "db_name"
val sqldw_port: Int = 0000 // usually 1433
val jdbc_url: String = s"jdbc:sqlserver://${sqldw_hostname}.database.windows.net:${sqldw_port};database=${sqldw_database}"
val driver: String = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

def connectionOptions = Map(
	"user" -> sqldw_user,
	"password" -> sqldw_pwd,
	"driver" -> driver,
	"query" -> "select * some_table"
	)

