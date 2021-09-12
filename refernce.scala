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

