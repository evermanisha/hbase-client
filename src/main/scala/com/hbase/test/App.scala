package com.hbase.test

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.Level
import org.apache.log4j.Logger

import scala.util.Properties

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    val tableName=args(0) //OPSMETRICS_DENGG_FINANCE:cost_feed_control
    val columnFamily=args(1) //cost_feed_control_cf
    val rowKey=args(2) //enacto_fusion
    val listOfCols=args(3).split(",").toList //description
    val keytabActualLocation=args(4)
    val principle= args(5)
    val connect =getHbaseConfiguration(principle,keytabActualLocation)
    executeReadQuery(connect,tableName,columnFamily,rowKey,listOfCols)
  }


  private def getHbaseConfiguration(principle:String,keytabActualLocation:String): Connection = {
    val configuration = HBaseConfiguration.create()
    configuration.addResource("/etc/hadoop/conf/core-site.xml")
    configuration.addResource("/etc/hadoop/conf/hdfs-site.xml")
    configuration.addResource("/etc/hbase/conf/hbase-site.xml")
    configuration.set("hadoop.security.authentication", "kerberos")
    UserGroupInformation.setConfiguration(configuration)
    UserGroupInformation.loginUserFromKeytab(principle, keytabActualLocation)
    print("Kinit authentication successfull")
    ConnectionFactory.createConnection(HBaseConfiguration.create(configuration))
  }
  def executeReadQuery(connect:Connection,tableName: String, columnFamily: String, rowKey: String,
                       listOfCols: List[String]): Map[String, String] = {
    val table = connect.getTable(TableName.valueOf(tableName))
    print("table")
    val theGet = new Get(Bytes.toBytes(rowKey))
    print("the get")
    val result = table.get(theGet)
    print("the result")
    val resultMap = listOfCols.map(x => (x, Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(x))))).toMap
    print(resultMap)
    resultMap
  }
}
