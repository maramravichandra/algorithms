package com.hbase.practice


import org.slf4j.LoggerFactory
import scala.collection.JavaConverters
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.rest.client.RemoteHTable
import org.apache.hadoop.hbase.rest.client.Client
import org.apache.hadoop.hbase.rest.client.Cluster
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.filter.RegexStringComparator
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.util.Bytes
import org.json.{JSONArray, JSONObject}
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.CellUtil
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.filter.PageFilter

object HbaseClient extends Serializable{
  val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    println("Creating Conf")
    val conf = createHbaseConf("52.7.140.25")
    println("creatred  Conf")

    println("Creating table")
    //    createTable(conf, "amp_Test", List("cf"))
    println("amp_Test table crated ")
    val tableHandle = getHbaseTable(conf, "Test_Hbase")


    //    val x = Map[(String, String), String](("cf1", "cn1") -> "value1", ("cf1", "cn2") -> "value2", ("cf1", "cn3") -> "value3", ("cf1", "cn10") -> "value10", ("cf1", "cn20") -> "value20", ("cf1", "cn30") -> "value30")
    //    val x1 = Map[(String, String), String](("cf1", "cn1") -> "value110", ("cf1", "cn2") -> "value210", ("cf1", "cn3") -> "value310")
    //    println("inserting Record  to Table" )
    //    upsertBatch(tableHandle, "k1~1", x)
    //    upsertBatch(tableHandle, "k1~2", x1)
    //
    //
    //    println("done inserting Record  to Table" )
    //
    //
    //    println("Start data for key k1~1 is :" )
    //      printHbaseRow(getRecordByKey(tableHandle, "k1"))
    //    println("end data for key k1~1       " )
    println(" Json => " + convertHbaseRowstoJSON(getRecordsForPreview(tableHandle)))
    println(" Schema => " + getTableSchema(conf, "Test_Hbase"));

    //    printHbaseRows(getRecordsByPartialRowScan(tableHandle, ".*"))

    //    val data = convertHbaseRowtoJSON(getRecordsByPartialRowScan(tableHandle, "~1"))
    //    deleteRecord(tableHandle, "k1~1")
    //    deleteTable(conf, "amp_Test")
  }

  def createHbaseConf(zookeeperQuorum: String, zookeeperPort: Int = 2181): Configuration = {
    val conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    conf.set("hbase.zookeeper.property.clientPort", zookeeperPort.toString());
    conf
  }

  def createRemoteHbaseClient(zookeeperQuorum: String, zookeeperPort: Int = 2181): Client = {
    val cluster = new Cluster()
    cluster.add(zookeeperQuorum, zookeeperPort)
    val client = new Client(cluster)
    client
  }

  def createTable(conf: Configuration, tableName: String, columnFamaily: List[String]) = {
    val admin: HBaseAdmin = new HBaseAdmin(conf)
    if (admin.tableExists(tableName)) {
      log.info("table is already exist " + tableName)
    } else {
      val tableDef = new HTableDescriptor(tableName)
      columnFamaily.map(x => tableDef.addFamily(new HColumnDescriptor(x)))
      admin.createTable(tableDef)
      log.info("table Created " + tableName)
    }
  }

  def deleteTable(conf: Configuration, tableName: String) = {
    val admin = new HBaseAdmin(conf)
    admin.disableTable(tableName)
    admin.deleteTable(tableName)
  }

  def getHbaseTable(conf: Configuration, tableName: String): HTable = {
    new HTable(conf, tableName);

  }

  def createPut(rKey: String, cf: String, qualifier: String, value: String): Put = {
    val put = new Put(Bytes.toBytes(rKey))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(value))
    put
  }

  /* 
   * insert the list of  Ley values into table
   */
  def upsertBatch(tbl: HTable, rKey: String, data: Map[(String, String), String]) = {
    data.map(x => x match {
      case (k, v) => addRecord(tbl, rKey, k._1, k._2, v)(false)
    })
    //tbl.close()
  }

  def RemoteUpsertBatch(tbl: RemoteHTable, rKey: String, data: Map[(String, String), String]) = {
    data.map(x => x match {
      case (k, v) => RemoteAddRecord(tbl, rKey, k._1, k._2, v)(false)
    })
    //tbl.close()
  }

  /*
  * Single record insert one key and one value
  */

  def addRecord(tbl: HTable, rKey: String, cf: String, qualifier: String, value: String)(closeTable: Boolean = true) = {
    val put = createPut(rKey, cf, qualifier, value)
    tbl.put(put);
    if (closeTable) tbl.close()
  }

  def RemoteAddRecord(tbl: RemoteHTable, rKey: String, cf: String, qualifier: String, value: String)(closeTable: Boolean = true) = {
    val put = createPut(rKey, cf, qualifier, value)
    tbl.put(put);
    if (closeTable) tbl.close()
  }

  def deleteRecord(tbl: HTable, rKey: String) = {
    tbl.delete(new Delete(rKey.getBytes()))
    //tbl.close()
  }

  def deleteRecords(tbl: HTable, rKeys: List[String]) = {
    val keysList = rKeys.map(x => new Delete(x.getBytes))
    import scala.collection.JavaConverters._
    tbl.delete(keysList.asJava)
  }

  def convertResultToMap(rs: Result): Map[(String, String), String] = {
    def bytesToString(in: Array[Byte]): String = Bytes.toString(in)

    rs.rawCells().map(x => ((bytesToString(x.getFamily),
      bytesToString(x.getQualifier)),
      bytesToString(x.getValue))).toMap
  }

  def getRecordByKey(hbaseTable: HTable, rowKey: String): Map[(String, String), String] = {
    log.info("Getting Record for rowKey:- " + rowKey)
    val criteria: Get = new Get(Bytes.toBytes(rowKey))
    val result: Result = hbaseTable.get(criteria)
    val resultMap = convertResultToMap(result)
    resultMap
  }

  def getRemoteRecordByKey(hbaseTable: RemoteHTable, rowKey: String): Map[(String, String), String] = {
    log.info("Getting Record for rowKey:- " + rowKey)
    val criteria: Get = new Get(Bytes.toBytes(rowKey))
    val result: Result = hbaseTable.get(criteria)
    val resultMap = convertResultToMap(result)
    resultMap
  }

  def getCoumnValue(result: Result, columnFamily: String, columnName: String): String = {
    Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName)));

  }

  def getRecordsForPreview(tbl: HTable): Map[String, Map[(String, String), String]] = {
    log.info("Fetching rows for preview ");

    val scan = new Scan();
    scan.setBatch(1000)
    val scs = tbl.getScanner(scan);
    import scala.collection.JavaConverters._
    scs.asScala.take(50).map(x => (Bytes.toString(x.getRow), convertResultToMap(x))).toMap
  }


  /*
 * @input : (TableObject, PartialKey)
 * @outPut : Map(key , Map(columnFamily, ColumnName, ColumnValue))
 */

  def getRecordsByPartialRowScan(tbl: HTable, regexPattern: String): Map[String, Map[(String, String), String]] = {
    log.info("Fetching Paginated rows for pattern " + regexPattern);

    val regexFilter: Filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(regexPattern));

    val scan = new Scan();
    scan.setFilter(regexFilter);
    val scs = tbl.getScanner(scan);
    import scala.collection.JavaConverters._
    scs.asScala.map(x => (Bytes.toString(x.getRow), convertResultToMap(x))).toMap
  }

  def printHbaseRow(v: Map[(String, String), String]) = {
    v.foreach(x => x match {
      case (k, v) => println(k._1 + ":" + k._2 + "=" + v)
    })
  }

  def printHbaseRows(v: Map[String, Map[(String, String), String]]) = {
    v.foreach(x => x match {
      case (k, v) =>
        println("-------------" + k + "-------------")
        printHbaseRow(v)
        println("-----------------------------------")
    })
  }

  def getRecordsByPartialRowScanWithRegx(tbl: HTable, rowkeyStart: String, rowkeyEnd: String): Map[String, Map[(String, String), String]] = {
    log.info("Fetching rows form " + rowkeyStart + " to " + rowkeyEnd)
    val scan = new Scan();
    scan.setStartRow(Bytes.toBytes(rowkeyStart))
    scan.setStopRow(Bytes.toBytes(rowkeyEnd))
    val scs = tbl.getScanner(scan);
    import scala.collection.JavaConverters._
    scs.asScala.map(x => (Bytes.toString(x.getRow), convertResultToMap(x))).toMap
  }

  def convertHbaseRowtoJSON(v: Map[String, Map[(String, String), String]]) = {
    val hbaseRec = new Array[String](v.size)
    val row = new Array[String](10)
    var json = new JSONObject()
    val jSONObject = new JSONArray()
    v.foreach(x => x match {
      case (k, v) => v.foreach(x => x match {
        case (k, v) => json.put(k._2, v)
      })
        jSONObject.put(json)
        json = new JSONObject()
    })
    jSONObject
  }

  def convertHbaseRowstoJSON(v: Map[String, Map[(String, String), String]]) = {
    val hbaseRec = new Array[String](v.size)
    val row = new Array[String](10)
    var json = new JSONObject()
    val jSONObject = new JSONArray()
    v.foreach(x => x match {
      case (k, v) => v.foreach(x => x match {
        case (k, v) => json.put(k._2, v)
      })
        json.put("rowkey", k)
        jSONObject.put(json)
        json = new JSONObject()
    })
    jSONObject
  }

  def getHbaseStartRowkey(v: Map[String, Map[(String, String), String]]) = {
    val list = new ListBuffer[Int]
    val lst = new ListBuffer[String]
    v.foreach(x => x match {
      case (k, v) =>
        val accnum = k.split("~")
        list += accnum(0).trim.toInt
    })
    val accountListforStartRowkey = list.toList
    val minAccNo = accountListforStartRowkey.reduceLeft((x, y) => if (x < y) x else y)
    v.foreach(x => x match {
      case (k, v) => val accnum = k.split("~")
        if (minAccNo == accnum(0).trim.toInt) lst += k
    })
    lst.toList
  }

  def getHbaseEndRowkey(v: Map[String, Map[(String, String), String]]) = {
    val list = new ListBuffer[Int]
    val lst = new ListBuffer[String]
    v.foreach(x => x match {
      case (k, v) =>
        val accnum = k.split("~")
        list += accnum(0).trim.toInt
    })
    val accountListforStartRowkey = list.toList
    val minAccNo = accountListforStartRowkey.reduceLeft((x, y) => if (x > y) x else y)
    v.foreach(x => x match {
      case (k, v) => val accnum = k.split("~")
        if (minAccNo == accnum(0).trim.toInt) lst += k
    })
    lst.toList
  }

  def listTables(conf: Configuration): Int = {
    val admin: HBaseAdmin = new HBaseAdmin(conf)
    val tableDescriptors = admin.listTables
    log.info("number of tables: " + tableDescriptors.length)
    tableDescriptors.length
  }

  def getTableNames(conf: Configuration): List[String] = {
    val admin: HBaseAdmin = new HBaseAdmin(conf)
    val tableDescriptors = admin.listTables
    log.info("number of tables: " + tableDescriptors.length)
    val tbls = tableDescriptors.map(x => x.getTableName.toString()).toList
    tbls
  }

  def getTableSchema(conf: Configuration, tableName: String): Map[String, List[String]] = {
    log.info("conf :::::::: " + conf)
    val admin: HBaseAdmin = new HBaseAdmin(conf)
    val td: HTableDescriptor = admin.getTableDescriptor(tableName.getBytes)
    val hTable: HTable = new HTable(conf, tableName)
    var cfmap = Map[String, List[String]]()
    for (cd <- td.getColumnFamilies) {
      val cf: String = new String(cd.getName)
      log.info("Column Family: " + cf)
      val scan: Scan = new Scan()
      scan.addFamily(cd.getName)
      scan.setFilter(new PageFilter(1))
      val scanner: ResultScanner = hTable.getScanner(scan)
      val result: Result = scanner.next()
      if (result != null) {
        val colList = new ListBuffer[String]()
        for (cell <- result.listCells()) {
          val family: String = new String(CellUtil.cloneFamily(cell))
          val colQual: String = new String(CellUtil.cloneQualifier(cell))
          log.info("Family: " + family + " Qualifier: " + colQual)
          colList += colQual
        }
        cfmap += (cf -> colList.toList)
      }
    }

    val m1 = cfmap.values.flatten

    val fields = for {
      v <- m1
    } yield (new Tuple4(v, "String", "true", ""))


    cfmap
  }

  def getColumnSchema(conf: Configuration, tableName: String): String = {
    log.info("conf :::::::: " + conf)
    val admin: HBaseAdmin = new HBaseAdmin(conf)
    val td: HTableDescriptor = admin.getTableDescriptor(tableName.getBytes)
    val hTable: HTable = new HTable(conf, tableName)

    val colList = new ListBuffer[String]()
    for (cd <- td.getColumnFamilies) {
      val cf: String = new String(cd.getName)
      log.info("Column Family: " + cf)
      val scan: Scan = new Scan()
      scan.addFamily(cd.getName)
      scan.setFilter(new PageFilter(1))
      val scanner: ResultScanner = hTable.getScanner(scan)
      val result: Result = scanner.next()
      if (result != null) {
        for (cell <- result.listCells()) {
          val family: String = new String(CellUtil.cloneFamily(cell))
          val colQual: String = new String(CellUtil.cloneQualifier(cell))
          log.info("Family: " + family + " Qualifier: " + colQual)
          val str = s"""$family:$colQual"""
          colList += str
        }
      }
    }
    colList.mkString(",")
  }

  def getColumnFamily(conf: Configuration, tableName: String): String = {
    log.info("conf :::::::: " + conf)
    val admin: HBaseAdmin = new HBaseAdmin(conf)
    val td: HTableDescriptor = admin.getTableDescriptor(tableName.getBytes)
    val hTable: HTable = new HTable(conf, tableName)

    val colList = new ListBuffer[String]()
    for (cd <- td.getColumnFamilies) {
      val cf: String = new String(cd.getName)
      log.info("Column Family: " + cf)
      colList += cf
    }
    if (colList.size == 1)
      colList(0).toString()
    else
      colList.mkString(",")
  }

  def checktableExists(conf: Configuration, tableName: String): Boolean = {
    val admin: HBaseAdmin = new HBaseAdmin(conf)
    if (admin.tableExists(tableName)) {
      return true
    }
    else {
      return false
    }
  }
  
  
  def getTableDetails(conf: Configuration, tableName: String): Seq[HbaseTable] = {
    log.info("conf :::::::: " + conf)
    val admin: HBaseAdmin = new HBaseAdmin(conf)
    val td: HTableDescriptor = admin.getTableDescriptor(tableName.getBytes)
    val hTable: HTable = new HTable(conf, tableName)
    //var cfmap = Map[String, Seq[HbaseColumn]]()
    var cfmap: Vector[HbaseTable] = Vector.empty;
    for (cd <- td.getColumnFamilies) {
      val cf: String = new String(cd.getName)
      log.info("Column Family: " + cf)
      val scan: Scan = new Scan()
      scan.addFamily(cd.getName)
      scan.setFilter(new PageFilter(1))
      val scanner: ResultScanner = hTable.getScanner(scan)
      val result: Result = scanner.next()
      if (result != null) {
        log.info("result : " + result)
        //val colList = new ListBuffer[HbaseColumn]()
        var colList: Vector[HbaseColumn] = Vector.empty;
        for (cell <- result.listCells()) {
          log.info("cell : " + cell)
          val family: String = new String(CellUtil.cloneFamily(cell))
          val colQual = HbaseColumn(new String(CellUtil.cloneQualifier(cell)), "String")
          log.info("Family: " + family + " Qualifier: " + colQual)
          colList :+= colQual
        }
        cfmap :+= HbaseTable(cf, colList)
      }
    }

    log.info("cfmap : " + cfmap)
    cfmap
  }
}

case class HbaseColumn(columnName: String, columnType: String)
case class HbaseTable(columFamily: String, columns: Seq[HbaseColumn])