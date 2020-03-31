package org.apache.spark.deploy.history

import java.io.{BufferedInputStream, Closeable, InputStream}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{ExecutorService, Future}

import com.google.common.util.concurrent.MoreExecutors
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.FsHistoryProvider.{APPL_END_EVENT_PREFIX, APPL_START_EVENT_PREFIX, ENV_UPDATE_EVENT_PREFIX, LOG_START_EVENT_PREFIX}
import org.apache.spark.deploy.history.HistoryServer.{conf, initSecurity}
import org.apache.spark.deploy.history.config.EVENT_LOG_DIR
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.scheduler.ReplayListenerBus.ReplayEventsFilter
import org.apache.spark.util.Utils
import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option => ArgsOpt}

import collection.JavaConverters._
import scala.collection.mutable

class MyArgumentsParser{
  @ArgsOpt(name="-i", aliases=Array("--input-dir"), required=true, usage="Specify the history log directory")
  var inputDir: String = null

  @ArgsOpt(name="-b", aliases=Array("--before-date"), required=false, usage="Specify the before date: 2020-03-25 17:44:02")
  var beforeDate: String = null //new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis())

  @ArgsOpt(name="-a", aliases=Array("--after-date"), required=false, usage="Specify the after date: 2020-03-25 17:24:02")
  var afterDate: String = null //new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis() - 24 * 3600 * 1000)

  @ArgsOpt(name="-o", aliases=Array("--output-dir"), required=false, usage="Specify the output directory")
  var outputDir: String = null

  @ArgsOpt(name="-l", aliases=Array("--local"), required=false, usage="Run under local filesystem instead of HDFS")
  var localRun: Boolean = true

  def doParse(arguments: Array[String]): Unit = {
    val parser = new CmdLineParser(this)
    if (arguments.length < 1) {
      parser.printUsage(System.out)
      System.exit(-1)
    }
    try {
      parser.parseArgument(arguments.toList.asJava)
    }
    catch {
      case clEx: CmdLineException =>
        System.out.println("ERROR: Unable to parse command-line options: " + clEx)
    }
  }
}

object CarmelAppEventLoader extends Logging {

  private val conf = new SparkConf

  def initSecurity() {
    // If we are accessing HDFS and it has security enabled (Kerberos), we have to login
    // from a keytab file so that we can access HDFS beyond the kerberos ticket expiration.
    // As long as it is using Hadoop rpc (hdfs://), a relogin will automatically
    // occur from the keytab.
    if (conf.getBoolean("spark.history.kerberos.enabled", false)) {
      // if you have enabled kerberos the following 2 params must be set
      val principalName = conf.get("spark.history.kerberos.principal")
      val keytabFilename = conf.get("spark.history.kerberos.keytab")
      SparkHadoopUtil.get.loginUserFromKeytab(principalName, keytabFilename)
    }
  }

  def main(args: Array[String]): Unit = {
    val argumentsParser = new MyArgumentsParser()
    argumentsParser.doParse(args)
    if (argumentsParser.localRun) {
      val localConf = new SparkConf
      localConf.set(EVENT_LOG_DIR, argumentsParser.inputDir)

      val carmelFsHistoryProvider = new CarmelFsHistoryProvider(localConf)
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val beforeDate = if (argumentsParser.beforeDate != null) simpleDateFormat.parse(argumentsParser.beforeDate) else null
      val afterDate = if (argumentsParser.afterDate != null) simpleDateFormat.parse(argumentsParser.afterDate) else null
      carmelFsHistoryProvider.loadAllLogs(afterDate, beforeDate)
      carmelFsHistoryProvider.getListing().foreach(println(_))
      println(carmelFsHistoryProvider.getApplicationCount())
    } else {
      initSecurity()
      val providerName = conf.getOption("spark.history.provider")
        .getOrElse(classOf[FsHistoryProvider].getName())
      val provider = Utils.classForName(providerName)
        .getConstructor(classOf[SparkConf])
        .newInstance(conf)
        .asInstanceOf[ApplicationHistoryProvider]
      println(provider.getListing().length)
      provider.getListing().foreach(println(_))
    }

  }
}
