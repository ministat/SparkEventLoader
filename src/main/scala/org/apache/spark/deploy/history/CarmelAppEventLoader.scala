package org.apache.spark.deploy.history

import java.io.{BufferedInputStream, Closeable, InputStream}
import java.util.concurrent.{ExecutorService, Future}

import com.google.common.util.concurrent.MoreExecutors
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.FsHistoryProvider.{APPL_END_EVENT_PREFIX, APPL_START_EVENT_PREFIX, ENV_UPDATE_EVENT_PREFIX, LOG_START_EVENT_PREFIX}
import org.apache.spark.deploy.history.HistoryServer.conf
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

  @ArgsOpt(name="-o", aliases=Array("--output-dir"), required=false, usage="Specify the output directory")
  var outputDir: String = null

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

  def main(args: Array[String]): Unit = {
    val argumentsParser = new MyArgumentsParser()
    argumentsParser.doParse(args)
    val logDir= argumentsParser.inputDir
    val conf = new SparkConf
    conf.set(EVENT_LOG_DIR, argumentsParser.inputDir)

    val carmelFsHistoryProvider = new CarmelFsHistoryProvider(conf)
    carmelFsHistoryProvider.loadAllLogs()
  }
}
