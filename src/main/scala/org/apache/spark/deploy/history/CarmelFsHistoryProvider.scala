package org.apache.spark.deploy.history

import java.io.{BufferedInputStream, Closeable, File, InputStream}
import java.util.Date
import java.util.concurrent.{ExecutorService, Future}

import com.google.common.util.concurrent.MoreExecutors
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.FsHistoryProvider.CURRENT_LISTING_VERSION
import org.apache.spark.deploy.history.config.LOCAL_STORE_DIR
import org.apache.spark.scheduler.EventLoggingListener
import org.apache.spark.scheduler.ReplayListenerBus.SELECT_ALL_FILTER
import org.apache.spark.util.{Clock, SystemClock, Utils}
import org.apache.spark.deploy.history.config.EVENT_LOG_DIR
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.scheduler.ReplayListenerBus.ReplayEventsFilter
import org.apache.spark.scheduler.{ReplayListenerBus, SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerEnvironmentUpdate, SparkListenerEvent, SparkListenerLogStart}
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.KVUtils.{MetadataMismatchException, open}
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo}
import org.apache.spark.util.kvstore.{InMemoryStore, KVStore, UnsupportedStoreVersionException}
import org.fusesource.leveldbjni.internal.NativeDB

import scala.collection.mutable
import scala.collection.JavaConverters._

private[history] class CarmelAppListingListener(log: FileStatus, clock: Clock) extends SparkListener {

  private val app = new MutableApplicationInfo()
  private val attempt = new MutableAttemptInfo(log.getPath().getName(), log.getLen())

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    app.id = event.appId.orNull
    app.name = event.appName

    attempt.attemptId = event.appAttemptId
    attempt.startTime = new Date(event.time)
    attempt.lastUpdated = new Date(clock.getTimeMillis())
    attempt.sparkUser = event.sparkUser
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    attempt.endTime = new Date(event.time)
    attempt.lastUpdated = new Date(log.getModificationTime())
    attempt.duration = event.time - attempt.startTime.getTime()
    //println(s"${app.name} with ${attempt.sparkUser} runs ${attempt.duration} from ${attempt.startTime.toString} to ${attempt.endTime.toString} in queue ${attempt.sparkQueue.toString}")
    attempt.completed = true
  }

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    val allProperties = event.environmentDetails("Spark Properties").toMap
    attempt.viewAcls = allProperties.get("spark.ui.view.acls")
    attempt.adminAcls = allProperties.get("spark.admin.acls")
    attempt.viewAclsGroups = allProperties.get("spark.ui.view.acls.groups")
    attempt.adminAclsGroups = allProperties.get("spark.admin.acls.groups")
    attempt.sparkQueue = allProperties.get("spark.yarn.queue")
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case SparkListenerLogStart(sparkVersion) =>
      attempt.appSparkVersion = sparkVersion
    case _ =>
  }

  def applicationInfo: Option[ApplicationInfoWrapper] = {
    if (app.id != null) {
      Some(app.toView())
    } else {
      None
    }
  }

  private class MutableApplicationInfo {
    var id: String = null
    var name: String = null
    //var sparkYarnQueue: String = null
    var coresGranted: Option[Int] = None
    var maxCores: Option[Int] = None
    var coresPerExecutor: Option[Int] = None
    var memoryPerExecutorMB: Option[Int] = None


    def toView(): ApplicationInfoWrapper = {
      val apiInfo = ApplicationInfo(id, name, coresGranted, maxCores, coresPerExecutor,
        memoryPerExecutorMB, Nil)
      new ApplicationInfoWrapper(apiInfo, List(attempt.toView()))
    }

  }

  private class MutableAttemptInfo(logPath: String, fileSize: Long) {
    var attemptId: Option[String] = None
    var startTime = new Date(-1)
    var endTime = new Date(-1)
    var lastUpdated = new Date(-1)
    var duration = 0L
    var sparkUser: String = null
    var completed = false
    var appSparkVersion = ""

    var sparkQueue: Option[String] = None

    var adminAcls: Option[String] = None
    var viewAcls: Option[String] = None
    var adminAclsGroups: Option[String] = None
    var viewAclsGroups: Option[String] = None

    def toView(): AttemptInfoWrapper = {
      val apiInfo = ApplicationAttemptInfo(
        attemptId,
        startTime,
        endTime,
        lastUpdated,
        duration,
        sparkUser,
        completed,
        appSparkVersion)
      new AttemptInfoWrapper(
        apiInfo,
        logPath,
        fileSize,
        adminAcls,
        viewAcls,
        adminAclsGroups,
        viewAclsGroups)
    }
  }
}

import CarmelFsHistoryProvider._

private[history] class CarmelFsHistoryProvider(conf: SparkConf, clock: Clock) extends Logging {
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  private val codecMap = new mutable.HashMap[String, CompressionCodec]
  private val storePath = conf.get(LOCAL_STORE_DIR).map(new File(_))
  private val logDir = conf.get(EVENT_LOG_DIR)

  def getListing(): Iterator[ApplicationInfo] = {
    // Return the listing in end time descending order.
    listing.view(classOf[ApplicationInfoWrapper])
      .index("endTime")
      .reverse()
      .iterator()
      .asScala
      .map(_.toApplicationInfo())
  }

  def getApplicationCount(): Long = {
    return listing.count(classOf[ApplicationInfoWrapper])
  }

  private[history] val listing: KVStore = storePath.map { path =>
    require(path.isDirectory(), s"Configured store directory ($path) does not exist.")
    val dbPath = new File(path, "listing.ldb")
    val metadata = new FsHistoryProviderMetadata(CURRENT_LISTING_VERSION,
      AppStatusStore.CURRENT_VERSION, logDir.toString())

    try {
      open(dbPath, metadata)
    } catch {
      // If there's an error, remove the listing database and any existing UI database
      // from the store directory, since it's extremely likely that they'll all contain
      // incompatible information.
      case _: UnsupportedStoreVersionException | _: MetadataMismatchException =>
        logInfo("Detected incompatible DB versions, deleting...")
        path.listFiles().foreach(Utils.deleteRecursively)
        open(dbPath, metadata)
      case dbExc: NativeDB.DBException =>
        // Get rid of the corrupted listing.ldb and re-create it.
        logWarning(s"Failed to load disk store $dbPath :", dbExc)
        Utils.deleteRecursively(dbPath)
        open(dbPath, metadata)
    }
  }.getOrElse(new InMemoryStore())

  /**
   * Write the app's information to the given store. Serialized to avoid the (notedly rare) case
   * where two threads are processing separate attempts of the same application.
   */
  private def addListing(app: ApplicationInfoWrapper): Unit = listing.synchronized {
    val attempt = app.attempts.head

    val oldApp = try {
      load(app.id)
    } catch {
      case _: NoSuchElementException =>
        app
    }

    def compareAttemptInfo(a1: AttemptInfoWrapper, a2: AttemptInfoWrapper): Boolean = {
      a1.info.startTime.getTime() > a2.info.startTime.getTime()
    }

    val attempts = oldApp.attempts.filter(_.info.attemptId != attempt.info.attemptId) ++
      List(attempt)

    val newAppInfo = new ApplicationInfoWrapper(
      app.info,
      attempts.sortWith(compareAttemptInfo))
    listing.write(newAppInfo)
  }

  private def load(appId: String): ApplicationInfoWrapper = {
    listing.read(classOf[ApplicationInfoWrapper], appId)
  }

  private val replayExecutor: ExecutorService = {
    MoreExecutors.sameThreadExecutor()
  }

  def this(conf: SparkConf) = {
    this(conf, new SystemClock())
  }

  def tryWithResource[R <: Closeable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource) finally resource.close()
  }

  def openEventLog(log: Path, fs: FileSystem): InputStream = {
    val in = new BufferedInputStream(fs.open(log))
    try {
      val codec = codecName(log).map { c =>
        codecMap.getOrElseUpdate(c, CompressionCodec.createCodec(new SparkConf, c))
      }
      codec.map(_.compressedInputStream(in)).getOrElse(in)
    } catch {
      case e: Throwable =>
        in.close()
        throw e
    }
  }

  def codecName(log: Path): Option[String] = {
    val IN_PROGRESS = ".inprogress"
    // Compression codec is encoded as an extension, e.g. app_123.lzf
    // Since we sanitize the app ID to not include periods, it is safe to split on it
    val logName = log.getName.stripSuffix(IN_PROGRESS)
    logName.split("\\.").tail.lastOption
  }

  private def replay(
    eventLog: FileStatus,
    bus: ReplayListenerBus,
    fs: FileSystem,
    eventsFilter: ReplayEventsFilter = SELECT_ALL_FILTER): Unit = {
    val logPath = eventLog.getPath()
    val isCompleted = !logPath.getName().endsWith(EventLoggingListener.IN_PROGRESS)
    logInfo(s"Replaying log path: $logPath")
    tryWithResource(openEventLog(logPath, fs)) { in =>
      bus.replay(in, logPath.toString, !isCompleted, eventsFilter)
      logInfo(s"Finished parsing $logPath")
    }
  }

  protected def mergeApplicationListing(fileStatus: FileStatus, fs: FileSystem): Unit = {
    val eventsFilter: ReplayEventsFilter = { eventString =>
      eventString.startsWith(APPL_START_EVENT_PREFIX) ||
        eventString.startsWith(APPL_END_EVENT_PREFIX) ||
        eventString.startsWith(LOG_START_EVENT_PREFIX) ||
        eventString.startsWith(ENV_UPDATE_EVENT_PREFIX)
    }

    //val logPath = fileStatus.getPath()
    val bus = new ReplayListenerBus()
    val listener = new CarmelAppListingListener(fileStatus, clock)
    bus.addListener(listener)
    replay(fileStatus, bus, fs, eventsFilter)

    val (appId, attemptId) = listener.applicationInfo match {
      case Some(app) =>
        // Invalidate the existing UI for the reloaded app attempt, if any. See LoadedAppUI for a
        // discussion on the UI lifecycle.


        addListing(app)
        (Some(app.info.id), app.attempts.head.info.attemptId)

      case _ =>
        // If the app hasn't written down its app ID to the logs, still record the entry in the
        // listing db, with an empty ID. This will make the log eligible for deletion if the app
        // does not make progress after the configured max log age.
        (None, None)
    }
  }

  def loadAllLogs(after: Date, before: Date): Unit = {
    val logDir = conf.get(EVENT_LOG_DIR)
    hadoopConf.set("fs.hdfs.impl",
      classOf[DistributedFileSystem].getName)
    println(hadoopConf.get("fs.hdfs.impl"))
    val fs = new Path(logDir).getFileSystem(hadoopConf)

    val updated = Option(fs.listStatus(new Path(logDir))).map(_.toSeq).getOrElse(Nil)
      .filter { entry =>
        !entry.isDirectory() &&
          // FsHistoryProvider generates a hidden file which can't be read.  Accidentally
          // reading a garbage file is safe, but we would log an error which can be scary to
          // the end-user.
          !entry.getPath().getName().startsWith(".")
      }
      .filter { entry =>
        val modTime = new Date(entry.getModificationTime)
        if (after != null && before != null) {
          modTime.after(after) && modTime.before(before)
        } else if (after != null) {
          modTime.after(after)
        } else if (before != null ) {
          modTime.before(before)
        } else {
          true
        }
      }
      .filter { entry =>
        entry.getPath().getName().endsWith(".lz4")
      }
      .sortWith { case (entry1, entry2) =>
        entry1.getModificationTime() > entry2.getModificationTime()
      }

    val tasks = updated.flatMap { entry =>
      try {
        val task: Future[Unit] = replayExecutor.submit(new Runnable {
          override def run(): Unit = mergeApplicationListing(entry, fs) //println(entry.getPath) //mergeApplicationListing(entry, newLastScanTime)
        }, Unit)
        Some(task -> entry.getPath)
      } catch {
        // let the iteration over the updated entries break, since an exception on
        // replayExecutor.submit (..) indicates the ExecutorService is unable
        // to take any more submissions at this time
        case e: Exception =>
          logError(s"Exception while submitting event log for replay", e)
          None
      }
    }
  }
}

private[history] object CarmelFsHistoryProvider {
  private val APPL_START_EVENT_PREFIX = "{\"Event\":\"SparkListenerApplicationStart\""

  private val APPL_END_EVENT_PREFIX = "{\"Event\":\"SparkListenerApplicationEnd\""

  private val LOG_START_EVENT_PREFIX = "{\"Event\":\"SparkListenerLogStart\""

  private val ENV_UPDATE_EVENT_PREFIX = "{\"Event\":\"SparkListenerEnvironmentUpdate\","
}
