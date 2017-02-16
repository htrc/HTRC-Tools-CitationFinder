package org.hathitrust.htrc.tools.findcitations

import java.io.{File, PrintWriter}

import com.gilt.gfc.time.Timer
import org.apache.hadoop.fs.Path
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.hathitrust.htrc.tools.findcitations.Helper._
import org.hathitrust.htrc.tools.pairtreetotext.{HTRCVolume, TextOptions}
import org.hathitrust.htrc.tools.spark.errorhandling.ErrorAccumulator
import org.hathitrust.htrc.tools.spark.errorhandling.RddExtensions._
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.io.{Codec, Source, StdIn}

/**
  * This app processes a corpus of text to find all volumes containing potential citations
  * of other volumes in the corpus.  The candidate citations are found by looking at
  * each volume in the corpus in chronological order and searching for mentions of author
  * last names and volume (short) title in each of the volumes published "later" (chronologically)
  * than the volume in the current loop iteration.
  *
  * @author Boris Capitanu
  */

object Main {
  val appName = "find-citations"

  def main(args: Array[String]): Unit = {
    // parse command line arguments
    val conf = new Conf(args)
    val pairtreeRootPath = conf.pairtreeRootPath().toString
    val outputPath = conf.outputPath().toString
    val numPartitions = conf.numPartitions.toOption
    val htids = conf.htids.toOption match {
      case Some(file) => Source.fromFile(file).getLines().toSeq
      case None => Iterator.continually(StdIn.readLine()).takeWhile(_ != null).toSeq
    }

    conf.outputPath().mkdirs()

    // set up logging destination
    conf.sparkLog.toOption match {
      case Some(logFile) => System.setProperty("spark.logFile", logFile)
      case None =>
    }
    System.setProperty("logLevel", conf.logLevel().toUpperCase)

    // set up Spark context
    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    sparkConf.setIfMissing("spark.app.name", appName)

    val sc = new SparkContext(sparkConf)

    logger.info("Starting...")

    // record start time
    val t0 = System.nanoTime()

    val idsRDD = numPartitions match {
      case Some(n) => sc.parallelize(htids, n)
      case None => sc.parallelize(htids)
    }

    // load the HTRC volumes from pairtree (and save any errors)
    val errorsVol = new ErrorAccumulator[String, String](identity)(sc)
    val htrcDocsRDD = idsRDD.tryMap(HTRCVolume(_, pairtreeRootPath)(Codec.UTF8))(errorsVol)

    // load the JSON metadata for each HTRC volume and fix the pubDate field, if necessary
    // then extract the text from the volume and construct an ACSDoc record containing the
    // needed metadata fields and the text
    val errorsMeta = new ErrorAccumulator[HTRCVolume, String](_.id)(sc)
    val acsDocsRDD = htrcDocsRDD
      .tryMap(doc => {
        var meta = loadJsonMeta(doc.pairtreeDoc, pairtreeRootPath)
        meta = replaceInvalidPubDateWithDateFromEnumChronologyField(meta)
        val text = doc.getText(TextOptions.BodyOnly, TextOptions.FixHyphenation)
        val pubDate = (meta \ "pubDate").as[String].toInt
        val authors = (meta \ "names").as[Seq[String]]
        val title = (meta \ "title").as[String]

        (ACSDoc(doc.id, pubDate, authors, title), text)
      })(errorsMeta)

    // cache the result so it doesn't have to be re-processed
    acsDocsRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // generate a chronological ordering of the HTRC volume IDs based on pubDate
    // (from earliest to latest) and record the indexed ordering in a hash table;
    // this is needed to be able to tell what volumes came "later" for a particular
    // volume ID
    val acsDocsByYear = acsDocsRDD.map(_._1).collect().sortBy(_.pubDate)
    val idxMap = acsDocsByYear.map(_.id).zipWithIndex.toMap

    // send the index hash table to all workers
    val idxMapBcast = sc.broadcast(idxMap)

    val numDocs = acsDocsByYear.length

    // prepare the output file
    val pw = new PrintWriter(new File(outputPath, "citations.csv"))
    pw.println("src\tsrcPubDate\tcitation\tcitationPubDate\tauthorTotal\tauthorMatches\ttitle")

    // loop through each volume in chronological order and search
    // for citations in all "later" (in index order) volumes
    for ((doc, srcIdx) <- acsDocsByYear.zipWithIndex) {
      // log progress (if requested)
      if (logger.isDebugEnabled) {
        val i = srcIdx + 1
        logger.debug(f"[$i%,d/$numDocs%,d] Processing ${doc.id} (${doc.pubDate}): ${doc.title}")
      }

      val authorLastNames = doc.authorLastNames
      val shortTitle = doc.shortTitle

      val citations = acsDocsRDD
        .filter {
          // look only at the documents that occur "later" (in index order)
          case (ACSDoc(id, _, _, _), _) => idxMapBcast.value(id) > srcIdx
        }
        .map {
          case (ACSDoc(id, pubDate, _, _), text) =>
            // look for author matches
            val authorMatches: Seq[(String, Int)] = if (authorLastNames.nonEmpty) {
              val authorsRegex = authorLastNames.map(l => s"""\\b\\Q$l\\E\\b""".r)
              val matchCounts = authorsRegex.map(r => r.findAllIn(text).size)
              authorLastNames.zip(matchCounts)
            } else
              Seq.empty

            // look for short title matches
            val shortTitleRegex = s"""\\b\\Q$shortTitle\\E\\b""".r
            val shortTitleMatches = shortTitleRegex.findAllIn(text).size

            (id, pubDate, authorMatches, shortTitleMatches)
        }
        .filter {
          // keep only the documents where the matching is positive
          case (_, _, authorMatches, shortTitleMatches) =>
            authorMatches.exists(_._2 > 0) || shortTitleMatches > 0
        }
        .map {
          // and create the result format needed for reporting
          case (id, pubDate, authorMatches, shortTitleMatches) =>
            val authorTotalMatches = authorMatches.map(_._2).sum
            val authors =
              if (authorTotalMatches > 0)
                authorMatches
                  .map { case (name, count) => s"$name:$count" }
                  .mkString(",")
              else ""

            s"""${doc.id}\t${doc.pubDate}\t$id\t$pubDate\t$authorTotalMatches\t$authors\t$shortTitleMatches"""
        }
        .collect()

      // write the results to the output file
      citations.foreach(pw.println)
    }

    // close results file
    pw.close()

    if (errorsVol.nonEmpty || errorsMeta.nonEmpty)
      logger.info("Writing error report(s)...")

    // save any errors to the output folder
    if (errorsVol.nonEmpty)
      errorsVol.saveErrors(new Path(outputPath, "volume_errors.txt"), _.toString)

    if (errorsMeta.nonEmpty)
      errorsMeta.saveErrors(new Path(outputPath, "meta_errors.txt"), _.toString)

    // record elapsed time and report it
    val t1 = System.nanoTime()
    val elapsed = t1 - t0

    logger.info(f"All done in ${Timer.pretty(elapsed)}")
  }
}

/**
  * Command line argument configuration
  *
  * @param arguments The cmd line args
  */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val (appTitle, appVersion, appVendor) = {
    val p = getClass.getPackage
    val nameOpt = Option(p).flatMap(p => Option(p.getImplementationTitle))
    val versionOpt = Option(p).flatMap(p => Option(p.getImplementationVersion))
    val vendorOpt = Option(p).flatMap(p => Option(p.getImplementationVendor))
    (nameOpt, versionOpt, vendorOpt)
  }

  version(appVersion.flatMap(
    version => appVendor.map(
      vendor => s"${Main.appName} $version\n$vendor")).getOrElse(Main.appName))

  val sparkLog: ScallopOption[String] = opt[String]("spark-log",
    descr = "Where to write logging output from Spark to",
    argName = "FILE",
    noshort = true
  )

  val logLevel: ScallopOption[String] = opt[String]("log-level",
    descr = "The application log level; one of INFO, DEBUG, OFF",
    argName = "LEVEL",
    default = Some("INFO"),
    validate = level => Set("INFO", "DEBUG", "OFF").contains(level.toUpperCase)
  )

  val numPartitions: ScallopOption[Int] = opt[Int]("num-partitions",
    descr = "The number of partitions to split the input set of HT IDs into, " +
      "for increased parallelism",
    required = false,
    argName = "N",
    validate = 0 <
  )

  val pairtreeRootPath: ScallopOption[File] = opt[File]("pairtree",
    descr = "The path to the paitree root hierarchy to process",
    required = true,
    argName = "DIR"
  )

  val outputPath: ScallopOption[File] = opt[File]("output",
    descr = "Write the output to the file `citations.csv` in DIR",
    required = true,
    argName = "DIR"
  )

  val htids: ScallopOption[File] = trailArg[File]("htids",
    descr = "The file containing the HT IDs to be searched (if not provided, will read from stdin)",
    required = false
  )

  validateFileExists(pairtreeRootPath)
  validateFileExists(htids)
  verify()
}