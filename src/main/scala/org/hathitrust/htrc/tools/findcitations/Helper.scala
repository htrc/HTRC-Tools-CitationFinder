package org.hathitrust.htrc.tools.findcitations

import java.io.{OutputStreamWriter, _}
import java.nio.charset.StandardCharsets

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.hathitrust.htrc.tools.pairtreehelper.PairtreeHelper.PairtreeDocument
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsObject, JsValue, Json}
import resource._

object Helper {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(Main.appName)

  /**
    * Writes a JSON object to file
    *
    * @param json     The JSON object
    * @param file     The file to write to
    * @param compress True if compression is desired, False otherwise
    * @param indent   True if output should be pretty-printed, False otherwise
    */
  def writeJsonFile(json: JsValue, file: File, compress: Boolean, indent: Boolean): Unit = {
    val parent = file.getParentFile
    if (parent != null) parent.mkdirs()

    val outputStream = {
      var stream: OutputStream = new BufferedOutputStream(new FileOutputStream(file))
      if (compress)
        stream = new BZip2CompressorOutputStream(stream, BZip2CompressorOutputStream.MAX_BLOCKSIZE)

      new OutputStreamWriter(stream, StandardCharsets.UTF_8)
    }

    val jsonTxt = if (indent) Json.prettyPrint(json) else Json.stringify(json)

    for (writer <- managed(outputStream))
      writer.write(jsonTxt)
  }

  /**
    * Loads the JSON metadata file associated with a given pairtree document.
    *
    * @param pairtreeDoc      The pairtree document reference
    * @param pairtreeRootPath The path to the pairtree root
    * @return The JSON object containing the loaded metadata
    * @throws HTRCPairtreeDocumentException Thrown if errors were encountered while reading
    *                                       or parsing the JSON file
    */
  def loadJsonMeta(pairtreeDoc: PairtreeDocument, pairtreeRootPath: String): JsObject = {
    managed(
      new FileInputStream(pairtreeDoc.getDocumentPathPrefix(pairtreeRootPath) + ".json")
    ).map(Json.parse).either.either match {
      case Right(meta) => meta.as[JsObject]
      case Left(errors) =>
        throw HTRCPairtreeDocumentException(
          s"[${pairtreeDoc.getUncleanId}] Error while retrieving document metadata", errors.head
        )
    }
  }

  private val yearRegex = """(?<=\D|^)[12]\d{3}(?=\D|$)""".r

  /**
    * Attempts to extract a four digit year from a string
    *
    * @param s The string
    * @return An Option containing the year extracted, or None
    */
  private def extractYear(s: String): Option[String] = {
    yearRegex.findAllIn(s).map(_.toInt)
      .withFilter(y => y >= 1500 && y <= 2016)
      .toList.sorted.lastOption.map(_.toString)
  }

  /**
    * Replaces an invalid pubDate (one whose value is "9999") with a date extracted from the
    * enumerationChronology metadata attribute (if it contains a valid year)
    *
    * @param meta The metadata JSON
    * @return The new metadata JSON with the invalid pubDate replaced, or the same object if no
    *         replacement was made
    */
  def replaceInvalidPubDateWithDateFromEnumChronologyField(meta: JsObject): JsObject = {
    ((meta \ "pubDate").asOpt[String].map(_.trim),
      (meta \ "enumerationChronology").asOpt[String].flatMap(extractYear)) match {
      case (Some(pubDate), Some(ecDate)) if pubDate == "9999" => meta ++ Json.obj("pubDate" -> ecDate)
      case _ => meta
    }
  }
}