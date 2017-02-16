package org.hathitrust.htrc.tools.findcitations

/**
  * Structure representing an ACS document for purposes of citation finding.
  *
  * @param id      The volume id
  * @param pubDate The publication date
  * @param authors The list of authors
  * @param title   The volume title
  */
case class ACSDoc(id: String, pubDate: Int, authors: Seq[String], title: String) {
  protected val SHORT_TITLE_TOKENS = 6

  private val lastnameRegex = """(?<=^\s*)\p{L}+(?=,\s?\p{L}+)""".r
  lazy val authorLastNames: Seq[String] = authors.flatMap(lastnameRegex.findFirstIn)

  lazy val shortTitle: String = {
    val colonParts = title.split(":")
    if (colonParts.length > 1)
      colonParts(0).trim()
    else
      title.split(" ").take(SHORT_TITLE_TOKENS).mkString(" ")
  }
}
