package org.hathitrust.htrc.tools.findcitations

/**
  * Describes an exceptional condition involving a pairtree volume
  *
  * @param msg   The error message
  * @param cause The exception describing the reason for the error
  */
case class HTRCPairtreeDocumentException(msg: String, cause: Throwable) extends Exception(msg, cause)