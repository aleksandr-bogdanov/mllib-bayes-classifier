package org.abogdanov.bayes_realtime

import scala.collection.mutable
import scala.io.Source

// Parse TSV file with prefilled category for the message.
// CATEGORY \t Message of many words
object TsvParser {
	// Filter every record that has wrong category
	def possibleCategories = Seq("ham", "spam")

	def parseAll(tsvFilesPaths: Iterable[String]) = tsvFilesPaths.flatMap(parse)

	def parse(tsvFilePath: String) = {
		// Resulting list of Documents
		val docs = mutable.ArrayBuffer.empty[Document]
		var currentDoc: Document = null
		var currentId = 0

		// Creating new Document from every line of input TSV file
		for (line <- Source.fromFile(tsvFilePath).getLines()) {
			val row: Array[String] = line.split("\t")
			currentDoc = Document(currentId.toString, row(0), row(1))
			currentId += 1
			docs += currentDoc
		}

		docs
	}
}

// Case class for storing the data
case class Document(docId: String, category: String = "", body: String = "")
