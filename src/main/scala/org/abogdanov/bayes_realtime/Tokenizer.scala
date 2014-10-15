package org.abogdanov.bayes_realtime

import org.apache.lucene.util.Version

object Tokenizer {
	val LuceneVersion = Version.LUCENE_48

	def tokenizeAll(docs: Iterable[Document]) = docs.map(tokenize)

	def tokenize(doc: Document): TermDoc = TermDoc(doc.docId, Set(doc.category), tokenize(doc.body))

	def tokenize(content: String): Seq[String] = {
		// ToDo: implement stemming
		//		val tReader = new StringReader(content)
		//		val analyzer = new EnglishAnalyzer(LuceneVersion)
		//		val tStream = analyzer.tokenStream("contents", tReader)
		//		val term = tStream.addAttribute(classOf[CharTermAttribute])
		//		tStream.reset()
//		println("content " + content)
		val a: Seq[String] = content.split(" ").toSeq

		//		val result = mutable.ArrayBuffer.empty[String]
		//		while(tStream.incrementToken()) {
		//			val termValue = term.toString
		//			if (!termValue.matches(".*[\\d\\.].*")) {
		//				result += term.toString
		//			}
		//		}
		a
	}
}

case class TermDoc(doc: String, labels: Set[String], terms: Seq[String])