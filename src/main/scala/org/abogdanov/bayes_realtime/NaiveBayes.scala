package org.abogdanov.bayes_realtime

import java.io.{File, FilenameFilter}

import jline.ConsoleReader
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object NaiveBayesExample extends App {

	if (args.length != 1) {
		println("Usage: NaiveBayesExample <inputDir>")
		System.exit(1)
	}
	
	val sc = new SparkContext("local[4]", "NaiveBayesApp")
	val dataDir = args.head
	val trainedNaiveBayesModel = trainNaiveBayesModel(dataDir)
	console(trainedNaiveBayesModel)

	/**
	 * Continuing prompt of the sentence to classify
	 */
	def console(naiveBayesAndDictionaries: NaiveBayesAndDictionaries) = {
		println("Enter 'q' to quit")
		val consoleReader = new ConsoleReader()
		while ( {
			consoleReader.readLine("predict> ") match {
				case s if s == "q" => false
				case sentence: String =>
					predict(naiveBayesAndDictionaries, sentence)
					true
				case _ => true
			}
		}) {}

		sc.stop()
	}

	/**
	 * Classify the string using trained model
	 */
	def predict(trainedNaiveBayesModel: NaiveBayesAndDictionaries, input: String) = {
		// Tokenize and stem the string
		val tokens = Tokenizer.tokenize(input)
		// Compute TF-IDF
		val tfIdfs = trainedNaiveBayesModel.termDictionary.tfIdfs(tokens, trainedNaiveBayesModel.idfs)
		val vector = trainedNaiveBayesModel.termDictionary.vectorize(tfIdfs)
		val labelId = trainedNaiveBayesModel.model.predict(vector)

		// Convert the category from double and print it
		println("Label: " + trainedNaiveBayesModel.labelDictionary.valueOf(labelId.toInt))
	}

	/**
	 * Train the model using TSV files from input directory
	 */
	def trainNaiveBayesModel(directory: String) = {
		val inputFiles = new File(directory).list()
		val fullFileNames = inputFiles.map(directory + "/" + _)

		val docs = TsvParser.parseAll(fullFileNames)
		val termDocs = Tokenizer.tokenizeAll(docs)

		// Put it in the Spark
		val termDocsRdd = sc.parallelize[TermDoc](termDocs.toSeq)
		val numDocs = termDocs.size

		// Create "term => id" and "id => term" dictionaries
		val terms = termDocsRdd.flatMap(_.terms).distinct().collect().sortBy(identity)
		val termDict = new Dictionary(terms)

		val labels = termDocsRdd.flatMap(_.labels).distinct().collect()
		val labelDict = new Dictionary(labels)

		// Generate vectors for IDF and compute TF-IDF
		val idfs = (termDocsRdd.flatMap(termDoc => termDoc.terms.map((termDoc.doc, _))).distinct().groupBy(_._2) collect {
			// ToDo: Remove the feature if it is present in less than 3 documents
			case (term, doc)
				if doc.size > 0 =>
				term -> (numDocs.toDouble / doc.size.toDouble)
		}).collect().toMap

		val tfidfs = termDocsRdd flatMap {
			termDoc =>
				val termPairs = termDict.tfIdfs(termDoc.terms, idfs)
				// we consider here that a document only belongs to the first label
				termDoc.labels.headOption.map {
					label =>
						val labelId = labelDict.indexOf(label).toDouble
						val vector = Vectors.sparse(termDict.count, termPairs)
						LabeledPoint(labelId, vector)
				}
		}

		val model = NaiveBayes.train(tfidfs)
		NaiveBayesAndDictionaries(model, termDict, labelDict, idfs)
	}
}

case class NaiveBayesAndDictionaries(model: NaiveBayesModel,
                                     termDictionary: Dictionary,
                                     labelDictionary: Dictionary,
                                     idfs: Map[String, Double])