# Big Data Coursework
The objective of this project is to create a search engine that takes in keywords entered by the user and list out the top ten search results from a list of articles from a database. This project implements Spark Map-Reduce to complete the project.

## Instruction

Be reminded, this is only for test.
Before we start, make sure you've already installed hadoop on your laptop.
1. Store the document file to $INPUT_PATH;
2. Run `mvn install`;
3. Run `hadoop jar $PATH_TO_UoG-BD-MR-1.0-SNAPSHOT.jar MapReduce.DataPreprocess $INPUT_DOC_PATH $PREPROCESS_OUTDIR`
4. Then run `hadoop jar $PATH_TO_UoG-BD-MR-1.0-SNAPSHOT.jar MapReduce.PageRanking $PREPROCESS_OUTDIR $PAGERANKING_OUTDIR`
5. If everything w orks fine, the result will be stored in `$OUTPUT_DIR`