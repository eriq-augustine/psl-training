package edu.umd.cs.psl.er;

import edu.umd.cs.psl.er.evaluation.FileAtomPrintStream;
import edu.umd.cs.psl.er.evaluation.ModelEvaluation;
import edu.umd.cs.psl.er.similarity.DiceSimilarity;
import edu.umd.cs.psl.er.similarity.SameInitials;
import edu.umd.cs.psl.er.similarity.SameNumTokens;

import edu.umd.cs.psl.application.inference.MPEInference;
import edu.umd.cs.psl.application.learning.weight.maxlikelihood.MaxLikelihoodMPE;
import edu.umd.cs.psl.config.*;
import edu.umd.cs.psl.database.DataStore;
import edu.umd.cs.psl.database.Database;
import edu.umd.cs.psl.database.DatabasePopulator;
import edu.umd.cs.psl.database.Partition;
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type;
import edu.umd.cs.psl.groovy.*;
import edu.umd.cs.psl.groovy.experiments.ontology.*;
import edu.umd.cs.psl.model.argument.ArgumentType;
import edu.umd.cs.psl.model.argument.GroundTerm;
import edu.umd.cs.psl.model.argument.Variable;
import edu.umd.cs.psl.model.atom.GroundAtom;
import edu.umd.cs.psl.model.predicate.Predicate;
import edu.umd.cs.psl.ui.functions.textsimilarity.LevenshteinSimilarity;
import edu.umd.cs.psl.ui.loading.InserterUtils;
import edu.umd.cs.psl.util.database.Queries;

import java.io.FileReader;
import java.util.concurrent.TimeUnit;

/*
 * Start and end times for timing information.
 */
def startTime;
def endTime;

/*
 * First, we'll parse the command line arguments.
 */
if (args.size() < 1) {
   println "\nUsage: AuthorPaperER <data_dir> [ -l ]\n";
   return 1;
}
def datadir = args[0];
if (!datadir[datadir.size()-1].equals("/"))
   datadir += "/";
boolean learnWeights = false;
if (args.size() >= 2) {
   learnWeights = args[1..(args.size()-1)].contains("-l");
}
println "\n*** PSL ER EXAMPLE ***\n"
println "Data directory  : " + datadir;
println "Weight learning : " + (learnWeights ? "ON" : "OFF");


/*
 * We'll use the ConfigManager to access configurable properties.
 * This file is usually in <project>/src/main/resources/psl.properties
 */
ConfigManager cm = ConfigManager.getManager();
ConfigBundle cb = cm.getBundle("er");

DataStore data = new RDBMSDataStore(new H2DatabaseDriver(Type.Memory, "AuthorPaperER", true), cb);

/*** MODEL DEFINITION ***/
print "Creating the ER model ... "

PSLModel m = new PSLModel(this, data);

/*
 * These are the predicates for our model.
 * Predicates are precomputed.
 * Functions are computed online.
 * "Open" predicates are ones that must be inferred.
 */
m.add predicate: "authorName", types: [ArgumentType.UniqueID, ArgumentType.String];
m.add predicate: "paperTitle", types: [ArgumentType.UniqueID, ArgumentType.String];
m.add predicate: "authorOf",   types: [ArgumentType.UniqueID, ArgumentType.UniqueID];
m.add predicate: "sameAuthor", types: [ArgumentType.UniqueID, ArgumentType.UniqueID];
m.add predicate: "samePaper",  types: [ArgumentType.UniqueID, ArgumentType.UniqueID];

m.add function: "simName",       implementation: new LevenshteinSimilarity(0.5);
m.add function: "simTitle",      implementation: new DiceSimilarity(0.5);
m.add function: "sameInitials",  implementation: new SameInitials();
m.add function: "sameNumTokens", implementation: new SameNumTokens();

/*
 * Set comparison functions operate on sets and return a scalar.
 */
// HACK(eriq): Can't get this to work vis-a-vis preloading the target predicates.
// m.add setcomparison: "sameAuthorSet", using: SetComparison.CrossEquality, on : sameAuthor;

/*
 * Now we can put everything together by defining some rules for our model.
 */

/*
 * Here are some basic rules.
 */
// similar names => same author
m.add rule : (authorName(A1,N1) & authorName(A2,N2) & simName(N1,N2)) >> sameAuthor(A1,A2), weight : 1.0;
// similar titles => same paper
m.add rule : (paperTitle(P1,T1) & paperTitle(P2,T2) & simTitle(T1,T2) ) >> samePaper(P1,P2),  weight : 1.0;

/*
 * Here are some relational rules.
 * To see the benefit of the relational rules, comment this section out and re-run the script.
 */
// if two references share a common publication, and have the same initials, then => same author

m.add rule : (authorOf(A1,P1)   & authorOf(A2,P2)   & samePaper(P1,P2) &
              authorName(A1,N1) & authorName(A2,N2) & sameInitials(N1,N2)) >> sameAuthor(A1,A2), weight : 1.0;
// if two papers have a common set of authors, and the same number of tokens in the title, then => same paper
// HACK(eriq): See comment above about sameAuthorSet.
// m.add rule : (sameAuthorSet({P1.authorOf(inv)},{P2.authorOf(inv)}) & paperTitle(P1,T1) & paperTitle(P2,T2) &
//              sameNumTokens(T1,T2)) >> samePaper(P1,P2),  weight : 1.0;

/*
 * Now we'll add a prior to the open predicates.
 */
/* NOTE(eriq): Is this right?
m.add Prior.Simple, on : sameAuthor, weight: 1E-6;
m.add Prior.Simple, on : samePaper,  weight: 1E-6;
*/
m.add rule: ~sameAuthor(A, B), weight: 1E-6;
m.add rule: ~samePaper(A, B), weight: 1E-6;

/*
 * We'll also set the activation threshold
 * (Note: the default activation threshold is 0, but we'll override that for this project.)
 */
// NOTE(eriq): I don't think this exists anymore...
// m.setDefaultActivationParameter(1E-10);

println "done!"

/*** LOAD DATA ***/
println "Creating a new DB and loading data:"

/*
 * The setup command instructs the DB to use the H2 driver.
 * It can also tell it to use memory as its backing store, or alternately a
 * specific directory in the file system. If neither is specified, the default
 * location is a file in the project root directory.
 * NOTE: In our experiments, we have found that using the hard drive performed
 * better than using main memory, though this may vary from system to system.
 */
//DataStore data = new RelationalDataStore(m);
//data.setup db: DatabaseDriver.H2;
//data.setup db: DatabaseDriver.H2, type: "memory";
//data.setup db: DatabaseDriver.H2, folder: "/tmp/";

/*
 * These are just some constants that we'll use to reference data files and DB partitions.
 * To change the dataset (e.g. big, medium, small, tiny), change the dir variable.
 */
int trainingFold = 0;
int testingFold = 1;

int evidenceTrainingPartitionId = 1;
int evidenceTestingPartitionId = 2;
int targetTrainingPartitionId = 3;
int targetTestingPartitionId = 4;

Partition evidenceTrainingPartition = new Partition(evidenceTrainingPartitionId);
Partition evidenceTestingPartition = new Partition(evidenceTestingPartitionId);
Partition targetTrainingPartition = new Partition(targetTrainingPartitionId);
Partition targetTestingPartition = new Partition(targetTestingPartitionId);

/*
 * Now we'll load some data from tab-delimited files into the DB.
 * Note that the second parameter to each call to loadFromFile() determines the DB partition.
 */
def sep = java.io.File.separator;
def insert;

/*
 * We start by reading in the non-target (i.e. evidence) predicate data.
 */
for (Predicate p1: [authorName, paperTitle, authorOf]) {
   String trainFile = datadir + p1.getName() + "." + trainingFold + ".txt";
   print "  Reading " + trainFile + " ... ";
   insert = data.getInserter(p1, evidenceTrainingPartition);
   InserterUtils.loadDelimitedData(insert, trainFile);
   println "done!"

   String testFile = datadir + p1.getName() + "." + testingFold + ".txt";
   print "  Reading " + testFile + " ... ";
   insert = data.getInserter(p1, evidenceTestingPartition);
   InserterUtils.loadDelimitedData(insert, testFile);
   println "done!"
}

/*
 * Now we read the target predicate data.
 */
for (Predicate p3 : [sameAuthor, samePaper]) {
   //training data
   String trainFile = datadir + p3.getName() + "." + trainingFold + ".txt";
   print "  Reading " + trainFile + " ... ";
   insert = data.getInserter(p3, targetTrainingPartition)
   InserterUtils.loadDelimitedDataTruth(insert, trainFile);
   println "done!"

   //testing data
   String testFile = datadir + p3.getName() + "." + testingFold + ".txt";
   print "  Reading " + testFile + " ... ";
   insert = data.getInserter(p3, targetTestingPartition)
   InserterUtils.loadDelimitedDataTruth(insert, testFile);
   println "done!"
}

/*** WEIGHT LEARNING ***/

// HACK(eriq): This had to be all changed, I have zero confidence it will run correctly.
/*
 * This is how we perform weight learning.
 * Note that one must specify the open predicates and the evidence and target partitions.
 */
if (learnWeights) {
   /*
    * We need to setup some weight learning parameters.
    */
   /* HACK(eriq): I don't think WeightLearningConfiguration exists anymore... just try MaxLikelihoodMPE.
   def learningConfig = new WeightLearningConfiguration();
   learningConfig.setLearningType(WeightLearningConfiguration.Type.LBFGSB);   // limited-memory BFGS optimization
   learningConfig.setPointMoveConvergenceThres(1E-5);                         // convergence threshold
   learningConfig.setMaxOptIterations(100);                                   // maximum iterations
   learningConfig.setParameterPrior(1);                                       // 1 / variance
   learningConfig.setRuleMean(0.1);                                           // init weight value for rules
   learningConfig.setUnitRuleMean(1.0);                                       // init weight value for priors
   learningConfig.setActivationThreshold(1E-10);                              // rule activation threshold
   */

   /*
    * Now we run the learning algorithm.
    */
   print "\nStarting weight learning ... ";
   startTime = System.nanoTime();
   // m.learn data, evidence:evidenceTrainingPartition, infered:targetTrainingPartition, close:[sameAuthor,samePaper], configuration:learningConfig, config:cb;

   Database db2 = data.getDatabase(targetTrainingPartition, [sameAuthor, samePaper] as Set);
   Database trueDataDB = data.getDatabase(evidenceTrainingPartition);

   MaxLikelihoodMPE weightLearning = new MaxLikelihoodMPE(m, db2, trueDataDB, cb);
   weightLearning.learn();
   weightLearning.close();

   endTime = System.nanoTime();
   println "done!"
   println "  Elapsed time: " + TimeUnit.NANOSECONDS.toSeconds(endTime - startTime) + " secs";

   /*
    * Now let's print the model to see the learned weights.
    */
   println "Learned model:\n";
   println m;
}

/*** INFERENCE ***/

/*
 * Note: to run evaluation of ER inference, we need to specify the total number of
 * pairwise combinations of authors and papers, which we pass to evaluateModel() in an array.
 * This is for memory efficiency, since we don't want to actually load truth data for all
 * possible pairs (though one could).
 *
 * To get the total number of possible combinations, we'll scan the author/paper reference files,
 * counting the number of lines.
 */
int[] authorCnt = new int[2];
int[] paperCnt = new int[2];
FileReader rdr = null;
for (int i = 0; i < 2; i++) {
   rdr = new FileReader(datadir + "AUTHORNAME." + i + ".txt");
   while (rdr.readLine() != null) authorCnt[i]++;
   println "Authors fold " + i + ": " + authorCnt[i];
   rdr = new FileReader(datadir + "PAPERTITLE." + i + ".txt");
   while (rdr.readLine() != null) paperCnt[i]++;
   println "Papers  fold " + i + ": " + paperCnt[i];
}

/*
 * Let's create an instance of our evaluation class.
 */
def eval = new ModelEvaluation(data);

/*
 * Evalute inference on the training set.
 */
print "\nStarting inference on the training fold ... ";
startTime = System.nanoTime();
// def trainingInference = m.mapInference(data.getDatabase(read: evidenceTrainingPartition), cb);

Database db = data.getDatabase(targetTrainingPartition, [authorName, paperTitle, authorOf] as Set, evidenceTrainingPartition);

// Populate sameAuthor targets.
Set<GroundTerm> authors = new HashSet<GroundTerm>();
for (GroundAtom author: Queries.getAllAtoms(db, authorName)) {
   authors.add(data.getUniqueID(author.getArguments()[0]));
}

Map<Variable, Set<GroundTerm>> popMap = new HashMap<Variable, Set<GroundTerm>>();
popMap.put(new Variable("AuthorA"), authors);
popMap.put(new Variable("AuthorB"), authors);

DatabasePopulator dbPop = new DatabasePopulator(db);
dbPop.populate((sameAuthor(AuthorA, AuthorB)).getFormula(), popMap);
dbPop.populate((sameAuthor(AuthorB, AuthorA)).getFormula(), popMap);

// Populate samePaper targets.
Set<GroundTerm> papers = new HashSet<GroundTerm>();
for (GroundAtom paper: Queries.getAllAtoms(db, paperTitle)) {
   papers.add(data.getUniqueID(paper.getArguments()[0]));
}

popMap = new HashMap<Variable, Set<GroundTerm>>();
popMap.put(new Variable("PaperA"), papers);
popMap.put(new Variable("PaperB"), papers);

dbPop = new DatabasePopulator(db);
dbPop.populate((samePaper(PaperA, PaperB)).getFormula(), popMap);
dbPop.populate((samePaper(PaperB, PaperA)).getFormula(), popMap);

// Infer
MPEInference inferenceApp = new MPEInference(m, db, cb);
inferenceApp.mpeInference();
inferenceApp.close();

endTime = System.nanoTime();
println "done!";
println "  Elapsed time: " + TimeUnit.NANOSECONDS.toSeconds(endTime - startTime) + " secs";

// HACK(eriq): Don't bother trying to do the custom evaluation.
// eval.evaluateModel(trainingInference, [sameAuthor, samePaper], targetTrainingPartitionId, [authorCnt[0]*(authorCnt[0]-1), paperCnt[0]*(paperCnt[0]-1)]);

/*
* Now evaluate inference on the testing set (to check model generalization).
*/
print "\nStarting inference on the testing fold ... ";
startTime = System.nanoTime();
// def testingInference = m.mapInference(data.getDatabase(read: evidenceTestingPartition), cb);

db = data.getDatabase(targetTestingPartition, [authorName, paperTitle, authorOf] as Set, evidenceTestingPartition);

// Populate sameAuthor targets.
authors = new HashSet<GroundTerm>();
for (GroundAtom author: Queries.getAllAtoms(db, authorName)) {
   authors.add(data.getUniqueID(author.getArguments()[0]));
}

popMap = new HashMap<Variable, Set<GroundTerm>>();
popMap.put(new Variable("AuthorA"), authors);
popMap.put(new Variable("AuthorB"), authors);

dbPop = new DatabasePopulator(db);
dbPop.populate((sameAuthor(AuthorA, AuthorB)).getFormula(), popMap);
dbPop.populate((sameAuthor(AuthorB, AuthorA)).getFormula(), popMap);

// Populate samePaper targets.
papers = new HashSet<GroundTerm>();
for (GroundAtom paper: Queries.getAllAtoms(db, paperTitle)) {
   papers.add(data.getUniqueID(paper.getArguments()[0]));
}

popMap = new HashMap<Variable, Set<GroundTerm>>();
popMap.put(new Variable("PaperA"), papers);
popMap.put(new Variable("PaperB"), papers);

dbPop = new DatabasePopulator(db);
dbPop.populate((samePaper(PaperA, PaperB)).getFormula(), popMap);
dbPop.populate((samePaper(PaperB, PaperA)).getFormula(), popMap);

// Infer
inferenceApp = new MPEInference(m, db, cb);
inferenceApp.mpeInference();
inferenceApp.close();

endTime = System.nanoTime();
println "done!";
println "  Elapsed time: " + TimeUnit.NANOSECONDS.toSeconds(endTime - startTime) + " secs";

// HACK(eriq): Don't bother trying to do the custom evaluation.
// eval.evaluateModel(testingInference, [sameAuthor, samePaper], targetTestingPartitionId, [authorCnt[1]*(authorCnt[1]-1), paperCnt[1]*(paperCnt[1]-1)]);
