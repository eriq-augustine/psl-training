package com.eriqaugustine;

import edu.umd.cs.psl.application.inference.MPEInference;
import edu.umd.cs.psl.config.ConfigBundle;
import edu.umd.cs.psl.config.EmptyBundle;
import edu.umd.cs.psl.database.Database;
import edu.umd.cs.psl.database.DataStore;
import edu.umd.cs.psl.database.Partition;
import edu.umd.cs.psl.database.ReadOnlyDatabase;
import edu.umd.cs.psl.database.loading.Inserter;
import edu.umd.cs.psl.database.rdbms.RDBMSDataStore;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver;
import edu.umd.cs.psl.database.rdbms.driver.H2DatabaseDriver.Type;
import edu.umd.cs.psl.groovy.PSLModel;
import edu.umd.cs.psl.model.argument.ArgumentType;
import edu.umd.cs.psl.model.argument.DoubleAttribute;
import edu.umd.cs.psl.model.argument.GroundTerm;
import edu.umd.cs.psl.model.atom.GroundAtom;
import edu.umd.cs.psl.model.atom.QueryAtom;
import edu.umd.cs.psl.model.function.ExternalFunction;
import edu.umd.cs.psl.ui.loading.InserterUtils;
import edu.umd.cs.psl.util.database.Queries;

import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

ConfigBundle config = new EmptyBundle();
DataStore data = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, Paths.get("temp", "db").toString(), true), config);

PSLModel model = new PSLModel(this, data)

model.add predicate: "SourceSim", types: [ArgumentType.UniqueID, ArgumentType.UniqueID, ArgumentType.Double];
model.add predicate: "TargetSim", types: [ArgumentType.UniqueID, ArgumentType.UniqueID, ArgumentType.Double];
model.add predicate: "Link", types: [ArgumentType.UniqueID, ArgumentType.UniqueID];

model.add function: "Echo", implementation: new Echo()
model.add function: "Echo2", implementation: new Echo2()
model.add function: "IsSimilar", implementation: new IsSimilar()

// Pops assertion: edu.umd.cs.psl.database.rdbms.Formula2SQL.visitFunctionalAtom(Formula2SQL.java:99)
// model.add rule: ( Sim(A1, A2, S1) & Sim(B1, B2, S2) & LINK(A1, B1) & Echo(S1) & Echo(S2) ) >> Link(A2, B2), weight: 2;

// model.add rule: ( Sim(A1, A2, S1) & Sim(B1, B2, S2) & LINK(A1, B1) & IsSimilar(S1, S2) ) >> Link(A2, B2), weight: 2;

// Semetry?

// model.add rule: ( Sim(A1, A2, S1) & Sim(B1, B2, S2) & LINK(A1, B1) & Echo2(S1, S1) & Echo2(S2, S2) ) >> Link(A2, B2), weight: 10;
// model.add rule: ( Sim(A1, A2, S1) & Sim(B1, B2, S2) & LINK(A1, B1) & Echo2(S1, S1) & Echo2(S2, S2) & (A2 - B2) ) >> Link(A2, B2), weight: 10;

model.add rule: ( SourceSim(A1, A2, S1) & TargetSim(B1, B2, S2) & LINK(A1, B1) & Echo2(S1, S1) & Echo2(S2, S2) ) >> Link(A2, B2), weight: 10;
// Symetric rule? >> LINK(A!, B1)

// TODO(eriq): Prior does not make sense.
// TODO(eriq): Self links?
// model.add rule: ~Link(A, B), weight: 1; // Prior
model.add rule: ~Link(A, B), weight: 1; // Prior

Partition evidencePartition = new Partition(0);
Partition targetPartition = new Partition(1);

Inserter sourceSimInserter = data.getInserter(SourceSim, evidencePartition);
InserterUtils.loadDelimitedData(sourceSimInserter, "data/test2/src_sims");
// InserterUtils.loadDelimitedData(sourceSimInserter, "data/src_sims");
// InserterUtils.loadDelimitedData(sourceSimInserter, "data/src_sims_00");
// InserterUtils.loadDelimitedData(sourceSimInserter, "data/test/simple_sims_00");

Inserter targetSimInserter = data.getInserter(TargetSim, evidencePartition);
// InserterUtils.loadDelimitedData(targetSimInserter, "data/tgt_sims_00");
// InserterUtils.loadDelimitedData(targetSimInserter, "data/tgt_sims");
InserterUtils.loadDelimitedData(targetSimInserter, "data/test2/tgt_sims");

Inserter linkInserter = data.getInserter(Link, evidencePartition);
InserterUtils.loadDelimitedData(linkInserter, "data/test2/links");
// InserterUtils.loadDelimitedData(linkInserter, "data/links");
// InserterUtils.loadDelimitedData(linkInserter, "data/obs_links_fold_00");
// InserterUtils.loadDelimitedData(linkInserter, "data/test/simple_links_00");

// Fill in all the links where they do not already exist.
// TODO(eriq): There must be a better way of getting the evidince Sims.
Database tempDB = data.getDatabase(evidencePartition);

Set<GroundAtom> oldLinks = new HashSet<GroundAtom>();
for (GroundAtom link: Queries.getAllAtoms(tempDB, Link)) {
   oldLinks.add(link);
}

Set<QueryAtom> newLinks = new HashSet<QueryAtom>();
for (GroundAtom sourceSim : Queries.getAllAtoms(tempDB, SourceSim)) {
   for (GroundAtom targetSim : Queries.getAllAtoms(tempDB, TargetSim)) {
      QueryAtom newAtom = new QueryAtom(Link, sourceSim.getArguments()[0], targetSim.getArguments()[0]);
      if (!oldLinks.contains(newAtom)) {
         newLinks.add(newAtom);
      }

      newAtom = new QueryAtom(Link, sourceSim.getArguments()[0], targetSim.getArguments()[1]);
      if (!oldLinks.contains(newAtom)) {
         newLinks.add(newAtom);
      }

      newAtom = new QueryAtom(Link, sourceSim.getArguments()[1], targetSim.getArguments()[0]);
      if (!oldLinks.contains(newAtom)) {
         newLinks.add(newAtom);
      }

      newAtom = new QueryAtom(Link, sourceSim.getArguments()[1], targetSim.getArguments()[1]);
      if (!oldLinks.contains(newAtom)) {
         newLinks.add(newAtom);
      }
   }
}
tempDB.close();

Inserter targetLinkInserter = data.getInserter(Link, targetPartition);
for (QueryAtom newLink : newLinks) {
   targetLinkInserter.insert(newLink.getArguments()[0], newLink.getArguments()[1]);
}

Database db = data.getDatabase(targetPartition, [SourceSim, TargetSim] as Set, evidencePartition);

// TEST
println "TEST";
println model;

MPEInference inferenceApp = new MPEInference(model, db, config);
inferenceApp.mpeInference();
inferenceApp.close();

for (GroundAtom atom : Queries.getAllAtoms(db, Link)) {
   System.out.println(String.format("%s\t%3.2f", atom.toString(), atom.getValue()));
}

// TEST
println "END";

// Echo back a double value.
// Pops an assertion. Possible because it only takes one arg.
class Echo implements ExternalFunction {
   @Override
   public int getArity() {
      return 1;
   }

   @Override
   public ArgumentType[] getArgumentTypes() {
      return [ArgumentType.Double].toArray();
   }

   @Override
   public double getValue(ReadOnlyDatabase db, GroundTerm... args) {
      return ((DoubleAttribute)args[0]).getValue();
   }
}

// Echo back a double value.
// Trying for Echo() behavior, but avoiding the assertion.
// Just ignore the second param.
class Echo2 implements ExternalFunction {
   @Override
   public int getArity() {
      return 2;
   }

   @Override
   public ArgumentType[] getArgumentTypes() {
      return [ArgumentType.Double, ArgumentType.Double].toArray();
   }

   @Override
   public double getValue(ReadOnlyDatabase db, GroundTerm... args) {
      return ((DoubleAttribute)args[0]).getValue();
   }
}

class IsSimilar implements ExternalFunction {
   @Override
   public int getArity() {
      return 2;
   }

   @Override
   public ArgumentType[] getArgumentTypes() {
      return [ArgumentType.Double, ArgumentType.Double].toArray();
   }

   @Override
   public double getValue(ReadOnlyDatabase db, GroundTerm... args) {
      return (((DoubleAttribute)args[0]).getValue() > 0.7 && ((DoubleAttribute)args[0]).getValue() > 0.7) ? 1.0 : 0.0;
   }
}
