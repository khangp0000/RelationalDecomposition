package uwdb.discovery.dependency.approximate.search;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import uwdb.discovery.dependency.approximate.common.BitSetMatrixGraph;
import uwdb.discovery.dependency.approximate.common.dependency.AcyclicSchema;
import uwdb.discovery.dependency.approximate.common.dependency.JoinDependency;
import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;
import uwdb.discovery.dependency.approximate.entropy.NewSmallDBInMemory;
import uwdb.discovery.dependency.approximate.entropy.SmallDBInMemory;
import uwdb.discovery.dependency.approximate.entropy.NewSmallDBInMemory.DecompositionInfo;

public class AcyclicSchemaEnumerator implements Iterator<AcyclicSchema> {

	Set<JoinDependency> JDCollection;
	JoinDependency[] JDArr;
	BitSetMatrixGraph JDGraph;
	int numJDs;
	int numAtts;
	double totalEntropy;
	double AvgKeySize;
	int MaxKeySize;
	int MinKeySize;

	Set<IAttributeSet> P;
	Set<IAttributeSet> inQ;
	Queue<IAttributeSet> Q;

	@Override
	public boolean hasNext() {
		return !Q.isEmpty();
	}

	@Override
	public AcyclicSchema next() {
		// remove from Q
		IAttributeSet JDsInAcyclicSchema = Q.poll();
		inQ.remove(JDsInAcyclicSchema);
		// add to P
		P.add(JDsInAcyclicSchema);
		// for returning
		AcyclicSchema retVal = getAcyclicSchema(JDsInAcyclicSchema);
		AttributeSet cast = ((AttributeSet) JDsInAcyclicSchema);
		// for all nodes not in the independent set JDsInAcyclicSchema
		for (int i = cast.nextUnSetAttribute(0); i >= 0 && i < numJDs; i =
				cast.nextUnSetAttribute(i + 1)) {
			IAttributeSet extensioni = JDsInAcyclicSchema.clone();
			JDGraph.addNodeAndExtend(extensioni, i);
			if (!P.contains(extensioni) && !inQ.contains(extensioni)) {
				Q.add(extensioni);
				inQ.add(extensioni);
			}
		}
		return retVal;
	}

	private AcyclicSchema getAcyclicSchema(IAttributeSet schemaAsSet) {
		AcyclicSchema retVal = new AcyclicSchema(numAtts, totalEntropy);
		for (int i = schemaAsSet.nextAttribute(0); i >= 0; i = schemaAsSet.nextAttribute(i + 1)) {
			retVal.addJD(JDArr[i]);
		}
		return retVal;
	}

	private static double log_10_2 = Math.log10(2.0);

	public static double log2(double val) {
		return (Math.log10(val) / log_10_2);
	}

	public AcyclicSchemaEnumerator(String pathToJDFile, int numAtts, int numRows) {
		this.numAtts = numAtts;
		this.totalEntropy = log2(numRows);
		AvgKeySize = 0;
		MaxKeySize = Integer.MIN_VALUE;
		MinKeySize = Integer.MAX_VALUE;

		JDCollection = new HashSet<JoinDependency>();
		readJDsFromFile(pathToJDFile, JDCollection);
		numJDs = JDCollection.size();
		AvgKeySize = (AvgKeySize / (double) numJDs);

		// copy to array
		JDArr = new JoinDependency[JDCollection.size()];
		int i = 0;
		for (JoinDependency JD : JDCollection) {
			JDArr[i++] = JD;
		}
		createGraphFromJDs();

		P = new HashSet<IAttributeSet>();
		inQ = new HashSet<IAttributeSet>();
		Q = new LinkedList<IAttributeSet>();

		IAttributeSet firstIndSep = new AttributeSet(numJDs);
		JDGraph.extendToMaxIndependentSet(firstIndSep);
		inQ.add(firstIndSep);
		Q.add(firstIndSep);
	}

	private void createGraphFromJDs() {
		JDGraph = new BitSetMatrixGraph(numJDs);
		for (int i = 0; i < numJDs; i++) {
			for (int j = i + 1; j < numJDs; j++) {
				if (!AcyclicSchema.isCompatible(JDArr[i], JDArr[j]))
					JDGraph.addUndirectedEdge(i, j);
			}
		}
	}

	// receives 2, 5, 7, 8, 10, 12 and returns appropriuate attribute set
	private static IAttributeSet parseStringToAttSet(int numAtts, String strAttSet) {
		// String membersStr = strAttSet.trim().substring(1, strAttSet.length()-1); //2, 5, 7, 8,
		// 10, 12
		String membersStr = strAttSet.trim();
		IAttributeSet attSet = new AttributeSet(numAtts);
		if (strAttSet.trim().isEmpty())
			return attSet;
		try {
			String[] members = membersStr.split(",");
			for (String member : members) {
				Integer att = Integer.valueOf(member.trim());
				attSet.add(att);
			}
		} catch (Exception e) {
			System.out.println("error");
		}
		return attSet;
	}

	// 13,{{4, 9}|{1},{6, 11},{2, 5, 7, 8, 10, 12},{3}},8.881784197001252E-16
	private JoinDependency JDFromLine(String line) {
		String[] twoStringArray = line.split(",", 2);
		Integer numAtts = Integer.valueOf(twoStringArray[0].trim());


		int firstParenth = line.indexOf("{");
		int lastParenth = line.lastIndexOf("}");
		String JDStr = line.substring(firstParenth + 1, lastParenth); // holds: {4, 9}|{1},{6,
																		// 11},{2, 5, 7, 8, 10,
																		// 12},{3}
		int indexOfBar = JDStr.indexOf("|");
		String JDlhs = JDStr.substring(0, indexOfBar).trim(); // {4, 9}
		String JDComponents = JDStr.substring(indexOfBar + 1, JDStr.length()); // {1},{6, 11},{2, 5,
																				// 7, 8, 10, 12},{3}
		// String[] JDLeftRight = JDStr.split("|");

		// String JDlhs = JDLeftRight[0].trim(); //{4, 9}
		// String JDComponents = JDLeftRight[1].trim(); //{1},{6, 11},{2, 5, 7, 8, 10, 12},{3}
		IAttributeSet lhs = parseStringToAttSet(numAtts, JDlhs.substring(1, JDlhs.length() - 1));
		AvgKeySize += lhs.cardinality();
		MaxKeySize = (lhs.cardinality() > MaxKeySize ? lhs.cardinality() : MaxKeySize);
		MinKeySize = (lhs.cardinality() < MinKeySize ? lhs.cardinality() : MinKeySize);
		Pattern regex = Pattern.compile("\\{(.*?)\\}");

		// String[] components = JDComponents.split(",");

		JoinDependency JD = new JoinDependency(lhs);
		Matcher regexMatcher = regex.matcher(JDComponents);

		while (regexMatcher.find()) {
			String component = regexMatcher.group(1);
			IAttributeSet JDComponent = parseStringToAttSet(numAtts, component);
			JD.addComponent(JDComponent);
		}
		/*
		 * for(String component: components) { IAttributeSet JDComponent =
		 * parseStringToAttSet(numAtts, component); JD.addComponent(JDComponent); }
		 */

		int lastIndexOfComma = line.lastIndexOf(",");
		String measureStr = line.substring(lastIndexOfComma + 1);
		double JDMeasure = Double.valueOf(measureStr.trim());
		JD.setMeasure(JDMeasure);
		return JD;


	}

	public int readJDsFromFile(String filePath, Set<JoinDependency> JDs) {
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(filePath));
			String line = reader.readLine();
			while (line != null) {
				JoinDependency JD = JDFromLine(line);
				JDs.add(JD);
				line = reader.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return JDs.size();
	}

	private static boolean TEST_SPURIOUS_TUPLES = true;
	private static long TIME_INTERVAL = 2 * 60 * 1000; // 2 minute

	public static void main(String[] args) {
		String pathToSepDirectory = args[0];
		String outputDirPath = args[1];
		int numAtts = Integer.valueOf(args[2]);
		int numRows = Integer.valueOf(args[3]);
		long timeout = Long.valueOf(args[4]) * 1000;
		String dataFilePath = args[5];

		File sepFileDir = new File(pathToSepDirectory);
		File[] sepFiles = sepFileDir.listFiles();
		Arrays.sort(sepFiles, new Comparator<File>() {
			public int compare(File f1, File f2) {
				Path pathObjTof1 = Paths.get(f1.getPath());
				Path pathObjTof2 = Paths.get(f2.getPath());
				double thresh1 = getThresholdFromSepFile(pathObjTof1.getFileName().toString());
				double thresh2 = getThresholdFromSepFile(pathObjTof2.getFileName().toString());
				return Double.compare(thresh1, thresh2);
				// return Long.valueOf(f1.length()).compareTo(f2.length());
			}
		});
		CSVPrinter csvPrinter = null;
		Writer writer;
		try {
			String outputFileName = sepFileDir.getName() + ".enum.out.csv";
			Path outputPath = Paths.get(outputDirPath, outputFileName);
			writer = Files.newBufferedWriter(outputPath, StandardOpenOption.CREATE);
			csvPrinter =
					/*
					 * new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader ("#Attribtues", "#Rows",
					 * "threshold", "JD Key Size: MAX", "JD Key Size: MIN", "JD Key Size: AVG",
					 * "Elapsed Time (sec)", "#Schemas Returned", "MIN(JDMeasure)",
					 * "MAX(JDMeasure)", "MIN(MAX(Cluster))", "MIN(MAX(Separator))",
					 * "Max #Clusters", "AVG #JDs in Schema", "MIN(spuriousTuples)",
					 * "MAX(spuriousTuples)", "MIN(MAX(RelationSize)",
					 * "MAX(DecompositionSizeinTuples)", "MIN(DecompositionSizeinTuples)",
					 * "MAX(DecompositionSizeInCells)", "MIN(DecompositionSizeInCells)"));
					 */

					new CSVPrinter(writer,
							CSVFormat.DEFAULT.withHeader("#Attribtues", "#Rows", "JDMeasure",
									"Separator Size", "Elapsed Time (sec)", "#Schemas Returned",
									"Largest Relation", "#Relations", "#Spurious Tuples",
									"DecompositionSizeinTuples", "DecompositionSizeInCells"));


			try (NewSmallDBInMemory smallDB =
					new NewSmallDBInMemory(dataFilePath, numAtts, false)) {
				for (File sepFile : sepFiles) {
					// read threshold from sep file name

					Path pathObjToSepFile = Paths.get(sepFile.getPath());
					double threshold =
							getThresholdFromSepFile(pathObjToSepFile.getFileName().toString());
					enumerateSingleSepFile(sepFile.getPath(), csvPrinter, numAtts, numRows, timeout,
							threshold, TEST_SPURIOUS_TUPLES, dataFilePath, smallDB);
				}
				csvPrinter.close();
				// boolean testForSpuriousTuples, String pathToData
			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static double getThresholdFromSepFile(String sepFileName) {
		String REGEX = ".THRESH.(.*?).sep";
		Pattern pattern = Pattern.compile(REGEX);
		// Path pathObjToSepFile = Paths.get(sepFile.getPath());
		Matcher matcher = pattern.matcher(sepFileName);
		double threshold = 0;
		if (matcher.find()) {
			threshold = Double.valueOf(matcher.group(1));
			// System.out.println(matcher.group(1));
		}
		return threshold;

	}
	/*
	 * String pathToSepDirectory = args[0]; String outputDirPath = args[1]; int numAtts =
	 * Integer.valueOf(args[2]); int numRows = Integer.valueOf(args[3]); long timeout =
	 * Long.valueOf(args[4])*1000; double threshold = Double.valueOf(args[5]);
	 */

	public static void enumerateSingleSepFile(String pathToSepFile, CSVPrinter csvPrinter,
			int numAtts, int numRows, long timeout, double threshold, boolean testForSpuriousTuples,
			String pathToData, NewSmallDBInMemory smallDB) {

		System.out
				.println("Starting enumeration: file=" + pathToSepFile + " threshold=" + threshold);
		System.out.println("smallDB is " + (smallDB == null ? " null" : " not null"));
		/*
		 * String pathToSepFile = args[0]; String outputDirPath = args[1]; int numAtts =
		 * Integer.valueOf(args[2]); int numRows = Integer.valueOf(args[3]); long timeout =
		 * Long.valueOf(args[4])*1000;
		 * 
		 * Path inputPath = Paths.get(pathToSepFile); String inputFilename =
		 * inputPath.getFileName().toString(); String outputFileName = inputFilename+".out.csv";
		 * Path outputPath= Paths.get(outputDirPath, outputFileName);
		 * 
		 * CSVPrinter csvPrinter = null; Writer writer;
		 */


		try {
			/*
			 * writer = Files.newBufferedWriter(outputPath,StandardOpenOption.CREATE); csvPrinter =
			 * new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader ("#Attribtues", "#Rows",
			 * "Elapsed Time (sec)", "#Schemas Returned", "MIN(JDMeasure)", "MAX(JDMeasure)",
			 * "MIN(MAX(Cluster))", "AVG #JDs in Schema"));
			 */
			AcyclicSchemaEnumerator enumerator =
					new AcyclicSchemaEnumerator(pathToSepFile, numAtts, numRows);
			long firstStart = System.currentTimeMillis();
			long prevStart = System.currentTimeMillis();
			int schemasReturned = 0;
			double minSchemaMeasure = 0;
			double maxSchemaMeasure = Double.MAX_VALUE;
			int totalNumJDsInAllSchemas = 0;
			// information for saving schemas
			int maxNumClusters = 0;
			long minMaxClusterSize = Long.MAX_VALUE;
			long minMaxSeparator = Long.MAX_VALUE;
			ArrayList<AcyclicSchema> best_maxNumClusters = new ArrayList<AcyclicSchema>();
			ArrayList<AcyclicSchema> best_minMaxClusterSize = new ArrayList<AcyclicSchema>();
			ArrayList<AcyclicSchema> best_minMaxSeparator = new ArrayList<AcyclicSchema>();
			// information regarding spurious tuples
			long minSpuriousTuples = Long.MAX_VALUE;
			long maxSpuriousTuples = 0;
			// information regarding decomposition sizes
			long minMaxRelationSize = Long.MAX_VALUE;
			long maxSizeTuples = 0;
			long minSizeTuples = Long.MAX_VALUE;
			long maxSizeCells = 0;
			long minSizeCells = Long.MAX_VALUE;
			while (enumerator.hasNext()) {
				long currentTime = System.currentTimeMillis();
				if (currentTime - firstStart >= timeout) {
					System.out.println("Reached timeout: Exiting");
					break; // stop running
				}
				// if(System.currentTimeMillis()-prevStart >= TIME_INTERVAL) {
				prevStart = System.currentTimeMillis();
				long timeElapsed = (System.currentTimeMillis() - firstStart) / 1000;
				double AVGJDsInSchema = (double) totalNumJDsInAllSchemas / (double) schemasReturned;
				/*
				 * ("#Attribtues", "#Rows", "threshold", "JD Key Size: MAX", "JD Key Size: MIN",
				 * "JD Key Size: AVG", "Elapsed Time (sec)", "#Schemas Returned", "MIN(JDMeasure)",
				 * "MAX(JDMeasure)", "MIN(MAX(Cluster))", "MIN(MAX(Separator))", "Max #Clusters",
				 * "AVG #JDs in Schema", "MIN(spuriousTuples)", "MAX(spuriousTuples)",
				 * "MIN(MAX(RelationSize)", "MAX(DecompositionSizeinTuples)",
				 * "MIN(DecompositionSizeinTuples)", "MAX(DecompositionSizeInCells)",
				 * "MIN(DecompositionSizeInCells)"));
				 */

				/*
				 * csvPrinter.printRecord(numAtts, numRows, threshold, enumerator.MaxKeySize,
				 * enumerator.MinKeySize, enumerator.AvgKeySize, timeElapsed, schemasReturned,
				 * minSchemaMeasure, maxSchemaMeasure, minMaxClusterSize, minMaxSeparator,
				 * maxNumClusters, AVGJDsInSchema, minSpuriousTuples, maxSpuriousTuples,
				 * minMaxRelationSize, maxSizeTuples, minSizeTuples, maxSizeCells, minSizeCells);
				 * csvPrinter.flush();
				 */


				// }
				AcyclicSchema AS = enumerator.next();
				AS.getJoinTreeRepresentation();
				schemasReturned++;
				double ASMeasure = AS.getEstimatedMeasure();
				minSchemaMeasure = (ASMeasure < minSchemaMeasure ? ASMeasure : minSchemaMeasure);
				maxSchemaMeasure = (ASMeasure > minSchemaMeasure ? ASMeasure : maxSchemaMeasure);
				totalNumJDsInAllSchemas += AS.getNumJDs();
				int maxClusterSize = AS.getMaxCluster();
				int maxSeperatorSize = AS.getMaxSeparator();

				if (minMaxClusterSize > maxClusterSize) {
					minMaxClusterSize = maxClusterSize;
					best_minMaxClusterSize.clear();
					best_minMaxClusterSize.add(AS);
				} else if (minMaxClusterSize == maxClusterSize) {
					best_minMaxClusterSize.add(AS);
				}
				if (minMaxSeparator > maxSeperatorSize) {
					minMaxSeparator = maxSeperatorSize;
					best_minMaxSeparator.clear();
					best_minMaxSeparator.add(AS);
				} else if (minMaxSeparator == maxSeperatorSize) {
					best_minMaxSeparator.add(AS);
				}
				if (maxNumClusters < AS.numClusters()) {
					maxNumClusters = AS.numClusters();
					best_maxNumClusters.clear();
					best_maxNumClusters.add(AS);
				} else if (AS.numClusters() == maxNumClusters) {
					best_maxNumClusters.add(AS);
				}


				// print out stuff about the scheme
				System.out.println("Printin Scheme # " + schemasReturned);
				System.out.println(AS.toString());
				System.out.println("JMeasure: " + ASMeasure);
				System.out.println("Num of JDs: " + AS.getNumJDs());
				System.out.println("# clusters: " + AS.numClusters());
				System.out.println("Max cluster size: " + AS.getMaxCluster());
				System.out.println("Max sep size: " + AS.getMaxSeparator());


				long currSpurious = 0;
				long DecompositionSizeinTuples = 0;
				long DecompositionSizeInCells = 0;
				long largestRelation = 0;
				if (testForSpuriousTuples) {
					Set<IAttributeSet> seps = new HashSet<IAttributeSet>();
					Set<IAttributeSet> clusters = new HashSet<IAttributeSet>();
					AS.getSepsClusters(seps, clusters);
					DecompositionInfo dInfo;
					try {
						dInfo = smallDB.submitJobSynchronous(clusters);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						return;
					}
					currSpurious = dInfo.spuriousTuples;
					DecompositionSizeinTuples = dInfo.totalTuplesInDecomposition;
					DecompositionSizeInCells = dInfo.totalCellsInDecomposition;
					largestRelation = dInfo.largestRelation;
					minSpuriousTuples =
							(currSpurious < minSpuriousTuples) ? currSpurious : minSpuriousTuples;
					maxSpuriousTuples =
							(currSpurious > maxSpuriousTuples) ? currSpurious : maxSpuriousTuples;
					/*
					 * minMaxRelationSize, maxSizeTuples, minSizeTuples, maxSizeCells, minSizeCells
					 */
					minMaxRelationSize =
							(dInfo.largestRelation < minMaxRelationSize ? dInfo.largestRelation
									: minMaxRelationSize);
					maxSizeTuples = (dInfo.totalTuplesInDecomposition > maxSizeTuples
							? dInfo.totalTuplesInDecomposition
							: maxSizeTuples);
					minSizeTuples = (dInfo.totalTuplesInDecomposition < minSizeTuples
							? dInfo.totalTuplesInDecomposition
							: minSizeTuples);
					maxSizeCells = (dInfo.totalCellsInDecomposition > maxSizeCells
							? dInfo.totalCellsInDecomposition
							: maxSizeCells);
					minSizeCells = (dInfo.totalCellsInDecomposition < minSizeCells
							? dInfo.totalCellsInDecomposition
							: minSizeCells);

					System.out.println(" Data-intensive measurements:");
					System.out.println("# Spurious tuples: " + currSpurious + " total %: "
							+ ((double) currSpurious / (double) numRows) * 100.0);
					System.out.println(" Largest relation: " + dInfo.largestRelation);
					System.out.println(
							" total tuples in decomposition : " + dInfo.totalTuplesInDecomposition);
					System.out.println(
							" total cells in decomposition : " + dInfo.totalCellsInDecomposition);

				}


				/*
				 * "#Attribtues", "#Rows", "JDMeasure", "Max Separator Size", "Elapsed Time (sec)",
				 * "#Schemas Returned", "Largest Relation", "#Relations", "#Spurious Tuples",
				 * "DecompositionSizeinTuples", "DecompositionSizeInCells"
				 */
				csvPrinter.printRecord(numAtts, numRows, ASMeasure, AS.getMaxSeparator(),
						timeElapsed, schemasReturned, largestRelation, AS.numClusters(),
						currSpurious, DecompositionSizeinTuples, DecompositionSizeInCells);
				csvPrinter.flush();



			}

			long timeElapsed = (System.currentTimeMillis() - firstStart) / 1000;
			double AVGJDsInSchema = (double) totalNumJDsInAllSchemas / (double) schemasReturned;
			/*
			 * ("#Attribtues", "#Rows", "threshold", "JD Key Size: MAX", "JD Key Size: MIN",
			 * "JD Key Size: AVG", "Elapsed Time (sec)", "#Schemas Returned", "MIN(JDMeasure)",
			 * "MAX(JDMeasure)", "MIN(MAX(Cluster))", "MIN(MAX(Separator))", "Max #Clusters",
			 * "AVG #JDs in Schema", "MIN(spuriousTuples)", "MAX(spuriousTuples)",
			 * "MIN(MAX(RelationSize)", "MAX(DecompositionSizeinTuples)",
			 * "MIN(DecompositionSizeinTuples)", "MAX(DecompositionSizeInCells)",
			 * "MIN(DecompositionSizeInCells)"));
			 */
			/*
			 * csvPrinter.printRecord(numAtts, numRows, threshold, enumerator.MaxKeySize,
			 * enumerator.MinKeySize, enumerator.AvgKeySize, timeElapsed, schemasReturned,
			 * minSchemaMeasure, maxSchemaMeasure, minMaxClusterSize, minMaxSeparator,
			 * maxNumClusters, AVGJDsInSchema, minSpuriousTuples, maxSpuriousTuples,
			 * minMaxRelationSize, maxSizeTuples, minSizeTuples, maxSizeCells, minSizeCells);
			 * csvPrinter.flush();
			 */

			System.out.println("Finished Enumeration, file=" + pathToSepFile + ", threshold="
					+ threshold + ", smallDB=" + (smallDB == null ? "null" : " not null"));
			// conduct expensive tests for best acyclic schemas
			if (threshold <= 0.1 && smallDB != null) {
				System.out.println("Completed Enumeration, printing best acyclic schemas:");
				System.out.println("Printing " + MAX_BEST_PRINT
						+ " best schemas in terms of max num of clusters: " + maxNumClusters);
				printInfoForBestSchemas(smallDB, best_maxNumClusters, csvPrinter, threshold,
						maxNumClusters, minMaxClusterSize, minMaxSeparator);
				System.out.println("Printing " + MAX_BEST_PRINT
						+ " best schemas in terms of MIN(Max(Cluster Size)): " + minMaxClusterSize);
				printInfoForBestSchemas(smallDB, best_minMaxClusterSize, csvPrinter, threshold,
						maxNumClusters, minMaxClusterSize, minMaxSeparator);
				System.out.println("Printing " + MAX_BEST_PRINT
						+ " best schemas in terms of MIN(Max(Separator Size)): " + minMaxSeparator);
				printInfoForBestSchemas(smallDB, best_minMaxSeparator, csvPrinter, threshold,
						maxNumClusters, minMaxClusterSize, minMaxSeparator);

			}


			// csvPrinter.close();
		}

		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static int MAX_BEST_PRINT = 0;

	private static void printInfoForBestSchemas(NewSmallDBInMemory smallDB,
			List<AcyclicSchema> bestList, CSVPrinter csvPrinter, double threshold,
			long maxNumClusters, long minMaxClusterSize, long minMaxSeparator) {
		int i = 0;
		for (Iterator<AcyclicSchema> it = bestList.iterator(); it.hasNext()
				&& i < MAX_BEST_PRINT; i++) {
			AcyclicSchema currBest = it.next();
			System.out.println(currBest.toString());
			Set<IAttributeSet> seps = new HashSet<IAttributeSet>();
			Set<IAttributeSet> clusters = new HashSet<IAttributeSet>();
			currBest.getSepsClusters(seps, clusters);
			DecompositionInfo dInfo;
			try {
				dInfo = smallDB.submitJobSynchronous(clusters);
			} catch (Exception e1) {
				e1.printStackTrace();
				return;
			}
			long currSpurious = dInfo.spuriousTuples;
			System.out.println("Spurious tuples: " + currSpurious);
			System.out.println("Number of tuples in largest relation: " + dInfo.largestRelation);
			System.out.println("Number of tuples in smallest relation: " + dInfo.smallestRelation);
			System.out
					.println("Total tuples in decomposition: " + dInfo.totalTuplesInDecomposition);
			System.out.println("Total cells in decomposition: " + dInfo.totalCellsInDecomposition);

			try {
				csvPrinter.printRecord("NA", "NA", threshold, "NA", "NA", "NA", "NA", "NA", "NA",
						"NA", minMaxClusterSize, minMaxSeparator, maxNumClusters, "NA",
						currSpurious, dInfo.largestRelation, dInfo.smallestRelation,
						dInfo.totalTuplesInDecomposition, dInfo.totalTuplesInDecomposition,
						dInfo.totalCellsInDecomposition, dInfo.totalCellsInDecomposition);
				csvPrinter.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

}
