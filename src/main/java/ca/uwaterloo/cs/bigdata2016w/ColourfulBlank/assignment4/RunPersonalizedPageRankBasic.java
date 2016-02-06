package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment4;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.HashSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.map.HMapIF;
import tl.lin.data.map.MapIF;

import com.google.common.base.Preconditions;

/**
 * <p>
 * Main driver program for running the basic (non-Schimmy) implementation of
 * PageRank.
 * </p>
 *
 * <p>
 * The starting and ending iterations will correspond to paths
 * <code>/base/path/iterXXXX</code> and <code>/base/path/iterYYYY</code>. As a
 * example, if you specify 0 and 10 as the starting and ending iterations, the
 * driver program will start with the graph structure stored at
 * <code>/base/path/iter0000</code>; final results will be stored at
 * <code>/base/path/iter0010</code>.
 * </p>
 *
 * @see RunPageRankSchimmy
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class RunPersonalizedPageRankBasic extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);

  private static enum PageRank {
    nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
  };

  // Mapper, no in-mapper combining.
  private static class MapClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

    // The neighbor to which we're sending messages.
    private static final IntWritable neighbor = new IntWritable();

    // Contents of the messages: partial PageRank mass.
    private static final PageRankNode intermediateMass = new PageRankNode();

    // For passing along node structure.
    private static final PageRankNode intermediateStructure = new PageRankNode();

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {
      // Pass along node structure.
      intermediateStructure.setNodeId(node.getNodeId());
      intermediateStructure.setType(PageRankNode.Type.Structure);
      intermediateStructure.setAdjacencyList(node.getAdjacenyList());

      context.write(nid, intermediateStructure);

      int massMessages = 0;

      // Distribute PageRank mass to neighbors (along outgoing edges).
        if (node.getAdjacenyList().length > 0) { //<<<<<<<< May wrong
          // Each neighbor gets an equal share of PageRank mass.
          ArrayListOfIntsWritable [] list = node.getAdjacenyList();
          // float mass = node.getPageRank() - (float) StrictMath.log(list.size());
          float [] masses = new float [list.length];
          // float mass = node.getPageRank() / (float) list.size();
          for (int i = 0; i < masses.length; i++){
            masses[i] = node.getPageRank(i) / (float) (list[i]).size();
          }

          context.getCounter(PageRank.edges).increment(list[i].size());

          // Iterate over neighbors.
          for (int i = 0; i < list.length; i++){
            for (int j = 0; j < list[i].length; j++){
              neighbor.set(list[i].get(j));
              intermediateMass.setNodeId(list[i].get(j));
              intermediateMass.setType(PageRankNode.Type.Mass);
              intermediateMass.setPageRank(masses[i]);
              // Emit messages with PageRank mass to neighbors.
              context.write(neighbor, intermediateMass);
              massMessages++;
            }
          }
          // for (int i = 0; i < list.size(); i++) {
          //   neighbor.set(list.get(i));
          //   intermediateMass.setNodeId(list.get(i));
          //   intermediateMass.setType(PageRankNode.Type.Mass);
          //   intermediateMass.setPageRank(mass);
          //   // Emit messages with PageRank mass to neighbors.
          //   context.write(neighbor, intermediateMass);
          //   massMessages++;
          // }
        }

      // Bookkeeping.
      context.getCounter(PageRank.nodes).increment(1);
      context.getCounter(PageRank.massMessages).increment(massMessages);
    }
  }



  // Combiner: sums partial PageRank contributions and passes node structure along.
  private static class CombineClass extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    private static final PageRankNode intermediateMass = new PageRankNode();

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> values, Context context)
        throws IOException, InterruptedException {
      int massMessages = 0;

      // Remember, PageRank mass is stored as a log prob.
      // float mass = Float.NEGATIVE_INFINITY;
      // float mass = 0.0f;
      float [] masses = new float [values.getDumbLength()];
      for (int i = 0; i < masses.length; i++){
        masses[i] = 0.0f;
      }
      for (PageRankNode n : values) {
        if (n.getType() == PageRankNode.Type.Structure) {
          // Simply pass along node structure.
          context.write(nid, n);
        } else {
          // Accumulate PageRank mass contributions.
          // mass = sumLogProbs(mass, n.getPageRank());
          // mass = sumProbs(mass, n.getPageRank());
          for (int i = 0; i < masses.length; i++){
            masses[i] = sumProbs(masses[i] + n.getPageRank(i));
          }
          massMessages++;
        }
      }

      // Emit aggregated results.
      if (massMessages > 0) {
        intermediateMass.setNodeId(nid.get());
        intermediateMass.setType(PageRankNode.Type.Mass);
        // intermediateMass.setPageRank(mass);
        intermediateMass.setPageRankList(masses);

        context.write(nid, intermediateMass);
      }
    }
  }

  // Reduce: sums incoming PageRank contributions, rewrite graph structure.
  private static class ReduceClass extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
    // through dangling nodes.
    // private float totalMass = Float.NEGATIVE_INFINITY;
       // private float totalMass = 0.0f;
       private float [] totalMasses;
    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      Int numberOfSources = conf.getInt("NumberOfSources", 0);
      totalMasses = new float[numberOfSources];
    }


    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> iterable, Context context)
        throws IOException, InterruptedException {
      Iterator<PageRankNode> values = iterable.iterator();

      // Create the node structure that we're going to assemble back together from shuffled pieces.
      PageRankNode node = new PageRankNode();

      node.setType(PageRankNode.Type.Complete);
      node.setNodeId(nid.get());

      int massMessagesReceived = 0;
      int structureReceived = 0;

      // float mass = Float.NEGATIVE_INFINITY;
       // float mass = 0.0f;
      float [] masses = new float [totalMasses.length];
      // float mass = node.getPageRank() / (float) list.size();
      for (int i = 0; i < masses.length; i++){
        masses[i] = 0.0f;
      }
      while (values.hasNext()) {
        PageRankNode n = values.next();

        if (n.getType().equals(PageRankNode.Type.Structure)) {
          // This is the structure; update accordingly.
          ArrayListOfIntsWritable [] list = n.getAdjacenyList();//name override
          structureReceived++;

          node.setAdjacencyList(list);//name override
        } else {
          // This is a message that contains PageRank mass; accumulate.
          // mass = sumLogProbs(mass, n.getPageRank());
          // mass = sumProbs(mass, n.getPageRank());
          for (int i = 0; i < masses.length; i++){
            masses[i] = sumProbs(masses[i], n.getPageRank(i));
          }        
          massMessagesReceived++;
        }
      }

      // Update the final accumulated PageRank mass.
      node.setPageRankList(masses);
      context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

      // Error checking.
      if (structureReceived == 1) {
        // Everything checks out, emit final node structure with updated PageRank value.
        context.write(nid, node);

        // Keep track of total PageRank mass.
        // totalMass = sumLogProbs(totalMass, mass);
        for (int i = 0; i < totalMasses.length; i++){
          totalMasses[i] = sumProbs(totalMasses[i], masses[i]);  
        }
        
      } else if (structureReceived == 0) {
        // We get into this situation if there exists an edge pointing to a node which has no
        // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
        // log and count but move on.
        context.getCounter(PageRank.missingStructure).increment(1);
        LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
            + massMessagesReceived);
        // It's important to note that we don't add the PageRank mass to total... if PageRank mass
        // was sent to a non-existent node, it should simply vanish.
      } else {
        // This shouldn't happen!
        throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
            + " mass: " + massMessagesReceived + " struct: " + structureReceived);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      String taskId = conf.get("mapred.task.id");
      String path = conf.get("PageRankMassPath");

      Preconditions.checkNotNull(taskId);
      Preconditions.checkNotNull(path);

      // Write to a file the amount of PageRank mass we've seen in this reducer.
      FileSystem fs = FileSystem.get(context.getConfiguration());
      FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
      // out.writeFloat(totalMass);//<+++++++++++++++ need to change
      for (int i = 0; i < totalMasses.length; i++){
        out.writeFloat(totalMasses[i]);
      }
      out.close();
    }
  }

  // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
  // of the random jump factor.
  private static class MapPageRankMassDistributionClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    // private float missingMass = 0.0f;
    private float [] missingMasses;
    private int nodeCnt = 0;
    private int [] sources;
    private HashSet sourcesHashset = new HashSet<int>();
    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      // missingMasses = new float[sourceLength];
      // for (int i = 0; i < sourceLength; i++){
      //   missingMasses[i] = 0.0f;
      // }
      // missingMass = conf.getFloat("MissingMass", 0.0f);
      int missingMassLength = conf.getInt("MissingMassLength", 0);
      missingMasses = new float[missingMassLength];
      for (int i = 0; i < missingMassLength; i++){
        missingMasses[i] = conf.getInt("MissingMass" + i, 0.0f);
      }
      nodeCnt = conf.getInt("NodeCount", 0);

      int sourceLength = context.getConfiguration().getInt("NumberOfSources", 0);
      // System.out.println("sourceLength: " + sourceLength);
      sources = new int[sourceLength];
      for (int i = 0; i < sourceLength; i++){
        sources[i] = context.getConfiguration().getInt("source"+i, 0);
        sourcesHashset.add(sources[i]);
        // System.out.println("source" + i +": "+ sourceLength);
      }
      
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {
      // float p = node.getPageRank();
      float [] pList = node.getPageRankList();

      // float jump = (float) (Math.log(ALPHA) - Math.log(nodeCnt));
      float jump;
      float link;
      for (int i = 0; i < pList.length; i++){
        if (sourcesHashset.contains(node.getNodeId())){ //need chagen to multi source
          sourcesHashset.remove(node.getNodeId());
          jump = (float) ALPHA;
          link = (float) (1.0f - ALPHA) * (pList[i] + missingMasses[i]);
        } else {
          jump = (float) 0.0f;
          link = (float) (1.0f - ALPHA) * pList[i];
        }
        // float link = (float) Math.log(1.0f - ALPHA)
            // + sumLogProbs(p, (float) (Math.log(missingMass) - Math.log(nodeCnt)));
        // p = sumLogProbs(jump, link);
        pList[i] = sumProbs(jump, link);
        node.setPageRank(pList[i], i);
      } 
      context.write(nid, node);
    }
  }

  // Random jump factor.
  private static float ALPHA = 0.15f;
  private static NumberFormat formatter = new DecimalFormat("0000");

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
  }

  public RunPersonalizedPageRankBasic() {}

  private static final String BASE = "base";
  private static final String NUM_NODES = "numNodes";
  private static final String START = "start";
  private static final String END = "end";
  private static final String COMBINER = "useCombiner";
  private static final String INMAPPER_COMBINER = "useInMapperCombiner";
  private static final String RANGE = "range";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(COMBINER, "use combiner"));
    options.addOption(new Option(INMAPPER_COMBINER, "user in-mapper combiner"));
    options.addOption(new Option(RANGE, "use range partitioner"));

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("base path").create(BASE));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("start iteration").create(START));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("end iteration").create(END));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of nodes").create(NUM_NODES));

    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("sources").create(SOURCES)); 

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) ||
        !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String basePath = cmdline.getOptionValue(BASE);
    int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
    int s = Integer.parseInt(cmdline.getOptionValue(START));
    int e = Integer.parseInt(cmdline.getOptionValue(END));
    boolean useCombiner = cmdline.hasOption(COMBINER);
    boolean useInmapCombiner = cmdline.hasOption(INMAPPER_COMBINER);
    boolean useRange = cmdline.hasOption(RANGE);


    String [] sourcesString = cmdline.getOptionValue(SOURCES).split(",");
    int [] sources = new int[sourcesString.length];
    for (int i = 0; i < sources.length; i++){
      sources[i] = Integer.parseInt(sourcesString[i]);
      // System.out.println(sources[i]);
    }

    LOG.info("Tool name: RunPageRank");
    LOG.info(" - base path: " + basePath);
    LOG.info(" - num nodes: " + n);
    LOG.info(" - start iteration: " + s);
    LOG.info(" - end iteration: " + e);
    LOG.info(" - use combiner: " + useCombiner);
    LOG.info(" - use in-mapper combiner: " + useInmapCombiner);
    LOG.info(" - user range partitioner: " + useRange);
    LOG.info(" - sourceNum: " + sources.length);
    for (int i = 0; i < sources.length; i++){
      LOG.info("- source"+i+": " + sources[i]);
    }

    // Iterate PageRank.
    for (int i = s; i < e; i++) {
      iteratePageRank(i, i + 1, basePath, n, useCombiner, useInmapCombiner, sources);
    }
    
    return 0;
  }

  // Run each iteration.
  private void iteratePageRank(int i, int j, String basePath, int numNodes,
      boolean useCombiner, boolean useInMapperCombiner, int [] sources) throws Exception {
    // Each iteration consists of two phases (two MapReduce jobs).

    // Job 1: distribute PageRank mass along outgoing edges.
    // float mass = phase1(i, j, basePath, numNodes, useCombiner);
    float [] masses = phase1(i, j, basePath, numNodes, useCombiner);

    // Find out how much PageRank mass got lost at the dangling nodes.
    // float missing = 1.0f - (float) StrictMath.exp(mass);
    float [] missings = new float[masses.length];
    for (int k = 0; k < missings.length; k++){
      missings[k] = 1.0f - masses[k];
    }
    // float missing = 1.0f - mass;
    // Job 2: distribute missing mass, take care of random jump factor.
    // phase2(i, j, missing, basePath, numNodes, sources);
    phase2(i, j, missings, basePath, numNodes, sources);
  }

  private float phase1(int i, int j, String basePath, int numNodes,
      boolean useCombiner) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    String in = basePath + "/iter" + formatter.format(i);
    String out = basePath + "/iter" + formatter.format(j) + "t";
    String outm = out + "-mass";

    // We need to actually count the number of part files to get the number of partitions (because
    // the directory might contain _log).
    int numPartitions = 0;
    for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
      if (s.getPath().getName().contains("part-"))
        numPartitions++;
    }

    LOG.info("PageRank: iteration " + j + ": Phase1");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);
    LOG.info(" - nodeCnt: " + numNodes);
    LOG.info(" - useCombiner: " + useCombiner);
    LOG.info(" - useInmapCombiner: " + useInMapperCombiner);
    LOG.info("computed number of partitions: " + numPartitions);
    LOG.info(" - sourceNum: " + sources.length);
    for (int ii = 0; ii < sources.length; ii++){
      LOG.info("- source"+ii+": " + sources[ii]);
    }

    int numReduceTasks = numPartitions;

    job.getConfiguration().setInt("NodeCount", numNodes);
    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    //job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
    job.getConfiguration().set("PageRankMassPath", outm);
    
    job.getConfiguration().setInt("NumberOfSources", sources.length);
    for (int ii = 0; ii < sources.length; ii++){
      job.getConfiguration().setInt("source"+ii, sources[ii]);
    }

    job.setNumReduceTasks(numReduceTasks);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(MapClass.class);
    job.setCombinerClass(CombineClass.class);
    job.setReducerClass(ReduceClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);
    FileSystem.get(getConf()).delete(new Path(outm), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    // float mass = Float.NEGATIVE_INFINITY;
    // float mass = 0.0f;
    float [] masses = new float [sources.length];
    for (int ii = 0; ii < masseses.length; ii++){
      masses[i] = 0.0f;
    }
    FileSystem fs = FileSystem.get(getConf());
    for (FileStatus f : fs.listStatus(new Path(outm))) {
      FSDataInputStream fin = fs.open(f.getPath());
      // mass = sumLogProbs(mass, fin.readFloat());
      // mass = sumProbs(mass, fin.readFloat());
      for (int ii = 0; ii < masseses.length; ii++){
        masses[i] = sumProbs(masses[i], fin.readFloat());
      }
      fin.close();
    }

    return mass;
  }

  // private void phase2(int i, int j, float missing, String basePath, int numNodes, int [] sources) throws Exception {
  private void phase2(int i, int j, float [] missing, String basePath, int numNodes, int [] sources) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);
    for (int iter = 0; iter < missing.length; iter++){
      LOG.info("missing PageRank mass"+iter+": " + missing[iter]);
    }
    LOG.info("number of nodes: " + numNodes);

    String in = basePath + "/iter" + formatter.format(j) + "t";
    String out = basePath + "/iter" + formatter.format(j);

    LOG.info("PageRank: iteration " + j + ": Phase2");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);
    LOG.info(" - sourceNum: " + sources.length);
    for (int ii = 0; ii < sources.length; ii++){
      LOG.info("- source"+ii+": " + sources[ii]);
    }

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    // job.getConfiguration().setFloat("MissingMass", (float) missing);
    job.getConfiguration().setInt("MissingMassLength",  missing.length);
    for (int iter = 0; iter < missing.length; iter++){
      job.getConfiguration().setFloat("MissingMass"+iter, (float) missing[iter]);
    }
    job.getConfiguration().setInt("NodeCount", numNodes);
    
    job.getConfiguration().setInt("NumberOfSources", sources.length);
    for (int ii = 0; ii < sources.length; ii++){
      job.getConfiguration().setInt("source"+ii, sources[ii]);
    }

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(MapPageRankMassDistributionClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
  }

  // Adds two log probs.
  private static float sumLogProbs(float a, float b) {
    if (a == Float.NEGATIVE_INFINITY)
      return b;

    if (b == Float.NEGATIVE_INFINITY)
      return a;

    if (a < b) {
      return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
    }

    return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
  }
  private static float sumProbs(float a, float b) {
    if (a == Float.NEGATIVE_INFINITY)
      return b;

    if (b == Float.NEGATIVE_INFINITY)
      return a;

    return (float) (a + b);
  }
}
  // // Mapper with in-mapper combiner optimization.
  // private static class MapWithInMapperCombiningClass extends
  //     Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
  //   // For buffering PageRank mass contributes keyed by destination node.
  //   private static final HMapIF map = new HMapIF();

  //   // For passing along node structure.
  //   private static final PageRankNode intermediateStructure = new PageRankNode();

  //   @Override
  //   public void map(IntWritable nid, PageRankNode node, Context context)
  //       throws IOException, InterruptedException {
  //     // Pass along node structure.
  //     intermediateStructure.setNodeId(node.getNodeId());
  //     intermediateStructure.setType(PageRankNode.Type.Structure);
  //     intermediateStructure.setAdjacencyList(node.getAdjacenyList());

  //     context.write(nid, intermediateStructure);

  //     int massMessages = 0;
  //     int massMessagesSaved = 0;

  //     // Distribute PageRank mass to neighbors (along outgoing edges).
  //     if (node.getAdjacenyList().size() > 0) {
  //       // Each neighbor gets an equal share of PageRank mass.
  //       ArrayListOfIntsWritable list = node.getAdjacenyList();
  //       float mass = node.getPageRank() - (float) StrictMath.log(list.size());

  //       context.getCounter(PageRank.edges).increment(list.size());

  //       // Iterate over neighbors.
  //       for (int i = 0; i < list.size(); i++) {
  //         int neighbor = list.get(i);

  //         if (map.containsKey(neighbor)) {
  //           // Already message destined for that node; add PageRank mass contribution.
  //           massMessagesSaved++;
  //           map.put(neighbor, sumLogProbs(map.get(neighbor), mass));
  //         } else {
  //           // New destination node; add new entry in map.
  //           massMessages++;
  //           map.put(neighbor, mass);
  //         }
  //       }
  //     }

  //     // Bookkeeping.
  //     context.getCounter(PageRank.nodes).increment(1);
  //     context.getCounter(PageRank.massMessages).increment(massMessages);
  //     context.getCounter(PageRank.massMessagesSaved).increment(massMessagesSaved);
  //   }

  //   @Override
  //   public void cleanup(Context context) throws IOException, InterruptedException {
  //     // Now emit the messages all at once.
  //     IntWritable k = new IntWritable();
  //     PageRankNode mass = new PageRankNode();

  //     for (MapIF.Entry e : map.entrySet()) {
  //       k.set(e.getKey());

  //       mass.setNodeId(e.getKey());
  //       mass.setType(PageRankNode.Type.Mass);
  //       mass.setPageRank(e.getValue());

  //       context.write(k, mass);
  //     }
  //   }
  // }