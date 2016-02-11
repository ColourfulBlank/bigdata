package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment4;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.queue.TopScoredObjects;
import tl.lin.data.pair.PairOfIntFloat;
// import tl.lin.data.array.ArrayListOfIntsWritable;
// import tl.lin.data.array.ArrayListOfFloatsWritable;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  private static class MyMapper extends
      Mapper<IntWritable, PageRankNode, IntWritable, PairOfIntFloat> {
    // private TopScoredObjects<Integer> queue;
    private TopScoredObjects<Integer> [] queues;
    private int sourceNumber;

    @Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      // queue = new TopScoredObjects<Integer>(k);
      sourceNumber = context.getConfiguration().getInt("NumberOfSources", 0);
      // System.out.println(sourceNumber);
      queues = new TopScoredObjects[sourceNumber];
      for (int i = 0; i < sourceNumber; i++){
        queues[i] = new TopScoredObjects<Integer>(k);
      }

    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {
          if (nid.get() == 367){
            System.out.println(node.toString());
          } else if (nid.get() == 249){ 
            System.out.println(node.toString());
          }
      for (int i = 0; i < sourceNumber; i++){
        queues[i].add(node.getNodeId(), node.getPageRank(i));
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable sourceIndex = new IntWritable();
      for (int i = 0; i < sourceNumber; i++){
        for (PairOfObjectFloat<Integer> pair : queues[i].extractAll()) {
          sourceIndex.set(i);
          context.write(sourceIndex, new PairOfIntFloat(pair.getLeftElement(), pair.getRightElement()));
        }
      }
    }
  }

  private static class MyReducer extends
      Reducer< IntWritable, PairOfIntFloat, FloatWritable, IntWritable> {
    // private TopScoredObjects<Integer> queue;
    private static TopScoredObjects<Integer> [] queues; 
    private int sourceNumber;

    @Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      sourceNumber = context.getConfiguration().getInt("NumberOfSources", 0);
      // queue = new TopScoredObjects<Integer>(k);
      queues = new TopScoredObjects[sourceNumber]; // <<<<<<<<<<<<<<<<<<<<<<<<<<
      for (int i = 0; i < queues.length; i++){
        queues[i] = new TopScoredObjects<Integer>(k);
      }
    }

    @Override
    public void reduce(IntWritable nid, Iterable<PairOfIntFloat> iterable, Context context)
        throws IOException {
      Iterator<PairOfIntFloat> pair = iterable.iterator();
      while (pair.hasNext()){
        PairOfIntFloat current = pair.next();
        queues[nid.get()].add(current.getLeftElement(), current.getRightElement());
      }
      // Shouldn't happen. Throw an exception.
      if (pair.hasNext()) {
        throw new RuntimeException();
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();
      for (int i = 0; i < queues.length; i++){ 
        for (PairOfObjectFloat<Integer> pair : queues[i].extractAll()) {
          key.set(pair.getLeftElement());
          value.set((float) StrictMath.exp(pair.getRightElement()));
          context.write(value, key);
        }
      }
    }
  }
  private static class MyPartitioner extends Partitioner<IntWritable, PairOfIntFloat> {
    @Override
    public int getPartition(IntWritable key, PairOfIntFloat value, int numReduceTasks) {
      return (key.hashCode()& Integer.MAX_VALUE) % numReduceTasks;//(key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }

  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("top n").create(TOP));
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

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String [] sourcesString = cmdline.getOptionValue(SOURCES).split(",");
    int [] sources = new int[sourcesString.length];
    for (int i = 0; i < sources.length; i++){
      sources[i] = Integer.parseInt(sourcesString[i]);
    }
    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output: " + outputPath);
    LOG.info(" - top: " + n);
    LOG.info(" - sourceNum: " + sources.length);
    for (int i = 0; i < sources.length; i++){
      LOG.info(" - source"+i+": " + sources[i]);
    }

    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);
    conf.setInt("NumberOfSources", sources.length);
    for (int i = 0; i < sources.length; i++){
      conf.setInt("source"+i, sources[i]);
    }
    // conf.setInt(SOURCES, s);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(sources.length);//set up reducer jobs

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PairOfIntFloat.class);

    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    // job.setPartitionerClass(MyPartitioner.class);//

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }
}