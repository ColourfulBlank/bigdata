package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.ByteArrayInputStream;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.Combiner;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.file.tfile.ByteArray;


import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfStringInt;
import tl.lin.data.pair.PairOfObjects;
import tl.lin.data.pair.PairOfWritables;



public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<String>();
    private static final IntWritable VALUE = new IntWritable();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      String text = doc.toString();

      // Tokenize line.
      List<String> tokens = new ArrayList<String>();
      StringTokenizer itr = new StringTokenizer(text);
      while (itr.hasMoreTokens()) {
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        tokens.add(w);
      }

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
        VALUE.set( e.getRightElement() );
        context.write(new PairOfStringInt(e.getLeftElement(), (int) docno.get()), VALUE);
        VALUE.set(1);
        context.write(new PairOfStringInt(e.getLeftElement(), -1), VALUE);
      }
    }
  }
  // protected static class MyCombiner extends
  //     Reducer<PairOfStringInt, IntWritable, PairOfStringInt, IntWritable> {

  //   @Override
  //   public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
  //       throws IOException, InterruptedException {
  //         Iterator<IntWritable> iter = values.iterator();
  //         context.write(key, iter.next());

  //   }
  // }

  private static class MyReducer extends
      Reducer<PairOfStringInt, IntWritable, Text, BytesWritable> {
    private final static IntWritable DF = new IntWritable();
    private final static Text KEY = new Text();
    private final static BytesWritable INDEX = new BytesWritable();
    // private final static PairOfWritables<IntWritable, BytesWritable> PAIR = new PairOfWritables<IntWritable, BytesWritable>();
    private final static BytesWritable PAIR = new BytesWritable();
    private final static BytesWritable POSTINGS = new BytesWritable();
    private String currentKey = "";
    private int index;
    private int lastindex;
    private int df;
    private ByteArrayOutputStream out;
    private DataOutputStream bi;
    

    @Override
    public void setup(Context context){
      index = 0;
      lastindex = 0;
      df = 0;
      out = new ByteArrayOutputStream();
      bi = new DataOutputStream( out );
    }  

    @Override
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      if (key.getRightElement() == -1){
        if(!currentKey.equals("")){
          KEY.set(currentKey);
          byte [] aArray = out.toByteArray();
          out = new ByteArrayOutputStream();
          bi = new DataOutputStream( out );
          POSTINGS.set(aArray, 0 , aArray.length);
          PAIR.set(POSTINGS);
          context.write(KEY, PAIR); 
        }
        int df = 0;
        while (iter.hasNext()) {
          iter.next();
          df++;
        }
        WritableUtils.writeVInt(bi, df);
        currentKey = key.getLeftElement();
        index = key.getRightElement() == -1 ? 0 : key.getRightElement();
        lastindex = key.getRightElement() == -1 ? 0 : key.getRightElement();//
      } else if (currentKey.equals(key.getLeftElement())){
        index = key.getRightElement() - lastindex;
        lastindex = key.getRightElement();
      }
        while (iter.hasNext()) {
          WritableUtils.writeVInt(bi, index);
          WritableUtils.writeVInt(bi, iter.next().get());
        }
        
    }
     @Override
    public void cleanup(Context context) throws IOException,InterruptedException {
      KEY.set(currentKey);
      byte [] aArray = out.toByteArray();
      out = new ByteArrayOutputStream();
      bi = new DataOutputStream( out );
      POSTINGS.set(aArray, 0 , aArray.length);
      PAIR.set(POSTINGS);
      context.write(KEY, PAIR); 
    }
  }

  
  protected static class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }
 
  private BuildInvertedIndexCompressed() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;
    
    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;

    @Option(name = "-textOutput", required = false, usage = "use TextOutputFormat")
    public boolean textOutput = false;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info( "Tool: " + BuildInvertedIndexCompressed.class.getSimpleName() );
    LOG.info( " - input path: " + args.input);
    LOG.info( " - output path: " + args.output);
    LOG.info( " - reducers: " + args.numReducers);
    LOG.info(" - text output: " + args.textOutput);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks( args.numReducers );

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    if (args.textOutput) {
      job.setOutputFormatClass(TextOutputFormat.class);
    } else {
      job.setOutputFormatClass(MapFileOutputFormat.class);
    }
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
