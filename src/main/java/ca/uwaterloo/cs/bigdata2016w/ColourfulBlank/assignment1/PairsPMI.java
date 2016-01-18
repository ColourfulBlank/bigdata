package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment1;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.lang.Math;
import com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.pair.PairOfStrings;

public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  private static class MyFirstMapper extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
   private static final FloatWritable ONE = new FloatWritable(1);
    private static final PairOfStrings BIGRAM = new PairOfStrings();
///
      @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      StringTokenizer itr = new StringTokenizer(line);

      int cnt = 0;
      Set<String> set = new HashSet<String>();
      while (itr.hasMoreTokens()) {
        cnt++;
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        set.add(w);
        if (cnt >= 100) break;
      }

      String[] words = new String[set.size()];
      words = set.toArray(words);


      BIGRAM.set("*", "*");
      context.write(BIGRAM, ONE);
      for (int i = 0; i < words.length; i++){
        for (int j = i + 1; j < words.length; j++){
          BIGRAM.set(words[i], words[j]);
          context.write(BIGRAM, ONE);
        } 
        BIGRAM.set(words[i], "*");
        context.write(BIGRAM, ONE);
      }
    }
  }

protected static class MyFirstCombiner extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
    private static final FloatWritable SUM = new FloatWritable();
     private static final PairOfStrings BIGRAM = new PairOfStrings();

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      BIGRAM.set(key.getLeftElement(), key.getRightElement());
      context.write(BIGRAM, SUM);
    }
  }


  private static class MyFristReducer extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
    private static final FloatWritable VALUE = new FloatWritable();
    private static final PairOfStrings BIGRAM = new PairOfStrings();

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      float sum = 0.0f;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      if (sum >= 10){
        VALUE.set(sum);
        BIGRAM.set(key.getLeftElement(), key.getRightElement());
        context.write(BIGRAM, VALUE);
      }
    }
  }

  protected static class MyFirstPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
    @Override
    public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }
///////////////////////////////////////////////////////////////////////////////////////////////////
  private static class MySecondMapper extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
   private static final FloatWritable OCC = new FloatWritable();
    private static final PairOfStrings BIGRAM = new PairOfStrings();
    private String string1 = "";
    private String string2 = "";
    private String number = "";
      @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      StringTokenizer itr = new StringTokenizer(line);
      string1 = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
      string2 = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
      number = itr.nextToken();

      if (!(string1.equals("") || string2.equals(""))){
        BIGRAM.set(string1, string2);
        OCC.set(Float.parseFloat(number));
        context.write(BIGRAM, OCC);
      }
         
    }
  }
      

protected static class MySecondCombiner extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
    private static final FloatWritable SUM = new FloatWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<FloatWritable> iter = values.iterator();
      context.write(key, iter.next());
    }
  }


  private static class MySecondReducer extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, DoubleWritable> {
   private static final DoubleWritable PMI = new DoubleWritable();  
   private static final PairOfStrings BIGRAM = new PairOfStrings(); 
   private static final HashMap<String, String> WORDS = new HashMap<String, String>();
   private float TOTALNUMBER = 0.0f; 
    private String string1 = "";
    private String string2 = "";
    private String number = "";
   @Override
   public void setup(Context context){
     try{
          String stringPath = "temp/part-r-0000";
          for (int f = 0; f < 5; f++){
            Path pathOfTemp = new Path(stringPath + Integer.toString(f));//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pathOfTemp), "UTF-8"));
            String line;
            FloatWritable value = new FloatWritable();
            line=br.readLine();
            while (line != null){
              StringTokenizer itr = new StringTokenizer(line);
              string1 = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
              string2 = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
              number = itr.nextToken();
                  if ( string1.equals("") ){
                    TOTALNUMBER = Float.parseFloat(number);
                  } else if ( string2.equals("") ){
                    WORDS.put(string1, number);
                  }
              line=br.readLine();
            }
          }
        }catch(Exception e){
            System.out.println("\n\n\n\n" + "APPLE,APPLE, I LOVE APPLE!" + "\n\n\n\n");
              e.printStackTrace();
            System.out.println("\n\n\n\n" + "APPLE,APPLE, I LOVE APPLE!" + "\n\n\n\n");
        }
   }

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      double pmi = 0.0f;
       Iterator<FloatWritable> iter = values.iterator();
       float num = iter.next().get();
       System.out.println("TOTALNUMBER:" + Float.toString(TOTALNUMBER) + "P(x,y):" + num + key.getLeftElement()+":"+WORDS.get(key.getLeftElement()) + key.getRightElement()+":"+ WORDS.get(key.getRightElement()));
      pmi = Math.log10(TOTALNUMBER *  num /  Float.parseFloat(WORDS.get(key.getLeftElement())) / Float.parseFloat(WORDS.get(key.getRightElement())));
      PMI.set(pmi);
      BIGRAM.set(key.getLeftElement(), key.getRightElement());
      context.write(key, PMI);


    }    
  }

  protected static class MySecondPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
    @Override
    public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }
/**---------------------------------------------------------------------------------------------------------------
*/
  /**
   * Creates an instance of this tool.
   */
  public PairsPMI() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;

    @Option(name = "-window", metaVar = "[num]", required = false, usage = "cooccurrence window")
    public int window = 2;
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

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - window: " + args.window);
    LOG.info(" - number of reducers: " + args.numReducers);

    Job job_x = Job.getInstance(getConf());
    job_x.setJobName(PairsPMI.class.getSimpleName());
    job_x.setJarByClass(PairsPMI.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    Path midTemp = new Path("temp");
    FileSystem.get(getConf()).delete(outputDir, true);
    FileSystem.get(getConf()).delete(midTemp, true);

    job_x.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job_x, new Path(args.input));
    FileOutputFormat.setOutputPath(job_x, new Path("temp"));

    job_x.setMapOutputKeyClass(PairOfStrings.class);
    job_x.setMapOutputValueClass(FloatWritable.class);
    job_x.setOutputKeyClass(PairOfStrings.class);
    job_x.setOutputValueClass(FloatWritable.class);

    job_x.setMapperClass(MyFirstMapper.class);
    job_x.setCombinerClass(MyFirstCombiner.class);
    job_x.setReducerClass(MyFristReducer.class);
    job_x.setPartitionerClass(MyFirstPartitioner.class);
    
    long startTime = System.currentTimeMillis();
    job_x.waitForCompletion(true);

    Job job_y = Job.getInstance(getConf());
    job_y.setJobName(PairsPMI.class.getSimpleName());
    job_y.setJarByClass(PairsPMI.class);

    job_y.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job_y, new Path("temp"));
    FileOutputFormat.setOutputPath(job_y, new Path(args.output));

    job_y.setMapOutputKeyClass(PairOfStrings.class);
    job_y.setMapOutputValueClass(FloatWritable.class);
    job_y.setOutputKeyClass(PairOfStrings.class);
    job_y.setOutputValueClass(FloatWritable.class);

    job_y.setMapperClass(MySecondMapper.class);
    job_y.setCombinerClass(MySecondCombiner.class);
    job_y.setReducerClass(MySecondReducer.class);
    job_y.setPartitionerClass(MySecondPartitioner.class);
    
    job_y.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}


