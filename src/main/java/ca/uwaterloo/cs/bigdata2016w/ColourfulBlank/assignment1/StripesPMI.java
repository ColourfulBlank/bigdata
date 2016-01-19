package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment1;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Set;
import java.lang.Math;
import java.util.HashSet;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.MapKI;
import tl.lin.data.map.HMapKI;
// import tl.lin.data.map.MapKF;

public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

  private static class MyFirstMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {
    private static final HMapStIW MAP = new HMapStIW();
    private static final Text KEY = new Text();

    private int window = 2;

    @Override
    public void setup(Context context) {
      // window = context.getConfiguration().getInt("window", 2);
    }
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

      MAP.clear(); 
      MAP.increment("*");
      KEY.set("*");
      context.write(KEY, MAP);
      
      for (int i = 0; i < words.length; i++){
        MAP.clear();
        for (int j = i + 1; j < words.length; j++){
          MAP.increment(words[j]);
        }
        MAP.increment("*");
        KEY.set(words[i]);
        context.write(KEY, MAP);
      }
    }
  }
 private static class MyFirstCombiner extends Reducer<Text, HMapStIW, Text, HMapStIW> {
    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }

  private static class MyFirstReducer extends Reducer<Text, HMapStIW, Text, HMapStIW> {
    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      Set<String> keySet = map.keySet();
      String [] keyArray = new String[keySet.size()];
      keyArray = keySet.toArray(keyArray);
      for (int i = 0; i < keyArray.length; i++){
        if (map.get(keyArray[i]) < 10){
            map.remove(keyArray[i]);
        }
        // System.out.println(keyArray[i]);
      }

      if (map.size() > 0){
        context.write(key, map);
      }
    }
  }
/**---------------------------------------------------------------------------------------------------------------
*/
  private static class MySecondMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {
    private static final HMapStIW MAP = new HMapStIW();
    private static final Text KEY = new Text();
    private static final IntWritable NUMBER = new IntWritable();

    @Override
    public void setup(Context context) {
      // window = context.getConfiguration().getInt("window", 2);
    }
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      String newLine = line.replace("{", "").replace("}", "");
      StringTokenizer itr = new StringTokenizer(newLine);
      if (itr.hasMoreTokens()){ //the KEY
        KEY.set(itr.nextToken());
      }

      MAP.clear();
      while (itr.hasMoreTokens()) {
        String k = itr.nextToken();
        String v = k.substring(k.lastIndexOf("=")+1, k.length());
        if ( k.lastIndexOf(",") == k.length() - 1 ){
          v = v.substring(0, v.length()-1);
        }
        k = k.substring(0, k.lastIndexOf("="));
        if ( k.equals("*") ) {
          continue;
        }
        int num = Integer.parseInt( v );
        MAP.put(k, num);
      }
      context.write(KEY, MAP);
    }
  }

   private static class MySecondCombiner extends Reducer<Text, HMapStIW, Text, HMapStIW> {
    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }

  private static class MySecondReducer extends Reducer<Text, HMapStIW, Text, HMapStFW> {
    private static final HMapStFW MAP = new HMapStFW();
    private static final Text KEY = new Text();
    private static final HashMap<String, String> WORDS = new HashMap<String, String>();
    private int TOTALNUMBER = 0; 
    private String K = "";
    private int numReducers = 5;
    @Override
    public void setup(Context context){
     try{
          String stringPath = "temp/part-r-0000";
          numReducers = context.getConfiguration().getInt("NumberOfReducers", 5);
          for (int f = 0; f < numReducers; f++){
            Path pathOfTemp = new Path(stringPath + Integer.toString(f));//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pathOfTemp), "UTF-8"));
            String line;
            FloatWritable value = new FloatWritable();
            line = br.readLine();
            while (line != null){
              line = line.replace("{", "").replace("}", "");
              StringTokenizer itr = new StringTokenizer(line);
              if (itr.hasMoreTokens()){ //the KEY
                K = itr.nextToken();
              }

              while (itr.hasMoreTokens()) {
                String k = itr.nextToken();
                String v = k.substring(k.indexOf("=")+1, k.length());
                if ( k.lastIndexOf(",") == k.length()-1 ){
                  v = v.substring(0, v.length()-1);
                }
                k = k.substring(0, k.indexOf("="));
                if (K.equals("*")){
                  TOTALNUMBER = Integer.parseInt(v);
                } else {
                  if (k.equals("*")){
                    WORDS.put(K, v);  
                  }
                }
              }
              line = br.readLine();
            }
          }
        }catch(Exception e){
            System.out.println("\n\n\n\n" + "APPLE,APPLE, I LOVE APPLE!" + "\n\n\n\n");
              e.printStackTrace();
            System.out.println("\n\n\n\n" + "APPLE,APPLE, I LOVE APPLE!" + "\n\n\n\n");
        }
   }
    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();
      KEY.set(key);
      while (iter.hasNext()) {
        map.plus(iter.next());
      }
      double pmi = 0.0f;
      for (MapKI.Entry<String> entry : map.entrySet()) {
        String name = key.toString();
        // System.out.println("NAME:" + name);
        // System.out.println("\n"+"TOTALNUMBER:" + Float.toString(TOTALNUMBER) + "P(x,y):" + entry.getValue()+"_" + key+":"+WORDS.get( name ) + entry.getKey()+":"+ WORDS.get(entry.getKey()));
        pmi = Math.log10(TOTALNUMBER *  entry.getValue() /  Float.parseFloat(WORDS.get( name )) / Float.parseFloat(WORDS.get( entry.getKey() )));
        // set MAP
        MAP.put(entry.getKey(), (float) pmi);

      }
      if (MAP.size() > 0){
        context.write(KEY, MAP);
      }
    }
  }






/**---------------------------------------------------------------------------------------------------------------
*/
  /**
   * Creates an instance of this tool.
   */
  public StripesPMI() {}

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

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - window: " + args.window);
    LOG.info(" - number of reducers: " + args.numReducers);
    
    Job job_x = Job.getInstance(getConf());
    job_x.setJobName(StripesPMI.class.getSimpleName());
    job_x.setJarByClass(StripesPMI.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    Path midTemp = new Path("temp");
    FileSystem.get(getConf()).delete(outputDir, true);
    FileSystem.get(getConf()).delete(midTemp, true);

    // job_x.getConfiguration().setInt("window", args.window);
    job_x.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
    job_x.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job_x.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job_x.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job_x.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    job_x.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job_x, new Path(args.input));
    FileOutputFormat.setOutputPath(job_x, new Path("temp"));

    job_x.setMapOutputKeyClass(Text.class);
    job_x.setMapOutputValueClass(HMapStIW.class);
    job_x.setOutputKeyClass(Text.class);
    job_x.setOutputValueClass(HMapStIW.class);

    job_x.setMapperClass(MyFirstMapper.class);
    job_x.setCombinerClass(MyFirstCombiner.class);
    job_x.setReducerClass(MyFirstReducer.class);
    
    long startTime = System.currentTimeMillis();
    job_x.waitForCompletion(true);

    Job job_y = Job.getInstance(getConf());
    job_y.setJobName(StripesPMI.class.getSimpleName());
    job_y.setJarByClass(StripesPMI.class);

    job_y.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
    job_y.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job_y.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job_y.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job_y.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");
    job_y.getConfiguration().setInt("NumberOfReducers", args.numReducers);
    job_y.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job_y, new Path("temp"));
    FileOutputFormat.setOutputPath(job_y, new Path(args.output));

    job_y.setMapOutputKeyClass(Text.class);
    job_y.setMapOutputValueClass(HMapStIW.class);
    job_y.setOutputKeyClass(Text.class);
    job_y.setOutputValueClass(HMapStIW.class);

    job_y.setMapperClass(MySecondMapper.class);
    job_y.setCombinerClass(MySecondCombiner.class);
    job_y.setReducerClass(MySecondReducer.class);
    // job_y.setPartitionerClass(MySecondPartitioner.class);
    
    job_y.waitForCompletion(true);

    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
