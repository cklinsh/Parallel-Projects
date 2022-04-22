import java.io.IOException;
import java.io.File;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MapRedKNN {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {
    private double[] classify;

    public void configure( JobConf job ) {
      classify = stringToDoubleArray( job.get( "record" ).split( "," ) );
    }

    public static double[] stringToDoubleArray( String[] arr ) {
      final int N = arr.length;
      double[] doubles = new double[N];
      for( int i = 0; i < N; ++i ) doubles[i] = Double.parseDouble( arr[i] );

      return doubles;
    }

    public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
      double[] A = stringToDoubleArray( value.toString( ).split( "," ) );
      final int COLS = A.length;
      double res = 0;

      // carry out formula for distance res = sqrt( pow((x1 - y1), 2) + pow((x2 - y2),2) + ... )
      for( int col = 0; col < COLS; ++col ) res += Math.pow( A[col] - classify[col], 2 );

      output.collect( new DoubleWritable( Math.sqrt( res ) ), value );
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<DoubleWritable, Text, DoubleWritable, Text> {
    private int K;
    private TreeMap<Double, String> vals = new TreeMap<Double, String>();
    private OutputCollector<DoubleWritable, Text> out = null;
    private HashMap<Double, ArrayList<String>> duplicates = new HashMap<>();

    public void configure( JobConf job ) {
      K = Integer.parseInt( job.get( "K" ) );
    }

    public void reduce( DoubleWritable key, Iterator<Text> values, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
      if( out == null ) out = output;

      while( values.hasNext() ) {
        // check if key exists
        // if it exists add record to duplicates hashmap
        // check to see if the key exists in the duplicates hashmap
        // if it doesn't exist add it and add the values string array

        // if key already exists check if is in duplicates
        if( vals.containsKey( key.get() ) ) {
          // if it is in duplicates add it to the arraylist
          if( duplicates.containsKey( key.get() ) ) {
            ArrayList<String> l = duplicates.get( key.get() );

            // if we have more than K records in a list don't add any more
            if( l.size() < K )
              l.add( values.next().toString() );
          }
          // else create an arraylist for this key
          // and place the first point there
          else {
            ArrayList<String> l = new ArrayList<>();
            l.add( values.next().toString() );
            duplicates.put( key.get(), l);
          }
        }
        // else add the new key and its value
        else {
          vals.put( key.get(), values.next().toString() );
        }

        // maintain the size to be == K
        if( vals.size() > K ) {
          // when removing an entry from the main map 
          // check to see if we have a corresponding entry in the duplicates
          // and clear its arraylist
          // this way we aren't using any extra memory
          if( duplicates.containsKey( vals.lastKey() ) ) {
            ArrayList<String> l = duplicates.get( vals.lastKey() );
            l.clear();
          }

          // remove from TreeMap
          vals.pollLastEntry();
        }
      }
    }

    // process duplicates here
    // add all the duplicates to the treemap and 
    // maintain the K entries there
    public void close() throws IOException {
      Double[] keyArr = new Double[K];
      String[] valArr = new String[K];
      Set keySet = vals.keySet();
      Iterator<Double> keyIt = keySet.iterator();

      // go through the keySet
      // check if key is in duplicates
      // add all duplicates
      int inserted = 0;
      while( inserted < K && keyIt.hasNext() ) {
        Double key = keyIt.next();
        // add the key from the treemap
        keyArr[inserted] = key;
        valArr[inserted] = vals.get( key );
        ++inserted;

        // if there is still room add duplicate distances
        if( duplicates.containsKey( key ) ) {
          ArrayList<String> strArr = duplicates.get( key );

          // add each element to the sorted arrays
          int N = strArr.size();
          for( int i = 0; i < N && inserted < K; ++i, ++inserted ) {
            keyArr[inserted] = key;
            valArr[inserted] = strArr.get(i);
          }
        }
      }
      // collect the K closest points
      // using inserted in the event that the dataset is smaller than K
      for( int i = 0; i < inserted; ++i )
        out.collect( new DoubleWritable( keyArr[i] ), new Text( valArr[i] ) );
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(MapRedKNN.class);
    conf.setJobName("KNN");

    // sets the output key/value types
    conf.setOutputKeyClass(DoubleWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    // store the record that we are trying to classify
    conf.set( "record", args[3] );
    conf.set( "K", args[2] );

    long startTime = System.currentTimeMillis( ); 
    JobClient.runJob(conf);
    System.out.println( "Time (seconds): " + ( System.currentTimeMillis( ) - startTime ) / 1000.0 );
  }
}
