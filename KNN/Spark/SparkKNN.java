import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.lang.Math;


// MODE 1: INPUT THE TARGET POINT
// ARG 1 = input file name
// ARG 2 = k (number of neighbors)
// ARG 3 = target point, with each coordinate seperated by commas without spaces

// MODE 2: USE THE ORIGIN AS A TARGET [aka (0,0,0,0,....)]
// ARG 1 = input file name
// ARG 2 = k (number of neighbors)
// ARG 3 = number of dimensions in the space
// ARG 4 = 1 to print out the nearest neighbors, 2 to only print the runtime

public class SparkKNN {

    public static void main(String[] args) {
	// first check that inputs are given correctly
	if (args.length != 3 && args.length != 4){
            System.err.println("Parameters are: inputFileName K targetPoint in that order.");
	    System.err.println("    OR");
	    System.err.println("Parameters areL inputFileName K numDimensions 1");
	    System.err.println("      in this case, the origin will be used");
            System.exit(-1);
        }
	if (args.length == 3 && args[2].indexOf(",") == -1) {
	    System.err.println("When giving a target point, coordinates must be comma separated");
	    System.exit(-1);
	}
	// set up spark
        SparkConf conf = new SparkConf().setAppName("K Nearest Neighbors");
        JavaSparkContext jsc = new JavaSparkContext(conf);
	

        // Start Spark and read the specified given input file
        String inputFile = args[0];
        int k = Integer.parseInt(args[1]);
	// set up the target point
	// lambda variables must be final, so set up a temporary target placeholder
	double[] tar = new double[1];
	if (args.length == 3){
	    // if the user has indicated that they want to pass in a point, parse the point
	    String[] targetStrings = args[2].split(",");
	    tar = new double[targetStrings.length];
	    for (int i = 0; i < tar.length; i++){
		tar[i] = Double.parseDouble(targetStrings[i]);
	    }
	} else {
	    // if the user wants to use the origin, use an array of length dimension
	    int dims = Integer.parseInt(args[2]); // int[] has 0 as a default value.
	    tar = new double[dims];
	}
	// set a final array to be used in a lambda function.
	// In most circumstances, it would be better to clone the array, but tar will never be used again.
	final double[] target = tar;

	// read the lines from the file
        JavaRDD<String> lines = jsc.textFile(inputFile);

        // Start a timer
        long startTime = System.currentTimeMillis();
        
	// Using top(k) with a tuple is difficult, and using collect( ) uses a lot of memory
	// so instead we make use of the fact that a number stored as a string, sorted lexicographically, are sorted
	//     by size of value (because even as a string, 0 < 1 < 2 < 3 and so on
	JavaRDD<String> distances = lines.map( line -> {
		String[] point = line.split(",");
		double distance = 0.0;
		for (int i = 0; i < point.length; i++){
		    double val = Double.parseDouble(point[i]);
		    distance += (target[i] - val) * (target[i] - val);
		}
		// We can skip the sqrt, but it doesn't save much time
		distance = Math.sqrt(distance);
		// top() returns the largest things, so we have to invert order
		// Double.MAX_VALUE - distance would be slightly faster, but undoing it is hard
		// 1 / distance is only one operation, and has only 1 edge case to catch
		if (distance != 0.0){
		    // make sure not to divide by 0!
		    distance = 1.0 / distance;
		} else {
		    distance = Double.MAX_VALUE;
		}
		// both the distance and the point must be returned
		return Double.toString(distance) + " " + line; 
	    });
	// .collect( ) could work, but it is dangerous due to high memory consumption.
	// and you have to sort the list afterwards to get the top k.
	// List<Tuple2<Double, Double[]>> all = distances.collect();
        List<String> all = distances.top(k);

	// print out the k data points
	if (args.length == 4 && args[3].equals("2")){
	    // if testing mode is used, only print out the runtime.
	    System.out.print(System.currentTimeMillis() - startTime);
	} else {
	    // otherwise, print everything
	    for (String s : all){
		String[] split = s.split(" ");
		System.out.println(split[1] + " has distance: " + (1/Double.parseDouble(split[0])));
	    }
	    System.out.println("      Runtime was " +  (System.currentTimeMillis() - startTime) + " milliseconds.");
	} 
	jsc.stop( );
    }
}

