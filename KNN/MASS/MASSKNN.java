/*
  MASSKNN.java
  An implementation of K-Nearest Neighbors using an MASS.
  Implemented using the MASS library for UWB CSS534 AU A Programming Assignment 5

*/

// ARG 1: k, the number of data points to retreive. K MUST EXCEED 1.
// ARG 2: number of data points.
//            if this is incorrect, the program will crash.
// ARG 3: the target point, comma seperated doubles without spaces.
//            if the number of dimensions of the target is not the same
//            as the dimensions of the data, the prgoram will crash.
// ARG 4: data file with extension.
// ARG 5: number of computing nodes the code is being run on.
//            if this is incorrect, the program will crash.


package edu.uwb.css534;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import java.io.BufferedReader;
import java.io.FileReader;

import edu.uw.bothell.css.dsl.MASS.Agents;
import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Places;
import edu.uw.bothell.css.dsl.MASS.logging.LogLevel;

public class MASSKNN {
        
    private static final String NODE_FILE = "nodes.xml";
	
    

    @SuppressWarnings("unused")		// some unused variables left behind for easy debugging
    public static void main( String[] args ) {
	
	if (args.length != 5){
	    System.err.println("Arguments are numberOfNeighbors numberOfDataPoints targetPoint dataFileName.extension numberOfComputingNodes");
	    System.exit(-1);
	}
	    
	// initialize K
	int K = Integer.parseInt(args[0]);
	int N = Integer.parseInt(args[1]);
	double[] target = toDoubleArray(args[2]);
	int numNodes = Integer.parseInt(args[4]);

	// remember starting time
	long startTime = System.currentTimeMillis();
	
	// read data from file
	String[] data = new String[N];
	try {
	BufferedReader reader = new BufferedReader(new FileReader(args[3]));
	
	int counter = 0;
	String line = "";
	while((line = reader.readLine()) != null){
	    data[counter] = line;
	    counter++;
	}
	} catch (Exception e){
	    System.err.println(e);
	    System.exit(-1);
	}
	DatumArgs datumArgs = new DatumArgs(K, numNodes, data);
		
	// init MASS library
	MASS.setNodeFilePath( NODE_FILE );
	MASS.setLoggingLevel( LogLevel.DEBUG );
		
	// start MASS
	MASS.getLogger().debug( "MASSKNN initializing MASS library..." );
	MASS.init();
	MASS.getLogger().debug( "MASS library initialized" );
		
	/* 
	 * Create all Places (having dimensions of x, y, and z)
	 * ( the total number of Place objects that will be created is: x * y * z )
	 */
		
	MASS.getLogger().debug( "MASSKNN creating Places..." );
	Places places = new Places( 1, Datum.class.getName(), ( Object ) new Integer( 0 ), numNodes, 1);
	MASS.getLogger().debug( "Places created" );
	        
	// initialize all places with their data and neighbors
	MASS.getLogger().debug( "MASSKNN sending callAll INIT to Places..." );
	places.callAll( Datum.INIT_PLACE, (Object) datumArgs );
	MASS.getLogger().debug( "Places callAll INIT operation complete" );
		
	// All places calculate their distances  
	MASS.getLogger().debug( "MASSKNN sending callAll CALCULATE_DISTANCE to Places..." );
	places.callAll( Datum.CALCULATE_DISTANCE, (Object) target );
	MASS.getLogger().debug( "Places callAll CALCULATE_DISTANCE operation complete" );

	    
	    // tell all places to communicate with their neighbors
	MASS.getLogger().debug( "MASSKNN instructs all Places to pass on their KNN..." );
	places.exchangeAll(1, Datum.PASS_K_BEST);
	MASS.getLogger().debug( "Passing KNN complete." );

	// tell all places to communicate with their neighbors
	MASS.getLogger().debug( "MASSKNN instructs all Places to recalculate their KNN..." );
	places.callAll(Datum.CALC_K_BEST);
	MASS.getLogger().debug( "Recalculating KNN complete." );
        	
	// find index[0]'s KNN
        Object[] KNNs = places.callAll(Datum.GET_KNN, null);
        
		
	// orderly shutdown
	MASS.getLogger().debug( "MASSKNN instructs MASS library to finish operations..." );
	MASS.finish();
	MASS.getLogger().debug( "MASS library has stopped" );
		
	// calculate / display execution time
	System.out.println("The closest " + K + " points are...");
	System.out.println(KNNs[0]);
	System.out.println("      Runtime was " +  (System.currentTimeMillis() - startTime) + " milliseconds.");
		
    }

    public static double[] toDoubleArray(String str){
	String[] strArr = str.split(",");
	double[] toReturn = new double[strArr.length];
	for (int i = 0; i < toReturn.length; i++) {
	    toReturn[i] = Double.parseDouble(strArr[i]);
	}
	return toReturn;
    }
	 
}
