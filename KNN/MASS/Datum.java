/*

  MASS KNN
  Datum.java extends Place
  Each Datum object represents a single point of data

*/

package edu.uwb.css534;

import java.net.InetAddress;

import edu.uw.bothell.css.dsl.MASS.Place;
import edu.uw.bothell.css.dsl.MASS.MASS;

import java.lang.Math;
import java.util.Arrays;
import java.util.Vector;


public class Datum extends Place {

    public static final int GET_HOSTNAME = 0;
    public static final int CALCULATE_DISTANCE = 1;
    public static final int PASS_K_BEST = 2;
    public static final int CALC_K_BEST = 3;
    public static final int INIT_PLACE = 4;
    public static final int GET_KNN = 5;

    private double[] bestKDistances;
    private double[][] bestKPoints;
    private double[][] coordinates;
    private int k;
    private int size;
    private Vector<int[]> neighbors;
	
    /**
     * This constructor will be called upon instantiation by MASS
     * The Object supplied MAY be the same object supplied when Places was created
     * @param obj should be a datumArgs object
     */
    public Datum(Object obj) {//might need super();
    }
	
    /**
     * This method is called when "callAll" is invoked from the master node
     */
    public Object callMethod(int method, Object o) {
		
	switch (method) {
		
	case GET_HOSTNAME:
	    return findHostName(o);
		
	case CALCULATE_DISTANCE:
	    calculateDistance(o);
	    return null;

	case PASS_K_BEST:
	    return passKBest(o);

        case INIT_PLACE:
	    init(o);
	    return null;

	case GET_KNN:
	    return getKNN(); 


        case CALC_K_BEST:
	    calculateKBest(o);
	    return null;
		
	default:
	    return new String("Unknown Method Number: " + method);
		
	}
		
    }

    public Object callMethod(int method){
	switch(method) {

	case GET_KNN:
	    return getKNN();

        case CALC_K_BEST:
	    calculateKBest(null);
	    return null;

	default:
	    return new String("Unknown Method Number: " + method);
	}
    }



    /**
     * initialize this point, and set the neighbors
     * @param o an object representing a DatumArgs object.
     */ 
    public void init(Object o){
	DatumArgs datumArgs = (DatumArgs) o;
	int myIndex = getIndex()[0];
	this.k = datumArgs.getK();
	this.size = datumArgs.getSize(myIndex);
	this.coordinates = datumArgs.getCoordinates(myIndex);
	 this.bestKDistances = new double[k];
	this.bestKPoints = new double[k][coordinates.length];
	int maxInd = datumArgs.getMaxIndex();
	Vector<int[]> neighbors_ = new Vector<int[]>();
	// if (myIndex * 2 + 2 <= maxInd ){
	//     // this is a vertex with 2 children
	//     neighbors.add(new int[] {((myIndex * 2 + 1) - myIndex), 0});
	//     neighbors.add(new int[] {((myIndex * 2 + 2) - myIndex), 0});
	// } else if ( myIndex * 2 + 1 <= maxInd ) {
	//     // this is a vertex with 1 child
	//     neighbors.add(new int[] {((myIndex * 2 + 1) - myIndex), 0});
	// } else {
	//     neighbors.add(new int[] {0, 0});
	// }
	if (myIndex == 0){
	    for (int i = 1; i <= maxInd; i++){
		neighbors_.add(new int[] {i, 0});
	    }
	} else {
	    neighbors_.add(new int[] {0,0});
	}
	this.neighbors = neighbors_;
	setNeighbors(neighbors_);
    }
	
    /**
     * Return a String identifying where this Place is actually located
     * @param o
     * @return The hostname (as a String) where this Place is located
     */
    public Object findHostName(Object o){
		
	try{
	    return (String) "Place located at: " + InetAddress.getLocalHost().getCanonicalHostName() +" " + Integer.toString(getIndex()[0]);
        }
		
	catch (Exception e) {
	    return "Error : " + e.getLocalizedMessage() + e.getStackTrace();
	}
    
    }
    
    /**
     * Calculate the distance from all points in this place to the target point
     * 
     * @param o (object representing a double[], the coordinates of the target point)
     */
    public void calculateDistance(Object o){
        double[] target = (double[]) o;
	Pair[] pairs = new Pair[this.size];
	// iterate through each point in this place, setting distances
	for (int j = 0; j < this.size; j++){
	    double dist = 0;
	    for (int i = 0; i < target.length; i++){
		dist += Math.pow(target[i] - this.coordinates[j][i], 2); 
	    }
	    pairs[j] = new Pair(Math.sqrt(dist), this.coordinates[j]);
	}
        MASS.getLogger().debug( "in calculateDistance my index " + Arrays.toString(getIndex()));
        MASS.getLogger().debug( "my distances are " + Arrays.toString(this.bestKDistances));
	MASS.getLogger().debug("My best k points are: " + Arrays.toString(pairs));

	Arrays.sort(pairs, (Pair o1, Pair o2) -> { return o1.compareTo(o2); });
        
	for (int i = 0; (i < this.size && i < this.k); i++){
            this.bestKDistances[i] = pairs[i].dist;
	    this.bestKPoints[i] = pairs[i].point;
        }
	
	// if there are fewer than k points in this place, set the distances to MAX_VALUE
	// the coordinates will be sorted based on the distances, so leftover coords don't need initialization
	if (this.size < this.k) {
	    for (int i = this.size; i < this.k; i++){
		this.bestKDistances[i] = Double.MAX_VALUE;
	    }
	}
	MASS.getLogger().debug( "in calculateDistance my index " + Arrays.toString(getIndex()));
        MASS.getLogger().debug( "my best k points " + Arrays.toString(this.bestKPoints[0]) + " " + Arrays.toString(this.bestKPoints[1]));
        MASS.getLogger().debug( "my distances are " + Arrays.toString(this.bestKDistances));
    }




    /**
     * Send best k points and distances, as stored in a DatumMessage object
     * @param o required but not used.
     */
    public DatumMessage passKBest(Object o){
	DatumMessage dM = new DatumMessage(Arrays.copyOf(this.bestKDistances, this.k), this.bestKPoints);
	// I should set my bestKDistances to MAX_VALUE after I send, if I haven't already done so
	// this will prevent duplicate points from appearing due to being sent multiple times
	// if (this.bestKDistances[0] != Double.MAX_VALUE){
	//     for (int i = 0; (i < this.size && i < this.k); i++){
	// 	this.bestKDistances[i] = Double.MAX_VALUE;
	//     }	
	// }
	MASS.getLogger().debug( "in passKBest my index " + Arrays.toString(getIndex()));
        MASS.getLogger().debug( "my best k points " + Arrays.toString(this.bestKPoints[0]) + " " + Arrays.toString(this.bestKPoints[1]));
	MASS.getLogger().debug( "my distances are " + Arrays.toString(this.bestKDistances));
	return dM;
    }

    /**
     * merge sort all best k points given by neighbors (including this Datum's best k)
     */
    public void calculateKBest(Object o){
	if (getIndex()[0] == 0) {
	    // first, get the messages from this Places neighbors
	    Object[] inMess = getInMessages();
	    MASS.getLogger().debug( "----------------------------------------------------- I have index " + Arrays.toString(getIndex()) + " inMess.length " + inMess.length + " inMess: " + Arrays.toString(inMess));
	    // set up new arrays which will hold the new top k points
	    double[] newDists;
	    double[][] newCoords;
	    for (int i = 0; i < inMess.length; i++){
		// no message should ever be null, but if one is we prefer not to crash
		if (inMess[i] == null){
		    continue;
		}
		newDists = new double[this.k];
		newCoords = new double[this.k][this.coordinates.length];
		// each element of inMess is a DatumMessage as constructed by passKbest( )
		// because these are stored as objects, iterating over each element of inMess
		// is more straightforward than iterating through the indices of the elements of inMess
		// which would likely be slightly faster
		DatumMessage dM = (DatumMessage) inMess[i];
		double[] dists2 = dM.getDistances();
		double[][] coords2 = dM.getCoordinates();
		int ind1 = 0, ind2 = 0;
		MASS.getLogger().debug( "in calculateKBest my index " + Arrays.toString(getIndex()));
		MASS.getLogger().debug( "my neighbors best k points " + Arrays.toString(coords2[0]) + " " + Arrays.toString(coords2[1]));
		MASS.getLogger().debug( "my neighbors distances are " + Arrays.toString(dists2));

		// merge sort this places best k with a neighbors best k
		// as we are merging sorted lists, we don't need to worry about overwriting
		// each place sets its original bestKDistances to MAX_VALUE, so we don't have to worry about duplicates 
		for (int j = 0; j < this.k; j++){
		
		    MASS.getLogger().debug( "my old points " + Arrays.toString(newCoords[0]) + " " + Arrays.toString(newCoords[1]));
		    if (dists2[ind2] < this.bestKDistances[ind1]){
			newDists[j] = dists2[ind2];
			newCoords[j] = coords2[ind2];
			ind2++;
		    } else {
			newDists[j] = this.bestKDistances[ind1];
			newCoords[j] = this.bestKPoints[ind1];
			ind1++;
		    }
		    MASS.getLogger().debug( "my new points " + Arrays.toString(newCoords[0]) + " " + Arrays.toString(newCoords[1]));
		}
		// set this places best points inside the loop so they can be compared to the next neighbor's best points
		this.bestKDistances = newDists;
	    this.bestKPoints = newCoords;
	}
	    
	    
	}

	MASS.getLogger().debug( "ending calculateKBest my index " + Arrays.toString(getIndex()));
	MASS.getLogger().debug( "my best k points " + Arrays.toString(this.bestKPoints[0]) + " " + Arrays.toString(this.bestKPoints[1]));
	MASS.getLogger().debug( "my distances are " + Arrays.toString(this.bestKDistances));
    }

    public Object getKNN() {
	String toReturn = "";
	if (getIndex()[0] == 0){
	    MASS.getLogger().debug( "my index " + Arrays.toString(getIndex()));
	    MASS.getLogger().debug( "my best k points " + Arrays.toString(this.bestKPoints[0]) + " " + Arrays.toString(this.bestKPoints[1]));
	
	    for (int i = 0; i < this.bestKPoints.length; i++){
		if (this.bestKDistances[i] == Double.MAX_VALUE){
		    break;
		}
		toReturn += Arrays.toString(this.bestKPoints[i]) + " has distance " + this.bestKDistances[i] + " \n ";
	    }; 
	}
	return (Object) toReturn;
    }

    private class Pair implements Comparable<Pair> {

	public double dist;
	public double[] point;

	public Pair( double _dist, double[] _point ) {
		dist = _dist;
		point = _point;
	}

	@Override
	public int compareTo( Pair obj ) {
	    return Double.compare( dist, obj.dist );
	}
    }

}

