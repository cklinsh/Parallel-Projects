package edu.uwb.css534;

import java.io.Serializable;

public class DatumArgs implements Serializable {

    private int k;
    private int dimensions;
    private int maxIndex;
    private int N;
    private double[][] coordinates;
    private int numNodes;
    
    public DatumArgs(){
	super();
    }

    public DatumArgs(int k, int numNodes, String[] strArr){
	this.k = k;
	this.N = strArr.length;
	this.numNodes = numNodes;
	this.maxIndex = this.numNodes - 1;
	this.coordinates = new double[this.N][];
	for (int j = 0; j < this.N; j++){
	    String[] coordStr = strArr[j].split(",");
	    double[] coords = new double[coordStr.length];
	    for (int i = 0; i < coords.length; i++){
		coords[i] = Double.parseDouble(coordStr[i]);
	    }
	    this.coordinates[j] = coords;
	}
	this.dimensions = this.coordinates[0].length;
    }

    public int getK(){
	return this.k;
    }

    public int getN(){
	return this.N;
    }

    public int getMaxIndex(){
	return this.maxIndex;
    }

    public int getSize(int index){
	// if the requesting place is the root, assign leftovers
	if (index == 0){
	    return ((this.N % this.numNodes) + (this.N / this.numNodes));
	}
	return this.N/this.numNodes;
    }

    public double[][] getCoordinates(int index){
	int idealSize = this.N / this.numNodes; // size for all places if N % numPlaces == 0
	double[][] toReturn = new double[idealSize][this.dimensions];
	if (index == 0){
	    // the root gets the last points, because there could be more than N/numNodes points left over
	    // after that many points have been assigned to all other places
	    int largerSize = ((this.N % this.numNodes) + idealSize);
	    toReturn = new double[largerSize][this.dimensions];
	    for (int i = 0; i < largerSize; i++){
		toReturn[i] = this.coordinates[this.N - 1 - i];
	    }
	} else {
	    int displacement = (index - 1) * (idealSize);
	    // Non-root places get N/numNodes points
	    for (int i = 0; i < idealSize; i++){
		// since 0 gets the last (potentially extra) points, 1 gets 0 to k-1, 2 gets k to 2*k-1, ...
		toReturn[i] = this.coordinates[i + displacement];
	    }
	}
	return toReturn;
    }

}
