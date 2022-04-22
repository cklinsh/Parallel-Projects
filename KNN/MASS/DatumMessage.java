package edu.uwb.css534;

import java.io.Serializable;

public class DatumMessage implements Serializable {

    private double[] distances;
    private double[][] coordinates;

    public DatumMessage(){
        super();
    }

    public DatumMessage(double[] distances, double[][] coordinates){
        this.distances = distances;
	this.coordinates = coordinates;
    }

    public double[] getDistances(){
        return this.distances;
    }

    public double[][] getCoordinates(){
        return this.coordinates;
    }

}
