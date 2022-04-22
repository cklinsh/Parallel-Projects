import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.io.IOException;
import java.util.*;
import java.io.File;
import java.io.FileNotFoundException;

import mpi.*;

public class MPIKNN {
	private static double[] stringToDoubleArray( String[] arr ) {
    	final int N = arr.length;
      	double[] doubles = new double[N];
      	for( int i = 0; i < N; ++i ) {
    		doubles[i] = Double.parseDouble( arr[i] );
      	}

      	return doubles;
    }

	public static void main(String[] args) throws MPIException, FileNotFoundException {
		String inputFilename = args[2];
		File inputFile = new File(inputFilename);
		int K = Integer.parseInt(args[0]);

		// A is our list of coordinates
		ArrayList<double[]> A = new ArrayList<double[]>();

		// Add data in input file to A
		Scanner inputReader = new Scanner(inputFile);
		while (inputReader.hasNextLine()) {
			String line = inputReader.nextLine();
			A.add(stringToDoubleArray(line.split( "," )));
		}

		// our origin point to find the knn of
		double[] origin = stringToDoubleArray(args[1].split( "," ));
		int size = A.size();

		if (size == 0) {
			System.out.println("Input file can't be empty!!!");
			return;
		}
      	
		final int COLS = A.get(0).length;

		// Time starts when we start MPI and start performing KNN
		long start = System.currentTimeMillis();

		// Initialize MPI
		MPI.Init(args);
 		int myRank = MPI.COMM_WORLD.Rank();
		int mpiSize = MPI.COMM_WORLD.Size();

		// partitioned stripe
		int stripe = size / mpiSize; 

		// calculate offsets in case of uneven stripe distribution 
		int offset = myRank < size % mpiSize ? myRank + 1 : size % mpiSize;
		int lastOffset = myRank - 1 < size % mpiSize ? myRank : size % mpiSize;
		int rankPartitionStart = (stripe * myRank) + lastOffset;
		int rankPartitionEnd = stripe * (myRank + 1) + offset - 1;
		int rankParitionSize = rankPartitionEnd - rankPartitionStart + 1;

		// Array elements set to 0.0 by default
		double[][] res = new double[rankParitionSize][COLS + 1];
		double[][] allRankKNN = new double[K * mpiSize][COLS + 1];
		
		// Calculate distances
		for (int i = rankPartitionStart; i <= rankPartitionEnd; i++) {
			int localIdx = i - rankPartitionStart;

			// Each point has its Euclidian distance from the target origin calculated
			for(int col = 0; col < COLS; col++) {
				res[localIdx][0] += Math.pow(A.get(i)[col] - origin[col], 2);
				res[localIdx][col + 1] = A.get(i)[col];
			}

			res[localIdx][0] = Math.sqrt(res[localIdx][0]);
		}

		// Sort by distances so we can extract the KNN from the rank
		Arrays.sort(res, new java.util.Comparator<double[]>() {
			public int compare(double[] a, double[] b) {
				return Double.compare(a[0], b[0]);
			}
		});

		if (myRank == 0) {
			// Copy rank 0's KNN to allRankKNN
			for (int i = 0; i < K; i++) {
				allRankKNN[i] = res[i];
			}

			// Recieve the KNN calculated by all other ranks
			for (int i = 1; i < mpiSize; i++) {
				MPI.COMM_WORLD.Recv(allRankKNN, i * K, K, MPI.OBJECT, i, 0);
			}
			
			// Sort by distances so we can extract the overall KNN
			Arrays.sort(allRankKNN, new java.util.Comparator<double[]>() {
				public int compare(double[] a, double[] b) {
					return Double.compare(a[0], b[0]);
				}
			});

			// Output results
			for (int i = 0; i < K; i++) {
				System.out.print("Nearest Neighbor #" + i + ": (" + allRankKNN[i][1]);
				for (int j = 2; j < allRankKNN[0].length; j++) {
					System.out.print("," + allRankKNN[i][j]);
				} 
				System.out.println("), Distance: " + allRankKNN[i][0]);
			}

			// Output time
			long timeElapsed = System.currentTimeMillis() - start;
			System.out.println("Elapsed Time: " + timeElapsed + "ms");
		} else {
			// Send this rank's slices' KNN back to rank 0
			MPI.COMM_WORLD.Send(res, 0, K, MPI.OBJECT, 0, 0);
		}

		// Teardown MPI
		MPI.Finalize();
	}
}
