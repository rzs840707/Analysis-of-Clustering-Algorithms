package my.spark;

import java.util.regex.Pattern;
import java.util.*;
import java.io.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;





public class JavaKMeansExample {
  public static void main(String[] args) throws IOException {

    
    SparkConf conf = new SparkConf().setAppName("JavaKMeansExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    String path = "/home/vpinnaka/HDFS-K-means/K-means/Phones_accelerometer.txt";
	
	
	File file =new File("out.txt");
    file.createNewFile();
    long starttime = System.nanoTime(); 
    FileWriter writer = new FileWriter(file);
	
	
    JavaRDD<String> data = jsc.textFile(path);
    JavaRDD<Vector> parsedData = data.map(
      new Function<String, Vector>() {
        public Vector call(String s) {
          String[] sarray = s.split(",");
		  String decimalPattern = "([0-9]*)\\.([0-9]*)"; 
          double[] values = new double[12];
		
          for (int i = 0; i < sarray.length; i++) {
			  if(i==0)
				  continue;
			  boolean match = Pattern.matches(decimalPattern, sarray[i]);
			  if(match)
				values[i-1] = Double.parseDouble(sarray[i]);
			  else
			  {
			
				  if(i == 6 || i == 8 || i==7){
					  continue;
				  }
				 /*  else if(i == 7)
				  {
					  
					  if(sarray[i].equals("nexus4"))
					  {
						  values[6] = 1.0;
						  values[7] = 0.0;
						  values[8] = 0.0;
						  values[9] = 0.0;
						  values[10] = 0.0;
					  }
					  else if(sarray[i].equals("s3"))
					  {
						  values[6] = 0.0;
						  values[7] = 1.0;
						  values[8] = 0.0;
						  values[9] = 0.0;
						  values[10] = 0.0;
					  }
					  else if(sarray[i].equals("s3mini"))
					  {
						  values[6] = 0.0;
						  values[7] = 0.0;
						  values[8] = 1.0;
						  values[9] = 0.0;
						  values[10] = 0.0;
					  }
					  else if(sarray[i].equals("samsungold"))
					  {
						  values[6] = 0.0;
						  values[7] = 0.0;
						  values[8] = 0.0;
						  values[9] = 1.0;
						  values[10] = 0.0;
					  }
					  else{
						  values[6] = 0.0;
						  values[7] = 0.0;
						  values[8] = 0.0;
						  values[9] = 0.0;
						  values[10] = 1.0;
						  
					  }
				  } */
				  else if(i == 9)
				  {
					  if(sarray[i].equals("bike"))
					  {
						  values[5] = 1.0;
						  values[6] = 0.0;
						  values[7] = 0.0;
						  values[8] = 0.0;
						  values[9] = 0.0;
						  values[10] = 0.0;
						  values[11] = 0.0;
					  }
					  else if(sarray[i].equals("sit"))
					  {
						  values[5]  = 0.0;
						  values[6]  = 1.0;
						  values[7]  = 0.0;
						  values[8]  = 0.0;
						  values[9]  = 0.0;
						  values[10] = 0.0;
						  values[11] = 0.0;
					  }
					  else if(sarray[i].equals("stand"))
					  {
						  values[5]  = 0.0;
						  values[6]  = 0.0;
						  values[7]  = 1.0;
						  values[8]  = 0.0;
						  values[9]  = 0.0;
						  values[10] = 0.0;
						  values[11] = 0.0;
					  }
					  else if(sarray[i].equals("walk"))
					  {
						  values[5]  = 0.0;
						  values[6]  = 0.0;
						  values[7]  = 0.0;
						  values[8]  = 1.0;
						  values[9]  = 0.0;
						  values[10] = 0.0;
						  values[11] = 0.0;
					  }
					  else if(sarray[i].equals("stairsup"))
					  {
						  values[5]  = 0.0;
						  values[6]  = 0.0;
						  values[7]  = 0.0;
						  values[8]  = 0.0;
						  values[9]  = 1.0;
						  values[10] = 0.0;
						  values[11] = 0.0;
					  }
					  else if(sarray[i].equals("stairsdown"))
					  {
						  values[5]  = 0.0;
						  values[6]  = 0.0;
						  values[7]  = 0.0;
						  values[8]  = 0.0;
						  values[9]  = 0.0;
						  values[10] = 1.0;
						  values[11] = 0.0;
					  }
					  else if(sarray[i].equals("null"))
					  {
						  values[5]  = 0.0;
						  values[6]  = 0.0;
						  values[7]  = 0.0;
						  values[8]  = 0.0;
						  values[9]  = 0.0;
						  values[10] = 0.0;
						  values[11] = 1.0;
					  }
					
					  
				  }
				  
			    }
			}
		  
          return Vectors.dense(values);
        
		
		 }
				 
          
      }
    );
    parsedData.cache();

    int numClusters = 2;
    int numIterations = 5;
	//for(;numClusters < 7;numClusters++)
	//{
		KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

		System.out.println("Cluster centers:");
		writer.write("Cluster centers:");
		for (Vector center: clusters.clusterCenters()) {
		  System.out.println(" " + center);
		  writer.write(" " + center);
		}
		double cost = clusters.computeCost(parsedData.rdd());
		System.out.println("Cost: " + cost);
		writer.write("Cost: " + cost);

		double WSSSE = clusters.computeCost(parsedData.rdd());
		System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
		writer.write("Within Set Sum of Squared Errors = " + WSSSE);
	//}

    //clusters.save(jsc.sc(), "target/");
    //KMeansModel sameModel = KMeansModel.load(jsc.sc(),
    //  "target/org/apache/spark/JavaKMeansExample/KMeansModel");
	writer.flush();
    writer.close();
    jsc.stop();
  }
}
