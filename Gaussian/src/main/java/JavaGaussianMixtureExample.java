package my.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.GaussianMixture;
import org.apache.spark.mllib.clustering.GaussianMixtureModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.regex.Pattern;
import java.util.*;
import java.io.*;


public class JavaGaussianMixtureExample  {
  public static void main(String[] args) throws IOException {

    SparkConf conf = new SparkConf().setAppName("JavaGaussianMixtureExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

  
    String path = "/home/vpinnaka/HDFS-K-means/Gaussian/Phones_accelerometer.txt";
	
	File file =new File("out.txt");
    file.createNewFile();
    long starttime = System.nanoTime(); 
    FileWriter writer = new FileWriter(file);
	
    JavaRDD<String> data = jsc.textFile(path);
    JavaRDD<Vector> parsedData = data.map(
      new Function<String, Vector>() {
		  
        public Vector call(String s) {
          String[] sarray = s.trim().split(",");
          
          
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

    // Cluster the data into two classes using GaussianMixture
    GaussianMixtureModel gmm = new GaussianMixture().setK(6).run(parsedData.rdd());

    // Save and load GaussianMixtureModel
    //gmm.save(jsc.sc(), "target/org/apache/spark/JavaGaussianMixtureExample/GaussianMixtureModel");
    //GaussianMixtureModel sameModel = GaussianMixtureModel.load(jsc.sc(),
    //  "target/org.apache.spark.JavaGaussianMixtureExample/GaussianMixtureModel");

    // Output the parameters of the mixture model
    for (int j = 0; j < gmm.k(); j++) {
      System.out.printf("weight=%f\nmu=%s\nsigma=\n%s\n",
        gmm.weights()[j], gmm.gaussians()[j].mu(), gmm.gaussians()[j].sigma());
		writer.write("weight=%f\nmu=%s\nsigma=\n%s\n",
        gmm.weights()[j], gmm.gaussians()[j].mu(), gmm.gaussians()[j].sigma());
    }
    // $example off$

    jsc.stop();
  }
}