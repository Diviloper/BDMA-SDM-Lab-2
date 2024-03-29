import com.google.common.io.Files;

import exercise_1.Exercise_1;
import exercise_2.Exercise_2;
import exercise_3.Exercise_3;
import exercise_4.Exercise_4;
import exercise_4.Exercise_4_warmup;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import utils.Utils;

import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;


public class Main {

//	static String HADOOP_COMMON_PATH = "D:\\Master\\BDMA\\Courses\\Semester_2\\Semantic_Data_Management\\Lab2\\BDMA-SDM-Lab-2\\src\\main\\resources"; // "C:\\...\\SparkGraphXassignment\\src\\main\\resources"
	static String HADOOP_COMMON_PATH = "C:\\Users\\Victor\\Development\\BDMA\\UPC\\SDM\\DistributedGraphsLab\\src\\main\\resources"; // "C:\\...\\SparkGraphXassignment\\src\\main\\resources"

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);

		SparkConf conf = new SparkConf().setAppName("SparkGraphs_II").setMaster("local[*]");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		ctx.setCheckpointDir(Files.createTempDir().getAbsolutePath());
		
		SQLContext sqlctx = new SQLContext(ctx);
		
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(
                Level.ERROR);

		if (args.length != 1) throw new Exception("Parameter expected: exercise number");

		if (args[0].equals("exercise1")) {
		    Exercise_1.maxValue(ctx);
        }
        else if (args[0].equals("exercise2")) {
            Exercise_2.shortestPaths(ctx);
        }
        else if (args[0].equals("exercise3")) {
            Exercise_3.shortestPathsExt(ctx);
        }
        else if (args[0].equals("exercise4_warmup")) {
        	Exercise_4_warmup.warmup(ctx,sqlctx);
        }
        else if (args[0].equals("exercise4")) {
            Exercise_4.wikipedia(ctx,sqlctx);
        }
        else {
		    throw new Exception("Wrong exercise number");
        }

	}

}
