package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.util.List;

public class Exercise_4 {
	
	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {
		JavaRDD<String> vertices = ctx.textFile("src/main/resources/wiki-vertices.txt");
		JavaRDD<String> edges = ctx.textFile("src/main/resources/wiki-edges.txt");

		List<StructField> fieldsVertices = Lists.newArrayList();
		fieldsVertices.add(DataTypes.createStructField("id",DataTypes.LongType,false));
		fieldsVertices.add(DataTypes.createStructField("name",DataTypes.StringType,false));
		StructType schemaForVertices = DataTypes.createStructType(fieldsVertices);


		List<StructField> fieldsEdges = Lists.newArrayList();
		fieldsEdges.add(DataTypes.createStructField("src",DataTypes.LongType,false));
		fieldsEdges.add(DataTypes.createStructField("dst",DataTypes.LongType,false));
		StructType schemaForEdges = DataTypes.createStructType(fieldsEdges);

		Dataset<Row> V = sqlCtx.createDataFrame(vertices.map(v ->
				RowFactory.create(Long.parseLong(v.split("\t")[0]),v.split("\t")[1])),schemaForVertices);
		Dataset<Row> E = sqlCtx.createDataFrame(edges.map(e ->
				RowFactory.create(Long.parseLong(e.split("\t")[0]),Long.parseLong(e.split("\t")[1]))),schemaForEdges);
		GraphFrame G = GraphFrame.apply(V,E);

		org.graphframes.lib.PageRank pr = G.pageRank().resetProbability(0.15).maxIter(10);
		GraphFrame pageRankGraph = pr.run();
		for (Row r : pageRankGraph.vertices().sort(org.apache.spark.sql.functions.desc("pagerank")).toJavaRDD().take(10)) {
			System.out.println(r.getString(1));
		}

	}
	
}
