package exercise_4;

import com.google.common.collect.Lists;
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
        // ------VERTICES------
        // Read vertices file
        JavaRDD<String> vertices = ctx.textFile("src/main/resources/wiki-vertices.txt");

        // Create vertices metadata
        List<StructField> fieldsVertices = Lists.newArrayList(
                DataTypes.createStructField("id", DataTypes.LongType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false)
        );
        StructType schemaForVertices = DataTypes.createStructType(fieldsVertices);

        // Convert lines into rows (since file is tab separated, split by \t)
        Dataset<Row> V = sqlCtx.createDataFrame(
                vertices.map(v -> v.split("\t")).map(v -> RowFactory.create(Long.parseLong(v[0]), v[1])),
                schemaForVertices);


        // ------EDGES------
        // Read edges file
        JavaRDD<String> edges = ctx.textFile("src/main/resources/wiki-edges.txt");

        // Create edges metadata
        List<StructField> fieldsEdges = Lists.newArrayList(
                DataTypes.createStructField("src", DataTypes.LongType, false),
                DataTypes.createStructField("dst", DataTypes.LongType, false)
        );
        StructType schemaForEdges = DataTypes.createStructType(fieldsEdges);

        // Convert lines into rows (since file is tab separated, split by \t)
        Dataset<Row> E = sqlCtx.createDataFrame(
                edges.map(e -> e.split("\t"))
                        .map(e -> RowFactory.create(Long.parseLong(e[0]), Long.parseLong(e[1]))),
                schemaForEdges);

        // ------GRAPH------
        // Create graph from Vertices and Edges
        GraphFrame G = GraphFrame.apply(V, E);

        // Apply PageRank
        org.graphframes.lib.PageRank pr = G.pageRank().resetProbability(0.15).maxIter(15);
        GraphFrame pageRankGraph = pr.run();

        List<Row> topPages = pageRankGraph.vertices()
                .sort(org.apache.spark.sql.functions.desc("pagerank"))
                .toJavaRDD()
                .take(10);
        for (Row r : topPages) {
            System.out.println(r.getDouble(2) + ": " + r.getString(1));
        }
    }
}
