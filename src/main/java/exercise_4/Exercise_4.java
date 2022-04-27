package exercise_4;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;

import java.util.List;

public class Exercise_4 {

    public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {
        Dataset<Row> V = createVertexDataset(ctx, sqlCtx);
        Dataset<Row> E = createEdgeDataset(ctx, sqlCtx);

        GraphFrame G = GraphFrame.apply(V, E);
        PageRank pr = G.pageRank().resetProbability(0.15);

        Integer maxIterations = findBestIterations(pr);
//        Integer maxIterations = 20; // Uncomment this line and comment the previous one to run with predefined number of iterations
        System.out.println("Running with " + maxIterations + " iterations");
        pr.maxIter(maxIterations);

        GraphFrame pageRankGraph = pr.run();

        List<Row> topPages = pageRankGraph
                .vertices()
                .sort(functions.desc("pagerank"))
                .takeAsList(10);
        for (Row r : topPages) {
            System.out.println(r.getDouble(2) + ": " + r.getString(1));
        }
    }

    private static Dataset<Row> createEdgeDataset(JavaSparkContext ctx, SQLContext sqlCtx) {
        // Read edges file
        JavaRDD<String> edges = ctx.textFile("src/main/resources/wiki-edges.txt");

        // Create edges metadata
        StructType schemaForEdges = DataTypes.createStructType(Lists.newArrayList(
                DataTypes.createStructField("src", DataTypes.LongType, false),
                DataTypes.createStructField("dst", DataTypes.LongType, false)
        ));

        // Convert lines into rows (since file is tab separated, split by \t)
        return sqlCtx.createDataFrame(
                edges
                        .map(e -> e.split("\t"))
                        .map(e -> RowFactory.create(Long.parseLong(e[0]), Long.parseLong(e[1]))),
                schemaForEdges);
    }

    private static Dataset<Row> createVertexDataset(JavaSparkContext ctx, SQLContext sqlCtx) {
        // Read vertices file
        JavaRDD<String> vertices = ctx.textFile("src/main/resources/wiki-vertices.txt");

        // Create vertices metadata
        StructType schemaForVertices = DataTypes.createStructType(Lists.newArrayList(
                DataTypes.createStructField("id", DataTypes.LongType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false)
        ));

        // Convert lines into rows (since file is tab separated, split by \t)
        return sqlCtx.createDataFrame(
                vertices.map(v -> v.split("\t")).map(v -> RowFactory.create(Long.parseLong(v[0]), v[1])),
                schemaForVertices);
    }

    private static Integer findBestIterations(PageRank pr) {
        Dataset<Row> previous = pr.maxIter(1).run().vertices();
        int maxIter = 2;
        while (true) {
            pr.maxIter(maxIter);
            Dataset<Row> current = pr.run().vertices();
            Double difference = getAverageDifference(previous, current);
            System.out.println("" + maxIter + " iterations: " + difference);
            if (difference < 0.0001) break;
            previous = current;
            maxIter++;
        }
        return maxIter;
    }

    private static Double getAverageDifference(Dataset<Row> previous, Dataset<Row> next) {
        return previous.drop("name").withColumnRenamed("pagerank", "previous_pagerank")
                .join(next.drop("name").withColumnRenamed("pagerank", "next_pagerank"), "id")
                .selectExpr("id", "abs(next_pagerank - previous_pagerank) AS diff")
                .agg(functions.avg("diff"))
                .first().getDouble(0);
    }
}
