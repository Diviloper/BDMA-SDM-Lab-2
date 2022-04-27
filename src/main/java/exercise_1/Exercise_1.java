package exercise_1;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;
import utils.sendMessage;

import java.io.Serializable;
import java.util.List;

public class Exercise_1 {

    private static class VProg extends AbstractFunction3<Object, Integer, Integer, Integer> implements Serializable {
        @Override
        public Integer apply(Object vertexID, Integer vertexValue, Integer message) {
            System.out.println("Apply on vertex " + vertexID + " (" + vertexValue + ") <- " + message);
            return Math.max(vertexValue, message);
        }
    }

    private static class sendMsg extends sendMessage<EdgeTriplet<Integer, Integer>, Tuple2<Object, Integer>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Integer, Integer> triplet) {
            Tuple2<Object, Integer> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object, Integer> dstVertex = triplet.toTuple()._2();

            if (sourceVertex._2 <= dstVertex._2) {   // source vertex value is smaller than dst vertex?
//                System.out.println("Vertex " + sourceVertex._1 + " -> " + dstVertex._1 + ": No message");
                // do nothing
                return noMessages();
            } else {
                System.out.println("Vertex " + sourceVertex._1 + " -> " + dstVertex._1 + ": " + sourceVertex._2);
                // propagate source vertex value
                return message(new Tuple2<>(triplet.dstId(), sourceVertex._2));
            }
        }
    }

    private static class merge extends AbstractFunction2<Integer, Integer, Integer> implements Serializable {
        @Override
        public Integer apply(Integer o, Integer o2) {
            System.out.println("Merge messages (" + o + ") - (" + o2 + ")");
            return Math.max(o, o2);
        }
    }

    public static void maxValue(JavaSparkContext ctx) {
        List<Tuple2<Object, Integer>> vertices = Lists.newArrayList(
                new Tuple2<>(1L, 9),
                new Tuple2<>(2L, 1),
                new Tuple2<>(3L, 6),
                new Tuple2<>(4L, 8)
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<>(1L, 2L, 1),
                new Edge<>(2L, 3L, 1),
                new Edge<>(2L, 4L, 1),
                new Edge<>(3L, 4L, 1),
                new Edge<>(3L, 1L, 1)
        );

        JavaRDD<Tuple2<Object, Integer>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Integer, Integer> G = Graph.apply(
                verticesRDD.rdd(), edgesRDD.rdd(),
                1,
                StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                ClassTag$.MODULE$.apply(Integer.class), ClassTag$.MODULE$.apply(Integer.class)
        );

        GraphOps<Integer, Integer> ops = new GraphOps<>(
                G,
                ClassTag$.MODULE$.apply(Integer.class),
                ClassTag$.MODULE$.apply(Integer.class)
        );

        Tuple2<Object, Integer> max = ops.pregel(
                        Integer.MIN_VALUE,
                        Integer.MAX_VALUE,      // Run until convergence
                        EdgeDirection.Out(),
                        new VProg(),
                        new sendMsg(),
                        new merge(),
                        ClassTag$.MODULE$.apply(Integer.class))
                .vertices()
                .first();

        System.out.println(max._2 + " is the maximum value in the graph");
    }

}
