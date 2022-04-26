package exercise_2;

import com.google.common.collect.ImmutableMap;
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
import java.util.Map;

public class Exercise_2 {

    private static class VProg extends AbstractFunction3<Object, Integer, Integer, Integer> implements Serializable {
        @Override
        public Integer apply(Object vertexID, Integer vertexValue, Integer message) {
            return Math.min(vertexValue, message);
        }
    }

    private static class sendMsg extends sendMessage<EdgeTriplet<Integer, Integer>, Tuple2<Object, Integer>> implements Serializable {

        @Override
        public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Integer, Integer> triplet) {
            Tuple2<Object, Integer> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object, Integer> dstVertex = triplet.toTuple()._2();

            if (sourceVertex._2.equals(Integer.MAX_VALUE)) {
                // Source vertex has not been visited
                return noMessages();
            } else if (sourceVertex._2 + triplet.attr < dstVertex._2) {
                // Source vertex has been visited and yields a shorter path for destination vertex
                return message(new Tuple2<>(triplet.dstId(), sourceVertex._2 + triplet.attr));
            } else {
                // Source vertex has been visited but doesn't yield a shorter path for destination vertex
                return noMessages();
            }
        }
    }

    private static class merge extends AbstractFunction2<Integer, Integer, Integer> implements Serializable {
        @Override
        public Integer apply(Integer o, Integer o2) {
            return Math.min(o, o2);
        }
    }

    public static void shortestPaths(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1L, "A")
                .put(2L, "B")
                .put(3L, "C")
                .put(4L, "D")
                .put(5L, "E")
                .put(6L, "F")
                .build();

        List<Tuple2<Object, Integer>> vertices = Lists.newArrayList(
                new Tuple2<>(1L, 0),
                new Tuple2<>(2L, Integer.MAX_VALUE),
                new Tuple2<>(3L, Integer.MAX_VALUE),
                new Tuple2<>(4L, Integer.MAX_VALUE),
                new Tuple2<>(5L, Integer.MAX_VALUE),
                new Tuple2<>(6L, Integer.MAX_VALUE)
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<>(1L, 2L, 4), // A --> B (4)
                new Edge<>(1L, 3L, 2), // A --> C (2)
                new Edge<>(2L, 3L, 5), // B --> C (5)
                new Edge<>(2L, 4L, 10), // B --> D (10)
                new Edge<>(3L, 5L, 3), // C --> E (3)
                new Edge<>(5L, 4L, 4), // E --> D (4)
                new Edge<>(4L, 6L, 11) // D --> F (11)
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

        ops.pregel(Integer.MAX_VALUE,
                        Integer.MAX_VALUE,
                        EdgeDirection.Out(),
                        new VProg(),
                        new sendMsg(),
                        new merge(),
                        ClassTag$.MODULE$.apply(Integer.class))
                .vertices()
                .toJavaRDD()
                .sortBy(v -> labels.get((Long) v._1), true, 1)
                .foreach(v -> System.out.println("Minimum cost to get from " + labels.get(1L) + " to "
                        + labels.get((Long) v._1) + " is " + v._2));
    }

}
