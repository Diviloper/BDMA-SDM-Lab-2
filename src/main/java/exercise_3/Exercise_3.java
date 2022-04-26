package exercise_3;

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
import java.util.stream.Collectors;

public class Exercise_3 {

    private static class VProg extends AbstractFunction3<Object, Tuple2<Integer, List<Long>>, Tuple2<Integer, List<Long>>, Tuple2<Integer, List<Long>>> implements Serializable {
        @Override
        public Tuple2<Integer, List<Long>> apply(Object vertexID, Tuple2<Integer, List<Long>> vertexValue, Tuple2<Integer, List<Long>> message) {
            return vertexValue._1 <= message._1 ? vertexValue : message;
        }
    }

    private static class sendMsg extends sendMessage<EdgeTriplet<Tuple2<Integer, List<Long>>, Integer>,
            Tuple2<Object, Tuple2<Integer, List<Long>>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Tuple2<Integer, List<Long>>>> apply(EdgeTriplet<Tuple2<Integer, List<Long>>, Integer> triplet) {
            Tuple2<Object, Tuple2<Integer, List<Long>>> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object, Tuple2<Integer, List<Long>>> dstVertex = triplet.toTuple()._2();

            if (sourceVertex._2._1.equals(Integer.MAX_VALUE)) {
                // Source vertex has not been visited
                return noMessages();
            } else if (sourceVertex._2._1 + triplet.attr < dstVertex._2._1) {
                // Source vertex has been visited and yields a shorter path for destination vertex
                List<Long> path = sourceVertex._2._2;
                path.add((Long) dstVertex._1);
                return message(new Tuple2<>(triplet.dstId(), new Tuple2<>(sourceVertex._2._1 + triplet.attr, path)));
            } else {
                // Source vertex has been visited but doesn't yield a shorter path for destination vertex
                return noMessages();
            }
        }
    }

    private static class merge extends AbstractFunction2<Tuple2<Integer, List<Long>>, Tuple2<Integer, List<Long>>, Tuple2<Integer, List<Long>>> implements Serializable {
        @Override
        public Tuple2<Integer, List<Long>> apply(Tuple2<Integer, List<Long>> o, Tuple2<Integer, List<Long>> o2) {
            return o._1 <= o2._1 ? o : o2;
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1L, "A")
                .put(2L, "B")
                .put(3L, "C")
                .put(4L, "D")
                .put(5L, "E")
                .put(6L, "F")
                .build();

        List<Tuple2<Object, Tuple2<Integer, List<Long>>>> vertices = Lists.newArrayList(
                new Tuple2<>(1L, new Tuple2<>(0, Lists.newArrayList(1L))),
                new Tuple2<>(2L, new Tuple2<>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<>(3L, new Tuple2<>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<>(4L, new Tuple2<>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<>(5L, new Tuple2<>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<>(6L, new Tuple2<>(Integer.MAX_VALUE, Lists.newArrayList()))
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

        JavaRDD<Tuple2<Object, Tuple2<Integer, List<Long>>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Tuple2<Integer, List<Long>>, Integer> G = Graph.apply(
                verticesRDD.rdd(), edgesRDD.rdd(),
                new Tuple2<>(0, Lists.newArrayList()),
                StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                ClassTag$.MODULE$.apply(Tuple2.class), ClassTag$.MODULE$.apply(Integer.class));

        GraphOps<Tuple2<Integer, List<Long>>, Integer> ops = new GraphOps<>(
                G,
                ClassTag$.MODULE$.apply(Tuple2.class),
                ClassTag$.MODULE$.apply(Integer.class)
        );

        ops.pregel(new Tuple2<>(Integer.MAX_VALUE, Lists.newArrayList()),
                        Integer.MAX_VALUE,
                        EdgeDirection.Out(),
                        new Exercise_3.VProg(),
                        new Exercise_3.sendMsg(),
                        new Exercise_3.merge(),
                        ClassTag$.MODULE$.apply(Tuple2.class))
                .vertices()
                .toJavaRDD()
                .sortBy(v -> labels.get((Long) v._1), true, 1)
                .foreach(v -> {
                    String path = v._2._2.stream().map(labels::get).collect(Collectors.joining(" -> "));
                    System.out.println("Minimum path to get from " + labels.get(1L) + " to " + labels.get((Long) v._1) +
                            " is [" + path + "] with cost " + v._2._1);
                });
    }

}
