package exercise_1;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class Exercise_1 {

    private static class VProg extends AbstractFunction3<Long, Integer, Integer, Integer> implements Serializable {
        @Override
        public Integer apply(Long vertexID, Integer vertexValue, Integer message) {
            return Math.max(vertexValue, message);
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Integer, Integer>, Iterator<Tuple2<Object, Integer>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Integer, Integer> triplet) {
            Tuple2<Object, Integer> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object, Integer> dstVertex = triplet.toTuple()._2();

            if (sourceVertex._2 <= dstVertex._2) {   // source vertex value is smaller than dst vertex?
                // do nothing
                return JavaConverters.asScalaIteratorConverter(Collections.<Tuple2<Object, Integer>>emptyIterator()).asScala();
            } else {
                // propagate source vertex value
                return JavaConverters.asScalaIteratorConverter(Collections.singletonList(new Tuple2<Object, Integer>(triplet.dstId(), sourceVertex._2)).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Integer, Integer, Integer> implements Serializable {
        @Override
        public Integer apply(Integer o, Integer o2) {
            return Math.max(o, o2);
        }
    }

    public static void maxValue(JavaSparkContext ctx) {
        List<Tuple2<Object, Integer>> vertices = Lists.newArrayList(
                new Tuple2<Object, Integer>(1L, 9),
                new Tuple2<Object, Integer>(2L, 1),
                new Tuple2<Object, Integer>(3L, 6),
                new Tuple2<Object, Integer>(4L, 8)
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1L, 2L, 1),
                new Edge<Integer>(2L, 3L, 1),
                new Edge<Integer>(2L, 4L, 1),
                new Edge<Integer>(3L, 4L, 1),
                new Edge<Integer>(3L, 1L, 1)
        );

        JavaRDD<Tuple2<Object, Integer>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Integer, Integer> G = Graph.apply(verticesRDD.rdd(), edgesRDD.rdd(), 1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Integer.class), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        Tuple2<Long, Integer> max = (Tuple2<Long, Integer>) ops.pregel(
                        Integer.MIN_VALUE,
                        Integer.MAX_VALUE,      // Run until convergence
                        EdgeDirection.Out(),
                        new VProg(),
                        new sendMsg(),
                        new merge(),
                        scala.reflect.ClassTag$.MODULE$.apply(Integer.class))
                .vertices().toJavaRDD().first();

        System.out.println(max._2 + " is the maximum value in the graph");
    }

}
