package utils;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;

import java.util.Collections;

abstract public class sendMessage<T1, R> extends AbstractFunction1<T1, Iterator<R>> {

    protected Iterator<R> noMessages() {
        return JavaConverters.asScalaIteratorConverter(Collections.<R>emptyIterator()).asScala();
    }

    protected Iterator<R> message(R message) {
        return JavaConverters.asScalaIteratorConverter(Collections.singletonList(message).iterator()).asScala();
    }
}
