package org.apache.lucene.search;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Allows a query to be cancelled */
public class CancellableCollector implements Collector, CancellableTask {

    /** Thrown when a query gets cancelled */
    public static class QueryCancelledException extends RuntimeException {
    }

    private Collector collector;
    private AtomicBoolean isQueryCancelled;

    public CancellableCollector(Collector collector) {
        if (collector == null) {
            throw new IllegalStateException("Internal collector not provided but wrapper collector accessed");
        }

        this.collector = collector;
        this.isQueryCancelled = new AtomicBoolean();
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {

        if (isQueryCancelled.compareAndSet(true, false)) {
            throw new QueryCancelledException();
        }

        return new FilterLeafCollector(collector.getLeafCollector(context)) {

            @Override
            public void collect(int doc) throws IOException {
                if (isQueryCancelled.compareAndSet(true, false)) {
                    throw new QueryCancelledException();
                }

                in.collect(doc);
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return collector.scoreMode();
    }

    @Override
    public void cancelTask() {
        isQueryCancelled.compareAndSet(false, true);
    }

    public Collector getInternalCollector() {
        return collector;
    }
}
