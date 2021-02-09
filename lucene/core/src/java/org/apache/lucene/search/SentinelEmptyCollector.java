package org.apache.lucene.search;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;

/** Represents a non existent collector */
public class SentinelEmptyCollector implements Collector {
    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        throw new IllegalStateException("Empty sentinel collector accessed");
    }

    @Override
    public ScoreMode scoreMode() {
        throw new IllegalStateException("Empty sentinel collector accessed");
    }
}
