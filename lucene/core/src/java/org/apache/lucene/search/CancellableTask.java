package org.apache.lucene.search;

/** Interface which defines a task which can be cancelled */
public interface CancellableTask {
    void cancelTask();
}
