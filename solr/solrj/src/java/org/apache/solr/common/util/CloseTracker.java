package org.apache.solr.common.util;

import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.solr.common.AlreadyClosedException;

import java.io.Closeable;
import java.io.PrintWriter;

public class CloseTracker implements Closeable {
    private volatile boolean closed = false;
    private volatile String closeStack = "";

    @Override
    public void close() {
        if (closed) {
            throw new AlreadyClosedException(closeStack);
        }

        StringBuilderWriter sw = new StringBuilderWriter(4096);
        PrintWriter pw = new PrintWriter(sw);
        new ObjectReleaseTracker.ObjectTrackerException(this.getClass().getName()).printStackTrace(pw);
        closeStack = sw.toString();
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

}
