package org.apache.solr.common.util;

import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.solr.common.AlreadyClosedException;

import java.io.Closeable;
import java.io.PrintWriter;

public class CloseTracker implements Closeable {
    private volatile boolean closed = false;
    private volatile String closeStack = "";
    private volatile boolean closeLock;

    @Override
    public void close() {
        if (closeLock) {
            throw new IllegalCallerException("Attempt to close an object that is not owned");
        }
        if (closed) {
            StringBuilderWriter sw = new StringBuilderWriter(4096);
            PrintWriter pw = new PrintWriter(sw);
            new ObjectReleaseTracker.ObjectTrackerException(this.getClass().getName()).printStackTrace(pw);
            String fcloseStack = sw.toString();
            throw new AlreadyClosedException(fcloseStack + "\nalready closed by:\n");
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
    
    public void enableCloseLock() {
        closeLock = true;
    }

    public void disableCloseLock() {
        closeLock = false;
    }

}
