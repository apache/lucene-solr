/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.common.util;

import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.solr.common.AlreadyClosedException;

import java.io.PrintWriter;

public class CloseTracker {

    public static volatile AlreadyClosedException lastAlreadyClosedEx;
    public static volatile IllegalCallerException lastIllegalCallerEx;
    private boolean checkClosedTwice;

    private volatile boolean closed = false;
    private volatile String closeStack = "";
    private volatile boolean closeLock;

    public CloseTracker() {

    }

    public CloseTracker(boolean checkClosedTwice) {
        this.checkClosedTwice = checkClosedTwice;
    }

    public boolean close() {
        if (closeLock) {
            IllegalCallerException ex = new IllegalCallerException("Attempt to close an object that is not owned");
            lastIllegalCallerEx = ex;
            throw ex;
        }
        if (checkClosedTwice && closed) {
            StringBuilderWriter sw = new StringBuilderWriter(4096);
            PrintWriter pw = new PrintWriter(sw);
            new ObjectReleaseTracker.ObjectTrackerException(this.getClass().getName()).printStackTrace(pw);
            String fcloseStack = sw.toString();
            AlreadyClosedException ex = new AlreadyClosedException(fcloseStack + "\nalready closed by:\n" + closeStack);
            lastAlreadyClosedEx = ex;
            throw ex;

        }

        StringBuilderWriter sw = new StringBuilderWriter(4096);
        PrintWriter pw = new PrintWriter(sw);
        new ObjectReleaseTracker.ObjectTrackerException(this.getClass().getName()).printStackTrace(pw);
        closeStack = sw.toString();
        closed = true;
        return true;
    }

    public boolean isClosed() {
        return closed;
    }

    public String getCloseStack() {
        return closeStack;
    }

    public void enableCloseLock() {
        closeLock = true;
    }

    public void disableCloseLock() {
        closeLock = false;
    }

}
