/**
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

package org.apache.lucene.gdata.search.index;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Simon Willnauer
 * 
 */
class IndexLogWriter {
    private static final String LINE_BREAK = System
            .getProperty("line.separator");

    private static final String XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + LINE_BREAK;
    private static final String CHARSET = "UTF-8";
    private static final String ROOT_BEGIN = "<indexlog>" + LINE_BREAK;

    private static final String ROOT_END = "</indexlog>";

    

    private final BufferedWriter writer;

    private final AtomicBoolean isClosed;

    /**
     * @param file
     * @throws IOException
     * 
     */
    public IndexLogWriter(File file) throws IOException {

        this.writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(file), CHARSET));
        this.writer.write(XMLHEADER);
        this.writer.write(ROOT_BEGIN);
        this.writer.flush();
        this.isClosed = new AtomicBoolean(false);

    }

    synchronized void writeAction(String id, IndexAction action)
            throws IOException {
        if (this.isClosed.get())
            throw new IllegalStateException("Writer is already closed");
        this.writer.write(buildElement(id, action.name()));
        this.writer.flush();
    }

    static synchronized void tryCloseRoot(File file) throws IOException {
        /*
         * try to append the Root element end
         * this happens if the server crashes.
         * If it dies while writing an entry the log file has to be fixed manually
         */
        RandomAccessFile raFile = new RandomAccessFile(file, "rw");
        raFile.seek(raFile.length());
        raFile.write(IndexLogWriter.ROOT_END.getBytes(CHARSET));
        raFile.close();
        

    }

    private static String buildElement(String id, String action) {
        StringBuilder builder = new StringBuilder("\t<indexentry>")
                .append(LINE_BREAK);
        builder.append("\t\t<entryid>");
        builder.append(id);
        builder.append("</entryid>").append(LINE_BREAK);
        builder.append("\t\t<action>");
        builder.append(action);
        builder.append("</action>").append(LINE_BREAK);
        builder.append("\t</indexentry>").append(LINE_BREAK);
        return builder.toString();

    }

    synchronized void close() throws IOException {
        if (!this.isClosed.compareAndSet(false,true))
            throw new IllegalStateException("Writer is already closed");
        try {
            this.writer.write(ROOT_END);
            this.writer.flush();
        } finally {
            this.writer.close();
        }

    }

}
