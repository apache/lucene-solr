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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;

/**
 * Configurable decorator for a lucene {@link IndexWriter}
 * 
 * @author Simon Willnauer
 * 
 */
public class GDataIndexWriter extends IndexWriter {
    private static final Log LOG = LogFactory.getLog(GDataIndexWriter.class);

    private String serviceName;

    private void initialize(IndexSchema config) {
        this.serviceName = config.getName();
        setUseCompoundFile(config.isUseCompoundFile());
        if (config.getMaxBufferedDocs() != IndexSchema.NOT_SET_VALUE)
            setMaxBufferedDocs(config.getMaxBufferedDocs());
        if (config.getMaxMergeDocs() != IndexSchema.NOT_SET_VALUE)
            setMaxMergeDocs(config.getMaxMergeDocs());
        if (config.getMergeFactor() != IndexSchema.NOT_SET_VALUE)
            setMergeFactor(config.getMergeFactor());
        if (config.getMaxFieldLength() != IndexSchema.NOT_SET_VALUE)
            setMaxFieldLength(config.getMaxFieldLength());
        if (config.getWriteLockTimeout() != IndexSchema.NOT_SET_VALUE)
            setWriteLockTimeout(config.getWriteLockTimeout());
        //no commit lock anymore
        //TODO fix this
//        if (config.getCommitLockTimeout() != IndexSchema.NOT_SET_VALUE)
//            setCommitLockTimeout(config.getCommitLockTimeout());
    }

    /**
     * Creates and configures a new GdataIndexWriter
     * 
     * @param arg0 -
     *            the index directory
     * @param arg1 -
     *            create index
     * @param arg2 -
     *            the index schema configuration including all parameter to set
     *            up the index writer
     * @throws IOException
     *             -if the directory cannot be read/written to, or if it does
     *             not exist, and <code>create</code> is <code>false</code>
     */
    protected GDataIndexWriter(Directory arg0, boolean arg1, IndexSchema arg2)
            throws IOException {
        /*
         * Use Schema Analyzer rather than service analyzer. 
         * Schema analyzer returns either the service analyzer or a per field analyzer if configured.
         */
        super(arg0, (arg2 == null ? new StandardAnalyzer() : arg2.getSchemaAnalyzer()), arg1);
        if (arg2 == null) {
            /*
             * if no schema throw exception - schema is mandatory for the index writer.
             */
            try {
                this.close();
            } catch (IOException e) {
                //
            }
            throw new IllegalArgumentException("configuration must not be null");

        }
        this.initialize(arg2);
    }

    /**
     * @see org.apache.lucene.index.IndexWriter#close()
     */
    @Override
    public void close() throws IOException {
        super.close();
        LOG.info("Closing GdataIndexWriter for service " + this.serviceName);
    }

}
