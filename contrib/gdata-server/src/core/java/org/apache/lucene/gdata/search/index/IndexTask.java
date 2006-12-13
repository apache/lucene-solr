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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * Class to be used inside a
 * {@link org.apache.lucene.gdata.search.index.GDataIndexer} to process the task
 * queue. This class calls the commit method of the indexer if commit is
 * scheduled.
 * 
 * @author Simon Willnauer
 * 
 */
class IndexTask implements Runnable {
    private static final Log INNERLOG = LogFactory.getLog(IndexTask.class);

    private AtomicBoolean stopped = new AtomicBoolean(false);

    private final GDataIndexer indexer;

    protected AtomicBoolean commit = new AtomicBoolean(false);
    
    protected AtomicBoolean optimize = new AtomicBoolean(false);

    /*
     * keep protected for subclassing
     */
    protected final BlockingQueue<Future<IndexDocument>> taskQueue;

    IndexTask(final GDataIndexer indexer,
            final BlockingQueue<Future<IndexDocument>> taskQueue) {
        this.indexer = indexer;
        this.taskQueue = taskQueue;
    }

    /**
     * @see java.lang.Runnable#run()
     */
    public void run() {

        while (!this.stopped.get() || this.taskQueue.size() != 0) {

            try {
                /*
                 * get the future from the queue and wait until processing has
                 * been done
                 */
                Future<IndexDocument> future = getTask();
                if (future != null) {
                    IndexDocument document = future.get();
                    setOptimize(document);
                    processDocument(document); 
                    /*
                     * the document contains the info for commit or optimize -->
                     * this comes from the controller
                     */
                    if (document == null || document.commitAfter())
                        this.indexer.commit(document == null ? false : this.optimize.getAndSet(false));
                }
                if (this.commit.getAndSet(false))
                    this.indexer.commit(this.optimize.getAndSet(false));

            } catch (InterruptedException e) {
                INNERLOG.warn("Queue is interrupted exiting IndexTask -- ", e);

            } catch (GdataIndexerException e) {
                /*
                 * 
                 * TODO fire callback here as well
                 */
                INNERLOG.error("can not retrieve Field from IndexDocument  ", e);
            } catch (ExecutionException e) {
                /*
                 * TODO callback for fail this exception is caused by an
                 * exception while processing the document. call back for failed
                 * docs should be placed here
                 */
                INNERLOG.error("Future throws execution exception ", e);

            } catch (IOException e) {
                INNERLOG.error("IOException thrown while processing document ",
                        e);

            } catch (Throwable e) {
                /*
                 * catch all to prevent the thread from dieing
                 */
                INNERLOG.error(
                        "Unexpected exception while processing document -- "
                                + e.getMessage(), e);
            }
        }
        try {
            this.indexer.commit(this.optimize.getAndSet(false));
        } catch (IOException e) {
            INNERLOG.warn("commit on going down failed - "+e.getMessage(),e);
            
        }
        this.stop();
    }
    protected void setOptimize(IndexDocument document){
        if(document == null)
            return;
        this.optimize.set(document.optimizeAfter());
    }

    /*
     * keep this protected for subclassing see TimedIndexTask!
     */
    protected Future<IndexDocument> getTask() throws InterruptedException {
        return this.taskQueue.take();
    }

    private void processDocument(IndexDocument document) throws IOException {
        /*
         * a null document is used for waking up the task if the indexer has
         * been destroyed to finish up and commit. should I change this?! -->
         * see TimedIndexTask#getTask() also!!
         */
        if (document == null) {
            INNERLOG.warn("Can not process document -- is null -- run commit");
            return;
        }
        if (document.isDelete()) {
            this.indexer.deleteDocument(document);
            return;
        } else if (document.isInsert()) {
            this.indexer.addDocument(document);
            return;
        } else if (document.isUpdate()) {
            this.indexer.updateDocument(document);
            return;
        }
        /*
         * that should not happen -- anyway skip the document and write it to
         * the log
         */
        INNERLOG.warn("IndexDocument has no Action " + document);

    }

    protected boolean isStopped() {
        return this.stopped.get();
    }

    protected void stop() {
        this.stopped.set(true);
    }

}