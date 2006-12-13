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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * This {@link IndexTask} will idle the given time if no task is on the queue.
 * If the idle time exceeds the task will force a commit on the index. The timer
 * will be reset if a task is on the queue.
 * 
 * @author Simon Willnauer
 * 
 */
class TimedIndexTask extends IndexTask {
    protected final static TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    protected final static long DEFAULT_IDLE_TIME = 30;

    private final long idleTime;

    TimedIndexTask(final GDataIndexer indexer,
            final BlockingQueue<Future<IndexDocument>> taskQueue,
            final long idleTime) {
        super(indexer, taskQueue);
        this.idleTime = idleTime < DEFAULT_IDLE_TIME ? DEFAULT_IDLE_TIME
                : idleTime;

    }

    /**
     * @see org.apache.lucene.gdata.search.index.IndexTask#getTask()
     */
    @Override
    protected Future<IndexDocument> getTask() throws InterruptedException {
        /*
         * wait for a certain time and return null if no task is on the queue.
         * If return null --> commit will be called
         */
        Future<IndexDocument> retVal = this.taskQueue.poll(this.idleTime, TIME_UNIT);
        if(retVal== null)
            this.commit.set(true);
        return retVal;
        
    }
    
    protected long getIdleTime(){
        return this.idleTime;
    }

}
