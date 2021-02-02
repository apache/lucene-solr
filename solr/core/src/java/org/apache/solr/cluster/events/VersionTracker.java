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

package org.apache.solr.cluster.events;

import java.util.concurrent.TimeoutException;

/**
 * Allows for tracking state change from test classes. Typical use will be to set a version tracker on a stateful
 * object, which will call {@link #increment()} every time state changes. Test clients observing the state will call
 * {@link #waitForVersionChange(int, int)} to be notified of the next increment call.
 */
public interface VersionTracker {
    /**
     * Called by the stateful objects to note that state has changed.
     */
    void increment();

    /**
     * Called by test observers interested in knowing when state changes. The returned version should be used as the
     * argument to this method the next time it is called.
     *
     * @param currentVersion the last known version, to compare against current version
     * @param timeoutSec how long to wait, in seconds
     * @return the new version seen.
     */
    int waitForVersionChange(int currentVersion, int timeoutSec) throws InterruptedException, TimeoutException;
}
