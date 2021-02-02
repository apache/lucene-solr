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

package org.apache.solr.cluster;

/**
 * Allows for tracking state change from test classes. Typical use will be to set a tracker on a stateful
 * object, which will call {@link #stateChanged()} every time state changes. Test clients observing the state will
 * provide their own implementation of what to do with state change events. They may be counted, awaited, etc.
 */
public interface StateChangeListener {
    /**
     * Called by the stateful objects to note that state has changed.
     */
    void stateChanged();
}
