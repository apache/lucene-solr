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

package org.apache.lucene.gdata.server.registry;

import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.data.ServerBaseFeed;

/**
 * The EntryEventListener interface should be implemented by any class needs to be informed about any changes on entries.
 * To register a class as a EntryEventListener use:
 * <p>
 * <tt>
 * GdataServerRegistry.registerEntryEventListener(EntryEventListener);
 * <tt>
 * </p>
 * @author Simon Willnauer
 *
 */
public interface EntryEventListener {
    /**
     * will be invoked on every successful update on every entry
     * @param entry the updated entry
     */
    public abstract void fireUpdateEvent(ServerBaseEntry entry);
    /**
     * will be invoked on every successful entry insert
     * @param entry
     */
    public abstract void fireInsertEvent(ServerBaseEntry entry);
    /**
     * will be invoked on every successful entry delete
     * @param entry
     */
    public abstract void fireDeleteEvent(ServerBaseEntry entry);
    
    /**
     * will be invoked on every successful feed delete
     * @param feed - the feed containing the feed id to delete all entries for
     */
    public abstract void fireDeleteAllEntries(ServerBaseFeed feed);
}
