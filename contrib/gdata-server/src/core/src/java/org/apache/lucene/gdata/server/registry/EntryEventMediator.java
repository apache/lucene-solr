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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.data.ServerBaseFeed;

/**
 * This class will be informed about every successful entry event and
 * distributes all event to all registered
 * {@link org.apache.lucene.gdata.server.registry.EntryEventListener}
 * 
 * @author Simon Willnauer
 * 
 */
public abstract class EntryEventMediator {

    private final List<EntryEventListener> entryEventListener = new ArrayList<EntryEventListener>(
            5);

    /**
     * @return - a entry event mediator instance
     */
    public abstract EntryEventMediator getEntryEventMediator();

    /**
     * Registers a {@link EntryEventListener}. This listener will be fired if an
     * entry update, insert or delete occurs
     * 
     * @param listener -
     *            listener to register
     */
    public void registerEntryEventListener(final EntryEventListener listener) {
        if (listener == null || this.entryEventListener.contains(listener))
            return;
        this.entryEventListener.add(listener);
    }

    /**
     * @param entry -
     *            the updated entry
     */
    public void entryUpdated(final ServerBaseEntry entry) {
        for (EntryEventListener listener : this.entryEventListener) {
            listener.fireUpdateEvent(entry);
        }
    }

    /**
     * @param entry -
     *            the added entry
     */
    public void entryAdded(final ServerBaseEntry entry) {
        for (EntryEventListener listener : this.entryEventListener) {
            listener.fireInsertEvent(entry);
        }
    }
    
    /**
     * @param feed - the feed to delete all entries for
     */
    public void allEntriesDeleted(final ServerBaseFeed feed){
        for (EntryEventListener listener : this.entryEventListener) {
            listener.fireDeleteAllEntries(feed);
        }
    }

    /**
     * @param entry -
     *            the deleted entry
     */
    public void entryDeleted(final ServerBaseEntry entry) {
        for (EntryEventListener listener : this.entryEventListener) {
            listener.fireDeleteEvent(entry);
        }
    }
    
    /**
     * checks if the listener is already registered.
     * @param listner - the listener to check
     * @return <code>true</code> if and only if the given listener is already registered, otherwise <code>false</code>.
     */
    public boolean isListenerRegistered(final EntryEventListener listner){
        return listner!=null&&this.entryEventListener.contains(listner);
    }

}
