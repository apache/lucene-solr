/** 
 * Copyright 2004 The Apache Software Foundation 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 */

package org.apache.lucene.gdata.storage.lucenestorage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.ExtensionProfile;
import com.google.gdata.data.Link;

/**
 * The StorageBuffer is used to buffer incoming updates, deletes and inserts to
 * the storage. The storage uses an lucene index to store the enries. As
 * modifying the index all the time an altering request comes in is not
 * efficent. The entries will be added to the buffer to be available for
 * incoming storage queries. If the loadfactor for the
 * {@link org.apache.lucene.gdata.storage.lucenestorage.StorageModifier} is
 * reached the modifier will perform a batch update on the index. Each entry
 * will be associated with a feed id inside a associative datastructure to
 * return a requested entry efficiently.
 * <p>
 * This implementation uses {@link java.util.concurrent.locks.ReadWriteLock}.
 * The read lock may be held simultaneously by multiple reader threads, so long
 * as there are no writers. The write lock is exclusive.
 * </p>
 * 
 * @see java.util.concurrent.locks.ReentrantReadWriteLock
 * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier
 * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController
 * 
 * @author Simon Willnauer
 * 
 */
public class StorageBuffer {
	private static final Log LOG = LogFactory.getLog(StorageBuffer.class);

	private final Map<String, Map<String, StorageEntryWrapper>> bufferMap;

	private final Map<String, Long> modifiyMap;

	private final List<String> excludeList;

	private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

	private final Lock readLock = this.lock.readLock();

	private final Lock writeLock = this.lock.writeLock();

	private final static int DEFAULT_BUFFER_COUNT = 10;

	/**
	 * Constructs a new StorageBuffer.
	 * <p>
	 * The expectedBufferCount sould be higher than the maximum of entries added
	 * to the buffer, resizing the buffer is very efficient. For detailed
	 * infomation {@link HashMap} as this is used inside the buffer
	 * </p>
	 * 
	 * @param expectedBufferCount -
	 *            the expected size of the buffer
	 * 
	 */
	protected StorageBuffer(final int expectedBufferCount) {
		this.bufferMap = new HashMap<String, Map<String, StorageEntryWrapper>>(
				expectedBufferCount < DEFAULT_BUFFER_COUNT ? DEFAULT_BUFFER_COUNT
						: expectedBufferCount);
		this.excludeList = new ArrayList<String>(
				expectedBufferCount < DEFAULT_BUFFER_COUNT ? DEFAULT_BUFFER_COUNT
						: expectedBufferCount);
		this.modifiyMap = new HashMap<String, Long>(
				expectedBufferCount < DEFAULT_BUFFER_COUNT ? DEFAULT_BUFFER_COUNT
						: expectedBufferCount);
	}

	/**
	 * Adds a {@link StorageEntryWrapper} to the buffer. If a wrapper
	 * representing the same entry are already in the buffer the wrapper will be
	 * replaced.
	 * 
	 * @param wrapper -
	 *            the wrapper to buffer
	 */
	public void addEntry(final StorageEntryWrapper wrapper) {
		this.writeLock.lock();
		try {
			if (LOG.isInfoEnabled())
				LOG.info(" Buffering wrapper - " + wrapper.getOperation()
						+ " ID: " + wrapper.getEntryId() + " FeedID: "
						+ wrapper.getFeedId());
			if (wrapper.getOperation().equals(StorageOperation.DELETE))
				return;

			String feedId = wrapper.getFeedId();
			if (this.bufferMap.containsKey(feedId))
				this.bufferMap.get(feedId).put(wrapper.getEntryId(), wrapper);
			else {
				Map<String, StorageEntryWrapper> newFeedMap = new HashMap<String, StorageEntryWrapper>(
						20);
				newFeedMap.put(wrapper.getEntryId(), wrapper);
				this.bufferMap.put(feedId, newFeedMap);

			}
			addLastModified(wrapper.getFeedId(), wrapper.getTimestamp());
		} finally {
			/*
			 * add all to exclude from searches doc will be available via the
			 * buffer
			 */
			this.excludeList.add(wrapper.getEntryId());
			this.writeLock.unlock();
		}
	}

	private void addLastModified(final String feedId, Long timestamp) {
		if (this.modifiyMap.containsKey(feedId))
			this.modifiyMap.remove(feedId);
		this.modifiyMap.put(feedId, timestamp);

	}

	protected Long getFeedLastModified(final String feedId) {
		return this.modifiyMap.get(feedId);
	}

	protected Set<Entry<String, Long>> getLastModified() {
		return this.modifiyMap.entrySet();
	}

	/**
	 * Returns all entries for the given feed id sorted by the update timestamp
	 * desc.
	 * 
	 * @param feedId -
	 *            the feed id
	 * @return a {@link List} of all {@link StorageEntryWrapper} object buffered
	 *         in this buffer or an empty list if not entry has been buffered
	 *         for the given feed
	 */
	public List<StorageEntryWrapper> getSortedEntries(String feedId) {
		this.readLock.lock();
		try {
			if (!this.bufferMap.containsKey(feedId))
				return null;
			Map<String, StorageEntryWrapper> tempMap = this.bufferMap
					.get(feedId);
			if (tempMap == null)
				return null;
			Collection<StorageEntryWrapper> col = tempMap.values();
			List<StorageEntryWrapper> returnList = new ArrayList<StorageEntryWrapper>(
					col);
			Collections.sort(returnList);
			return returnList;

		} finally {
			this.readLock.unlock();
		}

	}

	/**
	 * Adds a deleted entry to the buffer.
	 * 
	 * @param entryId -
	 *            the deleted entry id
	 * @param feedId -
	 *            the feed of the entry
	 */
	public void addDeleted(final String entryId, final String feedId) {
		this.writeLock.lock();
		try {
			this.excludeList.add(entryId);
			Map<String, StorageEntryWrapper> tempMap = this.bufferMap
					.get(feedId);
			if (tempMap == null)
				return;
			tempMap.remove(entryId);
			this.addLastModified(feedId, new Long(System.currentTimeMillis()));
		} finally {
			this.writeLock.unlock();

		}

	}

	/**
	 * Returns an entry for the given entry id in the feed context spezified by
	 * the feed id;
	 * 
	 * @param entryId -
	 *            the id of the entry to return
	 * @param feedId -
	 *            the feed containing the entry
	 * @return - the entry or <code>null</code> if the corresponding entry is
	 *         not in the buffer.
	 */
	public StorageEntryWrapper getEntry(final String entryId,
			final String feedId) {
		this.readLock.lock();
		try {

			if (this.bufferMap.containsKey(feedId))
				return this.bufferMap.get(feedId).get(entryId);
			return null;

		} finally {
			this.readLock.unlock();
		}
	}

	/**
	 * The buffer contains updated and delete entries. These entries are already
	 * available in the lucene index but should not be found during search.
	 * 
	 * <p>
	 * This list contains all entries should not be found by the index searcher.
	 * This method creates a copy of the current list to prevent concurrent
	 * modification exceptions while iteration over the collection.
	 * </p>
	 * 
	 * 
	 * @see ModifiedEntryFilter
	 * @return - a String array of entries to be omitted from a lucene index
	 *         search
	 */
	public String[] getExculdList() {
		this.readLock.lock();
		try {
			return this.excludeList
					.toArray(new String[this.excludeList.size()]);
		} finally {
			this.readLock.unlock();
		}
	}

	// not synchronized
	private void clearBuffer() {
		this.bufferMap.clear();
		this.excludeList.clear();
		this.modifiyMap.clear();

	}

	/**
	 * clears the buffer -
	 */
	public void close() {
		this.writeLock.lock();
		try {
			clearBuffer();
		} finally {
			this.writeLock.unlock();
		}

	}

	static class BufferableEntry extends BaseEntry {

		/**
		 * 
		 */
		@SuppressWarnings("unchecked")
		public BufferableEntry() {
			super();
			this.links = new LinkedList<Link>();
		}

		/**
		 * @param arg0
		 */
		@SuppressWarnings("unchecked")
		public BufferableEntry(BaseEntry arg0) {
			super(arg0);
			this.links = new LinkedList<Link>();
		}

		/**
		 * @see com.google.gdata.data.BaseEntry#declareExtensions(com.google.gdata.data.ExtensionProfile)
		 */
		@Override
		public void declareExtensions(ExtensionProfile arg0) {
			//
		}

	}

}
