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

import org.apache.lucene.document.Document;

/**
 * A interface to be implemented by <tt>StorageWrapper</tt> sub classes to
 * provide a lucene document for each entity wrapped.
 * 
 * @see org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper
 * @see org.apache.lucene.gdata.storage.lucenestorage.StorageAccountWrapper
 * @see org.apache.lucene.gdata.storage.lucenestorage.StorageFeedWrapper
 * @author Simon Willnauer
 * 
 */
public interface StorageWrapper {
    /**
     * Returns a Lucene document representing the Wrapped Entry
     * 
     * @return a Lucene Document
     */
    public abstract Document getLuceneDocument();
}
