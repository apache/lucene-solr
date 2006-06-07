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
package org.apache.lucene.gdata.storage; 
 
import java.io.IOException; 
 
import org.apache.lucene.gdata.storage.lucenestorage.StorageImplementation; 
 
/** 
 *TODO document me 
 * @author Simon Willnauer 
 * 
 */ 
public class StorageFactory { 
    /** 
     * Creates a {@link Storage} instance 
     * @return - a storage instance 
     * @throws StorageException  - if the storage can not be created 
     */ 
    public static Storage getStorage()throws StorageException{ 
        try { 
            return new StorageImplementation(); 
        } catch (IOException e) { 
            StorageException ex = new StorageException("Can't create Storage instance -- " 
                    + e.getMessage(), e); 
            ex.setStackTrace(e.getStackTrace()); 
            throw ex;  
             
        }  
    } 
} 
