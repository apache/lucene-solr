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

package org.apache.lucene.gdata.storage.lucenestorage.recover;

import java.io.IOException;
import java.io.Writer;

import org.apache.lucene.gdata.server.registry.ProvidedService;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.util.common.xml.XmlWriter;

/**
 * Writes the recover objects to the hard disc.
 * @author Simon Willnauer
 *
 */
public class RecoverWriter {
    protected static final String META_DATA_SEPARATOR = ";";
    protected static final String META_DATA_ENTRY_SEPARATOR = System.getProperty("line.separator");
    protected static final String STORAGE_OPERATION_SEPARATOR = "###########";
    protected static final String OPERATION_DELETE = "D";
    protected static final String OPERATION_UPDATE = "U";
    protected static final String OPERATION_INSERT = "I";
    protected static final String FILE_PREFIX = ".strg";
   
    
    
    /**
     * @param wrapper 
     * @throws IOException 
     * 
     * 
     * 
     */
    public void writeEntry(StorageEntryWrapper wrapper,Writer writer)throws IOException{
        
        writeOperation(wrapper.getOperation(),writer);
        writeFeedID(wrapper.getFeedId(),writer);
        writeEntryID(wrapper.getEntryId(),writer);
        writeTimeStamp(wrapper.getTimestamp().toString(),writer);
        if(!wrapper.getOperation().equals(StorageOperation.DELETE)){
        writeService(wrapper,writer);
            writer.write(META_DATA_ENTRY_SEPARATOR);
            BaseEntry entry = wrapper.getEntry();
            XmlWriter xmlWriter = new XmlWriter(writer);
            entry.generateAtom(xmlWriter,wrapper.getConfigurator().getExtensionProfile());
        }
        writer.write(META_DATA_ENTRY_SEPARATOR);
        writer.write(STORAGE_OPERATION_SEPARATOR);
        writer.write(META_DATA_ENTRY_SEPARATOR);
    }

   

    private void writeTimeStamp(String timestamp, Writer writer) throws IOException{
        writer.write(timestamp);
        writer.write(META_DATA_SEPARATOR);
    }
    private void writeFeedID(String feedId,Writer writer) throws IOException{
        writer.write(feedId);
        writer.write(META_DATA_SEPARATOR);
    }
    private void writeEntryID(String entryId,Writer writer) throws IOException{
        writer.write(entryId);
        writer.write(META_DATA_SEPARATOR);
    }
    
    private void writeService(StorageEntryWrapper wrapper, Writer writer) throws IOException{
        ProvidedService config = wrapper.getConfigurator();
        writer.write(config.getName());
        writer.write(META_DATA_SEPARATOR);
    }
 
    private void writeOperation(StorageOperation operation, Writer writer) throws IOException{
        if(operation.equals(StorageOperation.INSERT))
            writer.write(OPERATION_INSERT);
        else if (operation.equals(StorageOperation.UPDATE)) 
            writer.write(OPERATION_UPDATE);
        else if (operation.equals(StorageOperation.DELETE)) 
            writer.write(OPERATION_DELETE);
        writer.write(META_DATA_SEPARATOR);
    }
    
    
    

}
