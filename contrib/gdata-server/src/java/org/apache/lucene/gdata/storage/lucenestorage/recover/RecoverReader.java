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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.server.GDataEntityBuilder;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.server.registry.ProvidedService;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.util.ParseException;

/**
 * Recovers the written object from the harddisc
 * @author Simon Willnauer
 *
 */
public class RecoverReader {
    
    private static final Log LOG = LogFactory.getLog(RecoverReader.class);
    private RecoverStrategy strategy; 
    protected RecoverReader(){
        this.strategy = new RecoverStrategy();
    }
    /**
     * @param reader
     * @return
     * @throws IOException
     */
    public List<StorageEntryWrapper> recoverEntries(final BufferedReader reader) throws IOException{
        List<StorageEntryWrapper> actionList = new ArrayList<StorageEntryWrapper>();
        this.strategy = new RecoverStrategy();
        String input = null;
        String metaData = null;
        String entryData = null;
        while((input=reader.readLine())!= null){
            if(metaData == null){
                metaData = input;
                continue;
            }
            if(input.equals(RecoverWriter.STORAGE_OPERATION_SEPARATOR)){
                try{
                actionList.add(this.strategy.recover(metaData,entryData));
                }catch (RecoverException e) {
                  LOG.error("Skipping recover entry for metadata: "+metaData,e);
                }
                this.strategy = new RecoverStrategy();
                metaData = null;
                entryData = null;
             continue;   
            }
            if(entryData == null){
                entryData = input;
            }
            
        }
        
        
        
        return actionList;
        
    }
    
    
    
    
    
  
    
    
    private static  class RecoverStrategy{
        private StorageOperation operation;
        private ProvidedService config;
        private String feedId;
        private String entryId;
        private long timestamp;
        /**
         * @param metaData
         * @param entry
         * @return
         * @throws RecoverException
         */
        public StorageEntryWrapper recover(String metaData, String entry) throws RecoverException{
                fillMetaData(metaData);
                ServerBaseEntry retVal = null;
                if(entry != null && this.operation == StorageOperation.DELETE)
                    throw new RecoverException("Can not recover -- Delete operation has entry part");
                if(entry != null)
                    try {
                        retVal = new ServerBaseEntry(buildEntry(entry,this.config));
                    } catch (Exception e) {
                        throw new RecoverException("Exception occured while building entry -- "+e.getMessage(),e);
                    }
                else
                    retVal = new ServerBaseEntry();
                retVal.setId(this.entryId);
                retVal.setFeedId(this.feedId);
                retVal.setServiceConfig(this.config);
                
           try{
            return new StorageEntryWrapper(retVal,this.operation);
           }catch (IOException e) {
               throw new RecoverException("Can't create StorageWrapper -- "+e.getMessage(),e);
        }
        }
        private void fillMetaData(String recoverString) throws RecoverException{
            StringTokenizer tokenizer = new StringTokenizer(recoverString,RecoverWriter.META_DATA_SEPARATOR);
            String temp = tokenizer.nextToken();
            if(temp.equals("D"))
                this.operation = StorageOperation.DELETE;
            else if(temp.equals("U"))
                this.operation = StorageOperation.UPDATE;
            else if(temp.equals("I"))
                this.operation = StorageOperation.INSERT;
            else
                throw new RecoverException("Illegal metadata --- "+recoverString);
            temp = tokenizer.nextToken();
            if(temp == null)
                throw new RecoverException("Can't recover feed Id -- "+temp);
            this.feedId = temp;
            temp = tokenizer.nextToken();
            if(temp == null)
                throw new RecoverException("Can't recover entry Id -- "+temp);
             this.entryId = temp;
            
            temp = tokenizer.nextToken();
            try{
                this.timestamp = Long.parseLong(temp);
            }catch (Exception e) {
                throw new RecoverException("Can't recover timestamp -- "+temp,e);
            }
            
            if(this.operation != StorageOperation.DELETE){
                temp = tokenizer.nextToken();
                if(temp == null)
                    throw new RecoverException("Can't recover service -- "+temp);  
                if(!GDataServerRegistry.getRegistry().isServiceRegistered(temp))
                    throw new RecoverException("Service in recover metadata is not registered  - "+temp);
                this.config = GDataServerRegistry.getRegistry().getProvidedService(temp);
                
            }
            
        }
        
        private BaseEntry buildEntry(String entry, ProvidedService serviceConfig) throws ParseException, IOException{
           StringReader reader = new StringReader(entry);
           return GDataEntityBuilder.buildEntry(reader,serviceConfig);
        }
    }
}
