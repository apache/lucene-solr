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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.storage.StorageException;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper;
import org.apache.lucene.gdata.storage.lucenestorage.StorageModifier;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation;


/**
 * @author Simon Willnauer
 * 
 */
public class RecoverController {
    private static final Log LOG = LogFactory.getLog(RecoverController.class);
    private final File recoverDirectory;

    private static final String FILE_SUFFIX = ".rec";

    private File currentRecoverFile;

    private RecoverWriter writer;

    private Writer fileWriter;

    private BufferedReader fileReader;

    private RecoverReader reader;

    private Lock lock = new ReentrantLock();
    
    private final boolean recover;
    private final boolean keepRecoverFiles;

    public RecoverController(final File recoverDirectory, boolean recover, boolean keepRecoverFiles) {
        if (recoverDirectory == null)
            throw new IllegalArgumentException("directory must not be null");
        if (!recoverDirectory.isDirectory())
            throw new IllegalStateException("the given File is not a directory");
        this.recover = recover;
        this.keepRecoverFiles = keepRecoverFiles;
        this.recoverDirectory = recoverDirectory;
       
    }

   public void storageModified(StorageEntryWrapper wrapper)
            throws RecoverException {
        // prevent deadlock either recovering or writing
        if(this.recover){
            LOG.warn("Can't write entry, Recovercontroller is initialized in recover mode");
            return;
        }
        this.lock.lock();
        try {

            this.writer.writeEntry(wrapper, this.fileWriter);
        } catch (Exception e) {
            LOG.error("Writing entry failed -- create new recover file",e);
            throw new RecoverException(
                    "Writing entry failed -- create new recover file",e);

        } finally {
            this.lock.unlock();
        }
    }
    
    public void recoverEntries(final StorageModifier modifier){
        // prevent deadlock either recovering or writing
        if(!this.recover){
            LOG.warn("Can't recover entries, Recovercontroller is initialized in write mode");
            return;
        }
        this.lock.lock();
        try{
            this.reader = new RecoverReader();
        File[] files = this.recoverDirectory.listFiles();
        for (int i = 0; i < files.length; i++) {
            if(!files[i].isDirectory()){
                try{
                this.fileReader = new BufferedReader(new FileReader(files[i]));
                List<StorageEntryWrapper> entryList = this.reader.recoverEntries(this.fileReader);
                if(entryList.size() == 0)
                    continue;
                storeEntries(entryList,modifier);
                this.fileReader.close();
                if(!this.keepRecoverFiles)
                    files[i].delete();
                }catch (StorageException e) {
                    LOG.error("Can't store recover entries for file: "+files[i].getName()+" -- keep file "+e.getMessage(),e);
                }catch (IOException e) {
                    LOG.error("Can't recover entries for file: "+files[i].getName()+" -- keep file",e);
                }
            }
        }
        
        }finally{
            this.lock.unlock();
        }
    }
    
    protected void storeEntries(final List<StorageEntryWrapper> entries, final StorageModifier modifier) throws StorageException{
        for (StorageEntryWrapper wrapper : entries) {
            if(wrapper.getOperation() == StorageOperation.DELETE)
                modifier.deleteEntry(wrapper);
            else if(wrapper.getOperation() == StorageOperation.INSERT)
                modifier.insertEntry(wrapper);
            else if(wrapper.getOperation() == StorageOperation.UPDATE)
                modifier.updateEntry(wrapper);
                
            
        }
    }

    protected synchronized void initialize() throws IOException {
        if(this.recover)
            return;
        String filename = System.currentTimeMillis() + FILE_SUFFIX;
        this.currentRecoverFile = new File(this.recoverDirectory, filename);
        this.writer = new RecoverWriter();
        this.fileWriter = new BufferedWriter(new FileWriter(
                this.currentRecoverFile));

    }

    protected void destroy() throws RecoverException {
        if (this.fileWriter != null) {
            this.lock.lock();
            try {
                this.fileWriter.flush();
                this.fileWriter.close();
            } catch (IOException e) {
                throw new RecoverException("Can't close recover writer ", e);
            } finally {
                this.lock.unlock();
            }
        }
    }
    
    

}
