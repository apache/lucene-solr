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
 
package org.apache.lucene.gdata.storage.lucenestorage.configuration; 
 
import java.io.InputStream; 
import java.util.Properties; 
 
/** 
 * This clas loads the Storage configuration file and sets all properties. If 
 * the properties can not be loaded an {@link java.lang.Error} will be thrown. 
 * The configuration file <i>lucenestorage.properties.xml</i> should be available in the classpath. 
 *  
 * @author Simon Willnauer 
 *  
 */ 
public class StorageConfigurator { 
    private final int storageBufferSize; 
 
    private final int storagepersistFactor; 
 
    private final String storageDirectory; 
 
    private final boolean keepRecoveredFiles; 
 
    private final boolean recover; 
    
    private final boolean ramDirectory;
    private static StorageConfigurator INSTANCE = null; 
 
    private final int indexOptimizeInterval; 
 
    private StorageConfigurator() { 
        InputStream stream = StorageConfigurator.class 
                .getResourceAsStream("/lucenestorage.properties.xml"); 
        Properties properties = new Properties(); 
        try { 
            properties.loadFromXML(stream); 
 
        } catch (Exception e) { 
            throw new StorageConfigurationError("Could not load properties", e); 
        } 
        this.storageBufferSize = Integer.parseInt(properties 
                .getProperty("gdata.server.storage.lucene.buffersize")); 
        this.storagepersistFactor = Integer.parseInt(properties 
                .getProperty("gdata.server.storage.lucene.persistFactor")); 
        this.recover = Boolean.parseBoolean(properties 
                .getProperty("gdata.server.storage.lucene.recover")); 
        this.keepRecoveredFiles = Boolean.parseBoolean(properties 
                .getProperty("gdata.server.storage.lucene.recover.keepFiles")); 
        this.storageDirectory = properties 
                .getProperty("gdata.server.storage.lucene.directory"); 
        this.indexOptimizeInterval = Integer.parseInt(properties 
                .getProperty("gdata.server.storage.lucene.optimizeInterval")); 
        this.ramDirectory = Boolean.parseBoolean(properties 
                .getProperty("gdata.server.storage.lucene.directory.ramDirectory"));
    } 
 
    /** 
     * @return - the storage configurator 
     */ 
    public static synchronized StorageConfigurator getStorageConfigurator() { 
        if (INSTANCE == null) 
            INSTANCE = new StorageConfigurator(); 
        return INSTANCE; 
    } 
 
    /** 
     * Keep recovering files. -- will use a lot of disk space 
     *  
     * @return <code>true</code> if the storage is supposed to keep the 
     *         recovering files. 
     */ 
    public boolean isKeepRecoveredFiles() { 
        return this.keepRecoveredFiles; 
    } 
 
    /** 
     * @return <code>true</code> if the storage is supposed to use recovering. 
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier 
     */ 
    public boolean isRecover() { 
        return this.recover; 
    } 
 
    /** 
     * @return - the configured storage buffer size 
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageBuffer 
     */ 
    public int getStorageBufferSize() { 
        return this.storageBufferSize; 
    } 
 
    /** 
     * @return - the configured storage directory 
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier 
     */ 
    public String getStorageDirectory() { 
        return this.storageDirectory; 
    } 
 
    /** 
     * @return - the persist factor 
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier 
     */ 
    public int getStoragepersistFactor() { 
        return this.storagepersistFactor; 
    } 
 
    protected class StorageConfigurationError extends Error { 
 
        /** 
         *  
         */ 
        private static final long serialVersionUID = 5261674332036111464L; 
 
        protected StorageConfigurationError(String arg0, Throwable arg1) { 
            super(arg0, arg1); 
 
        } 
 
    } 
 
    /** 
     * @return - the optimize interval 
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier 
     */ 
    public int getIndexOptimizeInterval() { 
 
        return this.indexOptimizeInterval; 
    }

    /**
     * @return Returns the ramDirectory.
     */
    public boolean isRamDirectory() {
        return this.ramDirectory;
    } 
 
} 
