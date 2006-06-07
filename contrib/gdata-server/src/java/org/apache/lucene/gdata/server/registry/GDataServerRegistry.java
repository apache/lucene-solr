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
package org.apache.lucene.gdata.server.registry; 
 
import java.util.HashMap; 
import java.util.Map; 
 
import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory; 
import org.apache.lucene.gdata.storage.StorageController; 
 
import com.google.gdata.data.ExtensionProfile; 
 
/** 
 *  
 * The FeedRegistry represents the registry component of the GData Server. All 
 * feed configurations will be registered here. Feed configurations contain 
 * several informationsa about GData feed like: 
 * <ol> 
 * <li>the feed id - where the feed can be accessed via http methodes</li> 
 * <li>the feed type - feed types are implementations of the abstract 
 * {@link com.google.gdata.data.BaseFeed}</li> 
 * </ol> 
 * The registry will be set up at start up of the server application and can be 
 * accessed from other components to get configurations according to incoming 
 * requests. 
 *  
 * @author Simon Willnauer 
 *  
 */ 
public class GDataServerRegistry { 
    private static GDataServerRegistry INSTANCE; 
 
    private StorageController storageInstance; 
 
    private static final Log LOGGER = LogFactory 
            .getLog(GDataServerRegistry.class); 
 
    private final Map<String, FeedInstanceConfigurator> feedTypMap = new HashMap<String, FeedInstanceConfigurator>(); 
 
    private GDataServerRegistry() { 
        // private - singleton 
    } 
 
    /** 
     * @return a Sinleton registry instance 
     */ 
    public static synchronized GDataServerRegistry getRegistry() { 
        if (INSTANCE == null) 
            INSTANCE = new GDataServerRegistry(); 
        return INSTANCE; 
    } 
 
    /** 
     * Registers a {@link FeedInstanceConfigurator} 
     *  
     * @param configurator - 
     *            the configurator to register in the registry 
     */ 
    public void registerFeed(FeedInstanceConfigurator configurator) { 
        if (configurator == null) { 
            LOGGER.warn("Feedconfigurator is null -- skip registration"); 
            return; 
        } 
        this.feedTypMap.put(configurator.getFeedId(), configurator); 
    } 
 
    /** 
     * Looks up the {@link FeedInstanceConfigurator} by the given feed id. 
     *  
     * @param feedId 
     * @return - the {@link FeedInstanceConfigurator} or <code>null</code> if 
     *         the no configuration for this feed has been registered 
     */ 
    public FeedInstanceConfigurator getFeedConfigurator(String feedId) { 
        if (feedId == null) 
            throw new IllegalArgumentException( 
                    "Feed URL is null - must not be null to get registered feedtype"); 
        return this.feedTypMap.get(feedId); 
    } 
 
    protected void flushRegistry() { 
        this.feedTypMap.clear(); 
    } 
 
    /** 
     * @param feedId - 
     *            the id of the feed as the feed is registered 
     * @return - <code>true</code> if and only if the feed is registered, 
     *         otherwise <code>false</code>. 
     */ 
    public boolean isFeedRegistered(String feedId) { 
        return this.feedTypMap.containsKey(feedId); 
 
    } 
 
    /** 
     * @param storage 
     */ 
    public void registerStorage(StorageController storage) { 
        if (this.storageInstance != null) 
            throw new IllegalStateException( 
                    "Storage already registered -- Instance of " 
                            + this.storageInstance.getClass()); 
        this.storageInstance = storage; 
    } 
 
    /** 
     * Destroys the registry and release all resources 
     */ 
    public void destroy() { 
        flushRegistry(); 
        this.storageInstance.destroy(); 
        this.storageInstance = null; 
 
    } 
 
    /** 
     * Creates the  {@link ExtensionProfile} for a registered feed 
     * @param feedId - the feed id  
     * @return - the extension profil for this feed of <code>null</code> if 
     *         the feed is not registered or the extension profile could not be 
     *         instanciated 
     */ 
    public ExtensionProfile getExtensionProfile(final String feedId) { 
        FeedInstanceConfigurator configurator = this.feedTypMap.get(feedId); 
        if (configurator == null) 
            return null; 
        Class clazz = configurator.getExtensionProfilClass(); 
        try { 
            return (ExtensionProfile) clazz.newInstance(); 
        } catch (Exception e) { 
            LOGGER 
                    .error("Can not create instance of ExtensionProfil for class: " 
                            + clazz + " -- feedId: " + feedId); 
 
        } 
        return null; 
    } 
 
} 
