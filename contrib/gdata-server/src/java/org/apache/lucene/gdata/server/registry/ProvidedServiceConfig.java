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

import com.google.gdata.data.ExtensionProfile;

/**
 * Standart implementation of
 * {@link org.apache.lucene.gdata.server.registry.ProvidedService} to be used
 * inside the
 * {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry}
 * 
 * @author Simon Willnauer
 * 
 */
public class ProvidedServiceConfig implements ProvidedService {
    private String serviceName;

    private Class entryType;

    private Class feedType;

    private ExtensionProfile extensionProfile;

   
    ProvidedServiceConfig(ExtensionProfile profile, Class feedType,
            Class entryType, String serviceName) {
        this.extensionProfile = profile;
        this.feedType = feedType;
        this.entryType = entryType;
        this.serviceName = serviceName;

    }

    /**
     * Default constructor to instanciate via reflection 
     */
    public ProvidedServiceConfig() {
        //
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ProvidedService#getFeedType()
     */
    public Class getFeedType() {
        return this.feedType;
    }

    /**
     * @param feedType
     *            The feedType to set.
     */
    public void setFeedType(Class feedType) {
        this.feedType = feedType;
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ProvidedService#getExtensionProfile()
     */
    public ExtensionProfile getExtensionProfile() {
        return this.extensionProfile;
    }

    /**
     * @param extensionProfil -
     *            the extensionprofile for this feed configuration
     */
    public void setExtensionProfile(ExtensionProfile extensionProfil) {
        this.extensionProfile = extensionProfil;
    }

    
    /**
     *TODO add comment
     * @param <E>
     * @param extensionProfileClass
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public <E extends ExtensionProfile> void setExtensionProfileClass(
            Class<E> extensionProfileClass) throws InstantiationException,
            IllegalAccessException {
        if (extensionProfileClass == null)
            throw new IllegalArgumentException(
                    "ExtensionProfile class must not be null");

        this.extensionProfile = extensionProfileClass.newInstance();
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ProvidedService#getEntryType()
     */
    public Class getEntryType() {
        return this.entryType;
    }
    
    /**
     * @param entryType
     */
    public void setEntryType(Class entryType) {
        this.entryType = entryType;
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ProvidedService#getName()
     */
    public String getName() {
        return this.serviceName;
    }

    /**
     * @param serviceName
     */
    public void setName(String serviceName) {
        this.serviceName = serviceName;
    }

}
