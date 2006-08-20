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

import java.lang.reflect.Constructor;

import javax.xml.transform.Templates;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.gdata.utils.Pool;
import org.apache.lucene.gdata.utils.PoolObjectFactory;
import org.apache.lucene.gdata.utils.SimpleObjectPool;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.BaseFeed;
import com.google.gdata.data.ExtensionProfile;

/**
 * Standard implementation of
 * {@link org.apache.lucene.gdata.server.registry.ProvidedService} to be used
 * inside the
 * {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry}
 * <p>
 * ExtensionProfiles are used to generate and parse xml by the gdata api. For
 * that case all methods are synchronized. This will slow down the application
 * when performing lots of xml generation concurrently. For that case the
 * extensionProfile for a specific service will be pooled and reused.
 * </p>
 * 
 * 
 * @author Simon Willnauer
 * 
 */
@Scope(scope = Scope.ScopeType.REQUEST)
public class ProvidedServiceConfig implements ProvidedService, ScopeVisitor {
    private final static Log LOG = LogFactory
            .getLog(ProvidedServiceConfig.class);

    private static final int DEFAULT_POOL_SIZE = 5;
    private IndexSchema indexSchema;
    /*
     * To ensure a extension profile instance will not be shared within multiple
     * threads each thread requesting a config will have one instance for the
     * entire request.
     */
    protected final ThreadLocal<ExtensionProfile> extProfThreadLocal = new ThreadLocal<ExtensionProfile>();

    /*
     * ExtensionProfiles are used to generate and parse xml by the gdata api.
     * For that case all methodes are synchronized. This will slow down the
     * application when performing lots of xml generation concurrently. for that
     * case the extensionProfile for a specific service will be pooled and
     * reused.
     */
    private Pool<ExtensionProfile> profilPool;

    private String serviceName;

    private Class<? extends BaseEntry> entryType;

    private Class<? extends BaseFeed> feedType;

    private ExtensionProfile extensionProfile;

    private int poolSize = DEFAULT_POOL_SIZE;
    
    private Templates transformerTemplate;
    

    /**
     * @return Returns the poolSize.
     */
    public int getPoolSize() {
        return this.poolSize;
    }

    /**
     * @param poolSize
     *            The poolSize to set.
     */
    public void setPoolSize(int poolSize) {
        
        this.poolSize = poolSize >= DEFAULT_POOL_SIZE ? poolSize
                : DEFAULT_POOL_SIZE;
    }

    /**
     * Default constructor to instantiate via reflection
     */
    public ProvidedServiceConfig() {
        try {
            GDataServerRegistry.getRegistry().registerScopeVisitor(this);
        } catch (RegistryException e) {
            throw new RuntimeException("Can not register ScopeVisitor -- "
                    + e.getMessage(), e);
        }
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
        ExtensionProfile ext = this.extProfThreadLocal.get();
        if (ext != null) {
            return ext;
        }
        if(this.extensionProfile == null)
            return null;
        if (this.profilPool == null)
            createProfilePool();
        ext = this.profilPool.aquire();
        this.extProfThreadLocal.set(ext);
        return ext;
    }

    /**
     * @param extensionProfil -
     *            the extension profile for this feed configuration
     */
    @SuppressWarnings("unchecked")
    public void setExtensionProfile(ExtensionProfile extensionProfil) {
        if (extensionProfil == null)
            throw new IllegalArgumentException(
                    "ExtensionProfile  must not be null");
        if (this.extensionProfile != null)
            return;
        this.extensionProfile = extensionProfil;

    }

    private void createProfilePool() {
        if (LOG.isInfoEnabled())
            LOG.info("Create ExtensionProfile pool with pool size:"
                    + this.poolSize + " for service " + this.serviceName);
        this.profilPool = new SimpleObjectPool<ExtensionProfile>(this.poolSize,
                new ExtensionProfileFactory<ExtensionProfile>(
                        this.extensionProfile.getClass(),this.entryType,this.feedType));
    }

    /**
     * TODO add comment
     * 
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

        setExtensionProfile(extensionProfileClass.newInstance());

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

    /**
     * @see org.apache.lucene.gdata.server.registry.ProvidedService#destroy()
     */
    public void destroy() {
        if (this.profilPool != null)
            this.profilPool.destroy();
        if (LOG.isInfoEnabled())
            LOG.info("Destroy Service " + this.serviceName
                    + " -- release all resources");
        this.feedType = null;
        this.entryType = null;
        this.extensionProfile = null;
    }

    private static class ExtensionProfileFactory<Type extends ExtensionProfile>
            implements PoolObjectFactory<Type> {
        private final Class<? extends ExtensionProfile> clazz;

        private final Constructor<? extends ExtensionProfile> constructor;

        private static final Object[] constArray = new Object[0];
        
        private BaseEntry entry;
        private BaseFeed feed;

        ExtensionProfileFactory(Class<? extends ExtensionProfile> clazz, Class<? extends BaseEntry> entryClass, Class<? extends BaseFeed> feedClass) {
            this.clazz = clazz;
            try {
                this.constructor = clazz.getConstructor(new Class[0]);
                this.entry = entryClass.newInstance();
                this.feed = feedClass.newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "The given class has no default constructor -- can not use as a ExtensionProfile -- "
                                + this.clazz.getName(), e);
            }
        }

        /**
         * @see org.apache.lucene.gdata.utils.PoolObjectFactory#getInstance()
         */
        @SuppressWarnings("unchecked")
        public Type getInstance() {

            try {
                Type retValue = (Type) this.constructor.newInstance(constArray);
                this.entry.declareExtensions(retValue);
                this.feed.declareExtensions(retValue);
                return retValue; 
            } catch (Exception e) {
                throw new RuntimeException(
                        "Can not instantiate new ExtensionProfile -- ", e);

            }
        }

        /**
         * @param type -
         *            the ExtensionProfile to destroy
         * @see org.apache.lucene.gdata.utils.PoolObjectFactory#destroyInstance(Object)
         */
        public void destroyInstance(Type type) {
            //
        }

    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ScopeVisitor#visiteInitialize()
     */
    public void visiteInitialize() {
        if(this.profilPool == null)
            createProfilePool();
        /*
         * don't set a extension profile for each thread. The current thread
         * might use another service and does not need the extension profile of
         * this service
         */
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ScopeVisitor#visiteDestroy()
     */
    public void visiteDestroy() {
        /*
         * Check every thread after request destroyed to release all profiles to
         * the pool
         */
        ExtensionProfile ext = this.extProfThreadLocal.get();
        if (ext == null) {
            if(LOG.isDebugEnabled())
            LOG.debug("ThreadLocal owns no ExtensionProfile in requestDestroy for service "
                            + this.serviceName);
            return;
        }
        this.extProfThreadLocal.set(null);
        this.profilPool.release(ext);
    }

    /**
     * @return Returns the indexSchema.
     */
    public IndexSchema getIndexSchema() {
        return this.indexSchema;
    }

    /**
     * @param indexSchema The indexSchema to set.
     */
    public void setIndexSchema(IndexSchema indexSchema) {
        this.indexSchema = indexSchema;
        if(this.indexSchema != null)
            this.indexSchema.setName(this.serviceName);
    }

    /**
     * @see org.apache.lucene.gdata.server.registry.ProvidedService#getTransformTemplate()
     */
    public Templates getTransformTemplate() {
        
        return this.transformerTemplate;
    }
    
    /**
     * Sets and creates the preview transformer xslt template to provide a html formate for feeds and entries.
     * The given file name must be available in the classpath. 
     * @param filename - the name of the file in the classpath
     */
    public void setXsltStylesheet(String filename){
        if(filename == null || filename.length() == 0){
            LOG.info("No preview stylesheet configured for service "+this.serviceName);
            return;
        }
        
        TransformerFactory factory = TransformerFactory.newInstance();
        
        try {
            this.transformerTemplate = factory.newTemplates(new StreamSource(ProvidedServiceConfig.class.getResourceAsStream(filename.startsWith("/")?filename:"/"+filename)));
        } catch (TransformerConfigurationException e) {
            throw new RuntimeException("Can not compile xslt stylesheet path: "+filename,e);
        }
        
    }
    
}
