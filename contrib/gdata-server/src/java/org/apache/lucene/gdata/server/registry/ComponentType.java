package org.apache.lucene.gdata.server.registry;

import org.apache.lucene.gdata.server.ServiceFactory;
import org.apache.lucene.gdata.server.authentication.AuthenticationController;
import org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory;
import org.apache.lucene.gdata.storage.StorageController;

/**
 * The enmueration {@link ComponentType} defines the GDATA-Server Components 
 * available via {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry#lookup(Class, ComponentType)} 
 * method.
 * @see org.apache.lucene.gdata.server.registry.Component
 * @see org.apache.lucene.gdata.server.registry.GDataServerRegistry 
 * @author Simon Willnauer
 *
 */
public enum ComponentType {
    /**
     * StorageController Type
     * 
     * @see StorageController
     */
    @SuperType(superType = StorageController.class)
    STORAGECONTROLLER,
    /**
     * RequestHandlerFactory Type
     * 
     * @see RequestHandlerFactory
     */
    @SuperType(superType = RequestHandlerFactory.class)
    REQUESTHANDLERFACTORY,
    /**
     * INDEXER TYPE
     * 
     */
    // TODO not available yet
    @SuperType(superType = Object.class)
    INDEXER,
    /**
     * ServiceFactory Type
     * 
     * @see ServiceFactory
     */
    @SuperType(superType = ServiceFactory.class)
    SERVICEFACTORY,
    /**
     * Supertype for AuthenticationController implementations
     * @see AuthenticationController
     */
    @SuperType(superType = AuthenticationController.class)
    AUTHENTICATIONCONTROLLER

}
