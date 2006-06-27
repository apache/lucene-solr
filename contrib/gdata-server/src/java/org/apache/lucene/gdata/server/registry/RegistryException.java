package org.apache.lucene.gdata.server.registry;

/**
 * This exception is thrown by the
 * {@link org.apache.lucene.gdata.server.registry.GDataServerRegistry} if
 * registering a service or a component fails.
 * 
 * @author Simon Willnauer
 * 
 */
public class RegistryException extends Exception {

 
    private static final long serialVersionUID = -3563720639871194466L;

    /**
     * Constructs a new Registry Exception.
     */
    public RegistryException() {
        super();
        
    }

    /**
     * Constructs a new Registry Exception with the specified detail message.
     * @param arg0 - detail message
     */
    public RegistryException(String arg0) {
        super(arg0);
        
    }

    /**
     * Constructs a new Registry Exception with the specified detail message and nested exception.
     * @param arg0 - detail message
     * @param arg1 - nested exception
     */
    public RegistryException(String arg0, Throwable arg1) {
        super(arg0, arg1);
        
    }

    /** Constructs a new Registry Exception with a nested exception.
     * @param arg0 - nested exception
     */
    public RegistryException(Throwable arg0) {
        super(arg0);
        
    }

}
