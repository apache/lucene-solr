package org.apache.lucene.gdata.server; 
 
 
/** 
 * Will be thrown if a requested feed could not be found or is not 
 * registerd. 
 *  
 * @author Simon Willnauer 
 *  
 */ 
public class FeedNotFoundException extends ServiceException { 
 
    private static final long serialVersionUID = 1L; 
 
    /** 
     * Constructs a FeedNotFoundException 
     */ 
    public FeedNotFoundException() { 
        super(); 
 
    } 
 
    /** 
     * @param arg0 - 
     *            message 
     * @param arg1 - 
     *            cause 
     */ 
    public FeedNotFoundException(String arg0, Throwable arg1) { 
        super(arg0, arg1); 
 
    } 
 
    /** 
     * @param arg0 - 
     *            message 
     */ 
    public FeedNotFoundException(String arg0) { 
        super(arg0); 
 
    } 
 
    /** 
     * @param arg0 - 
     *            cause 
     */ 
    public FeedNotFoundException(Throwable arg0) { 
        super(arg0); 
 
    } 
 
} 