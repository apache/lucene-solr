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
     * Constructs a new FeedNotFoundException
     * @param errorCode - gdata request errorcode
     */
    public FeedNotFoundException(int errorCode) {
        super(errorCode);
        

    }

    /**
     * Constructs a new FeedNotFoundException
     * @param arg0 - the exception message
     * @param errorCode - gdata request errorcode
     */
    public FeedNotFoundException(String arg0,int errorCode) {
        super(arg0, errorCode);
        
    }

    /**
     * Constructs a new FeedNotFoundException
     * @param arg0 - the exceptin message
     * @param arg1 - the exception cause
     * @param errorCode - gdata request errorcode
     */
    public FeedNotFoundException(String arg0, Throwable arg1,int errorCode) {
        super(arg0, arg1, errorCode);
        
        
    }

    /**
     * Constructs a new FeedNotFoundException
     * @param arg0 - the exception cause
     * @param errorCode - gdata request errorcode
     */
    public FeedNotFoundException(Throwable arg0,int errorCode) {
        super(arg0, errorCode);
        
    }
 
} 