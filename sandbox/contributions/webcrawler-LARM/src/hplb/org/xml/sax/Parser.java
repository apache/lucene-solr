// $Id$

package hplb.org.xml.sax;


/**
  * A standard interface for event-driven XML parsers.
  * <p><em>This interface is part of the Java implementation of SAX, 
  * the Simple API for XML.  It is free for both commercial and 
  * non-commercial use, and is distributed with no warrantee, real 
  * or implied.</em></p>
  * <p>All SAX-conformant XML parsers (or their front-end SAX drivers)
  * <em>must</em> implement this interface, together with a zero-argument
  * constructor.</p>
  * <p>You can plug three different kinds of callback interfaces into
  * a basic SAX parser: one for entity handling, one for basic document
  * events, and one for error reporting.  It is not an error to start
  * a parse without setting any handlers.</p>
  * @author David Megginson, Microstar Software Ltd.
  */
public interface Parser {


  /**
    * Register the handler for basic entity events.
    * <p>If you begin a parse without setting an entity handler,
    * the parser will by default resolve all entities to their
    * default system IDs.</p>
    * @param handler An object to receive callbacks for events.
    * @see hplb.org.xml.sax.EntityHandler
    */
  public void setEntityHandler (EntityHandler handler);


  /**
    * Register the handler for basic document events.
    * <p>You may begin the parse without setting a handler, but
    * in that case no document events will be reported.</p>
    * @param handler An object to receive callbacks for events.
    * @see hplb.org.xml.sax.DocumentHandler
    */
  public void setDocumentHandler (DocumentHandler handler);


  /**
    * Register the handler for errors and warnings.
    * <p>If you begin a parse without setting an error handlers,
    * warnings will be printed to System.err, and errors will
    * throw an unspecified exception.</p>
    * @param handler An object to receive callbacks for errors.
    * @see hplb.org.xml.sax.ErrorHandler
    */
  public void setErrorHandler (ErrorHandler handler);


  /**
    * Parse an XML document.
    * <p>Nothing exciting will happen unless you have set handlers.</p>
    * @param publicID The public identifier for the document, or null
    *                 if none is available.
    * @param systemID The system identifier (URI) for the document.
    * @exception java.lang.Exception This method may throw any exception, 
    *            but the parser itself
    *            will throw only exceptions derived from java.io.IOException;
    *            anything else will come from your handlers.
    * @see #setEntityHandler
    * @see #setDocumentHandler
    * @see #setErrorHandler
    */
  void parse (String publicID, String systemID) throws java.lang.Exception;
}
