// $Id$

package hplb.org.xml.sax;


/**
  * A simple base class for deriving SAX event handlers.
  * <p><em>This class is part of the Java implementation of SAX, 
  * the Simple API for XML.  It is free for both commercial and 
  * non-commercial use, and is distributed with no warrantee, real 
  * or implied.</em></p>
  * <p>This class implements the default behaviour when no handler
  * is specified (though parsers are not actually required to use
  * this class).</p>
  * @author David Megginson, Microstar Software Ltd.
  * @see hplb.org.xml.sax.XmlException
  * @see hplb.org.xml.sax.EntityHandler
  * @see hplb.org.xml.sax.DocumentHandler
  * @see hplb.org.xml.sax.ErrorHandler
  */
public class HandlerBase
  implements EntityHandler, DocumentHandler, ErrorHandler 
{


  //////////////////////////////////////////////////////////////////////
  // Implementation of hplb.org.xml.sax.EntityHandler.
  //////////////////////////////////////////////////////////////////////

  /**
    * Resolve an external entity.
    * <p>By default, simply return the system ID supplied.</p>
    * @exception java.lang.Exception When you override this method,
    *                                you may throw any exception.
    * @see hplb.org.xml.sax.EntityHandler#resolveEntity
    */
  public String resolveEntity (String ename, String publicID, String systemID)
    throws Exception
  {
    return systemID;
  }


  /**
    * Handle an entity-change event.
    * <p>By default, do nothing.</p>
    * @exception java.lang.Exception When you override this method,
    *                                you may throw any exception.
    * @see hplb.org.xml.sax.EntityHandler#changeEntity
    */
  public void changeEntity (String systemID)
    throws Exception
  {
  }



  //////////////////////////////////////////////////////////////////////
  // Implementation of hplb.org.xml.sax.DocumentHandler.
  //////////////////////////////////////////////////////////////////////


  /**
    * Handle a start document event.
    * <p>By default, do nothing.</p>
    * @exception java.lang.Exception When you override this method,
    *                                you may throw any exception.
    * @see hplb.org.xml.sax.DocumentHandler#startDocument
    */
  public void startDocument ()
    throws Exception
  {}


  /**
    * Handle a end document event.
    * <p>By default, do nothing.</p>
    * @exception java.lang.Exception When you override this method,
    *                                you may throw any exception.
    * @see hplb.org.xml.sax.DocumentHandler#endDocument
    */
  public void endDocument ()
    throws Exception
  {}

  
  /**
    * Handle a document type declaration event.
    * <p>By default, do nothing.</p>
    * @exception java.lang.Exception When you override this method,
    *                                you may throw any exception.
    * @see hplb.org.xml.sax.DocumentHandler#doctype
    */
  public void doctype (String name, String publicID, String systemID)
    throws Exception
  {}
  

  /**
    * Handle a start element event.
    * <p>By default, do nothing.</p>
    * @exception java.lang.Exception When you override this method,
    *                                you may throw any exception.
    * @see hplb.org.xml.sax.DocumentHandler#startElement
    */
  public void startElement (String name, AttributeMap attributes) 
    throws Exception
  {}
  

  /**
    * Handle an end element event.
    * <p>By default, do nothing.</p>
    * @exception java.lang.Exception When you override this method,
    *                                you may throw any exception.
    * @see hplb.org.xml.sax.DocumentHandler#endElement
    */
  public void endElement (String name) 
    throws Exception
  {}
  

  /**
    * Handle a character data event.
    * <p>By default, do nothing.</p>
    * @exception java.lang.Exception When you override this method,
    *                                you may throw any exception.
    * @see hplb.org.xml.sax.DocumentHandler#characters
    */
  public void characters (char ch[], int start, int length) 
    throws Exception
  {}


  /**
    * Handle an ignorable whitespace event.
    * <p>By default, do nothing.</p>
    * @exception java.lang.Exception When you override this method,
    *                                you may throw any exception.
    * @see hplb.org.xml.sax.DocumentHandler#ignorable
    */
  public void ignorable (char ch[], int start, int length) 
    throws Exception
  {}


  /**
    * Handle a processing instruction event.
    * <p>By default, do nothing.</p>
    * @exception java.lang.Exception When you override this method,
    *                                you may throw any exception.
    * @see hplb.org.xml.sax.DocumentHandler#processingInstruction
    */
  public void processingInstruction (String name, String remainder) 
    throws Exception
  {}



  //////////////////////////////////////////////////////////////////////
  // Implementation of ErrorHandler.
  //////////////////////////////////////////////////////////////////////


  /**
    * Handle a non-fatal error.
    * <p>By default, report the warning to System.err.</p>
    * @exception java.lang.Exception When you override this method,
    *                                you may throw any exception.
    * @see hplb.org.xml.sax.ErrorHandler#warning
    */
  public void warning (String message, String systemID, int line, int column)
    throws Exception
  {
    System.err.println("Warning (" +
		       systemID +
		       ',' +
		       line +
		       ',' +
		       column +
		       "): " +
		       message);
  }


  /**
    * Handle a fatal error.
    * <p>By default, throw an instance of XmlException.</p>
    * @exception hplb.org.xml.sax.XmlException A fatal parsing error
    *                has been found.
    * @exception java.lang.Exception When you override this method,
    *                                you may throw any exception.
    * @see hplb.org.xml.sax.ErrorHandler#fatal
    */
  public void fatal (String message, String systemID, int line, int column)
    throws XmlException, Exception
  {
    throw new XmlException(message, systemID, line, column);
  }

}
