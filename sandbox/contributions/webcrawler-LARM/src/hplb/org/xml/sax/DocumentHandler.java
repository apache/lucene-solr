// $Id$

package hplb.org.xml.sax;


/**
  * A callback interface for basic XML document events.
  * <p><em>This interface is part of the Java implementation of SAX, 
  * the Simple API for XML.  It is free for both commercial and 
  * non-commercial use, and is distributed with no warrantee, real 
  * or implied.</em></p>
  * <p>This is the main handler for basic document events; it provides
  * information on roughly the same level as the ESIS in full SGML,
  * concentrating on logical structure rather than lexical 
  * representation.</p>
  * <p>If you do not set a document handler, then by default all of these
  * events will simply be ignored.</p>
  * @author David Megginson, Microstar Software Ltd.
  * @see hplb.org.xml.sax.Parser@setDocumentHandler
  */
public interface DocumentHandler {


  /**
    * Handle the start of a document.
    * <p>This is the first event called by a
    * SAX-conformant parser, so you can use it to allocate and
    * initialise new objects for the document.</p>
    * @exception java.lang.Exception You may throw any exception.
    */
  public void startDocument ()
    throws Exception;


  /**
    * Handle the end of a document.
    * <p>This is the last event called by a
    * SAX-conformant parser, so you can use it to finalize and
    * clean up objects for the document.</p>
    * @exception java.lang.Exception You may throw any exception.
    */
  public void endDocument ()
    throws Exception;


  /**
    * Handle the document type declaration.
    * <p>This will appear only if the XML document contains a
    * <code>DOCTYPE</code> declaration.</p>
    * @param name The document type name.
    * @param publicID The public identifier of the external DTD subset
    *                 (if any), or null.
    * @param systemID The system identifier of the external DTD subset
    *                 (if any), or null.
    * @param name The document type name.
    * @exception java.lang.Exception You may throw any exception.
    */
  public void doctype (String name, String publicID, String systemID)
    throws Exception;


  /**
    * Handle the start of an element.
    * <p>Please note that the information in the <code>attributes</code>
    * parameter will be accurate only for the duration of this handler:
    * if you need to use the information elsewhere, you should copy 
    * it.</p>
    * @param name The element type name.
    * @param attributes The available attributes.
    * @exception java.lang.Exception You may throw any exception.
    */
  public void startElement (String name, AttributeMap attributes)
    throws Exception;


  /**
    * Handle the end of an element.
    * @exception java.lang.Exception You may throw any exception.
    */
  public void endElement (String name)
    throws Exception;


  /**
    * Handle significant character data.
    * <p>Please note that the contents of the array will be
    * accurate only for the duration of this handler: if you need to
    * use them elsewhere, you should make your own copy, possible
    * by constructing a string:</p>
    * <pre>
    * String data = new String(ch, start, length);
    * </pre>
    * @param ch An array of characters.
    * @param start The starting position in the array.
    * @param length The number of characters to use in the array.
    * @exception java.lang.Exception You may throw any exception.
    */
  public void characters (char ch[], int start, int length)
    throws Exception;


  /**
    * Handle ignorable whitespace.
    * <p>Please note that the contents of the array will be
    * accurate only for the duration of this handler: if you need to
    * use them elsewhere, you should make your own copy, possible
    * by constructing a string:</p>
    * <pre>
    * String whitespace = new String(ch, start, length);
    * </pre>
    * @param ch An array of whitespace characters.
    * @param start The starting position in the array.
    * @param length The number of characters to use in the array.
    * @exception java.lang.Exception You may throw any exception.
    */
  public void ignorable (char ch[], int start, int length)
    throws Exception;


  /**
    * Handle a processing instruction.
    * <p>XML processing instructions have two parts: a target, which
    * is a name, followed optionally by data.</p>
    * @exception java.lang.Exception You may throw any exception.
    */
  public void processingInstruction (String name, String remainder)
    throws Exception;

}
