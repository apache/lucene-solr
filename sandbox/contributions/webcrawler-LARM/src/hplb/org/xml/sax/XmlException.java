// $Id$

package hplb.org.xml.sax;


/**
  * An exception for reporting XML parsing errors.
  * <p><em>This interface is part of the Java implementation of SAX, 
  * the Simple API for XML.  It is free for both commercial and 
  * non-commercial use, and is distributed with no warrantee, real 
  * or implied.</em></p>
  * <p>This exception is not a required part of SAX, and it is not
  * referenced in any of the core interfaces.  It is used only in
  * the optional HandlerBase base class, as a means of signalling
  * parsing errors.</p>
  * @author David Megginson, Microstar Software Ltd.
  * @see hplb.org.xml.sax.HandlerBase#fatal
  */
public class XmlException extends Exception {


  /**
    * Construct a new exception with information about the location.
    */
  public XmlException (String message, String systemID, int line, int column)
  {
    super(message);
    this.systemID = systemID;
    this.line = line;
    this.column = column;
  }


  /**
    * Find the system identifier (URI) where the error occurred.
    * @return A string representing the URI, or null if none is available.
    */
  public String getSystemID ()
  {
    return systemID;
  }


  /**
    * Find the line number where the error occurred.
    * @return The line number, or -1 if none is available.
    */
  public int getLine ()
  {
    return line;
  }


  /**
    * Find the column number (line offset) where the error occurred.
    * @return The column number, or -1 if none is available.
    */
  public int getColumn ()
  {
    return column;
  }



  //
  // Internal state.
  //

  private String systemID;
  private int line;
  private int column;

}
