// $Id$

package hplb.org.xml.sax;


/**
  * A callback interface for basic XML error events.
  * <p><em>This interface is part of the Java implementation of SAX, 
  * the Simple API for XML.  It is free for both commercial and 
  * non-commercial use, and is distributed with no warrantee, real 
  * or implied.</em></p>
  * <p>If you do not set an error handler, then a parser will report
  * warnings to <code>System.err</code>, and will throw an (unspecified)
  * exception for fata errors.</p>
  * @author David Megginson, Microstar Software Ltd.
  * @see hplb.org.xml.sax.Parser#setErrorHandler
  */
public interface ErrorHandler {

  /**
    * Handle a non-fatal warning.
    * <p>A SAX parser will use this callback to report a condition
    * that is not serious enough to stop the parse (though you may
    * still stop the parse if you wish).</p>
    * @param message The warning message.
    * @param systemID The URI of the entity that caused the warning, or
    *                 null if not available.
    * @param line The line number in the entity, or -1 if not available.
    * @param column The column number in the entity, or -1 if not available.
    * @exception java.lang.Exception You may throw any exception.
    */
  public void warning (String message, String systemID, int line, int column)
    throws java.lang.Exception;

  /**
    * Handle a fatal error.
    * <p>A SAX parser will use this callback to report a condition
    * that is serious enough to invalidate the parse, and may not
    * report all (or any) significant parse events after this.  Ordinarily,
    * you should stop immediately with an exception, but you can continue
    * to try to collect more errors if you wish.</p>
    * @param message The error message.
    * @param systemID The URI of the entity that caused the error, or
    *                 null if not available.
    * @param line The line number in the entity, or -1 if not available.
    * @param column The column number in the entity, or -1 if not available.
    * @exception java.lang.Exception You may throw any exception.
    */
  public void fatal (String message, String systemID, int line, int column)
    throws Exception;

}
