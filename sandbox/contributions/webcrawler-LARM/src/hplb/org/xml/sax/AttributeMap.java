// $Id$

package hplb.org.xml.sax;

import java.util.Enumeration;

/**
  * A map of attributes for the current element.
  * <p><em>This interface is part of the Java implementation of SAX, 
  * the Simple API for XML.  It is free for both commercial and 
  * non-commercial use, and is distributed with no warrantee, real 
  * or implied.</em></p>
  * <p>This map will be valid only during the invocation of the
  * <code>startElement</code> callback: if you need to use attribute
  * information elsewhere, you will need to make your own copies.</p>
  * @author David Megginson, Microstar Software Ltd.
  * @see hplb.org.xml.sax.DocumentHandler#startElement
  */
public interface AttributeMap {


  /**
    * Find the names of all available attributes for an element.
    * <p>This applies to the current element, and can be called only
    * during an invocation of <code>startElement</code>.</p> 
    * @return An enumeration of zero or more Strings.
    * @see java.util.Enumeration
    * @see hplb.org.xml.sax.DocumentHandler#startElement
    */
  public Enumeration getAttributeNames ();


  /**
    * Get the value of an attribute as a String.
    * <p>This applies to the current element, and can be called only
    * during an invocation of <code>startElement</code>.</p> 
    * @return The value as a String, or null if the attribute has no value.
    * @see hplb.org.xml.sax.DocumentHandler#startElement
    */
  public String getValue (String attributeName);


  /**
    * Check if an attribute value is the name of an entity.
    * <p>This applies to the current element, and can be called only
    * during an invocation of <code>startElement</code>.</p> 
    * @return true if the attribute is an entity name.
    * @see #getEntityPublicID
    * @see #getEntitySystemID
    * @see #getNotationName
    * @see #getNotationPublicID
    * @see #getNotationSystemID
    * @see hplb.org.xml.sax.DocumentHandler#startElement
    */
  public boolean isEntity (String aname);


  /**
    * Check if an attribute value is the name of a notation.
    * <p>This applies to the current element, and can be called only
    * during an invocation of <code>startElement</code>.</p> 
    * @return true if the attribute is a notation name.
    * @see #getNotationPublicID
    * @see #getNotationSystemID
    * @see hplb.org.xml.sax.DocumentHandler#startElement
    */
  public boolean isNotation (String aname);


  /**
    * Check if an attribute value is a unique identifier.
    * <p>This applies to the current element, and can be called only
    * during an invocation of <code>startElement</code>.</p> 
    * @return true if the attribute is a unique identifier.
    * @see hplb.org.xml.sax.DocumentHandler#startElement
    */
  public boolean isId (String aname);


  /**
    * Check if an attribute value is a reference to an ID.
    * <p>This applies to the current element, and can be called only
    * during an invocation of <code>startElement</code>.</p> 
    * @return true if the attribute is a reference to an ID.
    * @see hplb.org.xml.sax.DocumentHandler#startElement
    */
  public boolean isIdref (String aname);


  /**
    * Get the public identifier for an ENTITY attribute.
    * <p>This applies to the current element, and can be called only
    * during an invocation of <code>startElement</code>.</p> 
    * @return The public identifier or null if there is none (or if
    *         the attribute value is not an entity name)
    * @see #isEntity
    */
  public String getEntityPublicID (String aname);


  /**
    * Get the system identifer for an ENTITY attribute.
    * <p>This applies to the current element, and can be called only
    * during an invocation of <code>startElement</code>.</p> 
    * @return The system identifier or null if there is none (or if
    *         the attribute value is not an entity name)
    * @see #isEntity
    */
  public String getEntitySystemID (String aname);


  /**
    * Get the notation name for an ENTITY attribute.
    * <p>This applies to the current element, and can be called only
    * during an invocation of <code>startElement</code>.</p> 
    * @return The notation name or null if there is none (or if
    *         the attribute value is not an entity name)
    * @see #isEntity
    */
  public String getNotationName (String aname);


  /**
    * Get the notation public ID for an ENTITY or NOTATION attribute.
    * <p>This applies to the current element, and can be called only
    * during an invocation of <code>startElement</code>.</p> 
    * @return The public identifier or null if there is none (or if
    *         the attribute value is not an entity or notation name)
    * @see #isEntity
    * @see #isNotation
    */
  public String getNotationPublicID (String aname);


  /**
    * Get the notation system ID for an ENTITY or NOTATION attribute.
    * <p>This applies to the current element, and can be called only
    * during an invocation of <code>startElement</code>.</p> 
    * @return The system identifier or null if there is none (or if
    *         the attribute value is not an entity or notation name)
    * @see #isEntity
    * @see #isNotation
    */
  public String getNotationSystemID (String aname);

}
