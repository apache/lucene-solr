// $Id$

package hplb.org.xml.sax;


/**
  * A callback interface for basic XML entity-related events.
  * <p><em>This interface is part of the Java implementation of SAX, 
  * the Simple API for XML.  It is free for both commercial and 
  * non-commercial use, and is distributed with no warrantee, real 
  * or implied.</em></p>
  * <p>If you do not set an entity handler, then a parser will
  * resolve all entities to the suggested system ID, and will take no
  * action for entity changes.</p>
  * @author David Megginson, Microstar Software Ltd.
  * @see hplb.org.xml.sax.Parser#setEntityHandler
  */
public interface EntityHandler {


  /**
    * Resolve a system identifier.
    * <p>Before loading any entity (including the document entity),
    * SAX parsers will filter the system identifier through this
    * callback, and you can return a different system identifier if you
    * wish, or null to prevent the parser from reading any entity.</p>
    * @param ename The name of the entity, "[document]" for the
    *              document entity, or "[external DTD]" for the external
    *              DTD subset.
    * @param publicID The public identifier, or null if there is none.
    * @param systemID The system identifier suggested in the XML document.
    * @return A system identifier, or null to skip the entity.
    * @exception java.lang.Exception You may throw any exception.
    */
  public String resolveEntity (String ename, String publicID, String systemID)
    throws Exception;

  /**
    * Handle a change in the current entity.
    * <p>Whenever the parser switches the entity (URI) that it is reading
    * from, it will call this handler to report the change.</p>
    * @param systemID The URI of the new entity.
    * @exception java.lang.Exception You may throw any exception.
    */
  public void changeEntity (String systemID)
    throws Exception;

}
