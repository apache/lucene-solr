
/**
 * Title: LARM Lanlab Retrieval Machine<p>
 *
 * Description: <p>
 *
 * Copyright: Copyright (c)<p>
 *
 * Company: <p>
 *
 *
 *
 * @author
 * @version   1.0
 */
package de.lanlab.larm.storage;
import de.lanlab.larm.util.*;

/**
 * This interface stores documents provided by a fetcher task
 * @author    Clemens Marschner
 */
public interface DocumentStorage
{
    /**
     * called once when the storage is supposed to be initialized
     */
    public void open();


    /**
     * called to store a web document
     *
     * @param doc  the document
     */
    public void store(WebDocument doc);
}
