
/**
 * Title:        LARM Lanlab Retrieval Machine<p>
 * Description:  <p>
 * Copyright:    Copyright (c)<p>
 * Company:      <p>
 * @author
 * @version 1.0
 */
package de.lanlab.larm.storage;
import de.lanlab.larm.util.*;

/**
 * doesn't do a lot
 */
public class NullStorage implements DocumentStorage
{

    public NullStorage()
    {
    }

    public void open() {}
    public void store(WebDocument doc) {}

}