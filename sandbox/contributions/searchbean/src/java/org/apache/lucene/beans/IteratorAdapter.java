package org.apache.lucene.beans;

import java.util.Iterator;
import java.io.IOException;

/**
 * Acts as an adapter for HitsIterator to comply with the Collections
 * API.
 *
 * @author <a href="mailto:kelvint@apache.org">Kelvin Tan</a>
 * @version $Id$
 */
public final class IteratorAdapter implements Iterator
{
    private HitsIterator hitsIterator;

    public IteratorAdapter(HitsIterator it)
    {
        this.hitsIterator = it;
    }

    public boolean hasNext()
    {
        return hitsIterator.hasNext();
    }

    public Object next()
    {
        Object obj = null;
        try {
            obj = hitsIterator.next();
        } catch (IOException e) {
            // ignore for now, returning null might be good enough?
        }
        return obj;
    }

    public void remove()
    {
        throw new UnsupportedOperationException(
                "HitsIterator does not " +
                "support modification of the hits!");
    }

    public HitsIterator getHitsIterator()
    {
        return hitsIterator;
    }
}
