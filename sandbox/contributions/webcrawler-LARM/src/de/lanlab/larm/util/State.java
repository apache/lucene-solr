/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

package de.lanlab.larm.util;

import java.io.Serializable;
/**
 * Title:        LARM Lanlab Retrieval Machine
 * Description:
 * Copyright:    Copyright (c)
 * Company:
 * @author
 * @version 1.0
 */

/**
 * thread safe state information.
 * The get methods are not synchronized. Clone the state object before using them
 * If you use a state object in a class, always return a clone
 * <pre>public class MyClass {
 *     State state = new State("Running");
 *     public State getState() { return state.cloneState() }</pre>
 *
 * note on serialization: if you deserialize a state, the state string will be newly created.
 * that means you then have to compare the states via equal() and not ==
 */
public class State implements Cloneable, Serializable
{

    private String state;
    private long stateSince;
    private Object info;

    public State(String state)
    {
        setState(state);
    }


    private State(String state, long stateSince)
    {
        init(state, stateSince, null);
    }

    private State(String state, long stateSince, Object info)
    {
        init(state, stateSince, info);
    }

    private void init(String state, long stateSince, Object info)
    {
        this.state = state;
        this.stateSince = stateSince;
        this.info = info;
    }

    public void setState(String state)
    {
        setState(state, null);
    }

    public synchronized void setState(String state, Object info)
    {
        this.state = state;
        this.stateSince = System.currentTimeMillis();
        this.info = info;
    }

    public String getState()
    {
        return state;
    }

    public long getStateSince()
    {
        return stateSince;
    }

    public Object getInfo()
    {
        return info;
    }

    public synchronized Object clone()
    {
        return new State(state, stateSince, info);
    }

    public State cloneState()
    {
        return (State)clone();
    }

}