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

package de.lanlab.larm.graph;

import java.io.*;

import java.util.*;

/**
 * this is just a test to use some graph algorithms on the URL graph
 *
 * @author    Administrator
 * @created   30. Januar 2002
 * @version $Id$
 */

class Node implements Comparable
{

    LinkedList incoming;

    // 16 + 4 per entry

    //HashSet incomingNodes; // 16 + 16 per entry, 11 x 16 default size = 192

    LinkedList outgoing;

    // 16 + 4 per entry

    //Object o;

    //HashSet outgoingNodes; // 16 + 16 per entry, 11 x 16 default size = 192

    //LinkedList shortestIncoming;

    int id;

    // 4

    float distance;

    // 8

    String name;

    // 4 + String object

    String title;

    // 4 + String object

    float nodeRank[] = new float[2];

    // 16

    // 470 bytes + 2 string objects

    /**
     * Description of the Field
     */

    public static int sortType = 0;



    /**
     * Description of the Method
     *
     * @param n  Description of the Parameter
     * @return   Description of the Return Value
     */

    public int compareTo(Object n)
    {

        if (sortType < 2)
        {

            double diff = ((Node) n).nodeRank[sortType] - nodeRank[sortType];

            return diff < 0 ? -1 : diff > 0 ? 1 : 0;
        }

        else
        {

            return (((Node) n).incoming.size() - incoming.size());
        }

    }



    /**
     * Constructor for the Node object
     *
     * @param id     Description of the Parameter
     * @param name   Description of the Parameter
     * @param title  Description of the Parameter
     */

    public Node(int id, String name, String title)
    {

        this.id = id;

        this.name = name;

        this.title = title;

        this.incoming = new LinkedList();

        this.outgoing = new LinkedList();

        //this.incomingNodes = new HashSet();

        //this.outgoingNodes = new HashSet();

        this.distance = Float.MAX_VALUE;

        this.nodeRank[0] = this.nodeRank[1] = 1;

    }



    /**
     * Adds a feature to the Incoming attribute of the Node object
     *
     * @param incomingT  The feature to be added to the Incoming attribute
     * @return           Description of the Return Value
     */

    public boolean addIncoming(Transition incomingT)
    {

        Integer id = new Integer(incomingT.getFrom().id);

        if (!incoming.contains(id))
        {

            // attn: doesn't scale well, but also saves memory

            incoming.addLast(incomingT);

            //incomingNodes.add(id);

            return true;
        }

        else
        {

            return false;
        }

    }



    /**
     * Adds a feature to the Outgoing attribute of the Node object
     *
     * @param outgoingT  The feature to be added to the Outgoing attribute
     * @return           Description of the Return Value
     */

    public boolean addOutgoing(Transition outgoingT)
    {

        Integer id = new Integer(outgoingT.getTo().id);

        if (!outgoing.contains(id))
        {

            outgoing.addLast(outgoingT);

            //outgoingNodes.add(id);

            return true;
        }

        else
        {

            return false;
        }

    }



    /**
     * Gets the incoming attribute of the Node object
     *
     * @return   The incoming value
     */

    public LinkedList getIncoming()
    {

        return incoming;
    }



    /**
     * Gets the outgoing attribute of the Node object
     *
     * @return   The outgoing value
     */

    public LinkedList getOutgoing()
    {

        return outgoing;
    }



    /**
     * Sets the distance attribute of the Node object
     *
     * @param distance  The new distance value
     */

    public void setDistance(float distance)
    {

        this.distance = distance;

    }



    /**
     * Gets the distance attribute of the Node object
     *
     * @return   The distance value
     */

    public float getDistance()
    {

        return distance;
    }



    /**
     * Gets the name attribute of the Node object
     *
     * @return   The name value
     */

    public String getName()
    {

        return name;
    }



    /**
     * Sets the title attribute of the Node object
     *
     * @param title  The new title value
     */

    public void setTitle(String title)
    {

        this.title = title;

    }



    /**
     * Gets the title attribute of the Node object
     *
     * @return   The title value
     */

    public String getTitle()
    {

        return title;
    }



    /**
     * Gets the nodeRank attribute of the Node object
     *
     * @param idx  Description of the Parameter
     * @return     The nodeRank value
     */

    public float getNodeRank(int idx)
    {

        return nodeRank[idx];
    }



    /**
     * Sets the nodeRank attribute of the Node object
     *
     * @param nodeRank  The new nodeRank value
     * @param idx       The new nodeRank value
     */

    public void setNodeRank(float nodeRank, int idx)
    {

        this.nodeRank[idx] = nodeRank;

    }

}

/**
 * Description of the Class
 *
 * @author    Administrator
 * @created   30. Januar 2002
 */

class Transition
{



    Node from;

    Node to;

    float distance;

    float linkRank[] = new float[2];

    boolean isFrame;



    /**
     * Constructor for the Transition object
     *
     * @param from     Description of the Parameter
     * @param to       Description of the Parameter
     * @param isFrame  Description of the Parameter
     */

    public Transition(Node from, Node to, boolean isFrame)
    {

        LinkedList l = from.getOutgoing();

        Iterator i = l.iterator();

        while (i.hasNext())
        {

            Transition t = (Transition) i.next();

            if (t.getTo() == to)
            {

                return;
                // schon enthalten

            }

        }

        this.from = from;

        this.to = to;

        from.addOutgoing(this);

        to.addIncoming(this);

        this.distance = Integer.MAX_VALUE;

        this.isFrame = isFrame;

        this.linkRank[0] = this.linkRank[1] = 1;

    }



    /**
     * Gets the to attribute of the Transition object
     *
     * @return   The to value
     */

    public Node getTo()
    {

        return to;
    }



    /**
     * Gets the from attribute of the Transition object
     *
     * @return   The from value
     */

    public Node getFrom()
    {

        return from;
    }



    /**
     * Gets the distance attribute of the Transition object
     *
     * @return   The distance value
     */

    public float getDistance()
    {

        return distance;
    }



    /**
     * Sets the distance attribute of the Transition object
     *
     * @param distance  The new distance value
     */

    public void setDistance(float distance)
    {

        this.distance = distance;

    }



    /**
     * Gets the frame attribute of the Transition object
     *
     * @return   The frame value
     */

    public boolean isFrame()
    {

        return isFrame;
    }



    /**
     * Gets the linkRank attribute of the Transition object
     *
     * @param idx  Description of the Parameter
     * @return     The linkRank value
     */

    public float getLinkRank(int idx)
    {

        return linkRank[idx];
    }



    /**
     * Sets the linkRank attribute of the Transition object
     *
     * @param linkRank  The new linkRank value
     * @param idx       The new linkRank value
     */

    public void setLinkRank(float linkRank, int idx)
    {

        this.linkRank[idx] = linkRank;

    }

}

/**
 * Description of the Class
 *
 * @author    Administrator
 * @created   30. Januar 2002
 */

public class DistanceCount
{



    HashMap nodes = new HashMap(100000);

    LinkedList nodesToDo = new LinkedList();

    static int id = 0;



    /**
     * Gets the orCreateNode attribute of the DistanceCount object
     *
     * @param name   Description of the Parameter
     * @param title  Description of the Parameter
     * @return       The orCreateNode value
     */

    Node getOrCreateNode(String name, String title)
    {

        Node node = (Node) nodes.get(name);

        if (node != null)
        {

            if (title != null)
            {

                node.setTitle(title);

            }

            return node;
        }

        else
        {

            node = new Node(id++, name, title);

            nodes.put(name, node);

            return node;
        }

    }



    /**
     * Constructor for the DistanceCount object
     *
     * @param filename         Description of the Parameter
     * @exception IOException  Description of the Exception
     */

    public DistanceCount(String filename)
        throws IOException
    {

        System.out.println("reading file...");

        long t1 = System.currentTimeMillis();

        BufferedReader b = new BufferedReader(new FileReader(filename));

        String line;

        boolean firstNotFound = true;

        Node firstNode = null;

        int lines = 0;

        while ((line = b.readLine()) != null)
        {

            lines++;

            String title = null;

            try
            {

                //StringTokenizer st = new StringTokenizer(line, " ");

                StringTokenizer st = new StringTokenizer(line, "\t");

                String from = st.nextToken();

                if (from.endsWith("/"))
                {

                    from = from.substring(0, from.length() - 1);

                }

                from = from.toLowerCase();

                String to = st.nextToken();

                if (to.endsWith("/"))
                {

                    to = to.substring(0, to.length() - 1);

                }

                to = to.toLowerCase();

                boolean isFrame = (Integer.parseInt(st.nextToken()) == 1);

                if (st.countTokens() > 3)
                {

                    title = "<untitled>";

                    //StringBuffer sb = new StringBuffer();

                    st.nextToken();

                    // result

                    st.nextToken();

                    // Mime Type

                    st.nextToken();

                    // Size

                    /*
                     *  while(st.hasMoreTokens())
                     *  {
                     *  sb.append(st.nextToken()).append(" ");
                     *  }
                     */
                    title = st.nextToken();

                    if (title.length() > 2)
                    {

                        title = title.substring(1, title.length() - 1);

                        int indexOfPara = title.indexOf("\"");

                        if (indexOfPara > -1)
                        {

                            title = title.substring(0, indexOfPara);

                        }

                    }

                }

                Node fromNode = getOrCreateNode(from, null);

                Node toNode = getOrCreateNode(to, title);

                Transition t = new Transition(fromNode, toNode, isFrame);

                /*
                 *  if(firstNotFound && to.equals("http://127.0.0.1"))
                 *  {
                 *  firstNode = toNode;
                 *  firstNotFound = false;
                 *  }
                 */
                if (lines % 10000 == 0)
                {

                    System.out.println("" + lines + " Lines; " + nodes.size() + " nodes");

                }

            }

            catch (NoSuchElementException e)
            {

                System.out.println("Malformed line " + lines + ": field number doesn't match");

            }

            catch (NumberFormatException e)
            {

                System.out.println("Malformed line " + lines + ": NumberFormat wrong");

            }

        }

        System.out.println("finished; b" + lines + " Lines; " + nodes.size() + " nodes");

        long t2 = System.currentTimeMillis();

        System.out.println("" + (t2 - t1) + " ms");

        /*
         *  if(firstNotFound)
         *  {
         *  System.out.println("Couldn't find start page");
         *  System.exit(-1);
         *  }
         */
    }



    /**
     * Description of the Method
     *
     * @param firstNode  Description of the Parameter
     */

    public void calculateShortestDistance(Node firstNode)
    {

        clearDistances();

        firstNode.setDistance(0);

        nodesToDo.addLast(firstNode);

        int calculations = 0;

        while (!nodesToDo.isEmpty())
        {

            if (calculations % 100000 == 0)
            {

                System.out.println("Calculations: " + calculations + "; nodes to go: " + nodesToDo.size() + " total Mem: " + Runtime.getRuntime().totalMemory() + "; free mem: " + Runtime.getRuntime().freeMemory());

            }

            calculations++;

            Node act = (Node) nodesToDo.removeFirst();

            LinkedList outTrans = act.getOutgoing();

            float distance = act.getDistance();

            Iterator i = outTrans.iterator();

            //distance++;

            while (i.hasNext())
            {

                Transition t = (Transition) i.next();

                float transDistance = t.getDistance();

                /*
                 *  if (t.isFrame())
                 *  {
                 *  System.out.println("Frame from " + t.from.getName() + " to " + t.to.getName());
                 *  }
                 */
                float newDistance = distance + (t.isFrame() ? 0.25f : 1f);

                if (transDistance > newDistance)
                {

                    t.setDistance(newDistance);

                    Node to = t.getTo();

                    if (to.distance > distance)
                    {

                        to.setDistance(newDistance);

                        nodesToDo.addLast(to);

                    }

                }

            }

            /*
             *  if(looksGood)
             *  {
             *  System.out.println("Node " + act.id + " looks good");
             *  }
             */
        }

        System.out.println("Calculations: " + calculations);

    }



    /**
     * Description of the Method
     */
    public void clearDistances()
    {

        System.out.println("Clearing distance data...");

        Iterator it = nodes.values().iterator();

        int nr = 0;

        while (it.hasNext())
        {

            Node n = (Node) it.next();

            nr++;

            n.setDistance(Float.MAX_VALUE);

        }

        System.out.println("cleared " + nr + " nodes. done");

    }


    /**
     * Description of the Method
     *
     * @param nodeFrom  Description of the Parameter
     * @param nodeTo    Description of the Parameter
     */

    public void printDistance(String nodeFrom, String nodeTo)
    {

        Node firstNode = (Node) nodes.get(nodeFrom);

        if (firstNode == null)
        {

            System.out.println("FROM node not found");

            return;
        }

        Node toNode = (Node) nodes.get(nodeTo);

        if (toNode == null)
        {

            System.out.println("TO node not found");

            return;
        }

        //System.out.println("resetting node distance...");

        //clearDistances();

        System.out.println("calculating...");

        calculateShortestDistance(firstNode);

        //t1 = System.currentTimeMillis();

        //System.out.println("" + (t1-t2) + " ms");


        System.out.println("\nSorting...");

        /*
         *  Collection nodeCollection = nodes.values();
         *  Object[] nodeArray = nodeCollection.toArray();
         *  Arrays.sort(nodeArray);
         *  t2 = System.currentTimeMillis();
         *  System.out.println("" + (t2-t1) + " ms");
         *  int from = 0;
         *  int to = 1;
         */
        /*
         *  /calculate page Rank
         *  for(int i = 0; i< 1; i++)
         *  {
         *  from = i%2;
         *  to = (i+1) % 2;
         *  for(int j = 0; j<nodeArray.length; j++)
         *  {
         *  Node act = (Node)nodeArray[j];
         *  LinkedList inc = act.getIncoming();
         *  float pageRank = 0;
         *  Iterator it = inc.iterator();
         *  while(it.hasNext())
         *  {
         *  Transition t = (Transition)it.next();
         *  pageRank += t.getLinkRank(from);
         *  }
         *  act.setNodeRank(pageRank, to);
         *  LinkedList out = act.getOutgoing();
         *  int size = out.size();
         *  if(size > 0)
         *  {
         *  float linkRank = pageRank / size;
         *  it = out.iterator();
         *  while(it.hasNext())
         *  {
         *  Transition t = (Transition)it.next();
         *  t.setLinkRank(linkRank, to);
         *  }
         *  }
         *  }
         *  }
         */
        /*
         *  System.out.println("\nLink Count:");
         *  for(int i=0; i<10; i++)
         *  {
         *  Node n = ((Node)nodeArray[i]);
         *  System.out.println("Node " + n.name + ": " + n.getIncoming().size() + "; pageRank: " + n.getNodeRank(to));
         *  }
         *  for(int i=nodeArray.length/2; i<nodeArray.length/2+10; i++)
         *  {
         *  Node n = ((Node)nodeArray[i]);
         *  System.out.println("Node " + n.name + ": " + n.getIncoming().size() + "; pageRank: " + n.getNodeRank(to));
         *  }
         *  for(int i=nodeArray.length-10; i<nodeArray.length; i++)
         *  {
         *  Node n = ((Node)nodeArray[i]);
         *  System.out.println("Node " + n.name + ": " + n.getIncoming().size() + "; pageRank: " + n.getNodeRank(to));
         *  }
         *  Node.sortType = to;
         *  Arrays.sort(nodeArray);
         *  System.out.println("\nPageRank Count:");
         *  for(int i=0; i<10; i++)
         *  {
         *  Node n = ((Node)nodeArray[i]);
         *  System.out.println("Node " + n.name + ": " + n.getIncoming().size() + "; pageRank: " + n.getNodeRank(to));
         *  }
         *  for(int i=nodeArray.length/2; i<nodeArray.length/2+10; i++)
         *  {
         *  Node n = ((Node)nodeArray[i]);
         *  System.out.println("Node " + n.name + ": " + n.getIncoming().size() + "; pageRank: " + n.getNodeRank(to));
         *  }
         *  for(int i=nodeArray.length-10; i<nodeArray.length; i++)
         *  {
         *  Node n = ((Node)nodeArray[i]);
         *  System.out.println("Node " + n.name + ": " + n.getIncoming().size() + "; pageRank: " + n.getNodeRank(to));
         *  }
         *  System.out.println("\nStats...");
         *  float distanceAccumulated=0;
         *  float distanceMax = 0;
         *  int notCounted = 0;
         *  for(int j = 0; j<nodeArray.length; j++)
         *  {
         *  Node n = (Node)nodeArray[j];
         *  if(n.distance != Integer.MAX_VALUE)
         *  {
         *  distanceAccumulated += n.distance;
         *  distanceMax = Math.max(distanceMax, n.distance);
         *  }
         *  else
         *  {
         *  notCounted++;
         *  }
         *  }
         *  System.out.println("Mean Distance:          " + ((double)distanceAccumulated)/nodeArray.length);
         *  System.out.println("Max Distance:           " + (distanceMax));
         *  System.out.println("Not reachable nodes(?): " + notCounted);
         *  System.out.println("Referer Median:         " + ((Node)(nodeArray[Math.round(nodeArray.length/2)])).incoming.size());
         *  System.out.println("\nSamples:");
         */
        printShortestRoute(toNode, 0, 0);

    }



    /**
     * Description of the Method
     */

    public void printRandomRoute()
    {

        Random r = new java.util.Random(System.currentTimeMillis());

        Collection nodeColl = nodes.values();

        Object[] nodeArray = (Object[]) nodeColl.toArray();

        int rnd = (int) (r.nextDouble() * nodeArray.length);

        Node from = (Node) nodeArray[rnd];

        rnd = (int) (r.nextDouble() * nodeArray.length);

        Node to = (Node) nodeArray[rnd];

        System.out.println("Calculating distance...");

        calculateShortestDistance(from);

        System.out.println("printing...");

        printShortestRoute(to, 0, 0);

    }



    /**
     * Description of the Method
     *
     * @param n          Description of the Parameter
     * @param indent     Description of the Parameter
     * @param linkCount  Description of the Parameter
     */

    public void printShortestRoute(Node n, int indent, int linkCount)
    {

        String spaces = "                                                            ".substring(0, indent);

        if (n.getIncoming().isEmpty())
        {

            System.out.println(spaces + "<start>");

        }

        else
        {

            System.out.print(spaces + "+- " + n.name + "    (" + (n.getTitle() != null ? n.getTitle().substring(0, Math.min(n.getTitle().length(), 25)) : "") + "\")     D:" + n.distance + "; L:" + n.getIncoming().size() + "; C:" + linkCount);

            Iterator it = n.getIncoming().iterator();

            float dist = n.distance;

            if (dist > 10000000)
            {

                System.out.println(spaces + "\n--no link--");

                return;
            }

            while (it.hasNext())
            {

                Transition t = (Transition) it.next();

                if (t.distance <= dist)
                {

                    if (t.isFrame())
                    {

                        System.out.println(" **F** ->");

                    }

                    else
                    {

                        System.out.println(" -> ");

                    }

                    printShortestRoute(t.getFrom(), indent + 1, linkCount + n.getIncoming().size());

                }

            }

        }

        //System.out.println("");

    }



    /**
     * this class reads in store.log, constructs a graph of the crawled web and
     * is able to perform a breadth-first search for the shortest distance
     * between two nodes<br>
     * Note: this is experimental stuff. get into the source code to see how it
     * works
     *
     * @param args  args[0] must point to the store.log file
     */

    public static void main(String[] args)
    {

        // Syntax: DistanceCount <store.log>

        try
        {

            DistanceCount dc = new DistanceCount(args[0]);

            boolean running = true;

            BufferedReader in = new BufferedReader(new InputStreamReader(System.in), 400);

            while (running)
            {

                System.out.print("\n\nCommand (? for help) > ");

                String newL;

                String input = "";

                //while((newL = in.readLine()) != null)

                //{

                input = in.readLine();

                StringTokenizer st = new StringTokenizer(input, " ");

                String command;

                boolean printHelp = false;

                if (!st.hasMoreTokens())
                {

                    printHelp = true;

                    command = "?";

                }

                else
                {

                    command = st.nextToken();

                }

                try
                {

                    if ("?".equals(command))
                    {

                        printHelp = true;

                    }

                    else if ("d".equals(command))
                    {

                        String from = st.nextToken();

                        String to = st.nextToken();

                        dc.printDistance(from, to);

                    }

                    else if ("q".equals(command))
                    {

                        running = false;

                    }

                    else if ("r".equals(command))
                    {

                        dc.printRandomRoute();

                    }

                    else
                    {

                        System.out.println("unknown command '" + command + "'");

                    }

                }

                catch (java.util.NoSuchElementException e)
                {

                    System.out.println("Syntax error");

                    e.printStackTrace();

                    printHelp = true;

                }

                catch (Exception e)
                {

                    e.printStackTrace();

                }

                if (printHelp)
                {

                    System.out.println("\nSyntax\n" +
                            "?   print this help message\n" +
                            "d <page1> <page2>   print shortest route from page1 to page2\n" +
                            "r                   print random walk\n" +
                            "q                   quit");

                }

            }

        }

        catch (IOException e)
        {

            e.printStackTrace();

        }

        catch (ArrayIndexOutOfBoundsException e)
        {

            System.out.println("Syntax: java ... store.log");

        }

    }

}

