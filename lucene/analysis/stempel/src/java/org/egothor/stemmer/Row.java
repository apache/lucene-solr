/*
                    Egothor Software License version 1.00
                    Copyright (C) 1997-2004 Leo Galambos.
                 Copyright (C) 2002-2004 "Egothor developers"
                      on behalf of the Egothor Project.
                             All rights reserved.

   This  software  is  copyrighted  by  the "Egothor developers". If this
   license applies to a single file or document, the "Egothor developers"
   are the people or entities mentioned as copyright holders in that file
   or  document.  If  this  license  applies  to the Egothor project as a
   whole,  the  copyright holders are the people or entities mentioned in
   the  file CREDITS. This file can be found in the same location as this
   license in the distribution.

   Redistribution  and  use  in  source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:
    1. Redistributions  of  source  code  must retain the above copyright
       notice, the list of contributors, this list of conditions, and the
       following disclaimer.
    2. Redistributions  in binary form must reproduce the above copyright
       notice, the list of contributors, this list of conditions, and the
       disclaimer  that  follows  these  conditions  in the documentation
       and/or other materials provided with the distribution.
    3. The name "Egothor" must not be used to endorse or promote products
       derived  from  this software without prior written permission. For
       written permission, please contact Leo.G@seznam.cz
    4. Products  derived  from this software may not be called "Egothor",
       nor  may  "Egothor"  appear  in  their name, without prior written
       permission from Leo.G@seznam.cz.

   In addition, we request that you include in the end-user documentation
   provided  with  the  redistribution  and/or  in the software itself an
   acknowledgement equivalent to the following:
   "This product includes software developed by the Egothor Project.
    http://egothor.sf.net/"

   THIS  SOFTWARE  IS  PROVIDED  ``AS  IS''  AND ANY EXPRESSED OR IMPLIED
   WARRANTIES,  INCLUDING,  BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
   MERCHANTABILITY  AND  FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
   IN  NO  EVENT  SHALL THE EGOTHOR PROJECT OR ITS CONTRIBUTORS BE LIABLE
   FOR   ANY   DIRECT,   INDIRECT,  INCIDENTAL,  SPECIAL,  EXEMPLARY,  OR
   CONSEQUENTIAL  DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
   SUBSTITUTE  GOODS  OR  SERVICES;  LOSS  OF  USE,  DATA, OR PROFITS; OR
   BUSINESS  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
   WHETHER  IN  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
   OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
   IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   This  software  consists  of  voluntary  contributions  made  by  many
   individuals  on  behalf  of  the  Egothor  Project  and was originally
   created by Leo Galambos (Leo.G@seznam.cz).
 */
package org.egothor.stemmer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * The Row class represents a row in a matrix representation of a trie.
 */
public class Row {
  TreeMap<Character,Cell> cells = new TreeMap<>();
  int uniformCnt = 0;
  int uniformSkip = 0;
  
  /**
   * Construct a Row object from input carried in via the given input stream.
   * 
   * @param is the input stream
   * @exception IOException if an I/O error occurs
   */
  public Row(DataInput is) throws IOException {
    for (int i = is.readInt(); i > 0; i--) {
      char ch = is.readChar();
      Cell c = new Cell();
      c.cmd = is.readInt();
      c.cnt = is.readInt();
      c.ref = is.readInt();
      c.skip = is.readInt();
      cells.put(ch, c);
    }
  }
  
  /**
   * The default constructor for the Row object.
   */
  public Row() {}
  
  /**
   * Construct a Row using the cells of the given Row.
   * 
   * @param old the Row to copy
   */
  public Row(Row old) {
    cells = old.cells;
  }
  
  /**
   * Set the command in the Cell of the given Character to the given integer.
   * 
   * @param way the Character defining the Cell
   * @param cmd the new command
   */
  public void setCmd(Character way, int cmd) {
    Cell c = at(way);
    if (c == null) {
      c = new Cell();
      c.cmd = cmd;
      cells.put(way, c);
    } else {
      c.cmd = cmd;
    }
    c.cnt = (cmd >= 0) ? 1 : 0;
  }
  
  /**
   * Set the reference to the next row in the Cell of the given Character to the
   * given integer.
   * 
   * @param way the Character defining the Cell
   * @param ref The new ref value
   */
  public void setRef(Character way, int ref) {
    Cell c = at(way);
    if (c == null) {
      c = new Cell();
      c.ref = ref;
      cells.put(way, c);
    } else {
      c.ref = ref;
    }
  }
  
  /**
   * Return the number of cells in use.
   * 
   * @return the number of cells in use
   */
  public int getCells() {
    Iterator<Character> i = cells.keySet().iterator();
    int size = 0;
    for (; i.hasNext();) {
      Character c = i.next();
      Cell e = at(c);
      if (e.cmd >= 0 || e.ref >= 0) {
        size++;
      }
    }
    return size;
  }
  
  /**
   * Return the number of references (how many transitions) to other rows.
   * 
   * @return the number of references
   */
  public int getCellsPnt() {
    Iterator<Character> i = cells.keySet().iterator();
    int size = 0;
    for (; i.hasNext();) {
      Character c = i.next();
      Cell e = at(c);
      if (e.ref >= 0) {
        size++;
      }
    }
    return size;
  }
  
  /**
   * Return the number of patch commands saved in this Row.
   * 
   * @return the number of patch commands
   */
  public int getCellsVal() {
    Iterator<Character> i = cells.keySet().iterator();
    int size = 0;
    for (; i.hasNext();) {
      Character c = i.next();
      Cell e = at(c);
      if (e.cmd >= 0) {
        size++;
      }
    }
    return size;
  }
  
  /**
   * Return the command in the Cell associated with the given Character.
   * 
   * @param way the Character associated with the Cell holding the desired
   *          command
   * @return the command
   */
  public int getCmd(Character way) {
    Cell c = at(way);
    return (c == null) ? -1 : c.cmd;
  }
  
  /**
   * Return the number of patch commands were in the Cell associated with the
   * given Character before the Trie containing this Row was reduced.
   * 
   * @param way the Character associated with the desired Cell
   * @return the number of patch commands before reduction
   */
  public int getCnt(Character way) {
    Cell c = at(way);
    return (c == null) ? -1 : c.cnt;
  }
  
  /**
   * Return the reference to the next Row in the Cell associated with the given
   * Character.
   * 
   * @param way the Character associated with the desired Cell
   * @return the reference, or -1 if the Cell is <tt>null</tt>
   */
  public int getRef(Character way) {
    Cell c = at(way);
    return (c == null) ? -1 : c.ref;
  }
  
  /**
   * Write the contents of this Row to the given output stream.
   * 
   * @param os the output stream
   * @exception IOException if an I/O error occurs
   */
  public void store(DataOutput os) throws IOException {
    os.writeInt(cells.size());
    Iterator<Character> i = cells.keySet().iterator();
    for (; i.hasNext();) {
      Character c = i.next();
      Cell e = at(c);
      if (e.cmd < 0 && e.ref < 0) {
        continue;
      }
      
      os.writeChar(c.charValue());
      os.writeInt(e.cmd);
      os.writeInt(e.cnt);
      os.writeInt(e.ref);
      os.writeInt(e.skip);
    }
  }
  
  /**
   * Return the number of identical Cells (containing patch commands) in this
   * Row.
   * 
   * @param eqSkip when set to <tt>false</tt> the removed patch commands are
   *          considered
   * @return the number of identical Cells, or -1 if there are (at least) two
   *         different cells
   */
  public int uniformCmd(boolean eqSkip) {
    Iterator<Cell> i = cells.values().iterator();
    int ret = -1;
    uniformCnt = 1;
    uniformSkip = 0;
    for (; i.hasNext();) {
      Cell c = i.next();
      if (c.ref >= 0) {
        return -1;
      }
      if (c.cmd >= 0) {
        if (ret < 0) {
          ret = c.cmd;
          uniformSkip = c.skip;
        } else if (ret == c.cmd) {
          if (eqSkip) {
            if (uniformSkip == c.skip) {
              uniformCnt++;
            } else {
              return -1;
            }
          } else {
            uniformCnt++;
          }
        } else {
          return -1;
        }
      }
    }
    return ret;
  }
  
  /**
   * Write the contents of this Row to the printstream.
   */
  public void print(PrintStream out) {
    for (Iterator<Character> i = cells.keySet().iterator(); i.hasNext();) {
      Character ch = i.next();
      Cell c = at(ch);
      out.print("[" + ch + ":" + c + "]");
    }
    out.println();
  }
  
  Cell at(Character index) {
    return cells.get(index);
  }
}
