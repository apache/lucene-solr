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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * The Optimizer class is a Trie that will be reduced (have empty rows removed).
 * <p>
 * The reduction will be made by joining two rows where the first is a subset of
 * the second.
 */
public class Optimizer extends Reduce {
  /**
   * Constructor for the Optimizer object.
   */
  public Optimizer() {}
  
  /**
   * Optimize (remove empty rows) from the given Trie and return the resulting
   * Trie.
   * 
   * @param orig the Trie to consolidate
   * @return the newly consolidated Trie
   */
  @Override
  public Trie optimize(Trie orig) {
    List<CharSequence> cmds = orig.cmds;
    List<Row> rows = new ArrayList<>();
    List<Row> orows = orig.rows;
    int remap[] = new int[orows.size()];
    
    for (int j = orows.size() - 1; j >= 0; j--) {
      Row now = new Remap(orows.get(j), remap);
      boolean merged = false;
      
      for (int i = 0; i < rows.size(); i++) {
        Row q = merge(now, rows.get(i));
        if (q != null) {
          rows.set(i, q);
          merged = true;
          remap[j] = i;
          break;
        }
      }
      
      if (merged == false) {
        remap[j] = rows.size();
        rows.add(now);
      }
    }
    
    int root = remap[orig.root];
    Arrays.fill(remap, -1);
    rows = removeGaps(root, rows, new ArrayList<Row>(), remap);
    
    return new Trie(orig.forward, remap[root], cmds, rows);
  }
  
  /**
   * Merge the given rows and return the resulting Row.
   * 
   * @param master the master Row
   * @param existing the existing Row
   * @return the resulting Row, or <tt>null</tt> if the operation cannot be
   *         realized
   */
  public Row merge(Row master, Row existing) {
    Iterator<Character> i = master.cells.keySet().iterator();
    Row n = new Row();
    for (; i.hasNext();) {
      Character ch = i.next();
      // XXX also must handle Cnt and Skip !!
      Cell a = master.cells.get(ch);
      Cell b = existing.cells.get(ch);
      
      Cell s = (b == null) ? new Cell(a) : merge(a, b);
      if (s == null) {
        return null;
      }
      n.cells.put(ch, s);
    }
    i = existing.cells.keySet().iterator();
    for (; i.hasNext();) {
      Character ch = i.next();
      if (master.at(ch) != null) {
        continue;
      }
      n.cells.put(ch, existing.at(ch));
    }
    return n;
  }
  
  /**
   * Merge the given Cells and return the resulting Cell.
   * 
   * @param m the master Cell
   * @param e the existing Cell
   * @return the resulting Cell, or <tt>null</tt> if the operation cannot be
   *         realized
   */
  public Cell merge(Cell m, Cell e) {
    Cell n = new Cell();
    
    if (m.skip != e.skip) {
      return null;
    }
    
    if (m.cmd >= 0) {
      if (e.cmd >= 0) {
        if (m.cmd == e.cmd) {
          n.cmd = m.cmd;
        } else {
          return null;
        }
      } else {
        n.cmd = m.cmd;
      }
    } else {
      n.cmd = e.cmd;
    }
    if (m.ref >= 0) {
      if (e.ref >= 0) {
        if (m.ref == e.ref) {
          if (m.skip == e.skip) {
            n.ref = m.ref;
          } else {
            return null;
          }
        } else {
          return null;
        }
      } else {
        n.ref = m.ref;
      }
    } else {
      n.ref = e.ref;
    }
    n.cnt = m.cnt + e.cnt;
    n.skip = m.skip;
    return n;
  }
}
