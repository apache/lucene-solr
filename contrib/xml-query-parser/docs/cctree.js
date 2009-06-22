/* This code is based on the one originally provided by
   Geir Landrö in his dTree 2.05 package. You can get it
   at : www.destroydrop.com/javascript/tree/.
   
   Therefore, the DTDDoc team considers that this code is 
   Copyright (c) 2002-2003 Geir Landrö. Since the original
   author didn't clearly forbids copies of this part, we
   assume we're not doing anything wrong in porviding it
   to you, in a modified or non-modified form.
*/

/*   
   Geir Landrö : Orignal version, for dTree.
   
   Michael Koehrsen (10/2004) : Original modification to
      allow DTDDoc to use this.
   
   Stefan Champailler (10/2004) : Make sure that the first
      level of the tree is not shown (aesthetic stuff).
*/

//----------------------------------------------------------------
// CCTree
// Implements a DHTML tree with the following features:
// - Supports a general directed graph as the underlying model.
// - Supports a concept of an "always open" node.
//----------------------------------------------------------------

// Private class _CCTreeModelNode
function _CCTreeModelNode(id,label,link,alwaysOpen,initiallyOpen)
{
    this.id = id;
    this.label = label;
    this.link = link;
    this.alwaysOpen = alwaysOpen;
    this.initiallyOpen = initiallyOpen;

    // children and childLabels are parallel arrays.
    // By default, childLabels[i] == children[i].label,
    // but the link operation may optionally specify
    // a child label that is specific to that parent-child 
    // relationship.  
    this.children = new Array();
    this.childLabels = new Array();
}

_CCTreeModelNode.prototype.addChild = function(child,childLabel)
{
    this.children.push(child);
    if (childLabel) this.childLabels.push(childLabel);
    else this.childLabels.push(child.label);
}

// Private class _CCDisplayNode
function _CCDisplayNode(modelNode,parentNode,treeId,label)
{
    this.modelNode = modelNode;
    this.parentNode = parentNode;
    this.treeId = treeId;

    if (label) this.label = label;
    else this.label = modelNode.label;

    this.isLastChild = false;

    if (this.parentNode) {
        this.id = this.parentNode.id + ":" + this.modelNode.id;

        /* Stefan Champailler : This little fix is clever ! Let's check the
           following tree:
           
           alpha (1) -+-> beta (69)
                      \-> beta (69).
                      
           This describes a tree with three nodes. Two of them are links
           (beta). In that case, both the "beta" node have an id of "1:69".
           So if one calls openNode on any of them, what happens is that only
           one actually gets opened. To prevent that, I change the id of the
           node on the fly to make sure there are only unique id's. 
           
           Please note this code is not very efficient (and yes, the right
           number of slash will be added :)) */
           
        for( var k=0; k<parentNode.children.length; k++) {
            if( parentNode.children[k].id == this.id)
                this.id = this.id + "/";
        }
        
        /* end of fix */
    }
    else this.id = this.modelNode.id;

    CCTree.trees[this.treeId].allDisplayNodes[this.id] = this;

    this.isOpen = this.modelNode.alwaysOpen || this.modelNode.initiallyOpen;
    if (this.isOpen)
    {
        this.constructChildren();
    }
}

_CCDisplayNode.prototype.toInnerHTML = function()
{
    var indent = "";

    if (this.isOpen && !this.children) this.constructChildren();

    if (this.isLastChild)
    {
        if (this.modelNode.alwaysOpen)
            indent = this.imageTag("joinbottom.gif");
        else if (this.isOpen)
            indent = this.imageTag("minusbottom.gif","close")
        else if (this.modelNode.children.length)
            indent = this.imageTag("plusbottom.gif","open");
        else
            indent = this.imageTag("joinbottom.gif");
    }
    else
    {
        if (this.modelNode.alwaysOpen)
            indent = this.imageTag("join.gif");
        else if (this.isOpen)
            indent = this.imageTag("minus.gif","close");
        else if (this.modelNode.children.length)
            indent = this.imageTag("plus.gif","open");
        else
            indent = this.imageTag("join.gif");
    }

    /* Construct a horizontal line of the tree */

    var currAnc = this.parentNode;
    while (currAnc)
    {
        if (currAnc.isLastChild)
            indent = this.imageTag("empty.gif") + indent;
        else
            indent = this.imageTag("line.gif") + indent;
        currAnc = currAnc.parentNode;
    }

    var result = indent + this.nodeContent();

    /* Recurse deeper in the tree */
    
    if (this.isOpen && this.children)
    {
        var ix;
        for (ix in this.children)
        {
            result += this.children[ix];
        }
    }

    return result;
}

_CCDisplayNode.prototype.toString = function()
{
    return "<div class='cctree-node' id='" + this.divId() + "'>" + this.toInnerHTML() + "</div>";
}

_CCDisplayNode.prototype.constructChildren = function()
{
    if (this.modelNode.children.length > 0)
    {
        this.children = new Array();
        var ix;
        for (ix in this.modelNode.children)
        {
            this.children.push(new _CCDisplayNode(this.modelNode.children[ix],
                                                  this,
                                                  this.treeId,
                                                  this.modelNode.childLabels[ix]));
        }
        this.children[this.children.length-1].isLastChild = true;
    }
}

_CCDisplayNode.prototype.imageTag = function(imgName,action)
{
    var href =  null;

    if (action == "open") href="CCTree.trees[" + this.treeId + "].openNode('" + this.id + "')";
    if (action == "close") href="CCTree.trees[" + this.treeId + "].closeNode('" + this.id +  "')";

    if (href) return "<a href=\"javascript:" + href + "\"><img src='img/" + imgName + "' border='0'></a>";
    else return "<img src='img/" + imgName + "'>";
}

_CCDisplayNode.prototype.divId = function()
{
    return "CCTree_" + this.treeId + "_" + this.id;
}

_CCDisplayNode.prototype.nodeContent = function()
{
    var target = "";

    if (CCTree.trees[this.treeId].linkTarget) target = " target='" + CCTree.trees[this.treeId].linkTarget + "'";

    if (this.modelNode.link)
        return "<a href='" + this.modelNode.link + "'" + target + ">" + this.label + "</a>";
    else return this.label;
}

_CCDisplayNode.prototype.open = function()
{
    this.isOpen = true;
    // document.all is known to work on IE but not on Konqueror or Mozilla.
    // So I've changed it to something more portable.
    
    //document.all[this.divId()].innerHTML = this.toInnerHTML();
    document.getElementById(this.divId()).innerHTML = this.toInnerHTML();
}

_CCDisplayNode.prototype.close = function()
{
    this.isOpen = false;
    //document.all[this.divId()].innerHTML = this.toInnerHTML();
    document.getElementById(this.divId()).innerHTML = this.toInnerHTML();
}

// Public class CCTree
CCTree = function(linkTarget)
{
    // may have multiple roots:
    this.rootModelNodes = new Array();
    this.allModelNodes = new Array();
    this.treeId = CCTree.trees.length;
    this.allDisplayNodes = new Array(); // indexed by id
    this.linkTarget = linkTarget;
    CCTree.trees.push(this);
}

// static variables
CCTree.trees = new Array();

CCTree.prototype.addRootNode = function (id,label,link,alwaysOpen,initiallyOpen)
{
    this.rootModelNodes[id] = this.addNode(id,label,link,alwaysOpen,initiallyOpen);
}

CCTree.prototype.addNode = function(id,label,link,alwaysOpen,initiallyOpen)
{
    var newNode = new _CCTreeModelNode(id,label,link,alwaysOpen,initiallyOpen);
    this.allModelNodes[id] = newNode;
    return newNode;
}

CCTree.prototype.linkNodes = function(parentId,childId,childLabel)
{
    this.allModelNodes[parentId].addChild(this.allModelNodes[childId],childLabel);
}

CCTree.prototype.constructDisplayNodes = function()
{
    this.rootDisplayNodes = new Array();
    var ix;
    for (ix in this.rootModelNodes)
    {
        this.rootDisplayNodes.push(new _CCDisplayNode(this.rootModelNodes[ix],null,this.treeId));
    }
    this.rootDisplayNodes[this.rootDisplayNodes.length-1].isLastChild = true;
}

CCTree.prototype.openNode = function(displayNodeId)
{
    this.allDisplayNodes[displayNodeId].open();
}

CCTree.prototype.closeNode = function(displayNodeId) 
{
    this.allDisplayNodes[displayNodeId].close();
}

CCTree.prototype.toString = function()
{
    this.constructDisplayNodes();

    var ix;
    var result = "";

    for (ix in this.rootDisplayNodes)
    {
        result += this.rootDisplayNodes[ix].toString();
    }

    return result;
}
