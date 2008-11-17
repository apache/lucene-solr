if (typeof JSDOC == "undefined") JSDOC = {};

/** @constructor */
JSDOC.SymbolSet = function() {
	this.init();
}

JSDOC.SymbolSet.prototype.init = function() {
	this._index = {};
}

JSDOC.SymbolSet.prototype.keys = function() {
	var found = [];
	for (var p in this._index) {
		found.push(p);
	}
	return found;
}

JSDOC.SymbolSet.prototype.hasSymbol = function(alias) {
	return this._index.hasOwnProperty(alias);
}

JSDOC.SymbolSet.prototype.addSymbol = function(symbol) {
	if (this.hasSymbol(symbol.alias)) {
		LOG.warn("Overwriting symbol documentation for: "+symbol.alias + ".");
	}
	this._index[symbol.alias] = symbol;
}

JSDOC.SymbolSet.prototype.getSymbol = function(alias) {
	if (this.hasSymbol(alias)) return this._index[alias];
}

JSDOC.SymbolSet.prototype.getSymbolByName = function(name) {
	for (var p in this._index) {
		var symbol = this.getSymbol(p);
		if (symbol.name == name) return symbol;
	}
}

JSDOC.SymbolSet.prototype.toArray = function() {
	var found = [];
	for (var p in this._index) {
		found.push(this._index[p]);
	}
	return found;
}

JSDOC.SymbolSet.prototype.deleteSymbol = function(alias) {
	if (!this.hasSymbol(alias)) return;
	delete this._index[alias];
}

JSDOC.SymbolSet.prototype.renameSymbol = function(oldName, newName) {
	// todo: should check if oldname or newname already exist
	this._index[newName] = this._index[oldName];
	this.deleteSymbol(oldName);
	this._index[newName].alias = newName;
	return newName;
}

JSDOC.SymbolSet.prototype.relate = function() {
	this.resolveBorrows();
	this.resolveMemberOf();
	this.resolveAugments();
}

JSDOC.SymbolSet.prototype.resolveBorrows = function() {
	for (p in this._index) {
		var symbol = this._index[p];
		if (symbol.is("FILE") || symbol.is("GLOBAL")) continue;
		
		var borrows = symbol.inherits;
		for (var i = 0; i < borrows.length; i++) {
			var borrowed = this.getSymbol(borrows[i].alias);
			if (!borrowed) {
				LOG.warn("Can't borrow undocumented "+borrows[i].alias+".");
				continue;
			}
			
			var borrowAsName = borrows[i].as;
			var borrowAsAlias = borrowAsName;
			if (!borrowAsName) {
				LOG.warn("Malformed @borrow, 'as' is required.");
				continue;
			}
			
			if (borrowAsName.length > symbol.alias.length && borrowAsName.indexOf(symbol.alias) == 0) {
				borrowAsName = borrowAsName.replace(borrowed.alias, "")
			}
			else {
				var joiner = "";
				if (borrowAsName.charAt(0) != "#") joiner = ".";
				borrowAsAlias = borrowed.alias + joiner + borrowAsName;
			}
			
			borrowAsName = borrowAsName.replace(/^[#.]/, "");
					
			if (this.hasSymbol(borrowAsAlias)) continue;

			var clone = borrowed.clone();
			clone.name = borrowAsName;
			clone.alias = borrowAsAlias;
			this.addSymbol(clone);
		}
	}
}

JSDOC.SymbolSet.prototype.resolveMemberOf = function() {
	for (var p in this._index) {
		var symbol = this.getSymbol(p);
		
		if (symbol.is("FILE") || symbol.is("GLOBAL")) continue;
		
		// the memberOf value was provided in the @memberOf tag
		else if (symbol.memberOf) {
			var parts = symbol.alias.match(new RegExp("^("+symbol.memberOf+"[.#-])(.+)$"));
			
			// like foo.bar is a memberOf foo
			if (parts) {
				symbol.memberOf = parts[1];
				symbol.name = parts[2];
			}
			// like bar is a memberOf foo
			else {
				var joiner = symbol.memberOf.charAt(symbol.memberOf.length-1);
				if (!/[.#-]/.test(joiner)) symbol.memberOf += ".";
				
				this.renameSymbol(p, symbol.memberOf + symbol.name);
			}
		}
		// the memberOf must be calculated
		else {
			var parts = symbol.alias.match(/^(.*[.#-])([^.#-]+)$/);
			if (parts) {
				symbol.memberOf = parts[1];
				symbol.name = parts[2];				
			}
		}

		// set isStatic, isInner
		if (symbol.memberOf) {
			switch (symbol.memberOf.charAt(symbol.memberOf.length-1)) {
				case '#' :
					symbol.isStatic = false;
					symbol.isInner = false;
				break;
				case '.' :
					symbol.isStatic = true;
					symbol.isInner = false;
				break;
				case '-' :
					symbol.isStatic = false;
					symbol.isInner = true;
				break;
			}
		}
		
		// unowned methods and fields belong to the global object
		if (!symbol.is("CONSTRUCTOR") && !symbol.isNamespace && symbol.memberOf == "") {
			symbol.memberOf = "_global_";
		}
		
		// clean up
		if (symbol.memberOf.match(/[.#-]$/)) {
			symbol.memberOf = symbol.memberOf.substr(0, symbol.memberOf.length-1);
		}
		
		// add to parent's methods or properties list
		if (symbol.memberOf) {
			var container = this.getSymbol(symbol.memberOf);
			if (!container) {
				if (JSDOC.Lang.isBuiltin(symbol.memberOf)) container = JSDOC.Parser.addBuiltin(symbol.memberOf);
				else {
					LOG.warn("Can't document "+symbol.name +" as a member of undocumented symbol "+symbol.memberOf+".");
				}
			}
			
			if (container) container.addMember(symbol);
		}
	}
}

JSDOC.SymbolSet.prototype.resolveAugments = function() {
	for (var p in this._index) {
		var symbol = this.getSymbol(p);
		
		if (symbol.alias == "_global_" || symbol.is("FILE")) continue;
		JSDOC.SymbolSet.prototype.walk.apply(this, [symbol]);
	}
}

JSDOC.SymbolSet.prototype.walk = function(symbol) {
	var augments = symbol.augments;
	for(var i = 0; i < augments.length; i++) {
		var contributer = this.getSymbol(augments[i]);
		if (contributer) {
			if (contributer.augments.length) {
				JSDOC.SymbolSet.prototype.walk.apply(this, [contributer]);
			}
			
			symbol.inheritsFrom.push(contributer.alias);
			if (!isUnique(symbol.inheritsFrom)) {
				//LOG.warn("Can't resolve augments: Circular reference: "+symbol.alias+" inherits from "+contributer.alias+" more than once.");
			}
			else {
				var cmethods = contributer.methods;
				var cproperties = contributer.properties;
				
				for (var ci = 0, cl = cmethods.length; ci < cl; ci++)
					symbol.inherit(cmethods[ci]);
				for (var ci = 0, cl = cproperties.length; ci < cl; ci++)
					symbol.inherit(cproperties[ci]);
			}
		}
		else LOG.warn("Can't augment contributer: "+augments[i]+", not found.");
	}
}

