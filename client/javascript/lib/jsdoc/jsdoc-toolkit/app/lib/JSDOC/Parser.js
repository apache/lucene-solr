if (typeof JSDOC == "undefined") JSDOC = {};

/**
	@namespace
	@requires JSDOC.Walker
	@requires JSDOC.Symbol
	@requires JSDOC.DocComment
*/
JSDOC.Parser = {
	conf: {
		ignoreCode:               JSDOC.opt.n,
		ignoreAnonymous:           true, // factory: true
		treatUnderscoredAsPrivate: true, // factory: true
		explain:                   false // factory: false
	},
	
	addSymbol: function(symbol) {
		// if a symbol alias is documented more than once the last one with the user docs wins
		if (JSDOC.Parser.symbols.hasSymbol(symbol.alias)) {
			var oldSymbol = JSDOC.Parser.symbols.getSymbol(symbol.alias);

			if (oldSymbol.comment.isUserComment) {
				if (symbol.comment.isUserComment) { // old and new are both documented
					LOG.warn("The symbol '"+symbol.alias+"' is documented more than once.");
				}
				else { // old is documented but new isn't
					return;
				}
			}
		}
		
		// we don't document anonymous things
		if (JSDOC.Parser.conf.ignoreAnonymous && symbol.name.match(/\$anonymous\b/)) return;

		// uderscored things may be treated as if they were marked private, this cascades
		if (JSDOC.Parser.conf.treatUnderscoredAsPrivate && symbol.name.match(/[.#-]_[^.#-]+$/)) {
			symbol.isPrivate = true;
		}
		
		// -p flag is required to document private things
		if ((symbol.isInner || symbol.isPrivate) && !JSDOC.opt.p) return;
		
		// ignored things are not documented, this doesn't cascade
		if (symbol.isIgnored) return;
		
		JSDOC.Parser.symbols.addSymbol(symbol);
	},
	
	addBuiltin: function(name) {
		var builtin = new JSDOC.Symbol(name, [], "CONSTRUCTOR", new JSDOC.DocComment(""));
		builtin.isNamespace = true;
		builtin.srcFile = "";
		builtin.isPrivate = false;
		JSDOC.Parser.addSymbol(builtin);
		return builtin;
	},
	
	init: function() {
		JSDOC.Parser.symbols = new JSDOC.SymbolSet();
		JSDOC.Parser.walker = new JSDOC.Walker();
	},
	
	finish: function() {
		JSDOC.Parser.symbols.relate();		
		
		// make a litle report about what was found
		if (JSDOC.Parser.conf.explain) {
			var symbols = JSDOC.Parser.symbols.toArray();
			var srcFile = "";
			for (var i = 0, l = symbols.length; i < l; i++) {
				var symbol = symbols[i];
				if (srcFile != symbol.srcFile) {
					srcFile = symbol.srcFile;
					print("\n"+srcFile+"\n-------------------");
				}
				print(i+":\n  alias => "+symbol.alias + "\n  name => "+symbol.name+ "\n  isa => "+symbol.isa + "\n  memberOf => " + symbol.memberOf + "\n  isStatic => " + symbol.isStatic + ",  isInner => " + symbol.isInner);
			}
			print("-------------------\n");
		}
	}
}

JSDOC.Parser.parse = function(/**JSDOC.TokenStream*/ts, /**String*/srcFile) {
	JSDOC.Symbol.srcFile = (srcFile || "");
	JSDOC.DocComment.shared = ""; // shared comments don't cross file boundaries
	
	if (!JSDOC.Parser.walker) JSDOC.Parser.init();
	JSDOC.Parser.walker.walk(ts); // adds to our symbols
	
	// filter symbols by option
	for (p in JSDOC.Parser.symbols._index) {
		var symbol = JSDOC.Parser.symbols.getSymbol(p);
		
		if (!symbol) continue;
		
		if (symbol.is("FILE") || symbol.is("GLOBAL")) {
			continue;
		}
		else if (!JSDOC.opt.a && !symbol.comment.isUserComment) {
			JSDOC.Parser.symbols.deleteSymbol(symbol.alias);
		}
		
		if (/#$/.test(symbol.alias)) { // we don't document prototypes
			JSDOC.Parser.symbols.deleteSymbol(symbol.alias);
		}
	}
	
	return JSDOC.Parser.symbols.toArray();
}
