# Makefile for Lucene GCJ code
#
# Usually invoked by Ant.  Requires that core classes & jars are already built.

BUILD=../../build
LUCENE_OBJ=$(subst .jar,.a,$(wildcard $(BUILD)/lucene-*.jar))
DEST=$(BUILD)/gcj
CORE=$(BUILD)/classes/java
SRC=.

CORE_HEADERS=$(CORE)/org/apache/lucene/store/IndexInput.h
GCJ_JAVA=$(wildcard $(SRC)/org/apache/lucene/store/*.java)
GCJ_OBJ=$(DEST)/lucene-gcj.a

CFLAGS ?= -O3 -ffast-math 
GCJFLAGS ?= $(CFLAGS) -fno-bounds-check -fno-store-check

ifdef PROFILE_ARCS
  CFLAGS += -fprofile-arcs
endif

ifdef BRANCH_PROBABILITIES
  CFLAGS += -fbranch-probabilities
endif

LIBS = -lstdc++

# default rule build's command line executables
all: $(BUILD)/indexFiles $(BUILD)/searchFiles

# pattern rules to generate various things
%.a : %.jar
	gcj $(GCJFLAGS) -c -I $(CORE) -o $@ $<

$(DEST)/%.class : $(SRC)/%.java
	mkdir -p $(dir $@)
	gcj -C -I $(CORE) -d $(DEST) $<

$(CORE)/%.h : $(CORE)/%.class
	gcjh --classpath=$(CORE) -d $(CORE) \
	 $(subst /,.,$(subst .class,,$(subst $(CORE)/,,$<)))

$(DEST)/%.h : $(DEST)/%.class
	gcjh --classpath=$(DEST) -d $(DEST) \
	 $(subst /,.,$(subst .class,,$(subst $(DEST)/,,$<)))

$(DEST)/%.o : $(SRC)/%.cc $(DEST)/%.h $(CORE_HEADERS)
	g++ $(CFLAGS) -c -I $(CORE) -I $(DEST) -o $@ $<

$(GCJ_OBJ) : $(GCJ_JAVA)
	mkdir -p $(dir $@)
	gcj $(GCJFLAGS) -c -I $(CORE) -I $(DEST) -o $@ $^

# list of all object code to be linked
OBJ = $(LUCENE_OBJ) $(GCJ_OBJ) $(DEST)/org/apache/lucene/store/GCJIndexInput.o

USE_GCJ_DIRECTORY =\
 -Dorg.apache.lucene.FSDirectory.class=org.apache.lucene.store.GCJDirectory

PROPS = $(USE_GCJ_DIRECTORY)

# link together various applications
$(BUILD)/indexFiles: $(OBJ)
	gcj $(GCJFLAGS) $(PROPS) $(LIBS) $^ -o $@ \
         --main=org.apache.lucene.demo.IndexFiles

$(BUILD)/searchFiles: $(OBJ)
	gcj $(GCJFLAGS) $(PROPS) $(LIBS) $^ -o $@ \
         --main=org.apache.lucene.demo.SearchFiles

# remove generated files
clean:
	rm -rf $(DEST) $(BUILD)/{*.a,indexFiles,searchFiles}

