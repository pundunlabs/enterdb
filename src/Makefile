include ../vsn.mk
VSN=$(ENTERDB_VSN)

EBIN = ../ebin

APP_NAME = enterdb
APP_FILE= $(APP_NAME).app
APP_SRC= $(APP_FILE).src
APP_TARGET= $(EBIN)/$(APP_FILE)

MODULES = \
    enterdb_app \
    enterdb_sup \
    enterdb_db \
    enterdb_server \
    enterdb \
    enterdb_lib \
    enterdb_simple_sup \
    enterdb_ldb_worker \
    gb_chash

ERL_BEAMS = $(MODULES:%=$(EBIN)/%.beam)

INCLUDE_DIRS = \
	../include \
	../../gb_log/include

ERL_INCLUDES=$(INCLUDE_DIRS:%=-I %)

.PHONY: all clean

all: $(EBIN) $(APP_TARGET) $(ERL_BEAMS)

$(EBIN)/%.beam: %.erl
	erlc $(ERL_INCLUDES) -o $(EBIN) $<
	
$(EBIN):
	mkdir ../ebin

$(APP_TARGET): $(APP_SRC) 
	@set -e; mods=`echo $(MODULES) | sed -e 's; ;,;g'` ; \
	sed -e "s;%MODULES%;$$mods;" $< | \
	sed -e 's;%VSN%;$(VSN);' > $@

clean:
	rm ../ebin/*.app
	rm ../ebin/*.beam
