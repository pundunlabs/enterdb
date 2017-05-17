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
    gb_chash \
    enterdb_mem \
    enterdb_mem_wrp_mgr \
    enterdb_mem_wrp \

ERL_BEAMS = $(MODULES:%=$(EBIN)/%.beam)

INCLUDE_DIRS = \
	../include \
	../../gb_log/include \

ERL_INCLUDES=$(INCLUDE_DIRS:%=-I %)

.PHONY: all clean

all: $(EBIN) $(APP_TARGET) $(ERL_BEAMS)

$(EBIN)/%.beam: %.erl
	erlc $(ERL_INCLUDES) -o $(EBIN) $<

$(EBIN):
	mkdir $@

$(APP_TARGET): $(APP_SRC)
	@set -e; mods=`echo $(MODULES) | sed -e 's; ;,;g'` ; \
	sed -e "s;%MODULES%;$$mods;" $< | \
	sed -e 's;%VSN%;$(VSN);' > $@

clean:
	rm $(EBIN)/*.app
	rm $(EBIN)/*.beam
