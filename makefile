#
# FILE            makefile
#
# AUTHOR          Ken Zangelin
#
# Copyright 2026 Seamware
# SPDX-License-Identifier: Apache-2.0
#
# corMqttBridge is a PLUGIN - mqtt.so, dlopen'd by the broker, linked by
# nothing. It lives in its own repo for the same reason every bridge does: the
# broker's own tree should not grow one directory, or one library, per protocol
# it can speak. Until it moved here, libmosquitto was linked into every broker.
#
# libmosquitto is its one dependency (Debian/Ubuntu: libmosquitto-dev). It is
# not gated the way corDdsBridge's 21 MiB stack is: it is small, packaged
# everywhere, and was a hard dependency of the broker itself until this repo.
#
PLUGIN        = mqtt.so
CC            = gcc
#
# The directory the sibling repos live in - the parent, on a workstation
# (~/git/...) and on a CI runner (<workspace>/stack/...) alike.
#
COR_LIBS     ?= ..
PLUGIN_DIR   ?= /opt/seamware/plugins/bridge

INCLUDE       = -I$(COR_LIBS)
CFLAGS        = -std=gnu11 -O2 -Wall -Werror -fPIC $(INCLUDE) -MMD -MP

#
# BUILD - debug (the default) or release, as every cor lib; corLibs passes it down.
# Traces (COR_T) are compiled in for a debug build only - see corLog.h.
#
BUILD        ?= debug

ifeq ($(BUILD),debug)
CFLAGS       += -DCOR_T_ON
endif

#
# The broker is linked rdynamic, so corLog, corAlloc, corJson and corTree
# resolve from the running process at dlopen. Only the transport library is
# linked here.
#
LIBS          = -lmosquitto -lpthread

SOURCES       = mqttRegister.c
OBJS          = $(SOURCES:.c=.o)
DEPS          = $(SOURCES:.c=.d)

all: $(PLUGIN)

$(PLUGIN): $(OBJS)
	$(CC) -shared $(OBJS) -o $(PLUGIN) $(LIBS)

#
# The objects sit beside the sources, one flavour at a time: .flags holds the
# compile line they were built with, and a different one (BUILD=release after a
# debug build) rebuilds them instead of installing the other flavour's.
#
.flags: FORCE
	@echo '$(CFLAGS)' | cmp -s - $@ || echo '$(CFLAGS)' > $@

FORCE:

%.o: %.c .flags
	$(CC) $(CFLAGS) -c $< -o $@

#
# install - a NEW file, renamed over the installed one: never `cp` onto it. cp rewrites the file in
# place (the same inode), and a running broker has the plugin mapped (dlopen) - its code changes under
# the broker, which dies of SIGSEGV or SIGILL within seconds. The rename leaves a running broker the
# old file; its next start loads the new one.
#
install: all
	mkdir -p $(PLUGIN_DIR)
	cp -p $(PLUGIN) $(PLUGIN_DIR)/.$(PLUGIN).new && mv -f $(PLUGIN_DIR)/.$(PLUGIN).new $(PLUGIN_DIR)/$(PLUGIN)

di: install
ci: clean install

#
# contract - does this still satisfy corBridge's BridgeDriver.h? The whole
# plugin is one file, so this is simply its compile.
#
contract:
	@$(CC) -std=gnu11 -Wall -Werror -fPIC -I$(COR_LIBS) -c mqttRegister.c -o /tmp/corMqttBridge-contract.o
	@rm -f /tmp/corMqttBridge-contract.o
	@echo "corMqttBridge: contract OK - BridgeDriver.h is still satisfied"

clean:
	rm -f *.o *.d *.so *~ .flags

-include $(DEPS)

.PHONY: all install di ci clean contract
