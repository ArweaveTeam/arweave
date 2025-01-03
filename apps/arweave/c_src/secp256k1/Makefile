ERTS_INCLUDE_DIR ?= $(shell erl -noshell -eval 'io:format("~ts/erts-~ts/include/", [code:root_dir(), erlang:system_info(version)]).' -s init stop)
ERL_INTERFACE_INCLUDE_DIR ?= $(shell erl -noshell -eval 'io:format("~ts", [code:lib_dir(erl_interface, include)]).' -s init stop)
ERL_INTERFACE_LIB_DIR ?= $(shell erl -noshell -eval 'io:format("~ts", [code:lib_dir(erl_interface, lib)]).' -s init stop)

CC = cc
ifeq ($(MODE), debug)
	CFLAGS ?= -O0 -g
else
	CFLAGS ?= -O3
endif
CFLAGS += -fPIC -shared -I $(ERTS_INCLUDE_DIR) -I $(ERL_INTERFACE_INCLUDE_DIR) -I /usr/local/include -I ../../lib/secp256k1/src -I ../../lib/secp256k1/include
LDFLAGS = -L../../lib/secp256k1/build/lib
LDLIBS = -L $(ERL_INTERFACE_LIB_DIR) -L /usr/local/lib -lsecp256k1
NIF_SRC = secp256k1_nif.c
NIF_SO = $(shell pwd)/../../priv/secp256k1_nif.so

all: $(NIF_SO)

$(NIF_SO): $(NIF_SRC)
	$(CC) $(CFLAGS) $(NIF_SRC) -o $(NIF_SO) $(LDFLAGS) $(LDLIBS)

clean:
	rm -f $(NIF_SO)
