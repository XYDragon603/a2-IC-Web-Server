SRC_DIR := src
OBJ_DIR := obj

CC := gcc
CFLAGS := -g -Wall -Wextra -pthread
CPPFLAGS :=

# I use bison -y so it behaves like yacc.
# becausr on some machines, plain yacc was missing or had permission issues.
YACC ?= bison -y
LEX ?= flex


COMMON_OBJ := $(OBJ_DIR)/y.tab.o $(OBJ_DIR)/lex.yy.o $(OBJ_DIR)/parse.o

BIN := sample_parse icws

default: all
all: $(BIN)

# Build parser test program.
sample_parse: $(COMMON_OBJ) $(OBJ_DIR)/sample_parse.o
	$(CC) $(CFLAGS) $^ -o $@

#the web server.
icws: $(COMMON_OBJ) $(OBJ_DIR)/icws.o
	$(CC) $(CFLAGS) $^ -o $@

# lexer depends on y.tab.h because token definitions come from parser.y.
# If I generate lex.yy.c before y.tab.h exists, compilation can fail.
$(SRC_DIR)/lex.yy.c: $(SRC_DIR)/lexer.l $(SRC_DIR)/y.tab.h
	$(LEX) -o $@ $(SRC_DIR)/lexer.l

# Generate yacc/bison output files.
# I use $(YACC) instead of hardcoding yacc because my environment worked better with bison -y.
$(SRC_DIR)/y.tab.c $(SRC_DIR)/y.tab.h: $(SRC_DIR)/parser.y
	$(YACC) -d $(SRC_DIR)/parser.y
	mv y.tab.c $(SRC_DIR)/y.tab.c
	mv y.tab.h $(SRC_DIR)/y.tab.h

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c $(SRC_DIR)/parse.h | $(OBJ_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

clean:
	rm -rf $(OBJ_DIR) $(BIN) $(SRC_DIR)/lex.yy.c $(SRC_DIR)/y.tab.c $(SRC_DIR)/y.tab.h