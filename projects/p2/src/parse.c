#include "parse.h"

/*
 * This function parses one full HTTP request header block.
 * For this project, I only let yacc/lex parse the request line and headers,
 * so first I need to make sure I really collected a full header block.
 * A complete header block ends with \r\n\r\n.
 */
Request *parse(char *buffer, int size, int socketFd) {
    enum {
        STATE_START = 0,
        STATE_CR,
        STATE_CRLF,
        STATE_CRLFCR,
        STATE_CRLFCRLF
    };

    int i = 0;
    int state = STATE_START;
    size_t offset = 0;
    char ch;
    char buf[8192];

    /*
     * socketFd is not used inside this parser right now,
     * but I keep the parameter because it is part of the parse interface.
     */
    (void)socketFd;

    memset(buf, 0, sizeof(buf));

    /*
     * I use a small state machine to detect \r\n\r\n.
     * Reason:
     * an HTTP request may not end right after one line,
     * so I should not call yacc too early.
     * Before doing it this way, one common bug is trying to parse
     * incomplete request data.
     */
    while (state != STATE_CRLFCRLF) {
        char expected = 0;

        if (i >= size) {
            break;
        }

        /*
         * Safety check:
         * if the local parser buffer is full, stop and fail.
         * This avoids writing past the end of buf.
         */
        if (offset >= sizeof(buf) - 1) {
            return NULL;
        }

        ch = buffer[i++];
        buf[offset++] = ch;

        switch (state) {
            case STATE_START:
            case STATE_CRLF:
                expected = '\r';
                break;

            case STATE_CR:
            case STATE_CRLFCR:
                expected = '\n';
                break;

            default:
                state = STATE_START;
                continue;
        }

        if (ch == expected) {
            state++;
        } else {
            state = STATE_START;
        }
    }

    buf[offset] = '\0';

    /*
     * Only call yacc if I really found the full \r\n\r\n ending.
     * If not, this request block is incomplete or bad.
     */
    if (state == STATE_CRLFCRLF) {
        Request *request = (Request *)malloc(sizeof(Request));
        if (request == NULL) {
            return NULL;
        }

        memset(request, 0, sizeof(Request));
        /*
         * headers starts as NULL because parser.y uses realloc()
         * to grow the header array one line at a time. If this is not initialized properly, realloc logic can break.
         */
        request->headers = NULL;
        request->header_count = 0;

        /*
         * Restart lexer state, then tell lexer/parser to read from buf.
         * yacc/lex will fill the Request struct for me.
         */
        yyrestart(NULL);
        set_parsing_options(buf, offset, request);

        if (yyparse() == SUCCESS) {
            return request;
        }

        /*
         * If yacc parsing fails, free partially built request.
         * This avoids memory leaks on bad input.
         */
        free_request(request);
    }

    return NULL;
}

/*
 * Free all memory owned by one Request.
 * Right now headers is the only dynamically allocated field inside it.
 */
void free_request(Request *request) {
    if (request == NULL) {
        return;
    }

    free(request->headers);
    free(request);
}