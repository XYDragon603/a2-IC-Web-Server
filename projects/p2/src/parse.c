#include "parse.h"

/*
 * Parse one full HTTP request header block ending in \r\n\r\n.
 * yacc/lex handles request line + headers.
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

    (void)socketFd;
    memset(buf, 0, sizeof(buf));

    while (state != STATE_CRLFCRLF) {
        char expected = 0;

        if (i >= size) {
            break;
        }
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

    if (state == STATE_CRLFCRLF) {
        Request *request = (Request *)malloc(sizeof(Request));
        if (request == NULL) {
            return NULL;
        }

        memset(request, 0, sizeof(Request));
        request->headers = NULL;
        request->header_count = 0;

        yyrestart(NULL);
        set_parsing_options(buf, offset, request);

        if (yyparse() == SUCCESS) {
            return request;
        }

        free_request(request);
    }

    return NULL;
}

void free_request(Request *request) {
    if (request == NULL) {
        return;
    }
    free(request->headers);
    free(request);
}