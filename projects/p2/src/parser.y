/**
 * @file parser.y
 * @brief Grammar for a small subset of HTTP
 */

%{
#include "parse.h"

#define YYERROR_VERBOSE

void yyerror(const char *s);
void set_parsing_options(char *buf, size_t siz, Request *parsing_request);
extern int yylex(void);

/*
 * These globals are used by the lexer/parser pair.
 * The parser reads from parsing_buf instead of stdin.
 */
char *parsing_buf;
int parsing_offset;
size_t parsing_buf_siz;
Request *parsing_request;
%}

%union {
    char str[8192];
    int i;
}

%start request

%token t_crlf
%token t_backslash
%token t_slash
%token t_digit
%token t_dot
%token t_token_char
%token t_lws
%token t_colon
%token t_separators
%token t_sp
%token t_ws
%token t_ctl

%type<str> t_crlf
%type<i> t_backslash
%type<i> t_slash
%type<i> t_digit
%type<i> t_dot
%type<i> t_token_char
%type<str> t_lws
%type<i> t_colon
%type<i> t_separators
%type<i> t_sp
%type<str> t_ws
%type<i> t_ctl

%type<i> allowed_char_for_token
%type<i> allowed_char_for_text
%type<str> ows
%type<str> token
%type<str> text
%type<str> maybe_text

%%

/*
 * token is used for things like method name or header name.
 * I allow normal token chars, digits, and dot here.
 */
allowed_char_for_token:
      t_token_char
    | t_digit { $$ = '0' + $1; }
    | t_dot
;

/*
 * Build a token string one character at a time.
 * Example: G -> GE -> GET
 */
token:
      allowed_char_for_token {
          snprintf($$, 8192, "%c", $1);
      }
    | token allowed_char_for_token {
          memcpy($$, $1, strlen($1));
          $$[strlen($1)] = $2;
          $$[strlen($1) + 1] = 0;
      }
;

allowed_char_for_text:
      allowed_char_for_token
    | t_separators { $$ = $1; }
    | t_colon { $$ = $1; }
    | t_slash { $$ = $1; }
;

text:
      allowed_char_for_text {
          snprintf($$, 8192, "%c", $1);
      }
    | text ows allowed_char_for_text {
          memcpy($$, $1, strlen($1));
          memcpy($$ + strlen($1), $2, strlen($2));
          $$[strlen($1) + strlen($2)] = $3;
          $$[strlen($1) + strlen($2) + 1] = 0;
      }
;

/*
 * Some header values may be empty, so I use maybe_text.
 * Example:
 * Header: \r\n
 */
maybe_text:
      {
          $$[0] = 0;
      }
    | text {
          snprintf($$, 8192, "%s", $1);
      }
;

/*
 * Optional whitespace.
 * This helps the parser accept slightly different spacing styles.
 */
ows:
      {
          $$[0] = 0;
      }
    | t_sp {
          snprintf($$, 8192, "%c", $1);
      }
    | t_ws {
          snprintf($$, 8192, "%s", $1);
      }
;

/*
 * Request line:
 * METHOD SP URI SP VERSION CRLF
 *
 * Example:
 * GET /index.html HTTP/1.1
 */
request_line:
    token t_sp text t_sp text t_crlf {
        strcpy(parsing_request->http_method, $1);
        strcpy(parsing_request->http_uri, $3);
        strcpy(parsing_request->http_version, $5);
    }
;

/*
 * A single header line:
 * Header-Name : value
 * fix:
 * The starter code only had room for one header.
 * That is a bug because real HTTP requests can have many headers.
 * So here I realloc the header array every time I parse a new one.
 */
request_header:
    token ows t_colon ows maybe_text ows t_crlf {
        Request_header *new_headers;
        int new_count = parsing_request->header_count + 1;

        new_headers = realloc(parsing_request->headers,
                              sizeof(Request_header) * new_count);
        if (new_headers == NULL) {
            yyerror("realloc failed");
            YYABORT;
        }

        parsing_request->headers = new_headers;

        strcpy(parsing_request->headers[parsing_request->header_count].header_name, $1);
        strcpy(parsing_request->headers[parsing_request->header_count].header_value, $5);

        parsing_request->header_count = new_count;
    }
;

request_headers:
      /* empty */
    | request_headers request_header
;

/*
 * A full request for this project:
 * request line + zero/many headers + final blank line
 */
request:
    request_line request_headers t_crlf {
        return SUCCESS;
    }
;

%%

/*
 * Called before yyparse().
 * This tells lexer/parser which buffer to read from
 * and which Request struct to fill.
 */
void set_parsing_options(char *buf, size_t siz, Request *request) {
    parsing_buf = buf;
    parsing_offset = 0;
    parsing_buf_siz = siz;
    parsing_request = request;
}

void yyerror(const char *s) {
    fprintf(stderr, "%s\n", s);
}