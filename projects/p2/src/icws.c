#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "parse.h"

#define MAX_HEADER_SIZE 8192
#define CONN_BUFFER_SIZE 65536
#define IO_BUFFER_SIZE 4096
#define BACKLOG 128

static pthread_mutex_t parser_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * I added g_stop and g_server_fd for graceful shutdown.
 * Before this, when I pressed Ctrl+C, the server usually died in accept()
 * and valgrind showed open socket / thread-related leftovers.
 * This version tries to stop more cleanly.
 */
static volatile sig_atomic_t g_stop = 0;
static int g_server_fd = -1;

typedef struct JobNode {
    int client_fd;
    struct sockaddr_in client_addr;
    struct JobNode *next;
} JobNode;

typedef struct {
    JobNode *head;
    JobNode *tail;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} WorkQueue;

typedef struct {
    char root[4096];
    char cgi_handler[4096];
    int timeout_seconds;
    int listen_port;
    WorkQueue queue;
} ServerConfig;

//graceful shutdown helpers


static void handle_shutdown_signal(int signo) {
    (void)signo;
    g_stop = 1;

    /*
     * close() is async-signal-safe, so it is okay to call here.
     * I close the listening socket here because otherwise accept()
     * may keep blocking and the main loop may not stop quickly.
     */
    if (g_server_fd >= 0) {
        close(g_server_fd);
        g_server_fd = -1;
    }
}

static int install_signal_handlers(void) {
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_shutdown_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    if (sigaction(SIGINT, &sa, NULL) < 0) {
        return -1;
    }
    if (sigaction(SIGTERM, &sa, NULL) < 0) {
        return -1;
    }

    /*
     * If client disconnects while we are sending data,
     * I do not want the whole server to die because of SIGPIPE.
     */
    signal(SIGPIPE, SIG_IGN);
    return 0;
}

//work queue
 

static void work_queue_init(WorkQueue *queue) {
    queue->head = NULL;
    queue->tail = NULL;
    queue->shutting_down = 0;
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->cond, NULL);
}

static void work_queue_shutdown(WorkQueue *queue) {
    pthread_mutex_lock(&queue->mutex);
    queue->shutting_down = 1;

    /*
     * Wake up all sleeping workers.
     * Before this kind of logic, worker threads could stay blocked forever
     * on the condition variable when the server was stopping.
     */
    pthread_cond_broadcast(&queue->cond);
    pthread_mutex_unlock(&queue->mutex);
}

static void work_queue_destroy(WorkQueue *queue) {
    JobNode *cur;
    JobNode *next;

    cur = queue->head;
    while (cur != NULL) {
        next = cur->next;
        close(cur->client_fd);
        free(cur);
        cur = next;
    }

    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->cond);
}

static void work_queue_push(WorkQueue *queue, int client_fd, struct sockaddr_in *client_addr) {
    JobNode *node = (JobNode *)malloc(sizeof(JobNode));
    if (node == NULL) {
        close(client_fd);
        return;
    }

    node->client_fd = client_fd;
    node->client_addr = *client_addr;
    node->next = NULL;

    pthread_mutex_lock(&queue->mutex);

    /*
     * If shutdown has started, new jobs should not be accepted anymore.
     * This avoids pushing new connections into a queue that nobody will serve.
     */
    if (queue->shutting_down) {
        pthread_mutex_unlock(&queue->mutex);
        close(client_fd);
        free(node);
        return;
    }

    if (queue->tail == NULL) {
        queue->head = node;
        queue->tail = node;
    } else {
        queue->tail->next = node;
        queue->tail = node;
    }

    pthread_cond_signal(&queue->cond);
    pthread_mutex_unlock(&queue->mutex);
}

static int work_queue_pop(WorkQueue *queue, struct sockaddr_in *client_addr) {
    JobNode *node;
    int client_fd;

    pthread_mutex_lock(&queue->mutex);

    while (queue->head == NULL && !queue->shutting_down) {
        pthread_cond_wait(&queue->cond, &queue->mutex);
    }

    /*
     * Return -1 when shutting down and there is no more work.
     * This gives worker threads a clean way to exit.
     */
    if (queue->head == NULL && queue->shutting_down) {
        pthread_mutex_unlock(&queue->mutex);
        return -1;
    }

    node = queue->head;
    queue->head = node->next;
    if (queue->head == NULL) {
        queue->tail = NULL;
    }

    pthread_mutex_unlock(&queue->mutex);

    client_fd = node->client_fd;
    *client_addr = node->client_addr;
    free(node);
    return client_fd;
}

//utility helpers

static void format_http_time(time_t t, char *buf, size_t size) {
    struct tm tm_info;
    gmtime_r(&t, &tm_info);
    strftime(buf, size, "%a, %d %b %Y %H:%M:%S GMT", &tm_info);
}

static long now_millis(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

static const char *get_mime_type(const char *path) {
    const char *dot = strrchr(path, '.');
    if (dot == NULL) return "text/plain";

    if (strcasecmp(dot, ".html") == 0 || strcasecmp(dot, ".htm") == 0) return "text/html";
    if (strcasecmp(dot, ".css") == 0) return "text/css";
    if (strcasecmp(dot, ".txt") == 0) return "text/plain";
    if (strcasecmp(dot, ".js") == 0) return "text/javascript";
    if (strcasecmp(dot, ".jpg") == 0 || strcasecmp(dot, ".jpeg") == 0) return "image/jpg";
    if (strcasecmp(dot, ".png") == 0) return "image/png";
    if (strcasecmp(dot, ".gif") == 0) return "image/gif";
    if (strcasecmp(dot, ".json") == 0) return "application/json";
    if (strcasecmp(dot, ".pdf") == 0) return "application/pdf";

    return "application/octet-stream";
}

static const char *status_text(int code) {
    switch (code) {
        case 200: return "OK";
        case 400: return "Bad Request";
        case 404: return "Not Found";
        case 408: return "Request Timeout";
        case 411: return "Length Required";
        case 500: return "Internal Server Error";
        case 501: return "Not Implemented";
        case 505: return "HTTP Version Not Supported";
        default: return "Error";
    }
}
static int send_all_socket(int fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    size_t sent = 0;

    while (sent < len) {
        ssize_t n = send(fd, p + sent, len - sent, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}
static int write_all_fd(int fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    size_t written = 0;

    while (written < len) {
        ssize_t n = write(fd, p + written, len - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        written += (size_t)n;
    }
    return 0;
}

static int send_error_response(int fd, int status_code, int keep_alive) {
    char header[2048];
    char body[512];
    char date_buf[128];
    const char *conn_text = keep_alive ? "keep-alive" : "close";
    int body_len;
    int header_len;

    format_http_time(time(NULL), date_buf, sizeof(date_buf));
    body_len = snprintf(body, sizeof(body),
                        "<html><body><h1>%d %s</h1></body></html>\n",
                        status_code, status_text(status_code));

    header_len = snprintf(header, sizeof(header),
                          "HTTP/1.1 %d %s\r\n"
                          "Date: %s\r\n"
                          "Server: icws-student/3.0\r\n"
                          "Connection: %s\r\n"
                          "Content-Type: text/html\r\n"
                          "Content-Length: %d\r\n"
                          "\r\n",
                          status_code, status_text(status_code), date_buf, conn_text, body_len);

    if (send_all_socket(fd, header, (size_t)header_len) < 0) return -1;
    if (send_all_socket(fd, body, (size_t)body_len) < 0) return -1;
    return 0;
}
static int get_header_value(Request *request, const char *name, char *out, size_t out_size) {
    int i;
    for (i = 0; i < request->header_count; i++) {
        if (strcasecmp(request->headers[i].header_name, name) == 0) {
            snprintf(out, out_size, "%s", request->headers[i].header_value);
            return 1;
        }
    }
    return 0;
}

static int client_wants_close(Request *request) {
    char value[4096];
    if (get_header_value(request, "Connection", value, sizeof(value))) {
        if (strcasecmp(value, "close") == 0) {
            return 1;
        }
    }

    return 0;
}

static ssize_t find_header_end(const char *buf, size_t len) {
    size_t i;
    if (len < 4) return -1;
    for (i = 0; i + 3 < len; i++) {
        if (buf[i] == '\r' && buf[i + 1] == '\n' &&
            buf[i + 2] == '\r' && buf[i + 3] == '\n') {
            return (ssize_t)i;
        }
    }
    return -1;
}

static int wait_for_client_data(int client_fd, int timeout_ms) {
    struct pollfd pfd;
    int poll_ret;
    pfd.fd = client_fd;
    pfd.events = POLLIN;
    pfd.revents = 0;
    while (1) {
        poll_ret = poll(&pfd, 1, timeout_ms);
        if (poll_ret < 0 && errno == EINTR) {
            if (g_stop) return -1;
            continue;
        }
        return poll_ret;
    }
}

static Request *parse_request_thread_safe(char *request_bytes, int req_len, int client_fd) {
    Request *request;
    /*
     * The yacc/lex parser code is not thread-safe.
     * Without this mutex, different worker threads could parse at the same time
     * and corrupt parser state.
     */
    pthread_mutex_lock(&parser_mutex);
    request = parse(request_bytes, req_len, client_fd);
    pthread_mutex_unlock(&parser_mutex);

    return request;
}

static int build_file_path(const char *root, const char *uri, char *out, size_t out_size) {
    char uri_copy[4096];
    char *query_pos;

    if (uri == NULL || uri[0] != '/') {
        return -1;
    }

    snprintf(uri_copy, sizeof(uri_copy), "%s", uri);
    query_pos = strchr(uri_copy, '?');
    if (query_pos != NULL) {
        *query_pos = '\0';
    }

    /*
     * I reject ".." and backslash here to block simple path traversal.
     * Before adding checks like this, a user could try to escape wwwRoot.
     */
    if (strstr(uri_copy, "..") != NULL || strchr(uri_copy, '\\') != NULL) {
        return -1;
    }

    if (strcmp(uri_copy, "/") == 0) {
        snprintf(out, out_size, "%s/index.html", root);
    } else if (uri_copy[strlen(uri_copy) - 1] == '/') {
        snprintf(out, out_size, "%s%sindex.html", root, uri_copy);
    } else {
        snprintf(out, out_size, "%s%s", root, uri_copy);
    }

    return 0;
}

/*
 * I changed Content-Length parsing from atoi() to strtol().
 * Reason: atoi() is too weak for bad input. It cannot report errors well.
 * With strtol(), I can reject negative numbers, overflow, and junk text.
 */
static int parse_content_length_value(const char *value, int *out) {
    char *endptr;
    long parsed;

    if (value == NULL || out == NULL) {
        return 0;
    }

    while (isspace((unsigned char)*value)) {
        value++;
    }

    if (*value == '\0') {
        return 0;
    }

    errno = 0;
    parsed = strtol(value, &endptr, 10);

    if (errno == ERANGE || parsed < 0 || parsed > INT_MAX) {
        return 0;
    }

    while (isspace((unsigned char)*endptr)) {
        endptr++;
    }

    if (*endptr != '\0') {
        return 0;
    }

    *out = (int)parsed;
    return 1;
}

static int serve_static_file(int fd, const char *root, Request *request, int keep_alive) {
    char path[8192];
    char header[4096];
    char date_buf[128];
    char modified_buf[128];
    char file_buf[IO_BUFFER_SIZE];
    const char *mime_type;
    const char *conn_text = keep_alive ? "keep-alive" : "close";
    struct stat st;
    int file_fd;
    int header_len;

    if (build_file_path(root, request->http_uri, path, sizeof(path)) < 0) {
        return send_error_response(fd, 400, 0);
    }

    if (stat(path, &st) < 0 || !S_ISREG(st.st_mode)) {
        return send_error_response(fd, 404, keep_alive);
    }

    file_fd = open(path, O_RDONLY);
    if (file_fd < 0) {
        return send_error_response(fd, 404, keep_alive);
    }

    format_http_time(time(NULL), date_buf, sizeof(date_buf));
    format_http_time(st.st_mtime, modified_buf, sizeof(modified_buf));
    mime_type = get_mime_type(path);

    header_len = snprintf(header, sizeof(header),
                          "HTTP/1.1 200 OK\r\n"
                          "Date: %s\r\n"
                          "Server: icws-student/3.0\r\n"
                          "Connection: %s\r\n"
                          "Content-Type: %s\r\n"
                          "Content-Length: %ld\r\n"
                          "Last-Modified: %s\r\n"
                          "\r\n",
                          date_buf, conn_text, mime_type, (long)st.st_size, modified_buf);

    if (send_all_socket(fd, header, (size_t)header_len) < 0) {
        close(file_fd);
        return -1;
    }

    if (strcasecmp(request->http_method, "HEAD") == 0) {
        close(file_fd);
        return 0;
    }

    while (1) {
        ssize_t n = read(file_fd, file_buf, sizeof(file_buf));
        if (n < 0) {
            if (errno == EINTR) continue;
            close(file_fd);
            return -1;
        }
        if (n == 0) break;
        if (send_all_socket(fd, file_buf, (size_t)n) < 0) {
            close(file_fd);
            return -1;
        }
    }

    close(file_fd);
    return 0;
}

static int is_cgi_request(const char *uri) {
    return (strncmp(uri, "/cgi/", 5) == 0 || strcmp(uri, "/cgi") == 0 || strcmp(uri, "/cgi/") == 0);
}

static void split_uri_path_query(const char *uri, char *path_info, size_t path_info_size,
                                 char *query_string, size_t query_size) {
    const char *qmark = strchr(uri, '?');

    if (qmark == NULL) {
        snprintf(path_info, path_info_size, "%s", uri);
        query_string[0] = '\0';
    } else {
        size_t path_len = (size_t)(qmark - uri);
        if (path_len >= path_info_size) {
            path_len = path_info_size - 1;
        }
        memcpy(path_info, uri, path_len);
        path_info[path_len] = '\0';
        snprintf(query_string, query_size, "%s", qmark + 1);
    }
}

static int read_exact_body(int client_fd, char *body, int already_have, int total_len, int timeout_seconds) {
    int have = already_have;
    long start_ms = now_millis();

    while (have < total_len) {
        int wait_ms;
        int poll_ret;
        long elapsed = now_millis() - start_ms;

        if (elapsed >= timeout_seconds * 1000L) {
            return -2;
        }

        wait_ms = (int)(timeout_seconds * 1000L - elapsed);
        poll_ret = wait_for_client_data(client_fd, wait_ms);
        if (poll_ret == 0) {
            return -2;
        }
        if (poll_ret < 0) {
            return -1;
        }

        while (1) {
            ssize_t n = recv(client_fd, body + have, (size_t)(total_len - have), 0);
            if (n < 0) {
                if (errno == EINTR) continue;
                return -1;
            }
            if (n == 0) {
                return -1;
            }
            have += (int)n;
            break;
        }
    }

    return have;
}

static int serve_cgi_request(int fd,
                             const char *cgi_handler,
                             int listen_port,
                             Request *request,
                             const char *body,
                             int body_len,
                             struct sockaddr_in *client_addr) {
    int inpipe[2];
    int outpipe[2];
    pid_t pid;
    char path_info[4096];
    char query_string[4096];
    char remote_addr[INET_ADDRSTRLEN];
    char server_port[64];
    char content_length[64];
    char content_type[4096];
    char host_value[4096];
    char accept_value[4096];
    char referer_value[4096];
    char accept_encoding_value[4096];
    char accept_language_value[4096];
    char accept_charset_value[4096];
    char cookie_value[4096];
    char user_agent_value[4096];
    char connection_value[4096];
    int status;
    int bytes_forwarded = 0;

    memset(path_info, 0, sizeof(path_info));
    memset(query_string, 0, sizeof(query_string));
    memset(remote_addr, 0, sizeof(remote_addr));
    memset(server_port, 0, sizeof(server_port));
    memset(content_length, 0, sizeof(content_length));
    memset(content_type, 0, sizeof(content_type));
    memset(host_value, 0, sizeof(host_value));
    memset(accept_value, 0, sizeof(accept_value));
    memset(referer_value, 0, sizeof(referer_value));
    memset(accept_encoding_value, 0, sizeof(accept_encoding_value));
    memset(accept_language_value, 0, sizeof(accept_language_value));
    memset(accept_charset_value, 0, sizeof(accept_charset_value));
    memset(cookie_value, 0, sizeof(cookie_value));
    memset(user_agent_value, 0, sizeof(user_agent_value));
    memset(connection_value, 0, sizeof(connection_value));

    split_uri_path_query(request->http_uri, path_info, sizeof(path_info), query_string, sizeof(query_string));

    if (client_addr != NULL) {
        inet_ntop(AF_INET, &(client_addr->sin_addr), remote_addr, sizeof(remote_addr));
    }
    snprintf(server_port, sizeof(server_port), "%d", listen_port);

    if (get_header_value(request, "Content-Type", content_type, sizeof(content_type)) == 0) {
        content_type[0] = '\0';
    }
    if (get_header_value(request, "Host", host_value, sizeof(host_value)) == 0) {
        host_value[0] = '\0';
    }
    if (get_header_value(request, "Accept", accept_value, sizeof(accept_value)) == 0) {
        accept_value[0] = '\0';
    }
    if (get_header_value(request, "Referer", referer_value, sizeof(referer_value)) == 0) {
        referer_value[0] = '\0';
    }
    if (get_header_value(request, "Accept-Encoding", accept_encoding_value, sizeof(accept_encoding_value)) == 0) {
        accept_encoding_value[0] = '\0';
    }
    if (get_header_value(request, "Accept-Language", accept_language_value, sizeof(accept_language_value)) == 0) {
        accept_language_value[0] = '\0';
    }
    if (get_header_value(request, "Accept-Charset", accept_charset_value, sizeof(accept_charset_value)) == 0) {
        accept_charset_value[0] = '\0';
    }
    if (get_header_value(request, "Cookie", cookie_value, sizeof(cookie_value)) == 0) {
        cookie_value[0] = '\0';
    }
    if (get_header_value(request, "User-Agent", user_agent_value, sizeof(user_agent_value)) == 0) {
        user_agent_value[0] = '\0';
    }
    if (get_header_value(request, "Connection", connection_value, sizeof(connection_value)) == 0) {
        connection_value[0] = '\0';
    }

    snprintf(content_length, sizeof(content_length), "%d", body_len);

    if (pipe(inpipe) < 0) {
        return send_error_response(fd, 500, 0);
    }
    if (pipe(outpipe) < 0) {
        close(inpipe[0]);
        close(inpipe[1]);
        return send_error_response(fd, 500, 0);
    }

    pid = fork();
    if (pid < 0) {
        close(inpipe[0]);
        close(inpipe[1]);
        close(outpipe[0]);
        close(outpipe[1]);
        return send_error_response(fd, 500, 0);
    }

    if (pid == 0) {
        char *argv_exec[] = {(char *)cgi_handler, NULL};

        dup2(inpipe[0], STDIN_FILENO);
        dup2(outpipe[1], STDOUT_FILENO);

        close(inpipe[1]);
        close(outpipe[0]);
        close(inpipe[0]);
        close(outpipe[1]);

        /*
         * I used clearenv() because before this, the CGI program inherited
         * too many desktop / shell environment variables.
         * It still worked, but the output was messy and not very CGI-like.
         */
        clearenv();

        /*
         * Keep PATH because many scripts use:
         * #!/usr/bin/env python3
         * If PATH is missing, env may not find python3.
         */
        setenv("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", 1);
        setenv("LANG", "C.UTF-8", 1);

        setenv("GATEWAY_INTERFACE", "CGI/1.1", 1);
        setenv("REQUEST_METHOD", request->http_method, 1);
        setenv("REQUEST_URI", request->http_uri, 1);
        setenv("SERVER_PROTOCOL", "HTTP/1.1", 1);
        setenv("SERVER_SOFTWARE", "icws-student/3.0", 1);
        setenv("SCRIPT_NAME", "/cgi", 1);
        setenv("PATH_INFO", path_info, 1);
        setenv("QUERY_STRING", query_string, 1);
        setenv("REMOTE_ADDR", remote_addr, 1);
        setenv("SERVER_PORT", server_port, 1);

        if (body_len > 0) {
            setenv("CONTENT_LENGTH", content_length, 1);
        }
        if (content_type[0] != '\0') {
            setenv("CONTENT_TYPE", content_type, 1);
        }
        if (accept_value[0] != '\0') {
            setenv("HTTP_ACCEPT", accept_value, 1);
        }
        if (referer_value[0] != '\0') {
            setenv("HTTP_REFERER", referer_value, 1);
        }
        if (accept_encoding_value[0] != '\0') {
            setenv("HTTP_ACCEPT_ENCODING", accept_encoding_value, 1);
        }
        if (accept_language_value[0] != '\0') {
            setenv("HTTP_ACCEPT_LANGUAGE", accept_language_value, 1);
        }
        if (accept_charset_value[0] != '\0') {
            setenv("HTTP_ACCEPT_CHARSET", accept_charset_value, 1);
        }
        if (host_value[0] != '\0') {
            setenv("HTTP_HOST", host_value, 1);
        }
        if (cookie_value[0] != '\0') {
            setenv("HTTP_COOKIE", cookie_value, 1);
        }
        if (user_agent_value[0] != '\0') {
            setenv("HTTP_USER_AGENT", user_agent_value, 1);
        }
        if (connection_value[0] != '\0') {
            setenv("HTTP_CONNECTION", connection_value, 1);
        }

        execv(cgi_handler, argv_exec);
        exit(1);
    }

    close(inpipe[0]);
    close(outpipe[1]);

    if (body_len > 0) {
        /*
         * This writes POST body into the pipe for the CGI child.
         * Earlier, a common mistake is using socket-style send() here,
         * but this fd is a pipe, so write()/write_all_fd() is the correct idea.
         */
        if (write_all_fd(inpipe[1], body, (size_t)body_len) < 0) {
            close(inpipe[1]);
            close(outpipe[0]);
            waitpid(pid, NULL, 0);
            return send_error_response(fd, 500, 0);
        }
    }
    close(inpipe[1]);

    while (1) {
        char buf[IO_BUFFER_SIZE];
        ssize_t n = read(outpipe[0], buf, sizeof(buf));
        if (n < 0) {
            if (errno == EINTR) continue;
            close(outpipe[0]);
            waitpid(pid, NULL, 0);
            if (bytes_forwarded == 0) {
                return send_error_response(fd, 500, 0);
            }
            return -1;
        }
        if (n == 0) break;
        if (send_all_socket(fd, buf, (size_t)n) < 0) {
            close(outpipe[0]);
            waitpid(pid, NULL, 0);
            return -1;
        }
        bytes_forwarded += (int)n;
    }

    close(outpipe[0]);
    waitpid(pid, &status, 0);

    /*
     * If CGI failed before sending anything, return 500.
     * But if CGI already sent output, do not append another 500 page,
     * because that would corrupt the HTTP response.
     */
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        if (bytes_forwarded == 0) {
            return send_error_response(fd, 500, 0);
        }
        return 0;
    }

    return 0;
}

static void handle_client(int client_fd,
                          const char *root,
                          const char *cgi_handler,
                          int listen_port,
                          int timeout_seconds,
                          struct sockaddr_in *client_addr) {
    char conn_buf[CONN_BUFFER_SIZE];
    size_t used = 0;
    long request_start_ms = -1;
    char client_ip[INET_ADDRSTRLEN];

    memset(conn_buf, 0, sizeof(conn_buf));
    memset(client_ip, 0, sizeof(client_ip));

    if (client_addr != NULL) {
        inet_ntop(AF_INET, &(client_addr->sin_addr), client_ip, sizeof(client_ip));
    }

    while (!g_stop) {
        ssize_t header_pos = find_header_end(conn_buf, used);

        if (header_pos < 0) {
            int poll_ret;
            int wait_ms = timeout_seconds * 1000;

            if (used > 0) {
                long elapsed = now_millis() - request_start_ms;
                if (elapsed >= timeout_seconds * 1000L) {
                    send_error_response(client_fd, 408, 0);
                    break;
                }
                wait_ms = (int)(timeout_seconds * 1000L - elapsed);
            }

            if (used >= MAX_HEADER_SIZE) {
                send_error_response(client_fd, 400, 0);
                break;
            }

            poll_ret = wait_for_client_data(client_fd, wait_ms);
            if (poll_ret == 0) {
                send_error_response(client_fd, 408, 0);
                break;
            }
            if (poll_ret < 0) {
                break;
            }

            if (used == 0) {
                request_start_ms = now_millis();
            }

            while (1) {
                ssize_t n = recv(client_fd, conn_buf + used, sizeof(conn_buf) - 1 - used, 0);
                if (n < 0) {
                    if (errno == EINTR) {
                        if (g_stop) goto done;
                        continue;
                    }
                    goto done;
                }
                if (n == 0) {
                    goto done;
                }
                used += (size_t)n;
                conn_buf[used] = '\0';
                break;
            }
            continue;
        }

        {
            size_t req_len = (size_t)header_pos + 4;
            char request_bytes[MAX_HEADER_SIZE + 1];
            Request *request;
            char host_value[4096];
            char content_length_value[256];
            int content_length = 0;
            int keep_alive = 0;
            int should_close = 0;
            int body_already_in_buffer;
            char *body_ptr;
            char *body = NULL;

            if (req_len > MAX_HEADER_SIZE) {
                send_error_response(client_fd, 400, 0);
                break;
            }

            memcpy(request_bytes, conn_buf, req_len);
            request_bytes[req_len] = '\0';

            request = parse_request_thread_safe(request_bytes, (int)req_len, client_fd);
            if (request == NULL) {
                send_error_response(client_fd, 400, 0);
                break;
            }

            if (!get_header_value(request, "Host", host_value, sizeof(host_value))) {
                send_error_response(client_fd, 400, 0);
                free_request(request);
                break;
            }

            if (strcmp(request->http_version, "HTTP/1.1") != 0) {
                send_error_response(client_fd, 505, 0);
                free_request(request);
                break;
            }

            keep_alive = !client_wants_close(request);

            if (get_header_value(request, "Content-Length", content_length_value, sizeof(content_length_value))) {
                if (!parse_content_length_value(content_length_value, &content_length)) {
                    send_error_response(client_fd, 400, 0);
                    free_request(request);
                    break;
                }
            }

            body_already_in_buffer = (int)(used - req_len);
            body_ptr = conn_buf + req_len;

            if (strcasecmp(request->http_method, "POST") == 0) {
                if (!is_cgi_request(request->http_uri)) {
                    send_error_response(client_fd, 501, 0);
                    free_request(request);
                    break;
                }

                if (!get_header_value(request, "Content-Length", content_length_value, sizeof(content_length_value))) {
                    send_error_response(client_fd, 411, 0);
                    free_request(request);
                    break;
                }

                if (!parse_content_length_value(content_length_value, &content_length)) {
                    send_error_response(client_fd, 400, 0);
                    free_request(request);
                    break;
                }
            }

            if ((strcasecmp(request->http_method, "GET") != 0) &&
                (strcasecmp(request->http_method, "HEAD") != 0) &&
                (strcasecmp(request->http_method, "POST") != 0)) {
                send_error_response(client_fd, 501, 0);
                free_request(request);
                break;
            }

            if (content_length > 0) {
                body = (char *)malloc((size_t)content_length);
                if (body == NULL) {
                    send_error_response(client_fd, 500, 0);
                    free_request(request);
                    break;
                }

                if (body_already_in_buffer > 0) {
                    int copy_len = body_already_in_buffer;
                    if (copy_len > content_length) {
                        copy_len = content_length;
                    }
                    memcpy(body, body_ptr, (size_t)copy_len);

                    if (copy_len < content_length) {
                        int read_ret = read_exact_body(client_fd, body, copy_len, content_length, timeout_seconds);
                        if (read_ret == -2) {
                            send_error_response(client_fd, 408, 0);
                            free(body);
                            free_request(request);
                            break;
                        }
                        if (read_ret < 0) {
                            free(body);
                            free_request(request);
                            break;
                        }
                    }
                } else {
                    int read_ret = read_exact_body(client_fd, body, 0, content_length, timeout_seconds);
                    if (read_ret == -2) {
                        send_error_response(client_fd, 408, 0);
                        free(body);
                        free_request(request);
                        break;
                    }
                    if (read_ret < 0) {
                        free(body);
                        free_request(request);
                        break;
                    }
                }
            }

            if (is_cgi_request(request->http_uri)) {
                if (serve_cgi_request(client_fd,
                                      cgi_handler,
                                      listen_port,
                                      request,
                                      body,
                                      content_length,
                                      client_addr) < 0) {
                    free(body);
                    free_request(request);
                    break;
                }

                fprintf(stdout, "%s \"%s %s %s\" CGI\n",
                        client_ip[0] ? client_ip : "-",
                        request->http_method,
                        request->http_uri,
                        request->http_version);

                /*
                 * I close the connection after CGI on purpose.
                 * README says persistent/pipelined CGI support is not necessary here,
                 * and keeping it simple avoids extra bugs.
                 */
                should_close = 1;
            } else {
                if (strcasecmp(request->http_method, "POST") == 0) {
                    send_error_response(client_fd, 501, 0);
                    free(body);
                    free_request(request);
                    break;
                }

                if (content_length > 0) {
                    send_error_response(client_fd, 400, 0);
                    free(body);
                    free_request(request);
                    break;
                }

                if (serve_static_file(client_fd, root, request, keep_alive) < 0) {
                    free(body);
                    free_request(request);
                    break;
                }

                fprintf(stdout, "%s \"%s %s %s\" 200\n",
                        client_ip[0] ? client_ip : "-",
                        request->http_method,
                        request->http_uri,
                        request->http_version);
            }

            {
                size_t available_after_header = used - req_len;
                size_t consumed_from_buffer = req_len;

                if (content_length > 0) {
                    size_t body_taken_from_buffer = 0;
                    if ((size_t)content_length < available_after_header) {
                        body_taken_from_buffer = (size_t)content_length;
                    } else {
                        body_taken_from_buffer = available_after_header;
                    }
                    consumed_from_buffer += body_taken_from_buffer;
                }

                /*
                 * Move any extra bytes to the front.
                 * This is important for keep-alive and pipelining,
                 * because the next request may already be in the buffer.
                 */
                if (used > consumed_from_buffer) {
                    memmove(conn_buf, conn_buf + consumed_from_buffer, used - consumed_from_buffer);
                }
                used -= consumed_from_buffer;
                conn_buf[used] = '\0';
            }

            request_start_ms = (used > 0) ? now_millis() : -1;

            free(body);
            free_request(request);

            if (should_close || !keep_alive) {
                break;
            }
        }
    }

done:
    return;
}

static void *worker_main(void *arg) {
    ServerConfig *config = (ServerConfig *)arg;

    while (!g_stop) {
        struct sockaddr_in client_addr;
        int client_fd = work_queue_pop(&config->queue, &client_addr);

        if (client_fd < 0) {
            break;
        }

        handle_client(client_fd,
                      config->root,
                      config->cgi_handler,
                      config->listen_port,
                      config->timeout_seconds,
                      &client_addr);

        close(client_fd);
    }

    return NULL;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s --port <listenPort> --root <wwwRoot> --numThreads <numThreads> --timeout <timeout> --cgiHandler <cgiProgram>\n",
            prog);
}

int main(int argc, char **argv) {
    int num_threads = 0;
    ServerConfig config;
    int server_fd;
    int opt;
    struct sockaddr_in server_addr;
    int yes = 1;
    int i;
    pthread_t *threads;

    memset(&config, 0, sizeof(config));

    static struct option long_options[] = {
        {"port", required_argument, 0, 'p'},
        {"root", required_argument, 0, 'r'},
        {"numThreads", required_argument, 0, 'n'},
        {"timeout", required_argument, 0, 't'},
        {"cgiHandler", required_argument, 0, 'c'},
        {0, 0, 0, 0}
    };

    while ((opt = getopt_long(argc, argv, "", long_options, NULL)) != -1) {
        switch (opt) {
            case 'p':
                config.listen_port = atoi(optarg);
                break;
            case 'r':
                snprintf(config.root, sizeof(config.root), "%s", optarg);
                break;
            case 'n':
                num_threads = atoi(optarg);
                break;
            case 't':
                config.timeout_seconds = atoi(optarg);
                break;
            case 'c':
                snprintf(config.cgi_handler, sizeof(config.cgi_handler), "%s", optarg);
                break;
            default:
                usage(argv[0]);
                return 1;
        }
    }

    if (config.listen_port <= 0 || config.root[0] == '\0' || num_threads <= 0 ||
        config.timeout_seconds <= 0 || config.cgi_handler[0] == '\0') {
        usage(argv[0]);
        return 1;
    }

    work_queue_init(&config.queue);

    if (install_signal_handlers() < 0) {
        perror("sigaction");
        return 1;
    }

    threads = (pthread_t *)malloc(sizeof(pthread_t) * (size_t)num_threads);
    if (threads == NULL) {
        perror("malloc");
        return 1;
    }

    for (i = 0; i < num_threads; i++) {
        if (pthread_create(&threads[i], NULL, worker_main, &config) != 0) {
            perror("pthread_create");
            work_queue_shutdown(&config.queue);
            while (--i >= 0) {
                pthread_join(threads[i], NULL);
            }
            work_queue_destroy(&config.queue);
            free(threads);
            return 1;
        }
    }

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        work_queue_shutdown(&config.queue);
        for (i = 0; i < num_threads; i++) {
            pthread_join(threads[i], NULL);
        }
        work_queue_destroy(&config.queue);
        free(threads);
        return 1;
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
        perror("setsockopt");
        close(server_fd);
        work_queue_shutdown(&config.queue);
        for (i = 0; i < num_threads; i++) {
            pthread_join(threads[i], NULL);
        }
        work_queue_destroy(&config.queue);
        free(threads);
        return 1;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons((uint16_t)config.listen_port);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        close(server_fd);
        work_queue_shutdown(&config.queue);
        for (i = 0; i < num_threads; i++) {
            pthread_join(threads[i], NULL);
        }
        work_queue_destroy(&config.queue);
        free(threads);
        return 1;
    }

    if (listen(server_fd, BACKLOG) < 0) {
        perror("listen");
        close(server_fd);
        work_queue_shutdown(&config.queue);
        for (i = 0; i < num_threads; i++) {
            pthread_join(threads[i], NULL);
        }
        work_queue_destroy(&config.queue);
        free(threads);
        return 1;
    }

    g_server_fd = server_fd;

    printf("Server listening on port %d, root=%s, threads=%d, timeout=%d, cgi=%s\n",
           config.listen_port, config.root, num_threads, config.timeout_seconds, config.cgi_handler);

    while (!g_stop) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);

        if (client_fd < 0) {
            if (errno == EINTR) {
                if (g_stop) break;
                continue;
            }
            if (g_stop || errno == EBADF) {
                break;
            }
            perror("accept");
            continue;
        }

        work_queue_push(&config.queue, client_fd, &client_addr);
    }

    /*
     * Cleanup part for graceful shutdown.
     * Before adding this style of cleanup, Ctrl+C often left ugly valgrind output
     * because workers and socket were not shut down in an orderly way.
     */
    if (g_server_fd >= 0) {
        close(g_server_fd);
        g_server_fd = -1;
    }

    work_queue_shutdown(&config.queue);

    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    work_queue_destroy(&config.queue);
    free(threads);

    return 0;
}