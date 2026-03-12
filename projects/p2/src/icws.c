#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
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
#include <time.h>
#include <unistd.h>

#include "parse.h"

#define MAX_HEADER_SIZE 8192
#define CONN_BUFFER_SIZE 65536
#define IO_BUFFER_SIZE 4096
#define BACKLOG 128

// Must lock the parser because lex/yacc generated code is not thread-safe
static pthread_mutex_t parser_mutex = PTHREAD_MUTEX_INITIALIZER;

// Globals for graceful shutdown
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
    int timeout_seconds;
    int listen_port;
    WorkQueue queue;
} ServerConfig;

// ================== Signal Handlers ==================
static void handle_shutdown_signal(int signo) {
    (void)signo;
    g_stop = 1;
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

    if (sigaction(SIGINT, &sa, NULL) < 0) return -1;
    if (sigaction(SIGTERM, &sa, NULL) < 0) return -1;
    signal(SIGPIPE, SIG_IGN); // Ignore SIGPIPE to prevent server crash
    return 0;
}

//Work Queue Implementation
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
    pthread_cond_broadcast(&queue->cond); // Wake up all sleeping threads
    pthread_mutex_unlock(&queue->mutex);
}

static void work_queue_destroy(WorkQueue *queue) {
    JobNode *cur = queue->head;
    while (cur != NULL) {
        JobNode *next = cur->next;
        close(cur->client_fd);
        free(cur);
        cur = next;
    }
    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->cond);
}

static void work_queue_push(WorkQueue *queue, int client_fd, struct sockaddr_in *client_addr) {
    JobNode *node = (JobNode *)malloc(sizeof(JobNode));
    if (node == NULL) { close(client_fd); return; }

    node->client_fd = client_fd;
    node->client_addr = *client_addr;
    node->next = NULL;

    pthread_mutex_lock(&queue->mutex);
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
    // Sleep if there is no work
    while (queue->head == NULL && !queue->shutting_down) {
        pthread_cond_wait(&queue->cond, &queue->mutex);
    }
    
    if (queue->head == NULL && queue->shutting_down) {
        pthread_mutex_unlock(&queue->mutex);
        return -1;
    }
    
    node = queue->head;
    queue->head = node->next;
    if (queue->head == NULL) queue->tail = NULL;
    pthread_mutex_unlock(&queue->mutex);

    client_fd = node->client_fd;
    *client_addr = node->client_addr;
    free(node);
    return client_fd;
}

//Helper Function
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
    return "application/octet-stream";
}

static const char *status_text(int code) {
    switch (code) {
        case 200: return "OK";
        case 400: return "Bad Request";
        case 404: return "Not Found";
        case 408: return "Request Timeout";
        case 411: return "Length Required";
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

static int send_error_response(int fd, int status_code, int keep_alive) {
    char response[4096];
    char date_buf[128];
    const char *conn_text = keep_alive ? "keep-alive" : "close";
    int body_len;
    int total_len;

    format_http_time(time(NULL), date_buf, sizeof(date_buf));
    char body[512];
    body_len = snprintf(body, sizeof(body),
                        "<html><body><h1>%d %s</h1></body></html>\n",
                        status_code, status_text(status_code));

    // Combine header and body to reduce syscalls
    total_len = snprintf(response, sizeof(response),
                          "HTTP/1.1 %d %s\r\n"
                          "Date: %s\r\n"
                          "Server: icws-student/2.0\r\n"
                          "Connection: %s\r\n"
                          "Content-Type: text/html\r\n"
                          "Content-Length: %d\r\n"
                          "\r\n"
                          "%s",
                          status_code, status_text(status_code), date_buf, conn_text, body_len, body);

    if (send_all_socket(fd, response, (size_t)total_len) < 0) return -1;
    return 0;
}

static int get_header_value(Request *request, const char *name, char *out, size_t out_size) {
    for (int i = 0; i < request->header_count; i++) {
        if (strcasecmp(request->headers[i].header_name, name) == 0) {
            snprintf(out, out_size, "%s", request->headers[i].header_value);
            return 1;
        }
    }
    return 0;
}

static ssize_t find_header_end(const char *buf, size_t len) {
    if (len < 4) return -1;
    for (size_t i = 0; i + 3 < len; i++) {
        if (buf[i] == '\r' && buf[i + 1] == '\n' && buf[i + 2] == '\r' && buf[i + 3] == '\n') {
            return (ssize_t)i;
        }
    }
    return -1;
}

static int wait_for_client_data(int client_fd, int timeout_ms) {
    struct pollfd pfd;
    pfd.fd = client_fd;
    pfd.events = POLLIN;
    pfd.revents = 0;
    while (1) {
        int poll_ret = poll(&pfd, 1, timeout_ms);
        if (poll_ret < 0 && errno == EINTR) {
            if (g_stop) return -1;
            continue;
        }
        return poll_ret;
    }
}

static Request *parse_request_thread_safe(char *request_bytes, int req_len, int client_fd) {
    Request *request;
    pthread_mutex_lock(&parser_mutex);
    request = parse(request_bytes, req_len, client_fd);
    pthread_mutex_unlock(&parser_mutex);
    return request;
}

static int build_file_path(const char *root, const char *uri, char *out, size_t out_size) {
    char uri_copy[4096];
    if (uri == NULL || uri[0] != '/') return -1;
    snprintf(uri_copy, sizeof(uri_copy), "%s", uri);
    
    char *query_pos = strchr(uri_copy, '?');
    if (query_pos != NULL) *query_pos = '\0';

    // Prevent directory traversal attacks
    if (strstr(uri_copy, "..") != NULL || strchr(uri_copy, '\\') != NULL) return -1;

    if (strcmp(uri_copy, "/") == 0) {
        snprintf(out, out_size, "%s/index.html", root);
    } else if (uri_copy[strlen(uri_copy) - 1] == '/') {
        snprintf(out, out_size, "%s%sindex.html", root, uri_copy);
    } else {
        snprintf(out, out_size, "%s%s", root, uri_copy);
    }
    return 0;
}

static int serve_static_file(int fd, const char *root, Request *request, int keep_alive) {
    char path[8192];
    char header[4096];
    char date_buf[128];
    char modified_buf[128];
    char file_buf[IO_BUFFER_SIZE];
    struct stat st;
    
    if (build_file_path(root, request->http_uri, path, sizeof(path)) < 0) {
        return send_error_response(fd, 400, 0);
    }
    if (stat(path, &st) < 0 || !S_ISREG(st.st_mode)) {
        return send_error_response(fd, 404, keep_alive);
    }
    
    int file_fd = open(path, O_RDONLY);
    if (file_fd < 0) return send_error_response(fd, 404, keep_alive);

    format_http_time(time(NULL), date_buf, sizeof(date_buf));
    format_http_time(st.st_mtime, modified_buf, sizeof(modified_buf));
    
    int header_len = snprintf(header, sizeof(header),
                          "HTTP/1.1 200 OK\r\n"
                          "Date: %s\r\n"
                          "Server: icws-student/2.0\r\n"
                          "Connection: %s\r\n"
                          "Content-Type: %s\r\n"
                          "Content-Length: %ld\r\n"
                          "Last-Modified: %s\r\n"
                          "\r\n",
                          date_buf, keep_alive ? "keep-alive" : "close", get_mime_type(path), (long)st.st_size, modified_buf);

    if (send_all_socket(fd, header, (size_t)header_len) < 0) { close(file_fd); return -1; }
    
    if (strcasecmp(request->http_method, "HEAD") == 0) { close(file_fd); return 0; }

    while (1) {
        ssize_t n = read(file_fd, file_buf, sizeof(file_buf));
        if (n < 0) { if (errno == EINTR) continue; close(file_fd); return -1; }
        if (n == 0) break;
        if (send_all_socket(fd, file_buf, (size_t)n) < 0) { close(file_fd); return -1; }
    }
    close(file_fd);
    return 0;
}

//Main Client Handler
static void handle_client(int client_fd, const char *root, int timeout_seconds) {
    char conn_buf[CONN_BUFFER_SIZE];
    size_t used = 0;
    long request_start_ms = -1;

    memset(conn_buf, 0, sizeof(conn_buf));

    while (!g_stop) {
        ssize_t header_pos = find_header_end(conn_buf, used);

        if (header_pos < 0) {
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

            int poll_ret = wait_for_client_data(client_fd, wait_ms);
            if (poll_ret == 0) { send_error_response(client_fd, 408, 0); break; }
            if (poll_ret < 0) break;
            
            if (used == 0) request_start_ms = now_millis();

            while (1) {
                ssize_t n = recv(client_fd, conn_buf + used, sizeof(conn_buf) - 1 - used, 0);
                if (n < 0) { if (errno == EINTR && !g_stop) continue; goto done; }
                if (n == 0) goto done;
                used += (size_t)n;
                conn_buf[used] = '\0';
                break;
            }
            continue;
        }

        size_t req_len = (size_t)header_pos + 4;
        char request_bytes[MAX_HEADER_SIZE + 1];
        if (req_len > MAX_HEADER_SIZE) {
            send_error_response(client_fd, 400, 0);
            break;
        }

        memcpy(request_bytes, conn_buf, req_len);
        request_bytes[req_len] = '\0';

        Request *request = parse_request_thread_safe(request_bytes, (int)req_len, client_fd);
        if (request == NULL) { send_error_response(client_fd, 400, 0); break; }

        char host_value[4096];
        if (strcmp(request->http_version, "HTTP/1.1") == 0 && 
            !get_header_value(request, "Host", host_value, sizeof(host_value))) {
            send_error_response(client_fd, 400, 0);
            free_request(request); break;
        }
        
        if (strcmp(request->http_version, "HTTP/1.1") != 0 && strcmp(request->http_version, "HTTP/1.0") != 0) {
            send_error_response(client_fd, 505, 0);
            free_request(request); break;
        }

        int keep_alive = (strcmp(request->http_version, "HTTP/1.1") == 0) ? 1 : 0;
        char conn_val[4096];
        if (get_header_value(request, "Connection", conn_val, sizeof(conn_val))) {
            if (strcasecmp(conn_val, "keep-alive") == 0) keep_alive = 1;
            else if (strcasecmp(conn_val, "close") == 0) keep_alive = 0;
        }

        // Milestone 2 only supports GET and HEAD
        if (strcasecmp(request->http_method, "GET") != 0 && strcasecmp(request->http_method, "HEAD") != 0) {
            send_error_response(client_fd, 501, 0);
            free_request(request); break;
        }

        if (serve_static_file(client_fd, root, request, keep_alive) < 0) {
            free_request(request); break;
        }

        // Shift remaining bytes to the front for pipelined requests
        size_t available_after_header = used - req_len;
        if (used > req_len) {
            memmove(conn_buf, conn_buf + req_len, available_after_header);
        }
        used -= req_len;
        conn_buf[used] = '\0';
        request_start_ms = (used > 0) ? now_millis() : -1;

        free_request(request);
        if (!keep_alive) break;
    }
done:
    return;
}

static void *worker_main(void *arg) {
    ServerConfig *config = (ServerConfig *)arg;
    while (!g_stop) {
        struct sockaddr_in client_addr;
        int client_fd = work_queue_pop(&config->queue, &client_addr);
        if (client_fd < 0) break;
        
        handle_client(client_fd, config.root, config.timeout_seconds);
        close(client_fd);
    }
    return NULL;
}

static void usage(const char *prog) {
    fprintf(stderr, "Usage: %s --port <listenPort> --root <wwwRoot> --numThreads <numThreads> --timeout <timeout>\n", prog);
}

int main(int argc, char **argv) {
    int num_threads = 0;
    ServerConfig config;
    int opt;
    memset(&config, 0, sizeof(config));

    // Milestone 2 params: NO cgiHandler
    static struct option long_options[] = {
        {"port", required_argument, 0, 'p'},
        {"root", required_argument, 0, 'r'},
        {"numThreads", required_argument, 0, 'n'},
        {"timeout", required_argument, 0, 't'},
        {0, 0, 0, 0}
    };

    while ((opt = getopt_long(argc, argv, "", long_options, NULL)) != -1) {
        switch (opt) {
            case 'p': config.listen_port = atoi(optarg); break;
            case 'r': snprintf(config.root, sizeof(config.root), "%s", optarg); break;
            case 'n': num_threads = atoi(optarg); break;
            case 't': config.timeout_seconds = atoi(optarg); break;
            default: usage(argv[0]); return 1;
        }
    }

    if (config.listen_port <= 0 || config.root[0] == '\0' || num_threads <= 0 || config.timeout_seconds <= 0) {
        usage(argv[0]); return 1;
    }

    work_queue_init(&config.queue);
    if (install_signal_handlers() < 0) return 1;

    pthread_t *threads = (pthread_t *)malloc(sizeof(pthread_t) * (size_t)num_threads);
    for (int i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, worker_main, &config);
    }

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int yes = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons((uint16_t)config.listen_port);

    bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
    listen(server_fd, BACKLOG);
    g_server_fd = server_fd;

    printf("Milestone 2 Server listening on port %d, root=%s, threads=%d, timeout=%d\n",
           config.listen_port, config.root, num_threads, config.timeout_seconds);

    while (!g_stop) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) { if (g_stop) break; continue; }
        
        // Disable Nagle's algorithm to speed up pipelined benchmarking
        int flag = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));

        work_queue_push(&config.queue, client_fd, &client_addr);
    }

    work_queue_shutdown(&config.queue);
    for (int i = 0; i < num_threads; i++) pthread_join(threads[i], NULL);
    work_queue_destroy(&config.queue);
    free(threads);
    return 0;
}