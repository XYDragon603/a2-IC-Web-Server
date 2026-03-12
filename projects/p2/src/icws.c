#include <arpa/inet.h>
#include <fcntl.h>
#include <getopt.h>
#include <netinet/in.h>
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
#define BACKLOG 128

typedef struct {
    char root[4096];
    int listen_port;
} ServerConfig;

static void format_http_time(time_t t, char *buf, size_t size) {
    struct tm tm_info;
    gmtime_r(&t, &tm_info);
    strftime(buf, size, "%a, %d %b %Y %H:%M:%S GMT", &tm_info);
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
        if (n <= 0) return -1;
        sent += (size_t)n;
    }
    return 0;
}

static int send_error_response(int fd, int status_code) {
    char response[4096];
    char date_buf[128];
    int body_len;
    int total_len;

    format_http_time(time(NULL), date_buf, sizeof(date_buf));
    char body[512];
    body_len = snprintf(body, sizeof(body),
                        "<html><body><h1>%d %s</h1></body></html>\n",
                        status_code, status_text(status_code));

    // Milestone 1: 永远返回 Connection: close
    total_len = snprintf(response, sizeof(response),
                          "HTTP/1.1 %d %s\r\n"
                          "Date: %s\r\n"
                          "Server: icws-student/1.0\r\n"
                          "Connection: close\r\n"
                          "Content-Type: text/html\r\n"
                          "Content-Length: %d\r\n"
                          "\r\n"
                          "%s",
                          status_code, status_text(status_code), date_buf, body_len, body);

    if (send_all_socket(fd, response, (size_t)total_len) < 0) return -1;
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

static ssize_t find_header_end(const char *buf, size_t len) {
    if (len < 4) return -1;
    for (size_t i = 0; i + 3 < len; i++) {
        if (buf[i] == '\r' && buf[i + 1] == '\n' &&
            buf[i + 2] == '\r' && buf[i + 3] == '\n') {
            return (ssize_t)i;
        }
    }
    return -1;
}

static int build_file_path(const char *root, const char *uri, char *out, size_t out_size) {
    char uri_copy[4096];
    if (uri == NULL || uri[0] != '/') return -1;

    snprintf(uri_copy, sizeof(uri_copy), "%s", uri);
    char *query_pos = strchr(uri_copy, '?');
    if (query_pos != NULL) *query_pos = '\0';

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

static int serve_static_file(int fd, const char *root, Request *request) {
    char path[8192];
    char header[4096];
    char date_buf[128];
    char modified_buf[128];
    char file_buf[IO_BUFFER_SIZE];
    struct stat st;
    int file_fd;
    int header_len;

    if (build_file_path(root, request->http_uri, path, sizeof(path)) < 0) {
        return send_error_response(fd, 400);
    }

    if (stat(path, &st) < 0 || !S_ISREG(st.st_mode)) {
        return send_error_response(fd, 404);
    }

    file_fd = open(path, O_RDONLY);
    if (file_fd < 0) return send_error_response(fd, 404);

    format_http_time(time(NULL), date_buf, sizeof(date_buf));
    format_http_time(st.st_mtime, modified_buf, sizeof(modified_buf));
    const char *mime_type = get_mime_type(path);

    header_len = snprintf(header, sizeof(header),
                          "HTTP/1.1 200 OK\r\n"
                          "Date: %s\r\n"
                          "Server: icws-student/1.0\r\n"
                          "Connection: close\r\n"
                          "Content-Type: %s\r\n"
                          "Content-Length: %ld\r\n"
                          "Last-Modified: %s\r\n"
                          "\r\n",
                          date_buf, mime_type, (long)st.st_size, modified_buf);

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
        if (n <= 0) break;
        if (send_all_socket(fd, file_buf, (size_t)n) < 0) break;
    }

    close(file_fd);
    return 0;
}

static void handle_client(int client_fd, const char *root) {
    char conn_buf[MAX_HEADER_SIZE + 1];
    size_t used = 0;

    while (1) {
        ssize_t n = recv(client_fd, conn_buf + used, MAX_HEADER_SIZE - used, 0);
        if (n <= 0) break;
        used += (size_t)n;
        conn_buf[used] = '\0';

        ssize_t header_pos = find_header_end(conn_buf, used);
        if (header_pos >= 0) {
            size_t req_len = (size_t)header_pos + 4;
            
            // Milestone 1 no lock is needed here!
            Request *request = parse(conn_buf, (int)req_len, client_fd);
            if (request == NULL) {
                send_error_response(client_fd, 400);
                break;
            }

            char host_value[4096];
            if (strcmp(request->http_version, "HTTP/1.1") == 0 && 
                !get_header_value(request, "Host", host_value, sizeof(host_value))) {
                send_error_response(client_fd, 400);
                free_request(request);
                break;
            }

            if (strcmp(request->http_version, "HTTP/1.1") != 0 && 
                strcmp(request->http_version, "HTTP/1.0") != 0) {
                send_error_response(client_fd, 505);
                free_request(request);
                break;
            }

            // Milestone 1: 只支持 GET 和 HEAD
            if (strcasecmp(request->http_method, "GET") != 0 &&
                strcasecmp(request->http_method, "HEAD") != 0) {
                send_error_response(client_fd, 501);
                free_request(request);
                break;
            }

            serve_static_file(client_fd, root, request);
            
            fprintf(stdout, "Processed Milestone 1 Request: %s %s\n", request->http_method, request->http_uri);
            free_request(request);
            break; //Disconnect immediately after sending. There's no need for a keep-alive loop
            
        } else if (used >= MAX_HEADER_SIZE) {
            send_error_response(client_fd, 400);
            break;
        }
    }
}

static void usage(const char *prog) {
    fprintf(stderr, "Usage: %s --port <listenPort> --root <wwwRoot>\n", prog);
}

int main(int argc, char **argv) {
    ServerConfig config;
    int server_fd;
    int opt;
    struct sockaddr_in server_addr;
    int yes = 1;

    memset(&config, 0, sizeof(config));

    // Milestone 1 the port and root parameters are needed
    static struct option long_options[] = {
        {"port", required_argument, 0, 'p'},
        {"root", required_argument, 0, 'r'},
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
            default:
                usage(argv[0]);
                return 1;
        }
    }

    if (config.listen_port <= 0 || config.root[0] == '\0') {
        usage(argv[0]);
        return 1;
    }

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
        perror("setsockopt"); close(server_fd); return 1;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons((uint16_t)config.listen_port);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind"); close(server_fd); return 1;
    }

    if (listen(server_fd, BACKLOG) < 0) {
        perror("listen"); close(server_fd); return 1;
    }

    printf("Milestone 1 Server listening on port %d, root=%s\n", config.listen_port, config.root);

    // Milestone 1: 
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        
        if (client_fd < 0) continue;
        
        // Process the request directly in the main thread and close it immediately after processing
        handle_client(client_fd, config.root);
        close(client_fd);
    }

    close(server_fd);
    return 0;
}