#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "parse.h"

int main(int argc, char **argv) {
    int fd_in;
    int index;
    int read_ret;
    char buf[8192];
    Request *request;

    if (argc < 2) {
        printf("usage: %s <request-file>\n", argv[0]);
        return 1;
    }

    fd_in = open(argv[1], O_RDONLY);
    if (fd_in < 0) {
        printf("Failed to open the file\n");
        return 1;
    }

    read_ret = read(fd_in, buf, sizeof(buf));
    close(fd_in);

    if (read_ret <= 0) {
        printf("Failed to read the file\n");
        return 1;
    }

    request = parse(buf, read_ret, -1);
    if (request == NULL) {
        printf("Parse failed\n");
        return 1;
    }

    printf("Http Method %s\n", request->http_method);
    printf("Http Version %s\n", request->http_version);
    printf("Http Uri %s\n", request->http_uri);

    for (index = 0; index < request->header_count; index++) {
        printf("Request Header\n");
        printf("Header name %s Header Value %s\n",
               request->headers[index].header_name,
               request->headers[index].header_value);
    }

    free_request(request);
    return 0;
}