# PCSA Project 2: IC Web Server
(Credit: Aj. Kanat)

You are building a web server. In terms of features, your server will 

* handle a subset of the HTTP/1.1 protocol, as detailed in RFC2616, and
* support the common gateway interface (CGI), as detailed in RFC3875. 

In terms of performance, your server is expected to be highly concurrent&mdash;capable of serving tens of hundreds of requests of static requests per second.

This is an individual project, and it has remarkable depth. Your web server is expected to be fully functioning and capable of running interactive applications via CGI. Unquestionably, your codebase for this project will be the largest and the most sophisticated low-level programming project to date.

As you set out to design and implement this project, keep the following in mind:

* Code quality matters. Writing clean code with appropriate comments will help you debug and save you much frustration later.
* Code robustness is crucial. The Internet is a hostile environment. Be sure to handle errors. Be careful with buffer overflow. Be careful with possible nefarious actions from the network or the user side.
* Code efficiency pays. Efficient code can give you a higher throughput. Choose algorithms with the right complexity. Avoid useless buffer copying. Use an suitable buffer size. Avoid syscall overhead when possible.

## Code Management
**You are required to use `git` as your version control system.** While building a program of this size and complexity, version control is a godsend feature: things will inevitably go wrong and you will want to roll back. You will learn to appreciate the safety net that it offers.

**Commit to the repository as often as it makes sense.** If you commit too rarely, multiple changes will be clumped together. You can't select the pieces that work from pieces that break. In effect, you can't go back to the last working moment without tossing out a large chunk of code.

**At the completion of each milestone, you will be asked to tag it as a milestone release tag.** This is for record-keeping purposes.

**GitHub Classroom.** We are using GitHub classroom for this project. You are expected to push your changes to the repository as you make progress. The frequency and quality of your commits will factor into grading as well. At the end of each milestone, you will be asked tag your commit. Remember to push that tag. That will be used as a receipt for your submission of that milestone.

## Milestone Overview
You have about 4 weeks to deliver the project. To aid in planning, there are three milestones as outlined below:

**Milestone 1: Sequential `GET/HEAD` Requests for Static Content [30 points]**

_Due: 1 week after A02 was released_

You will extend the parser in the starter code to parse HTTP requests. Your code can robustly distinguish between proper requests and malformed requests. Your server can handle sequential requests from a user-specified port and respond suitably. If the request is `GET` or `HEAD` and the requested object is static, the server will return the appropriate data. In short, you will have a fully functioning Web server for static contents, except it cannot yet handle concurrent requests.

**Milestone 2: High-Throughput Concurrent Requests Using a Thread Pool [45 points]**

_Due: 2 week after A02 was released_

You will extend your Web server to serve multiple requests at once. More specifically, whereas the server at the end of Milestone 1 is only capable of handling one request at a time, your server for Milestone 2 will be able to handle tens of hundreds of requests per second. You will use a thread-based design (you learned about threads earlier and this is your chance to deeply understand it.) For performance, you are expected to design and implement a thread pool, so each request will be handled by a thread worker in the pool.

**Milestone 3: `POST` Request and Dynamic Content via CGI [25 points]**

_Due: 3 week after A02 was released_

You will your server to handle `POST` requests, as well as dynamic content via CGI. This means when a request of this kind comes in, the server will run the corresponding Linux program following the common gateway interface (CGI) protocol, as specified by RFC3875. Your server is expected to continue to be concurrent—for both static and dynamic requests.

## Server Design

You will implement this project in C/C++. You are not allowed to use any custom socket classes or libraries. Ultimately (though not relevant until Milestone 2), your server will handle concurrent requests using a thread pool. The server runs as a single process but it maintains a specified number of worker threads. The server code accepts a new connection and enters it into a queue of requests to be served. The first available worker picks up a connection from the queue and handles it.

## Milestone 1 Specifications
You will implement a robust sequential HTTP/1.1 server. RFC2616 (`https://tools.ietf.org/html/rfc2616`) has the full specification. For this milestone, your server only needs to support the following methods:

* `GET` — requests a specified resource; and
* `HEAD` — asks for an identical response as GET, without the actual body (i.e., no bytes from the requested resource)
For all other methods, the server will return `501` Method Unimplemented.

### Starter Code, Compilation, and Command-Line Argument

We are providing a starter pack, which you will clone from as soon as you join GitHub classroom. It contains an echo server program and a starter parser:

* The provided echo server code is an example of how you may wish to set up your signal handlers for long-running processes such as a server.
* As for the starter parser, we were told that parsing HTTP headers can be insanely tedious and we would like you to focus on other programming aspects. For this reason, we're providing you with a basic HTTP parser written in `lex` and Bison `yacc`. Take a look at `sample_parse.c` for an example of how to use the parser.

For the parser, look for TODOs in the source files for places that you will need to modify. The `lex` portion needs no modifications. However, you will need to extend the parsing code written in `yacc` and C. For this, you may find this tutorial useful.

### Compilation

There is a `Makefile` in the starter pack. Please modify it so that your code can be built using a single `make all` command. Notice that the provided `Makefile` calls `lex` and `yacc` to generate the relevant files already.

### Running Your Server

For now, the server will be passed the following arguments:
```
./icws --port <listenPort> --root <wwwRoot> 
```
The `listenPort` is the port the server will be listening to. The `wwwRoot` is the folder that is the root path for requests. For example, if the server receives a request for `/index.html`, it will attempt to deliver `<wwwRoot>/index.html` to fulfil the request.

**Pro Tips:** Learn to use `getopt_long` to help you parse the command-line argument.

### Submission

There are two things you will need to finish off the milestone. When you're ready to submit:

Tag your work with the tag `0.1.0` (see https://git-scm.com/book/en/v2/Git-Basics-Tagging for how).
Make sure to push the tags as well.

Sure! Here’s the raw Markdown content as plain text that you can copy and paste directly into your existing .md file:

## Milestone 2 Specifications

You will extend your code from the previous milestone to support concurrent requests. While there are many design options, you will implement a thread-pool-based design for pedagogical reasons, as well as practical reasons that make this the design of choice for many modern servers. The main aim of Milestone 2 will be robustness and performance. This means:

- You will measure the throughput of your server in this milestone: For a plain 1K .html file, we expect your server to be able to serve north of 500 requests/sec.
- Your server should not crash when receiving any kind of requests and should continue to function properly despite adverse requests.
- Care should be given to track down and fix memory leaks, as well as opened file descriptors that are no longer necessary.

### Thread Pool

You will implement a thread pool with a backing thread-safe work queue. When the pool is initially created, a number of threads will be spawned. This facilitates reuse of threads: instead of killing the thread after each job, the thread picks up a new job from the queue when it is no longer in use. The number of threads in the pool is configurable as a command-line argument.

In a nutshell, you will implement two components and the logic that enables them to work together:

#### Work Queue

This is a queue of jobs to be serviced. In the context of this project, a job is basically a connection that needs your service. It is up to you to design the work queue to be generic or cater it specifically to your web service. In either case, the work queue has to be thread-safe because it is highly likely accessed by multiple parties concurrently. Using mutexes from the previous segment will be sufficient. However, if you want to go overboard in the name of performance, learn about and implement a lock-free, atomic queue.

#### Thread Pool

This is simply a collection of threads. Initially all the threads in the pool are idle, awaiting work from the work queue. You are free to experiment with different scheduling heuristics. The simplest strategy is when a thread is idle and there is work in the queue, pick up that job and work on it. In your implementation, you will want your idle threads to sleep (i.e., not consuming any CPU) if the queue is empty. This means you cannot use busy wait— i.e., waiting in a (tight) loop. As we have explored in class, calling sleep or usleep can mitigate the problem, but we recommend that you implement proper signaling logic, conditional variables or semaphores. For this milestone, proper signaling will be sufficient. If you want to take it a step further, real professional-strength code use signaling and sleeping together. Ask the Internet for advice.

### Caveats

The parser code generated by lex/yacc is not thread-safe. You should avoid running parse from more than one thread at the same time. However, your server should not wait for a complete request to arrive before putting it into the pool because in pipelined requests (persistent connections), there may be more requests on this connection.

### Timeouts and Early Rejection: A Cure Against Nefarious Clients

In general, the syscall `read` will not return— i.e., it will block— until more bytes can be read or the connection is closed. However, connected clients can go out to lunch or are just malicious. They leave the connection opened without sending anything, so your thread will be stuck (forever) in `read`. For this reason, web servers need to abort after a suitable duration to avoid all threads being held up by slow or nefarious clients. In your code, learn to use the `poll` syscall.

```c
#include <poll.h>

int poll(struct pollfd *fds, nfds_t nfds, int timeout);
```

Using `poll`, you can determine whether your `connFd` is ready to be read. If so, calling `read` on it will not block. The `poll` function is handy in this situation because it has a timeout parameter, so `poll` will return the control to you after that duration has elapsed. You will use the timeout duration set in the command-line parameters passed to the server (see below).

Furthermore, some clients will (intentionally) send a few bytes punctuated by long pauses, just to prevent the server from disconnecting so they can hold up your threads. To guard against this type of clients, it is a good idea to ditch clients who could not complete a meaningful request within some duration (perhaps set this as a function of the user-defined timeout parameter).

Another optimization is to reject a client as soon as we detect that a request cannot be satisfied. For example, the method is GEH and is not supported.

### Other Details

#### Compilation

You will continue to update the Makefile so that your code can be built using a single `make all` command.

#### Running Your Server

For this milestone, the server will be passed the following arguments:

```bash
./icws --port <listenPort> --root <wwwRoot> --numThreads <numThreads> --timeout <timeout> 
```

The `listenPort` and `wwwRoot` parameters are like before. The new parameters are used as follows:

- `numThreads` is the number of threads you will pre-spawn and have ready in your thread pool
- `timeout` (in seconds) is how long the server is willing to wait for the client after the server doesn't hear anything from the client. When this duration has elapsed, the server can time out and close the connection.

### Submission for Milestone 2

There are two things you will need to finish off the milestone. When you're ready to submit:

- Tag your work with the tag `0.2.0` (see this guide for how).
- Make sure to push the tags as well.

Sure! Below is the raw Markdown text for Milestone 3 Specifications — you can copy and paste this directly into your .md file:

## Milestone 3 Specifications

You will extend your code from the previous milestone to support the common gateway interface (CGI), enabling the server to run an external program and serve dynamically-generated contents. You will aim to support CGI version 1.1, whose full specifications appear in RFC3875 (https://tools.ietf.org/html/rfc3875). Your server is expected to continue to be concurrent—for both static and dynamic requests.

### Lifecycle of Dynamic Requests

In a nutshell, CGI requests work as follows. For the purpose of this milestone, a CGI request differs from a traditional request in that the URI begins with the hard-coded path `/cgi/`. All requests going to `/cgi/*` will be handled by CGI, specifically by calling the `cgiProgram` as given in the command-line argument. This means the server code has to parse the URI to detect this special folder and dispatch it accordingly. Also, arguments passed via the URI may have to be "chopped off" according to the specifications.

For this milestone, CGI requests can only be GET, POST, and HEAD requests. You will discard all other types of requests.

After it parses the relevant headers, the job of your server is to pass along selected information to the CGI program. To execute the CGI program and allow the necessary communication, the server will fork a new process and run the CGI program in this process. There are 2 channels on which the server and the CGI child communicate:

1. The server will set certain environment variables (see below). Information obtained from request headers, as well general information about the server itself, will be passed to the child CGI in this way.
2. The server will pass any message body (e.g., for POST requests) via `stdin` to the CGI process.

Following that, the server will watch the CGI process for any output to `stdout` until the process terminates (the server code needs to monitor the process's status). The `stdout` output of the child will be relayed to the requesting client. Notice that according the RFC, the CGI child is responsible for generating the whole response, including crafting conformant response headers. If the CGI program fails in any way, return a `500` response to the client. Otherwise, send all bytes from the `stdout` of the spawned process to the requesting client.

Keep in mind that the server doesn't need to modify or inspect these bytes at all. In this role, the server is simply a messenger, facilitating communication between the client and the CGI program, so just deliver the data.

Finally, for CGI requests, the server should not attempt to support pipelined requests or persistent connections. This will only add unnecessary complications.

### Supported Environment Variables

Your server only needs to support the following CGI variables (refer to the RFC for further detail):

- `CONTENT_LENGTH` – taken directly from request  
- `CONTENT_TYPE` – taken directly from request  
- `GATEWAY_INTERFACE` – "CGI/1.1"  
- `PATH_INFO` – the `<path>` component of URI  
- `QUERY_STRING` – parsed from URI as everything after “?”  
- `REMOTE_ADDR` – taken when the `accept()` call is made  
- `REQUEST_METHOD` – taken directly from request  
- `REQUEST_URI` – taken directly from request  
- `SCRIPT_NAME` – hard-coded/configured application name (virtual path)  
- `SERVER_PORT` – as configured from command line  
- `SERVER_PROTOCOL` – "HTTP/1.1"  
- `SERVER_SOFTWARE` – the name of your server (set it as your heart contends)  
- `HTTP_ACCEPT` – taken directly from request  
- `HTTP_REFERER` – taken directly from request  
- `HTTP_ACCEPT_ENCODING` – taken directly from request  
- `HTTP_ACCEPT_LANGUAGE` – taken directly from request  
- `HTTP_ACCEPT_CHARSET` – taken directly from request  
- `HTTP_HOST` – taken directly from request  
- `HTTP_COOKIE` – taken directly from request  
- `HTTP_USER_AGENT` – taken directly from request  
- `HTTP_CONNECTION` – taken directly from request  

### UNIX Pipes

But how can the server communicate with a child process? The answer is **UNIX pipes**. Simply put, a pipe is a "fake" file allowing one process to write to and another process to read from. When a pipe is created, the kernel offers two file descriptors—one for writing into and the other for reading from. This tutorial has further detail. Notice that aside from pipe creation, pipes behave and act much the same way as other low-level I/O "files."

In our context, there is a pair of demo programs in `pipe-demo/`. The C program runs and talks to a child/"inferior" Python program. The Python program prints out the info passed to it from the C program and generates some numbers internally (It computes the Fibonacci sequence). Notice the `stdout` of the Python program is captured and the data is shown by the C code.

### Sample CGI Programs

To help test the server, we are providing the following CGI programs inside `cgi-demo/`:

- `dumper.py` displays the server environment (as indicated by the server) and all request parameters
- `hello.py` displays an appropriate greeting message to the query string `name`. For example, `http://serverAddr/cgi/?name=ICWS` will get a customized greeting message for the specified name.
- `banner.py` calls a remote service to generate an ASCII-art banner of the query string text. For example, `http://serverAddr/cgi/?text=ICWS` should return the following:

```
    _____ _______          _______
   |_   _/ ____\ \        / / ____|
     | || |     \ \  /\  / / (___
     | || |      \ \/  \/ / \___ \
    _| |_ |____   \  /\  /  ____) |
   |_____\_____|   \/  \/  |_____/
```

### Checklist

- (Optional) Log IP addresses, requests, and all errors to screen for debugging purposes. The recommended format is the common log format.
- Handle CGI requests correctly, including handling POST requests with a request body.
- (Optional) Set a timeout period for how long a CGI program can run. A good setting is the same as the timeout value for a normal request.
- Ensure you read and write files from disk with full error handling (permissions, file does not exist, IO errors, etc.).
- The server should also handle errors in a sane way. It should never completely crash (make it as robust as possible), send proper HTTP 1.1 error codes back to the browser and errors should be reported to a client as HTTP 1.1 error responses.

### Other Details

#### Compilation

You will continue to update the Makefile so that your code can be built using a single `make all` command.

#### Running Your Server

For this milestone, the server will be passed the following arguments:

```bash
./icws --port <listenPort> --root <wwwRoot> --numThreads <numThreads> --timeout <timeout> --cgiHandler <cgiProgram>
```

The `listenPort`, `wwwRoot`, `numThreads`, and `timeout` parameters are like before. The new parameter is used as follows:

- `cgiProgram` is a file that is a script or executable binary, which will be called for all requests to `/cgi/*` URIs. For example, this could be a Python script `cgi-demo/asciiart_cgi.py` and when requests to `/cgi/*` come in, this script will be run with the relevant env vars compliant with the CGI specifications.

### Benchmarking

Before you submit your work this milestone, we would like you to benchmark your server. Use the following steps:

1. Create a sample `test.html`, which will be 4096 bytes in size. The actual data doesn't really matter here.
2. Use Apache Bench to download this `test.html`. Adjust the amount of concurrency and the number of total requests to maximize your requests/second number.
3. Put the Apache Bench report in a file called `benchmarks.txt`.

### Submission for Milestone 3

There are a few things you will need to finish off this final milestone. Did you complete the benchmarking? When you're ready to submit:

- Delete all unnecessary files (e.g., `.o` and other generated binaries). This is to ensure that you have a clean work directory.
- Make sure your `benchmarks.txt` from above is included in the submission.
- Commit and tag your work with the tag `0.3.0` (see this guide for how).
- Make sure you've pushed the tags.

## Tips & Tricks

You probably have a pretty good idea about what a server must do from your in-class activities. The devil, however, is in the details. To this end, read (skim?) the relevant RFCs selectively. RFCs are written in a style that will be unfamiliar to most people. However, there are good reasons for you to become familiar with it as it is similar to the styles of many standards organizations. It is not our expectation that you read every page of the RFC, especially since you are only implementing a small subset of the full protocol, but you may well need to re-read critical sections a few times for the meaning to sink in.

Do not try to write the whole server for the milestone at once. Decompose the problem so that each piece is manageable and testable. For each request, identify the different cases that your server needs to handle. Find common tasks among different commands and group them into functions/classes to avoid writing the same code twice. You might start by implementing the routines that read and parse commands. Then implement commands one by one, testing each with telnet or write a quick Python script to test it.

When in doubt, be liberal in what you accept and conservative in what you send. Following this guiding principle of Internet design will help ensure your server works with many different and unexpected client behaviors.

When it comes to debugging, learn to use `gdb`, `efence`, `valgrind`. Compilation flags `-Wall` can help catch some common programming errors.

You are implementing a real, standards-compliant web server. Therefore, comparing protocol exchanges to existing web servers is both valid and encouraged. Install Apache web server or nginx, install Wireshark, and sniff the protocol exchanges and compare to your own. Or even use captured web browser requests to replay from files you save as input to your implementation.

## Frequently Asked Questions (FAQs)

**Is it guaranteed that the whole request headers will show up in the first read?**  
Not necessarily. For various reasons, your server should not assume that you will have the whole request block ready for parsing after a read (no matter how large the buffer is).

**Do we have to support condition GETs?**  
No.

**Can we assume/expect that HTTP/1.1 requests will fit in one buffer?**  
No, do not assume that they always fit inside one buffer. Be prepared to parse across buffer boundaries

**Do we have to support persistent HTTP?**  
Yes.

**Do we have to support pipelined requests?**  
Yes.

**What error codes does the server have to support?**  
You will need to support five HTTP/1.1 error codes: `400`, `404`, `408`, `501`, and `505`.  
- `404` is for files not found;  
- `408` is for connection timeouts (you're dealing with long, idle connection in Milestone 2);  
- `501` is for unsupported methods;  
- `505` is for bad version numbers.  
Everything else can be handled with a `400`.

**What MIME types do we need to support?**  
No need to support all well-known MIME types. Only the most common would be needed:

- `text/html`
- `text/css`
- `text/plain`
- `text/javascript`
- `image/jpg`
- `image/png`
- `image/gif`

Add other types as you need them. This [link](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types) may be useful.

**What header fields must be included in our response for GET and HEAD?**

- `Date`
- `Server`
- `Connection`
- `Content-Type`
- `Content-Length`
- `Last-Modified`

For Milestone 1, you may wish to return `Connection: close` and close the connection right after. For Milestone 2 and on, you will need to handle persistent HTTP properly if the client requests it.

**Can I assume that the request always has the Content-length field?**  
Yes, assume requests to your server have `Content-length` if applicable. You will not have to guess how many bytes the body will be. If it is missing, respond with `411`.

**In responding to a GET request, is it bad to read an entire file from disk and send the full response at that point?**  
Don't try sending the whole file— it can be huge! But do send a "full buffer" for every time you read from disk. Choosing a reasonable buffer size will be key.

**Are we allowed to reject requests with headers beyond a maximum size?**  
Please reject any header > 8192 bytes (8K). However, if the connection is persistent, your server must parse the next request properly.

**When parsing the headers, I have no idea how many header lines there are. What can my parser do?**  
It is true that you cannot know ahead of time how many header lines you'll have, but you can design a "grammar" to capture zero or more header lines. For example:

```c
request_headers: request_headers request_header {}
          | {};    /* Request headers can be empty */
```

However, each time you find a new `request_header` (i.e., a new header line), you will need to resize the array properly (the starter code has room for exactly 1 header line). You may find `realloc` useful, or if you're feeling somewhat adventurous and performance hungry, implement array doubling.

**What if I miss a milestone?**  
For each milestone due date, your hand-in will only be looked at briefly to see how much progress you have made. Only the final submission will be scrutinized. However, missing a milestone deadline (i.e., you have not delivered some features expected in that milestone) is an indication that you are behind on the timeline. With this in mind, you have one late token for this project, but you can only use it on the final code delivery (if you wish to).
