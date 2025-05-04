/* client for a small memory cache daemon
 * 
 */ 

#define _POSIX_C_SOURCE 199309L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <pthread.h>
#include <time.h>
#include <assert.h>

#include "mcached.h"

#define PORT 11211
#define MAX_THREADS 1000

// used for printf
pthread_mutex_t pmutex = PTHREAD_MUTEX_INITIALIZER;

struct thread_args {
    int thread_num;
    int port;
    char *server_ip;
};

char* get_opcode_string(char opcode) {
    switch (opcode) {
    case CMD_ADD:
        return "ADD";
    case CMD_DELETE:
        return "DELETE";
    case CMD_GET:
        return "GET";
    case CMD_SET:
        return "SET";
    case CMD_VERSION:
        return "VERSION";
    default:
        return "[UNKNOWN]";
    }
}

char *get_status_string(uint16_t status) {
    switch (status) {
    case RES_OK:
        return "OK";
    case RES_NOT_FOUND:
        return "NOT FOUND";
    case RES_EXISTS:
        return "ALREADY EXISTS";
    case RES_ERROR:
        return "ERROR";
    default:
        return "UNKNOWN";
    }
}

/* send a request to the server */ 
void send_request(int sock, uint8_t cmd, const uint8_t *key, const uint8_t *val, uint16_t keylen, uint32_t vallen, int thread_num) {
    char buffer[sizeof(memcache_req_header_t)] = {0};
    memcache_req_header_t *hdr = (memcache_req_header_t *)buffer;
    hdr->magic = 0x80;
    hdr->opcode = cmd;

    hdr->key_length = htons(keylen);
    hdr->total_body_length = htonl(keylen + vallen);

    write(sock, buffer, sizeof(memcache_req_header_t));
    write(sock, key, keylen);
    write(sock, val, vallen);

    // thread number; command; key (for add, delete, get, set); value (for add and set)
    pthread_mutex_lock(&pmutex);
    printf("Thread %d; ", thread_num);
    printf("REQUEST; Command: %s; ", get_opcode_string(cmd));
    if (keylen != 0) {
        printf("Key(hex): ");
        int j = 0;
        while (j < keylen) {
            printf("%02x", *(key + j));
            j++;
        }
        printf("; ");
    }
    if (vallen != 0) {
        printf(" Value(hex): ");
        int j = 0;
        while (j < vallen) {
            printf("%02x", *(val + j));
            j++;
        }
        printf("; ");
    }
    printf("\n");
    pthread_mutex_unlock(&pmutex);
}

/* receive a response from the server */ 
void receive_response(int sock, memcache_req_header_t* hdr, uint8_t **key, uint8_t **value, int thread_num) {
  //    uint32_t total_body_length;
  //    uint16_t vbucket_id;
  ssize_t n = read(sock, hdr, sizeof(memcache_req_header_t));
  if (n != sizeof(memcache_req_header_t)) {
    pthread_mutex_lock(&pmutex);
    printf("Thread %d; ", thread_num);
    printf("FAILURE: Couldn't read header from socket. `read` returned: %ld.\n", n);
    pthread_mutex_unlock(&pmutex);
    exit(-1);
  }
    uint32_t body_len = ntohl(hdr->total_body_length);
    uint16_t key_len = ntohs(hdr->key_length);
    
    if (body_len > 0) {
        uint8_t *body = malloc(body_len);
        n = read(sock, body, body_len);
        if (n != body_len) {
            free(body);
            pthread_mutex_lock(&pmutex);
            printf("Thread %d; ", thread_num);
            printf("FAILURE: Couldn't read body from socket. `read` returned: %ld.\n", n);
            pthread_mutex_unlock(&pmutex);
            exit(-1);
        }
        *key = body;
        *value = body + key_len;
    }


  

    pthread_mutex_lock(&pmutex);
    printf("Thread %d; ", thread_num);
    printf("RESPONSE; Magic: 0x%02x; Command: %s; ", hdr->magic, get_opcode_string(hdr->opcode));
    printf("Status (vbucket_id): %d:%s; Total body length: %d;",
        ntohs(hdr->vbucket_id), get_status_string(ntohs(hdr->vbucket_id)),
        body_len);

    if (key_len != 0) {
        printf("Key: 0x");
        int j = 0;
        while (j < key_len) {
	  printf("%p",  ( *(key + j)));
            j++;
        }
        printf("; ");
    }
    uint32_t vallen = body_len - key_len;
    if (vallen != 0) {
        printf(" Value: 0x");
        int j = 0;
        while (j < vallen) {
            printf("%02x", *(*value + j));
            j++;
        }
        printf("; ");
    }
    printf("\n");
    pthread_mutex_unlock(&pmutex);
}

/* make sure the server's response is valid
 * check the magic number, vbucket ID, and length
 */ 
void verify_correctness(int thread_num, memcache_req_header_t* hdr, memcache_req_header_t* exphdr,
    uint8_t *value, uint8_t *expvalue) {
    pthread_mutex_lock(&pmutex);
    if (hdr->magic != exphdr->magic) {
        printf("Thread %d; ", thread_num);
        printf("FAILURE: Unexpected magic value. Expected: 0x%02x. Got: 0x%02x\n", exphdr->magic, hdr->magic);
        exit(-1);
    }
    if (hdr->opcode != exphdr->opcode) {
        printf("Thread %d; ", thread_num);
        printf("FAILURE: Unexpected command. Expected: 0x%02x (%s). Got: 0x%02x (%s)\n",
            exphdr->opcode, get_opcode_string(exphdr->opcode), hdr->opcode, get_opcode_string(hdr->opcode));
        exit(-1);
    }
    if (hdr->vbucket_id != exphdr->vbucket_id) {
        printf("Thread %d; ", thread_num);
        printf("FAILURE: Unexpected status (vbucket_id). Expected: %d:%s; Got: %d:%s\n",
            ntohs(exphdr->vbucket_id), get_status_string(ntohs(exphdr->vbucket_id))
            ,ntohs(hdr->vbucket_id), get_status_string(ntohs(hdr->vbucket_id)));
        exit(-1);
    }
    if (hdr->total_body_length != exphdr->total_body_length) {
        printf("Thread %d; ", thread_num);
        printf("FAILURE: Unexpected total body length. Expected: %d. Got: %d\n",
            ntohl(exphdr->total_body_length), ntohl(hdr->total_body_length));
        exit(-1);
    }
    if (exphdr->total_body_length != 0) {
        if (memcmp(value, expvalue, ntohl(exphdr->total_body_length)) != 0) {
            printf("Thread %d; ", thread_num);
            printf("FAILURE: Value does not match. Please check REQUEST and RESPONSE logs for expected and obtained value");
            exit(-1);
        }
    }
    pthread_mutex_unlock(&pmutex);
}

void* worker_thread(void *arg) {
    struct thread_args targs = *(struct thread_args *)arg;

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in server = {0};
    server.sin_family = AF_INET;
    server.sin_port = htons(targs.port);
    inet_pton(AF_INET, targs.server_ip, &server.sin_addr);

    int thread_num = targs.thread_num;

    if (connect(sock, (struct sockaddr*)&server, sizeof(server)) != 0) {
        pthread_mutex_lock(&pmutex);
        printf("Thread %d; ", thread_num);
        printf("FAILURE: Couldn't connect to server.\n");
        pthread_mutex_unlock(&pmutex);
        exit(-1);
    }

    // create a key value pair in this format:
    // key: thread_num 0 thread_num 0 (to verify that 0s are allowed in keys)
    // value: {thread_num 0} * (thread_num + 1 * 100) -- * = repeat
    uint8_t *key = malloc(4);
    uint16_t keylen = 4;
    for (int i = 0; i < 4; i++) {
        key[i] = (i % 2 == 0)?thread_num:0;
    }
    uint32_t vallen = 2 * (thread_num + 1) * 5;
    uint8_t *value = malloc(vallen);
    for (int i = 0; i < vallen; i++) {
        value[i] = (i % 2 == 0)?thread_num:0;
    }

    memcache_req_header_t hdr;

    // check opcode, magic byte, status

    // ADD
    send_request(sock, CMD_ADD, key, value, keylen, vallen, thread_num);
    uint8_t *keyr = NULL, *valuer = NULL;
    receive_response(sock, &hdr, &keyr, &valuer, thread_num);
    memcache_req_header_t exp = {
        .magic = 0x81,
        .opcode = CMD_ADD,
        .vbucket_id = htons(RES_OK),
        .total_body_length = htonl(0),
    };
    verify_correctness(thread_num, &hdr, &exp, valuer, value);
    free((void *)keyr);
    // no need to free this since same block of memory
    // free((void *)valuer);

    // OUTPUT   
    printf("sending output \n \n");
    keyr = NULL, valuer = NULL;
    send_request(sock, CMD_OUTPUT, NULL, NULL, 0, 0, thread_num);
    receive_response(sock, &hdr, &keyr, &valuer, thread_num);
    assert(hdr.magic == 0x81);
    assert(hdr.opcode == CMD_OUTPUT);
    assert(hdr.vbucket_id == htons(RES_OK));
    free(keyr);

    
    // GET
    keyr = NULL, valuer = NULL;
    send_request(sock, CMD_GET, key, NULL, keylen, 0, thread_num);
    receive_response(sock, &hdr, &keyr, &valuer, thread_num);
    exp.opcode = CMD_GET;
    exp.vbucket_id = htons(RES_OK);
    exp.total_body_length = htonl(vallen);
    verify_correctness(thread_num, &hdr, &exp, valuer, value);
    free((void *)keyr);

    // SET
    keyr = NULL, valuer = NULL;
    value[vallen - 1] = 198;
    send_request(sock, CMD_SET, key, value, keylen, vallen, thread_num);
    receive_response(sock, &hdr, &keyr, &valuer, thread_num);
    exp.opcode = CMD_SET;
    exp.vbucket_id = htons(RES_OK);
    exp.total_body_length = htonl(0);
    verify_correctness(thread_num, &hdr, &exp, valuer, value);

    // GET
    keyr = NULL, valuer = NULL;
    send_request(sock, CMD_GET, key, NULL, keylen, 0, thread_num);
    receive_response(sock, &hdr, &keyr, &valuer, thread_num);
    exp.opcode = CMD_GET;
    exp.vbucket_id = htons(RES_OK);
    exp.total_body_length = htonl(vallen);
    verify_correctness(thread_num, &hdr, &exp, valuer, value);
    free((void *)keyr);

    // DELETE
    keyr = NULL, valuer = NULL;
    send_request(sock, CMD_DELETE, key, NULL, keylen, 0, thread_num);
    receive_response(sock, &hdr, &keyr, &valuer, thread_num);
    exp.opcode = CMD_DELETE;
    exp.vbucket_id = htons(RES_OK);
    exp.total_body_length = htonl(0);
    verify_correctness(thread_num, &hdr, &exp, valuer, value);

    // GET after DELETE
    keyr = NULL, valuer = NULL;
    send_request(sock, CMD_GET, key, NULL, keylen, 0, thread_num);
    receive_response(sock, &hdr, &keyr, &valuer, thread_num);
    exp.opcode = CMD_GET;
    exp.vbucket_id = htons(RES_NOT_FOUND);
    exp.total_body_length = htonl(0);
    verify_correctness(thread_num, &hdr, &exp, valuer, value);

    // VERSION
    keyr = NULL, valuer = NULL;
    send_request(sock, CMD_VERSION, key, NULL, keylen, 0, thread_num);
    receive_response(sock, &hdr, &keyr, &valuer, thread_num);
    exp.opcode = CMD_VERSION;
    exp.vbucket_id = htons(RES_OK);
    exp.total_body_length = htonl(strlen("C-Memcached 1.0"));
    verify_correctness(thread_num, &hdr, &exp, valuer, (uint8_t *) "C-Memcached 1.0");
    free((void*)keyr);

    // DELETE something that doesn't exist
    keyr = NULL, valuer = NULL;
    key[keylen - 1] = 1;
    send_request(sock, CMD_DELETE, key, NULL, keylen, 0, thread_num);
    receive_response(sock, &hdr, &keyr, &valuer, thread_num);
    exp.opcode = CMD_DELETE;
    exp.vbucket_id = htons(RES_NOT_FOUND);
    exp.total_body_length = htonl(0);
    verify_correctness(thread_num, &hdr, &exp, valuer, value);

    // OUTPUT   
    printf("sending output \n \n");
    keyr = NULL, valuer = NULL;
    send_request(sock, CMD_OUTPUT, NULL, NULL, 0, 0, thread_num);
    receive_response(sock, &hdr, &keyr, &valuer, thread_num);
    assert(hdr.magic == 0x81);
    assert(hdr.opcode == CMD_OUTPUT);
    assert(hdr.vbucket_id == htons(RES_OK));
    free(keyr);

    close(sock);
    free(arg);
    return NULL;
}

int main(int argc, char **argv) {
    if (argc != 4) {
        printf("Usage: <command> <server_ip_address> <server_port> <number of threads>\n");
        exit(-1);
    }
    int num_threads = argc > 1 ? atoi(argv[3]) : 4;
    if (num_threads > MAX_THREADS) num_threads = MAX_THREADS;

    struct timespec start, end;
    double elapsed;
    clock_gettime(CLOCK_MONOTONIC, &start);

    pthread_t threads[MAX_THREADS];
    for (int i = 0; i < num_threads; i++) {
        struct thread_args *targs = malloc(sizeof(struct thread_args));
        targs->server_ip = argv[1];
        targs->port = atoi(argv[2]);
        targs->thread_num = i;
        pthread_create(&threads[i], NULL, worker_thread, targs);
    }

    for (int i = 0; i < num_threads; ++i)
        pthread_join(threads[i], NULL);
    clock_gettime(CLOCK_MONOTONIC, &end);
    
    elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;

    printf("SUCCESS: Test case passed. Time: %f seconds\n", elapsed);

    return 0;
}
