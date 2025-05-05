#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include "mcached.h"
#include <netdb.h>
#include <fcntl.h>

#define MSG_SIZE 256

int numTableEntries;
int origSocket; // Socket for accepting connections (i.e. the fd)
pthread_mutex_t mutex;

// struct for entries of the table
typedef struct Entry {
    char *key;
    unsigned long hashKey;
    char *value;
    struct Entry *pNext;
} Entry;

Entry *table = NULL; // Create a table to hold key-value pairs as a LL

// DJB2 hash function from https://gist.github.com/MohamedTaha98/ccdf734f13299efb73ff0b12f7ce429f
unsigned long hash(char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++))
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    return hash % numTableEntries;
}

void get_timestamp(struct timespec *ts) {
    if (clock_gettime(CLOCK_REALTIME, ts) == -1) {
        perror("clock_gettime");
        exit(1);
    }
}

void *workerThread() {
    // Loop to keep server connecting to clients
    int flagConnect = 1; // Set to 0 to quit

    while (flagConnect) {
        int lengthClientName;
        struct sockaddr_in client_addr; // Client address information
        int newSocket; // Socket connected to client
        char buf[MSG_SIZE]; // Buffer for sending and receiving data

        // Accept a connection
        pthread_mutex_lock(&mutex); // Lock around accept to prevent race condition
        lengthClientName = sizeof(client_addr);
        if ((newSocket = accept(origSocket, (struct sockaddr *)&client_addr, &lengthClientName)) == -1) {
            perror("Accept()");
            exit(5);
        }
        pthread_mutex_unlock(&mutex);

        // Receive message on the socket, put contents in buf
        if (recv(newSocket, buf, sizeof(buf), 0) == -1) {
            perror("Recv()");
            exit(6);
        }
        printf("Server got message: %s \n", buf);

        // Put buf's contents in a temp mcached header for our use
        memcache_req_header_t tmpHeader;
        tmpHeader.magic = buf[0];
        tmpHeader.opcode = buf[1];
        tmpHeader.key_length = (uint16_t)((buf[2] << 8) | buf[3]);
        tmpHeader.extras_length = buf[4];
        tmpHeader.vbucket_id = (uint16_t)((buf[6] << 8) | buf[7]);
        tmpHeader.total_body_length = ((uint32_t)buf[8] << 24) | ((uint32_t)buf[9] << 16) | ((uint32_t)buf[10] << 8) | (uint32_t)buf[11];
        

        // Get the key and value
        // 1. Read in the rest of the body (extras, key, body)
        char body[tmpHeader.total_body_length];
        read(newSocket, body, tmpHeader.total_body_length);
        // 2. Set pKey to point to the start of the key in the body (right after the extras field)
        char *pKey = body + tmpHeader.extras_length;
        // 3. Set pValue to point to the the start of the value in the body (right after key)
        char *pValue = body + tmpHeader.extras_length + tmpHeader.key_length;
        // 4. Record the number of bytes that make up the value portion of the body
        uint32_t valueLen = tmpHeader.total_body_length - tmpHeader.extras_length - tmpHeader.key_length;
        // 5. Create buffers to hold copies of the key and value
        // char key[256] = {0}; // Initialized to 0s to ensure it's null-terminated
        // memcpy(key, pKey, tmpHeader.key_length);
        // char value[4096] = {0};
        // memcpy(value, pValue, valueLen);
        // 5. Use strndup to copy the key and value instead of pointing to the stack-allocated buffer
        char *key = strndup(pKey, tmpHeader.key_length);
        char *value = strndup(pValue, valueLen);


        // Print
        printf("Magic: 0x%02x\n", tmpHeader.magic);
        printf("Opcode: 0x%02x\n", tmpHeader.opcode);
        printf("Key length: %u\n", tmpHeader.key_length);
        printf("Vbucket ID: %u\n", tmpHeader.vbucket_id);
        printf("Total body length: %u\n", valueLen);
        
        // Calculate the key's hash
        unsigned long keyHash = hash(key);
        printf("Calculated the key's hash!\n");

        memcache_req_header_t res = {0};

        // Based on opcode, do one of the following operations...
        if (tmpHeader.opcode == CMD_GET) {
            pthread_mutex_lock(&mutex);
            // Search table for match
            int isFound = 0; // Represents false
            // Search table for match; iterate over the list
            Entry *current = table;
            while (current != NULL) {
                if (keyHash == current->hashKey) {
                    isFound = 1; // Set to true (we found the key!)
                }
                current = current->pNext;
            }
            if (isFound == 1) {
                // Send exists to the client
                res.magic = RES_MAGIC;
                res.opcode = CMD_GET;
                res.key_length = htons(0);
                res.extras_length = 0;
                res.vbucket_id = htons(RES_EXISTS);
                res.total_body_length = htonl(0);
                res.cas = htonl(0);

                // Send header to socket
                if (send(newSocket, &res, sizeof(res), 0) < 0) {
                    perror("Send()");
                    exit(7);
                }

                pthread_mutex_unlock(&mutex);
            }
            else {
                // Send not found to the client
                res.magic = RES_MAGIC;
                res.opcode = CMD_GET;
                res.key_length = htons(0);
                res.extras_length = 0;
                res.vbucket_id = htons(RES_NOT_FOUND);
                res.total_body_length = htonl(0);
                res.cas = htonl(0);

                pthread_mutex_unlock(&mutex);
            }
        }
        else if (tmpHeader.opcode == CMD_ADD) {
            pthread_mutex_lock(&mutex);
            int isFound = 0; // Represents false
            // Search table for match; iterate over the list
            Entry *current = table;
            while (current != NULL) {
                if (keyHash == current->hashKey) {
                    isFound = 1; // Set to true (we found the key!)
                }
                current = current->pNext;
            }
            if (isFound == 1) {
                // Send exists to the client
                res.magic = RES_MAGIC;
                res.opcode = tmpHeader.opcode;
                res.key_length = htons(0);
                res.extras_length = 0;
                res.vbucket_id = htons(RES_EXISTS);
                res.total_body_length = htonl(0);
                res.cas = htonl(0);

                pthread_mutex_unlock(&mutex);
            }
            else {
                // Allocate memory for a new entry and add to the table
                Entry *newEntry = (Entry *)malloc(sizeof(Entry));
                newEntry->hashKey = keyHash;
                newEntry->key = key;
                newEntry->value = value;
                newEntry->pNext = NULL;
                // If table is empty, the new entry becomes the head
                if (table == NULL) {
                    table = newEntry;
                }
                else {
                    Entry *last = table;
                    while (last->pNext != NULL) {
                        last = last->pNext;
                    }
                    last->pNext = newEntry;
                }
                // Send success response to client
                res.magic = RES_MAGIC;
                res.opcode = tmpHeader.opcode;
                res.key_length = htons(0);
                res.extras_length = 0;
                res.vbucket_id = htons(RES_OK);
                res.total_body_length = htonl(0);
                res.cas = htonl(0);

                pthread_mutex_unlock(&mutex);
            }
        }
        else if (tmpHeader.opcode == CMD_SET) {
            pthread_mutex_lock(&mutex);
            Entry *current = table;
            int isFound = 0;

            while (current != NULL) {
                if (keyHash == current->hashKey && strcmp(current->key, key) == 0) {
                    isFound = 1;
                    free(current->value);
                    current->value = strdup(value);
                    break;
                }
                current = current->pNext;
            }

            if (!isFound) {
                Entry *newEntry = malloc(sizeof(Entry));
                newEntry->hashKey = keyHash;
                newEntry->key = key;
                newEntry->value = value;
                newEntry->pNext = table;
                table = newEntry;
            }

            res.magic = RES_MAGIC;
            res.opcode = CMD_SET;
            res.key_length = htons(0);
            res.extras_length = 0;
            res.vbucket_id = htons(RES_OK);
            res.total_body_length = htonl(0);
            res.cas = htonl(0);
            pthread_mutex_unlock(&mutex);
        }
        else if (tmpHeader.opcode == CMD_OUTPUT) {
            pthread_mutex_lock(&mutex);
            struct timespec ts;
            get_timestamp(&ts);
            printf("%lx:%lx:", (unsigned long)ts.tv_sec, (unsigned long)ts.tv_nsec);
            Entry *current = table;
            while (current != NULL) {
                // Print the key and value in hexadecimal
                for (int i = 0; current->key[i] != '\0'; i++) {
                    printf("%02x", (unsigned char)current->key[i]);
                }
                printf(":");
                for (int i = 0; current->value[i] != '\0'; i++) {
                    printf("%02x", (unsigned char)current->value[i]);
                }
                printf("\n");

                current = current->pNext;            
            }
            pthread_mutex_unlock(&mutex);
        }
        else {
            pthread_mutex_lock(&mutex);
            // Send error
            res.magic = RES_MAGIC;
            res.opcode = res.opcode;
            res.key_length = htons(0);
            res.extras_length = 0;
            res.vbucket_id = htons(RES_ERROR);
            res.total_body_length = htonl(0);
            res.cas = htonl(0);

            pthread_mutex_unlock(&mutex);
        }

        // Send header to socket
        if (send(newSocket, &res, sizeof(res), 0) < 0) {
            perror("Send()");
            exit(7);
        }

        free(key);
        free(value);

        pthread_mutex_unlock(&mutex);
        sleep(1); // Hack so the OS reclaims the port sooner
        close(newSocket);
    }
}

int main (int argc, char **argv) {
    // Get server port and number of worker threads
    if (argc != 3) {
        fprintf(stderr, "Usage: %s port threads\n", argv[0]);
        exit(1);
    }
    unsigned short port = (unsigned short)atoi(argv[1]);
    int numThreads = atoi(argv[2]);
    numTableEntries = numThreads; // Number of entries in hash table is the number of threads

    // Create server socket
    if ((origSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket()");
        exit(2);
    }

    // Assign IP, port
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    // Bind to port
    if (bind(origSocket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind()");
        exit(3);
    }

    // Listen for connections; backlog is specified as 1
    if (listen(origSocket, 1) != 0) {
        perror("Listen()");
        exit(4);
    }

    // Create threads
    pthread_t threads[numThreads];
    pthread_mutex_init(&mutex, NULL);

    for (int i = 0; i < numThreads; i++) {
        if (pthread_create(&threads[i], NULL, &workerThread, NULL)) {
            perror("Failed to create a thread.\n");
            return 1;
        }
        printf("Thread %d has started.\n", i);
    }


    for (int i = 0 ; i < numThreads; i++) {
        if (pthread_join(threads[i], NULL) != 0) {
            return 2;
        }
        printf("Thread %d has finished execution.\n", i);
    }

    pthread_mutex_destroy(&mutex);

    // Free table
    Entry *current = table;
    while (current != NULL) {
        Entry *next = current->pNext;
        free(current->key);
        free(current->value);
        free(current);
        current = next;
    }

    close(origSocket);
    sleep(1);
    printf("Server ended successfully\n");
    return 0;    
}