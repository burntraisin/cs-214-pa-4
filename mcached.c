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
#include <stdint.h>

#define MSG_SIZE 256

int numTableEntries;
int origSocket; // Socket for accepting connections (i.e. the fd)
pthread_mutex_t table_lock = PTHREAD_MUTEX_INITIALIZER;

// struct for entries of the table
typedef struct Entry {
    uint8_t *key;
    uint32_t hashKey;
    uint8_t *value;
    struct Entry *pNext;
    uint16_t key_length;
    uint32_t value_length;
    pthread_mutex_t lock;
} Entry;

Entry *table = NULL; // Create a table to hold key-value pairs as a LL

// DJB2 hash function from https://gist.github.com/MohamedTaha98/ccdf734f13299efb73ff0b12f7ce429f
uint32_t hash(uint8_t *str) {
    uint32_t hash = 5381;
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

void send_response(int client_fd, uint8_t opcode, uint16_t vbucket_id, uint16_t key_length, uint32_t value_len, uint8_t *key, uint8_t *value) {
    memcache_req_header_t res;
    memset(&res, 0, sizeof(res));

    res.magic = RES_MAGIC;
    res.opcode = opcode;
    res.key_length = htons(key_length);
    res.extras_length = 0;
    res.vbucket_id = htons(vbucket_id);
    res.total_body_length = htonl(value_len);
    res.cas = 0;

    printf("Sending response: magic=0x%02x, opcode=%u, total_body_length=%u\n",
        res.magic, res.opcode, ntohl(res.total_body_length));
 

    // Send header
    if (send(client_fd, &res, sizeof(res), 0) < 0) {
        perror("Send() help");
        return;
    }

    // if (opcode == CMD_VERSION) {
    //     const char *version_string = "C-Memcached 1.0";
    //     size_t version_length = strlen(version_string);
    //     send(client_fd, version_string, version_length, 0);
    //     return;
    // }

    // Send key and value if present
    // if (key_length > 0) {
    //     printf("Key: ");
    //     for (int i = 0; i < key_length; i++) {
    //         printf("%02x ", key[i]);
    //     }
    //     printf("\n");

    //     if (send(client_fd, key, key_length, 0) < 0) {
    //         perror("Send key failed");
    //         return;
    //     }
    // }
    if (value_len > 0) {
        printf("Value: ");
        for (int i = 0; i < value_len; i++) {
            printf("%02x ", value[i]);
        }
        printf("\n");

        if (send(client_fd, value, value_len, 0) < 0) {
            perror("Send value failed");
            return;
        }
    }
}

void handle_client(int client_fd) {
    while (1) {
        char buf[24]; // Buffer for header data
        memset(buf, 0, sizeof(buf));

        // Receive message on the socket, put contents in buf
        // if (recv(client_fd, buf, 24, MSG_WAITALL) == -1) {
        //     perror("Recv()?!");
        //     fprintf(stderr, "errno = %d\n", errno);
        //     // close(client_fd);
        //     return;
        // }
        ssize_t n = recv(client_fd, buf, 24, MSG_WAITALL);
        if (n == 0) {
            fprintf(stderr, "Client closed connection.\n");
            break; // or return
        } else if (n < 0) {
            perror("recv header");
            fprintf(stderr, "errno = %d\n", errno);
            return;
        }


        printf("Raw header bytes:\n");
        for (int i = 0; i < sizeof(buf); ++i) {
            printf("%02x ", (uint8_t)buf[i]);
        }
        printf("\n");

        // Put buf's contents in a temp mcached header for our use
        memcache_req_header_t tmpHeader;
        tmpHeader.magic = buf[0];
        tmpHeader.opcode = buf[1];
        tmpHeader.key_length = ((uint16_t)(uint8_t)buf[2] << 8) | (uint16_t)(uint8_t)buf[3];
        tmpHeader.extras_length = buf[4];
        tmpHeader.vbucket_id = ((uint16_t)(uint8_t)buf[6] << 8) | (uint16_t)(uint8_t)buf[7];
        tmpHeader.total_body_length = ((uint32_t)(uint8_t)buf[8] << 24) |
                              ((uint32_t)(uint8_t)buf[9] << 16) |
                              ((uint32_t)(uint8_t)buf[10] << 8) |
                              (uint32_t)(uint8_t)buf[11];
                         
                              printf("Parsed header: opcode=0x%02x key_length=%u total_body_length=%u\n",
                                tmpHeader.opcode, tmpHeader.key_length, tmpHeader.total_body_length);
                         
        
        // Get the key and value
        // total_body_length includes the extras, key and value (body)
        uint32_t body_length = tmpHeader.total_body_length;
        uint8_t *body = malloc(body_length);

        // 1. Read in the rest of the body (extras, key, body)
        if (recv(client_fd, body, tmpHeader.total_body_length, MSG_WAITALL) != tmpHeader.total_body_length) {
            perror("recv body!!!");
            close(client_fd);
            return;
        }
        
        // 2. Set pKey to point to the start of the key in the body (right after the extras field)
        uint8_t *pKey = body + tmpHeader.extras_length;
        // 3. Set pValue to point to the start of the value in the body (right after the key)
        uint8_t *pValue = body + tmpHeader.extras_length + tmpHeader.key_length;
        // 4. Record the number of bytes that make up the value portion of the body
        uint32_t valueLen = tmpHeader.total_body_length - tmpHeader.extras_length - tmpHeader.key_length;
        uint32_t keyLen = tmpHeader.key_length;

        // 5. Allocate memory for key and value
        uint8_t *key = malloc(keyLen);
        if (key == NULL) {
            perror("malloc for key");
            return;
        }

        uint8_t *value = malloc(valueLen);
        if (value == NULL) {
            perror("malloc for value");
            free(key);
            return;
        }

        // 6. Copy key and value data into allocated memory
        memcpy(key, pKey, keyLen);
        memcpy(value, pValue, valueLen);

        // Print
        printf("Magic: 0x%02x\n", tmpHeader.magic);
        printf("Opcode: 0x%02x\n", tmpHeader.opcode);
        printf("Key length: %u\n", tmpHeader.key_length);
        printf("Vbucket ID: %u\n", tmpHeader.vbucket_id);
        printf("Total body length: %u\n", valueLen);
        
        // Calculate the key's hash
        uint32_t keyHash = hash(key);
        printf("Calculated the key's hash!\n");
        // Print the fucking hashes (collisions?)
        // printf("Key: %hn | Hash: %lu\n\n", key, keyHash);

        // Based on opcode, do one of the following operations...
        if (tmpHeader.opcode == CMD_GET) {
            // Search table for match
            int isFound = 0; // Represents false
            // Search table for match; iterate over the list
            Entry *current = table;
            Entry *target = NULL;

            while (current != NULL) {
                if (keyHash == current->hashKey && memcmp(key, current->key, current->key_length) == 0) {
                    pthread_mutex_lock(&current->lock);
                    isFound = 1; // Set to true (we found the key!)
                    target = current;
                    pthread_mutex_unlock(&current->lock);
                    break;
                }
                current = current->pNext;
            }
            if (isFound == 1) {
                printf("Is found bro!!!!!\n\n");
                printf("Sending GET response: keylen=%u valuelen=%u total_body_length=%u\n", target->key_length, target->value_length, target->key_length + target->value_length);
                send_response(client_fd, CMD_GET, RES_OK, 0, target->value_length, NULL, target->value);

            }
            else {
                // Send not found to the client
                send_response(client_fd, CMD_GET, RES_NOT_FOUND, 0, 0, NULL, NULL);
            }
        }
        else if (tmpHeader.opcode == CMD_ADD) {
            int isFound = 0; // Represents false
            // Search table for match; iterate over the list
            Entry *current = table;
            while (current != NULL) {
                if (keyHash == current->hashKey && memcmp(key, current->key, current->key_length) == 0) {
                    pthread_mutex_lock(&current->lock);
                    isFound = 1; // Set to true (we found the key!)
                    break;
                }
                current = current->pNext;
            }
            if (isFound == 1) {
                pthread_mutex_unlock(&current->lock);
                send_response(client_fd, CMD_ADD, RES_EXISTS, current->key_length, current->value_length, current->key, current->value);
            }
            else {
                // Allocate memory for a new entry and add to the table
                Entry *newEntry = (Entry *)malloc(sizeof(Entry));
                newEntry->key_length = tmpHeader.key_length;
                newEntry->key = malloc(newEntry->key_length);
                memcpy(newEntry->key, key, newEntry->key_length);

                newEntry->hashKey = keyHash;

                newEntry->value_length = tmpHeader.total_body_length - tmpHeader.key_length;
                newEntry->value = malloc(newEntry->value_length);
                memcpy(newEntry->value, value, newEntry->value_length);

                newEntry->pNext = NULL;
                pthread_mutex_init(&newEntry->lock, NULL);

                // Insert at the head
                pthread_mutex_lock(&table_lock);
                newEntry->pNext = table;
                table = newEntry;

                pthread_mutex_unlock(&table_lock);
                send_response(client_fd, CMD_ADD, RES_OK, 0, 0, key, value);
            }
        }
        else if (tmpHeader.opcode == CMD_SET) {
            Entry *current = table;
            int isFound = 0;

            while (current != NULL) {
                if (keyHash == current->hashKey && memcmp(key, current->key, current->key_length) == 0) {
                    isFound = 1;
                    pthread_mutex_lock(&current->lock);
                    free(current->value);

                    uint32_t new_value_len = tmpHeader.total_body_length - tmpHeader.key_length;
                    current->value = malloc(new_value_len);
                    memcpy(current->value, value, new_value_len);
                    current->value_length = new_value_len;

                    pthread_mutex_unlock(&current->lock);
                    break;
                }
                current = current->pNext;
            }

            if (!isFound) {
                Entry *newEntry = malloc(sizeof(Entry));
                pthread_mutex_init(&newEntry->lock, NULL);
                pthread_mutex_lock(&newEntry->lock);

                newEntry->hashKey = keyHash;
                newEntry->key_length = tmpHeader.key_length;
                newEntry->key = malloc(newEntry->key_length);
                memcpy(newEntry->key, key, newEntry->key_length);

                uint32_t new_value_len = tmpHeader.total_body_length - tmpHeader.key_length;
                newEntry->value = malloc(new_value_len);
                memcpy(newEntry->value, value, new_value_len);
                newEntry->value_length = new_value_len;

                // Insert at head of table (linked list)
                newEntry->pNext = table;
                table = newEntry;
                pthread_mutex_unlock(&newEntry->lock);
                printf("Sending SET response: keylen=%u valuelen=%u total_body_length=%u\n", newEntry->key_length, newEntry->value_length, newEntry->key_length + newEntry->value_length);
                send_response(client_fd, CMD_SET, RES_OK, 0, 0, NULL, NULL);
            }
            else {
                printf("Sending SET response: keylen=%u valuelen=%u total_body_length=%u\n", current->key_length, current->value_length, current->key_length + current->value_length);
                send_response(client_fd, CMD_SET, RES_OK, 0, 0, NULL, NULL);
            }
        }
        // else if (tmpHeader.opcode == CMD_OUTPUT) {
        //     struct timespec ts;
        //     get_timestamp(&ts);
        //     printf("%lx:%lx:", (unsigned long)ts.tv_sec, (unsigned long)ts.tv_nsec);
        //     Entry *current = table;
        //     while (current != NULL) {
        //         pthread_mutex_lock(&current->lock);
        //         // Print the key and value in hexadecimal
        //         for (int i = 0; current->key[i] != '\0'; i++) {
        //             printf("%02x", (unsigned char)current->key[i]);
        //         }
        //         printf(":");
        //         for (int i = 0; current->value[i] != '\0'; i++) {
        //             printf("%02x", (unsigned char)current->value[i]);
        //         }
        //         printf("\n");
        //         pthread_mutex_unlock(&current->lock);
        //         current = current->pNext;            
        //     }

        //     // Send response to the client
        //     res.magic = RES_MAGIC;
        //     res.opcode = CMD_OUTPUT;
        //     res.key_length = htons(0);
        //     res.extras_length = 0;
        //     res.vbucket_id = htons(RES_OK);
        //     res.total_body_length = htonl(0);
        //     res.cas = htonl(0);
        //     // The body should have the printfs?

        //     // Send header to socket
        //     if (send(client_fd, &res, sizeof(res), 0) < 0) {
        //         perror("Send() help");
        //         close(client_fd);
        //         return;
        //     }
        // }
        else if (tmpHeader.opcode == CMD_DELETE) {
            printf("I'm here girl!\n\n");
            int isFound = 0; // Represents false
            // Search table for match; iterate over the list
            Entry *current = table;
            
            Entry *previous = NULL;
            while (current != NULL) {
                pthread_mutex_lock(&current->lock);
                if (keyHash == current->hashKey && memcmp(key, current->key, current->key_length) == 0) {
                    isFound = 1; // Set to true (we found the key!)
                    pthread_mutex_unlock(&current->lock);
                    break;
                }
                pthread_mutex_unlock(&current->lock);
                previous = current;
                current = current->pNext;
            }
            printf("I'm here, after searching for a match!\n\n");
            if (isFound == 1) {
                // pthread_mutex_lock(&table_lock);
                // Remove entry from LL
                if (previous == NULL) {
                    table = current->pNext;
                }
                else {
                    previous->pNext = current->pNext;
                }
                pthread_mutex_destroy(&current->lock);
                free(current->value);
                free(current->key);
                free(current);
                // pthread_mutex_unlock(&table_lock);

                printf("Sending DELETE response\n\n");
                send_response(client_fd, CMD_DELETE, RES_OK, 0, 0, NULL, NULL);
            }
            else {
                printf("Sending DELETE response\n\n");
                send_response(client_fd, CMD_DELETE, RES_NOT_FOUND, 0, 0, NULL, NULL);
            }
        }
        else if (tmpHeader.opcode == CMD_VERSION) {
            send_response(client_fd, CMD_VERSION, RES_OK, 0, strlen("C-Memcached 1.0"), NULL, "C-Memcached 1.0");
            
        }
        else {
            // Send error
            send_response(client_fd, tmpHeader.opcode, RES_ERROR, 0, 0, NULL, NULL);
        }
        free(key);
        free(value);
        free(body);
    }
    sleep(1);
    close(client_fd);
}

void *workerThread(void *arg) {
    int server_fd = (intptr_t)arg;
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &addr_len);
        if (client_fd < 0) {
            perror("accept failed");
            continue;
        }

        // Call client request handling function
        handle_client(client_fd);
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

    // Listen for connections
    if (listen(origSocket, 128) != 0) {
        perror("Listen()");
        exit(4);
    }

    // Create threads
    pthread_t threads[numThreads];

    for (int i = 0; i < numThreads; i++) {
        if (pthread_create(&threads[i], NULL, &workerThread, (void *)(intptr_t)origSocket)) {
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
    
    // Free table
    Entry *current = table;
    while (current != NULL) {
        Entry *next = current->pNext;
        // Check if mutex is locked before unlocking
        if (pthread_mutex_trylock(&current->lock) == 0) {
            pthread_mutex_unlock(&current->lock);
        }
        pthread_mutex_destroy(&current->lock);
        free(current->key);
        free(current->value);
        free(current);
        current = next;
    }

    // Free table lock
    pthread_mutex_destroy(&table_lock);

    close(origSocket);
    sleep(1);
    printf("Server ended successfully\n");
    return 0;    
}