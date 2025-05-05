// https://www.ibm.com/docs/en/zos/2.4.0?topic=programs-c-socket-tcp-server

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
#include "mcached.h"
#include <netdb.h>
#include <fcntl.h>

#define MSG_SIZE 256

pthread_mutex_t mutex;

const int NUM_ENTRIES = 10; // The number of entries in the hash table
unsigned short port;       /* port server binds to                */
int orig_socket;                     /* socket for accepting connections    */
struct sockaddr_in server_addr; /* server address information          */

typedef struct Entry {
  char *key;
  uint16_t keyLength;
  char *value;
  uint32_t valueLength;
  unsigned long hash;
  pthread_mutex_t lock;
  struct Entry *pNext; // Pointer to next entry
} Entry;

Entry *table; // Dynamic array of structs

// Modified DJB2 hash function from https://gist.github.com/MohamedTaha98/ccdf734f13299efb73ff0b12f7ce429f
unsigned long hash(char *str, size_t c) {
  unsigned long hash = 5381;
  while ((c = *str++))
      hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
  return hash % NUM_ENTRIES;
}


void *routine() {
  char buf[MSG_SIZE];              /* buffer for sending & receiving data */
  struct sockaddr_in client_addr; /* client address information          */
  int new_socket;                    /* socket connected to client          */
  int namelen;               /* length of client name               */
  int sleep_time;
  int keep_going;            /* flag to keep the server accepting connections from clients */ 
  

  // While loop to keep the server connecting to clients
  keep_going = 1;   // Flag I could change to zero to exit the server 
  while (keep_going) { 
    // Accept a connection
    namelen = sizeof(client_addr);
    if ((new_socket = accept(orig_socket, (struct sockaddr *)&client_addr, &namelen)) == -1) {
      perror("Accept()");
      exit(5);
    }

    // Receive the message on the newly connected socket.
    if (recv(new_socket, buf, sizeof(buf), 0) == -1) {
      perror("Recv()");
      exit(6);
    }

    printf("Server got message: %s \n", buf);
  
    // Send the message back to the client.
    sleep_time = 1;
    printf("the server is on a lunch break for %d seconds \n",sleep_time); 
    sleep(sleep_time);

    // Put buffer content into the given struct from mcached.h
    memcache_req_header_t header;
    header.magic = buf[0];
    header.opcode = buf[1];
    header.key_length = (uint16_t)((buf[2] << 8) | buf[3]);
    header.extras_length = buf[4];
    header.vbucket_id = (uint16_t)((buf[6] << 8) | buf[7]);
    header.total_body_length = ((uint32_t)buf[8] << 24) | ((uint32_t)buf[9] << 16) | ((uint32_t)buf[10] << 8) | (uint32_t)buf[11];

    // Get key and body
    Entry entry;
    entry.keyLength = header.key_length;
    entry.valueLength = header.total_body_length;

    // Now read the rest of the body (header.total_body_length bytes)
    char body_buf[header.total_body_length];
    read(new_socket, body_buf, header.total_body_length);

    // Now extract key and value from the body buffer
    char *key = body_buf + header.extras_length;
    char *value = body_buf + header.extras_length + header.key_length;
    uint32_t value_len = header.total_body_length - header.extras_length - header.key_length;

    // Optional: null-terminate if it's a string
    char key_str[256] = {0};
    memcpy(key_str, key, header.key_length);
    char value_str[4096] = {0};
    memcpy(value_str, value, value_len);

    // Print
    printf("Magic: 0x%02x\n", header.magic);
    printf("Opcode: 0x%02x\n", header.opcode);
    printf("Key length: %u\n", header.key_length);
    printf("Vbucket ID: %u\n", header.vbucket_id);
    printf("Total body length: %u\n", value_len);
    printf("KEY!: ");

    entry.key = malloc(header.key_length + 1);  // +1 if you want to null-terminate

    for (int i = 0; i < header.key_length; i++) {
      entry.key[i] = (unsigned char)key[i];
      printf("%02x", (unsigned char)entry.key[i]);
    }
    entry.key[header.key_length] = '\0'; // Terminate the string
    printf("\n");

    printf("VALUE!: ");

    entry.value = malloc(value_len + 1);  // +1 if you want to null-terminate

    for (int i = 0; i < value_len; i++) {
      entry.value[i] = (unsigned char)value[i];
      printf("%02x", (unsigned char)entry.value[i]);
    }
    entry.value[value_len] = '\0'; // Terminate the string
    printf("\n");
    // Calculate hash of key and store
    unsigned long hashedKey = hash(entry.key, entry.keyLength);
    entry.hash = hashedKey;

    // GET!
    if (header.opcode == CMD_GET) {

    }

    // ADD!
    if (header.opcode == CMD_ADD) {
      // Calculate hash of key
      unsigned long hashedKey = hash(entry.key, entry.keyLength);
      printf("Hash: %ld\n", hashedKey);
      // // Lock the hash table
      // pthread_mutex_lock(&mutex);
      // Search the key
      // Just to traverse the array of structs to find what we want
      Entry *current = &table[hashedKey];

      while (current != NULL) {
        if (current->keyLength == entry.keyLength && memcmp(current->key, entry.key, entry.keyLength) == 0) {
          pthread_mutex_lock(&mutex);

          // Match found (same key)
          printf("Match found at index %lu\n", hashedKey);

          // Send an exists response - how do I do that lol (RES_EXISTS) - Put it in the virtual ID bucket
          header.vbucket_id = RES_EXISTS;
          current = current->pNext;
          pthread_mutex_unlock(&mutex);
          break;
        }
        else {
          pthread_mutex_lock(&mutex);

          Entry *e = malloc(sizeof(Entry));

          e->key = malloc(header.key_length + 1); // +1 if you want to null-terminate
          memcpy(entry.key, entry.key, header.key_length);
          e->key[header.key_length] = '\0'; // optional null termination

          e->value = malloc(entry.valueLength);
          memcpy(entry.value, entry.value, entry.valueLength);

          e->keyLength = header.key_length;
          e->valueLength = entry.valueLength;
          e->pNext = NULL;

          unsigned long index = e->hash;
          e->pNext = &table[index];
          table[index] = *e;

          header.vbucket_id = RES_OK;
          pthread_mutex_unlock(&mutex);
        }
      }
    }
  
    if (send(new_socket, buf, sizeof(buf), 0) < 0) {
      perror("Send()");
      exit(7);
    }

    /* hack so the OS reclaims the port sooner 
    Make the client close the socket first or the socket ends up in the TIME_WAIT state trying to close 
    See: https://hea-www.harvard.edu/~fine/Tech/addrinuse.html
    */
    sleep(1);    
    close(new_socket);
  }
  
  close(orig_socket);

  sleep(1);
  printf("Server ended successfully\n");
  exit(0);

}

int main(int argc, char **argv) {

  // Get command-line arguments for server port and number of worker threads
  if (argc != 3) {
    fprintf(stderr, "Usage: %s port threads\n", argv[0]);
    exit(1);
  }

  port = (unsigned short) atoi(argv[1]);
  int numThreads = atoi(argv[2]);

  // Initialize the dynamic array of structs
  table = malloc(sizeof(Entry) * numThreads);

  // Create server socket and bind to specified port
  if ((orig_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    perror("Socket()");
    exit(2);
  }
  server_addr.sin_family = AF_INET;
  server_addr.sin_port   = htons(port);
  server_addr.sin_addr.s_addr = INADDR_ANY;  

  if (bind(orig_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    perror("Bind()");
    exit(3);
  }

  // Listen for connections. Specify the backlog as 1.
  if (listen(orig_socket, 1) != 0) {
    perror("Listen()");
    exit(4);
  }

  // Create pthreads
  pthread_t threads[numThreads];
  pthread_mutex_init(&mutex, NULL);
  for (int i = 0; i < numThreads; i++) {
    if (pthread_create(&threads[i], NULL, &routine, NULL)) {
      perror("Failed to create a thread.\n");
      return 1;
    }
    printf("Thread %d has started.\n", i);
  }
  for (int i = 0; i < numThreads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      return 2;
    }
    printf("Thread %d has finished execution.\n", i);
  }

  pthread_mutex_destroy(&mutex);

  free(table);
  exit(0);
}
