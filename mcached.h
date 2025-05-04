#ifdef _CACHED_
#define _CACHED_
#else

#define CMD_GET     0x00
#define CMD_SET     0x01
#define CMD_ADD     0x02
#define CMD_DELETE  0x04
#define CMD_VERSION 0x0b
#define CMD_OUTPUT  0x0c
#define RES_OK         0x0000
#define RES_NOT_FOUND  0x0001
#define RES_EXISTS     0x0002
#define RES_ERROR      0x0004

#define RES_MAGIC 0x81

typedef struct {
    uint8_t magic;  // A number (0x81) to identify a cached header
    uint8_t opcode; // The command or response code
    uint16_t key_length;    // Length of key in bytes
    uint8_t extras_length;
    uint8_t data_type;
    uint16_t vbucket_id;    // Virtual bucket number. The response must match the vbucket_id of the request.
    uint32_t total_body_length; // Length of the data in bytes
    uint32_t opaque;
    uint64_t cas;
} __attribute__((packed)) memcache_req_header_t;

#endif

