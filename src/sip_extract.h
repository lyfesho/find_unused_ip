#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <rdkafka.h>
#include <math.h>
#include <stdbool.h>
#include <arpa/inet.h>
#include <MESA_prof_load.h>
#include <MESA_handle_logger.h>
#include "cJSON.h"
#include "maxminddb.h"

#define MAX_BROKER_LEN 128
#define MAX_GROUP_ID_LEN 128
#define MAX_TOPIC_LEN 128
#define MAX_PATH_LEN 128
#define IP_NUM 512
#define MAX_IP4_LEN 32
#define MAX_IP6_LEN 128\

#define CASE_IPV4 4
#define CASE_IPV6 6

#define ADD_OPT 1
#define MINUS_OPT 0

//#define IP_LIST_LINE_NUM 9930714
#define MAX_DMS_LINE_SIZE 512

typedef unsigned int uint;

const char * module_name = "sip_extract";
const char * conf_file = "../src/conf/sip_extract.conf";
const char * ipv4_dms_file = "../src/all_ip_info.dms";
const char * ipv6_dms_file = "../src/all_ipv6_info.dms";
const char * mmdb_v4_file = "../src/mmdb/all_ip_info_withindex.mmdb";
const char * mmdb_v6_file = "../src/mmdb/all_ipv6_info_withindex.mmdb";

uint rlog_lv;
void* rlog_handle;
char rlog_path[MAX_PATH_LEN];

int valid_time;
int output_time_interval;

typedef struct{
	uint64_t ipv6_high;
	uint64_t ipv6_low;
}uint128_t;

typedef struct{
	char ip_str[MAX_IP6_LEN];
	uint ipv4;
	uint128_t ipv6;
	int addr_type;
	int gid;
}input_info_t;

typedef struct{
	int ip_time;
	int gid;
}std_list_item_info_t;

typedef struct STNode_v4_t{
    int valid_time;
    uint start_ip_num;         //segment start ip num
    uint end_ip_num;           //segment end ip num
    int gid;
    STNode_v4_t * lChild;
    STNode_v4_t * rChild;
    bool used_ip_exist;       //if used_ip_exist in the seg, then used_ip_exists = 1; else 0.
};

typedef struct STNode_v6_t{
    int valid_time;
    uint128_t start_ip_num;         //segment start ip num
    uint128_t end_ip_num;           //segment end ip num
    int gid;
    STNode_v6_t * lChild;
    STNode_v6_t * rChild;
    bool used_ip_exist;       //if used_ip_exist in the seg, then used_ip_exists = 1; else 0.
};


//todo: v6 have different define on ip_num

#ifdef __cplusplus
extern "C"
{
#endif
int get_ip_idx(char * ip, MMDB_s  mmdb);      
unsigned int ip2int(char * ip);
void int2ip(char * ip_str, unsigned int ip_num);
void ipv6_str2int(char * ip, uint128_t * ipv6);
void ipv6_int2str(char * ip_str, uint128_t ip_num);
rd_kafka_conf_t * init_kafka_conf(const char * group_id);
int init_kafka_consumer(rd_kafka_t ** kafka_consumer, const char * topic_name, const char * group_id, const char * brokers);
void json_anal(rd_kafka_message_t * rm_message, input_info_t ** input_info);
void init_std_list(STNode_v4_t * * ip_std_list, int case_num);
void value_ipv6(char * ip_start, char * ip_end, STNode_v6_t * node);
void create_node(uint ip_num, int gid, int ip_valid_time, STNode_v4_t * root);
int ipv6_belong2seg(uint128_t ip_num, uint128_t start_ip_num, uint128_t end_ip_num);
void create_node_v6(uint128_t ip_num, int gid, int ip_valid_time, STNode_v6_t * root);
void msg_consume(rd_kafka_message_t * rk_message, STNode_v4_t * * ipv4_std_list, STNode_v6_t * * ipv6_std_list, MMDB_s mmdb_v4, MMDB_s mmdb_v6);
void STree_output(STNode_v4_t * root, FILE * fp, uint seg_start_ip, uint seg_end_ip);
int ipv6_eq(uint128_t src, uint128_t dest);
void STree_output_v6(STNode_v6_t * root, FILE * fp, uint128_t seg_start_ip, uint128_t seg_end_ip);
void output_unused_ip(STNode_v4_t * * ipv4_std_list, const int ipv4_list_line_num, STNode_v6_t * * ipv6_std_list, const int ipv6_list_line_num);
void destroy(void * * ip_std_list, int ip_list_line_num, int case_num);
void delete_node(int cur_time, STNode_v4_t * root);
int main();
#ifdef __cplusplus
}
#endif