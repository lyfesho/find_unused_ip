#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <MESA_prof_load.h>
#include <MESA_handle_logger.h>
#include "rdkafka.h"
#include "cJSON.h"
#include "field_stat2.h"


#define MAX_LEN 128
#define MAX_IP4_LEN 128
#define MAX_PATH_LEN 64
#define MAX_BROKER_LEN 128

const char * module_name = "rk_producer";
const char * conf_file = "../test/conf/rk_producer.conf";

uint rlog_lv;
void* rlog_handle;
char rlog_path[MAX_PATH_LEN];

typedef struct{
	int addr_type;
	char d_ip[MAX_IP4_LEN];
	char s_ip[MAX_IP4_LEN];
	int d_port;
	int s_port;
	int device_id;
}ip_log_t;

#ifdef __cplusplus
extern "C"
{
#endif

rd_kafka_conf_t * init_kafka_conf(const char * brokers);
int init_kafka_producer(rd_kafka_t ** rk_producer, rd_kafka_topic_t ** rk_topic, const char * topic_name, const char * brokers);
void gene_ip(char * ip);
void create_msg(char * buf);
screen_stat_handle_t init_stat_handle();

int main();

#ifdef __cplusplus
}
#endif

