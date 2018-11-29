#include "producer_test.h"

//char brokers[MAX_BROKER_LEN];
//char topic[MAX_LEN];

rd_kafka_conf_t * init_kafka_conf(const char * brokers){
	rd_kafka_conf_t *conf;
	char errString[512] = {0};

	//kafka conf
	conf = rd_kafka_conf_new();
	if(rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errString, sizeof(errString)) != RD_KAFKA_CONF_OK){
		MESA_handle_runtime_log(rlog_handle, RLOG_LV_FATAL, module_name, "rd_kafka_conf_set error");
	}
	rd_kafka_conf_set(conf, "queue.buffering.max.messages", "1000000", errString, sizeof(errString));
	rd_kafka_conf_set(conf, "topic.metadata.refresh.interval.ms", "600000",errString, sizeof(errString));
	rd_kafka_conf_set(conf, "security.protocol", "MG", errString, sizeof(errString)); 
	rd_kafka_conf_set(conf, "batch.num.messages", "1000000", errString, sizeof(errString));
	return conf;
}

int init_kafka_producer(rd_kafka_t ** rk_producer, rd_kafka_topic_t ** rk_topic, const char * topic_name, const char * brokers){
	rd_kafka_conf_t *rk_conf;
	char errString[512] = {0};
	
	rk_conf = init_kafka_conf(brokers);
	if(NULL == rk_conf){
		return -1;
	}

	if(!(*rk_producer = rd_kafka_new(RD_KAFKA_PRODUCER, rk_conf, errString, 512))){
		MESA_handle_runtime_log(rlog_handle, RLOG_LV_FATAL, module_name, "rd_kafka_new error");
		return -1;		
	}

	if(!(*rk_topic = rd_kafka_topic_new(*rk_producer, topic_name, NULL))){
		MESA_handle_runtime_log(rlog_handle, RLOG_LV_FATAL, module_name, "rd_kafka_topic_new error");
		rd_kafka_destroy(*rk_producer);
		return -1;
	}
	
	return 0;
}

void gene_ip_random(char * ip){

	struct timeval tpTime;
	float timeuse;
	gettimeofday(&tpTime, NULL);

	srand(unsigned(tpTime.tv_usec));
	unsigned int ip_num1 = rand()%255;
	srand(unsigned(tpTime.tv_usec)+1);
	unsigned int ip_num2 = rand()%255;
	srand(unsigned(tpTime.tv_usec)-4);
	unsigned int ip_num3 = rand()%255;
	srand(unsigned(tpTime.tv_usec)+3);
	unsigned int ip_num4 = rand()%255;
	snprintf(ip, MAX_IP4_LEN, "%d.%d.%d.%d",
		ip_num1,ip_num2,ip_num3,ip_num4);
}


void gene_ipv4(char * ip, int cnt){
	unsigned int ip_num1 = 58;

	unsigned int ip_num2 = 31;

	unsigned int ip_num3 = 241;
	
	srand(unsigned(time(NULL)));
	unsigned int ip_num4 = rand() % 255;
	snprintf(ip, MAX_IP4_LEN, "%d.%d.%d.%d",
		ip_num1,ip_num2,ip_num3,ip_num4);
}



void gene_ip_v6(char * ip, int cnt){

	int i;
	int seed = time((time_t *)NULL);
	
	
	unsigned char bufipv6[16] = {0};
	char to_ipv6[INET6_ADDRSTRLEN] = {0};
	
	bufipv6[0] = 32; bufipv6[1] = 1; bufipv6[2] = 2; bufipv6[3] = 24; bufipv6[4] = 32; bufipv6[5] = 128; bufipv6[6] = 188; bufipv6[7] = 255;
	bufipv6[8] = 112; bufipv6[9] = 23; bufipv6[10] = 145; bufipv6[11] = 125; bufipv6[12] = 221; bufipv6[13] = 192;
	
	//srand(unsigned(time(NULL)));
	bufipv6[14] = cnt % 255;
	srand(unsigned(time(NULL)) + 1);
	bufipv6[15] = rand() % 255;
	
	memcpy(ip, inet_ntop(AF_INET6, bufipv6, to_ipv6, INET6_ADDRSTRLEN), strlen(inet_ntop(AF_INET6, bufipv6, to_ipv6, INET6_ADDRSTRLEN)));
}

void create_msg(char * buf){
	//create ip_log
	ip_log_t * ip_log = (ip_log_t *)calloc(1, sizeof(ip_log_t));
	ip_log->addr_type = 4;
	gene_ip_random(ip_log->s_ip);
	gene_ip_random(ip_log->d_ip);
	//ip_log->s_ip = "0.0.0.12";
	//ip_log->d_ip = "123.123.123.123";
	ip_log->d_port = 123;
	ip_log->s_port = 11;
	srand(time(NULL));
	ip_log->device_id = rand(); 

	//change to json
	cJSON *root = cJSON_CreateObject();
	//cJSON *log_obj = cJSON_CreateObject();
	cJSON * item = cJSON_CreateNumber(ip_log->addr_type);
	cJSON_AddItemToObject(root, "addr_type", item);
	item = cJSON_CreateString(ip_log->d_ip);
	cJSON_AddItemToObject(root, "d_ip", item);
	item = cJSON_CreateString(ip_log->s_ip);
	cJSON_AddItemToObject(root, "s_ip", item);
	item = cJSON_CreateNumber(ip_log->d_port);
	cJSON_AddItemToObject(root, "d_port", item);
	item = cJSON_CreateNumber(ip_log->s_port);
	cJSON_AddItemToObject(root, "s_port", item);
	item = cJSON_CreateNumber(ip_log->device_id);
	cJSON_AddItemToObject(root, "device_id", item);

	char * out = cJSON_Print(root);
	memcpy(buf, out, strlen(out));

	cJSON_Delete(root);
	free(out);
	free(ip_log);
}

void create_msg1(char * buf, int cnt){
	//create ip_log
	ip_log_t * ip_log = (ip_log_t *)calloc(1, sizeof(ip_log_t));
	ip_log->addr_type = 4;
	gene_ip_random(ip_log->s_ip);
	gene_ip_random(ip_log->d_ip);
	ip_log->d_port = 123;
	ip_log->s_port = 11;
	srand(time(NULL));
	ip_log->device_id = rand(); 

	//change to json
	cJSON *root = cJSON_CreateObject();
	//cJSON *log_obj = cJSON_CreateObject();
	cJSON * item = cJSON_CreateNumber(ip_log->addr_type);
	cJSON_AddItemToObject(root, "addr_type", item);
	item = cJSON_CreateString(ip_log->d_ip);
	cJSON_AddItemToObject(root, "d_ip", item);
	item = cJSON_CreateString(ip_log->s_ip);
	cJSON_AddItemToObject(root, "s_ip", item);
	item = cJSON_CreateNumber(ip_log->d_port);
	cJSON_AddItemToObject(root, "d_port", item);
	item = cJSON_CreateNumber(ip_log->s_port);
	cJSON_AddItemToObject(root, "s_port", item);
	item = cJSON_CreateNumber(ip_log->device_id);
	cJSON_AddItemToObject(root, "device_id", item);

	char * out = cJSON_Print(root);
	memcpy(buf, out, strlen(out));

	cJSON_Delete(root);
	free(out);
	free(ip_log);
}

screen_stat_handle_t init_stat_handle(){
	screen_stat_handle_t handle = NULL;
	const char * stat_path = "./producer_test.status";
	const char * app_name = "producer_test";
	int value = 0;

	handle = FS_create_handle();

	FS_set_para(handle, APP_NAME, app_name, strlen(app_name)+1);
	value = 0;
	FS_set_para(handle, FLUSH_BY_DATE, &value, sizeof(value));
	FS_set_para(handle, OUTPUT_DEVICE, stat_path, strlen(stat_path)+1);
	value = 1;
	FS_set_para(handle, PRINT_MODE, &value, sizeof(value));
	value = 1;
	FS_set_para(handle, CREATE_THREAD, &value, sizeof(value));
	value = 2;
	FS_set_para(handle, STAT_CYCLE, &value, sizeof(value));
	value = 4096;
	//FS_set_para(handle, MAX_STAT_FIELD_NUM, &value, sizeof(value));
	FS_set_para(handle, STATS_SERVER_IP, "127.0.0.1", strlen("127.0.0.1"));
	value = 8100;
	FS_set_para(handle, STATS_SERVER_PORT, &value, sizeof(value));
	//value = FS_OUTPUT_STATSD;
	//FS_set_para(handle, STATS_FORMAT, &value, sizeof(value));

	return handle;
}


int main(){

	rd_kafka_t *rk_producer = NULL;
	rd_kafka_topic_t *rk_topic = NULL;
	char buf[MAX_IP4_LEN] = {0};
	char brokers[512];
	char topic[512];

	//MESA_load_profile_string_def(conf_file,"LOG_INFO","RLOG_PATH",rlog_path,MAX_PATH_LEN,"../test/log/rk_producer.log");
	//MESA_load_profile_uint_def(conf_file,"LOG_INFO","RLOG_LV",&rlog_lv,10);
	MESA_load_profile_string_def(conf_file,"KAFKA_INFO","BROKERS",brokers,MAX_BROKER_LEN,"localhost:2181");
	MESA_load_profile_string_def(conf_file,"KAFKA_INFO","TOPIC",topic,MAX_LEN,"IPD-HTTP-IP-LOG");
/*
	rlog_handle = MESA_create_runtime_log_handle(rlog_path,rlog_lv);
	if(rlog_handle == NULL){
		printf("rlog_handle create failed");
		return -1;
	}

*/
	init_kafka_producer(&rk_producer, &rk_topic, topic, brokers);
/*--------------random ip generate----------
	while(1){
		create_msg(buf);

		retry:
			if(rd_kafka_produce(rk_topic, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY, buf, strlen(buf), NULL, 0, NULL) == -1){
				fprintf(stderr, "%% Failed to produce to topic %s: %s\n",rd_kafka_topic_name(rk_topic),rd_kafka_err2str(rd_kafka_last_error()));
				if(rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL){
					rd_kafka_poll(rk_producer, 1000);
					goto retry;
				}
			}
			rd_kafka_poll(rk_producer, 0);
	}
*/

//field_state init
	screen_stat_handle_t handle = init_stat_handle();
	char buff[128];
	int i = 1;
	int field_ids;

	snprintf(buff, sizeof(buff), "field_%02d", i);
	field_ids = FS_register(handle, FS_STYLE_FIELD, FS_CALC_CURRENT, buff);


//certain ip generate : generate two ips from the same ip section
	FS_start(handle);
	while(1){
		create_msg(buf);
		FS_operate(handle, field_ids, 0, FS_OP_ADD, 1);

		retry:
			if(rd_kafka_produce(rk_topic, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY, buf, strlen(buf), NULL, 0, NULL) == -1){
				fprintf(stderr, "%% Failed to produce to topic %s: %s\n",rd_kafka_topic_name(rk_topic),rd_kafka_err2str(rd_kafka_last_error()));
				if(rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL){
					rd_kafka_poll(rk_producer, 1000);
					goto retry;
				}
			}
			rd_kafka_poll(rk_producer, 0);
		//usleep(1);
	}
	FS_stop(&handle);

	MESA_handle_runtime_log(rlog_handle, RLOG_LV_FATAL, module_name, "rk_produce error");
	return 0;
}
