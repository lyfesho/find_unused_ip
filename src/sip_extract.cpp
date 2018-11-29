#include "sip_extract.h"

int get_ip_idx(char * ip, MMDB_s  mmdb){
	int gai_error, mmdb_error;
	MMDB_lookup_result_s result = MMDB_lookup_string(&mmdb, ip, &gai_error, &mmdb_error);

	if(0 != gai_error){
		fprintf(stderr, "\n Error from getaddrinfo for %s - %s\n\n", ip, gai_strerror(gai_error));
		return -1;
	}
	if(MMDB_SUCCESS != mmdb_error){
		fprintf(stderr, "\n Got and error from libmaxminddb: %s\n\n", MMDB_strerror(mmdb_error));
		return -1;
	}

	MMDB_entry_data_s entry_data;

	if(result.found_entry){

		int idx_status = MMDB_get_value(&result.entry, &entry_data, "INDEX", NULL);
		if(MMDB_SUCCESS != idx_status){
			fprintf(stderr, "Got an error looking up the entry data - %s\n", MMDB_strerror(idx_status));
			return -1;
		}
		if(entry_data.has_data){
			//char * index_str = (char *)calloc(1, entry_data.data_size);
			int index;
			//memcpy(index_str, entry_data.utf8_string, entry_data.data_size);
			index = entry_data.uint32;
			//index = atoi(index_str);
			//free(index_str);
			return index;
		}
	}
	else{
		//fprintf(stderr, "\n No entry for this IP address (%s) was found\n\n", ip);
		return -2;
	}
	return 0;
}

unsigned int ip2int(char * ip){
	int ip1;
	int ip2;
	int ip3;
	int ip4;
	unsigned int ip_num;
	sscanf(ip, "%d.%d.%d.%d", &ip1, &ip2, &ip3, &ip4);
	ip_num = ip1*256*256*256 + ip2*256*256 + ip3*256 + ip4;
	return ip_num;
}

void int2ip(char * ip_str, unsigned int ip_num){
	int ip1;
	int ip2;
	int ip3;
	int ip4;
	char unused_sip[MAX_IP4_LEN] = {0};
	ip1 = ip_num/pow(256,3);
	ip2 = (ip_num-ip1*pow(256,3))/pow(256,2);
	ip3 = (ip_num-ip1*pow(256,3)-ip2*pow(256,2))/256;
	ip4 = ip_num-ip1*pow(256,3)-ip2*pow(256,2)-ip3*256;
	snprintf(unused_sip, MAX_IP4_LEN, "%d.%d.%d.%d", ip1, ip2, ip3, ip4);
	memcpy(ip_str, unused_sip, strlen(unused_sip));
}

//todo:need to check
void ipv6_str2int(char * ip, uint128_t * ipv6){
	struct in6_addr in6_ip;

	inet_pton(AF_INET6, ip, &in6_ip); 

	int i;
	uint64_t pow_value;
	//value ip_start
	for(i = 0; i < 8; i ++){
		if(i < 4){
			pow_value = pow(65536, i);
			(*ipv6).ipv6_low += htons(in6_ip.s6_addr16[7-i]) * pow_value;
		}
		else{
			pow_value = pow(65536, i-4);
			(*ipv6).ipv6_high += htons(in6_ip.s6_addr16[7-i]) * pow_value;
		}
	}	
}

void ipv6_int2str(char * ip_str, uint128_t ip_num){
	int i;
	
	unsigned char bufipv6[16] = {0};
	char to_ipv6[INET6_ADDRSTRLEN] = {0};
	
	for(i = 0; i < 8; i ++){
	   // printf("%x\t", bufipv6[i]);
	   bufipv6[i] = ip_num.ipv6_high / pow(256, 7-i);
	   bufipv6[8+i] = ip_num.ipv6_low/ pow(256, 7-i);
	   ip_num.ipv6_high = ip_num.ipv6_high % (uint64_t)pow(256, 7-i);
	   ip_num.ipv6_low = ip_num.ipv6_low % (uint64_t)pow(256, 7-i);
	}
	memcpy(ip_str, inet_ntop(AF_INET6, bufipv6, to_ipv6, INET6_ADDRSTRLEN), strlen(inet_ntop(AF_INET6, bufipv6, to_ipv6, INET6_ADDRSTRLEN)));
}

//whether two uint128_t num equal
int ipv6_eq(uint128_t src, uint128_t dest){
	if(src.ipv6_high == dest.ipv6_high && src.ipv6_low == src.ipv6_low){
		return 1;
	}
	else{
		return 0;
	}
}

void ipv6_value(uint128_t src, uint128_t * dest){
	(*dest).ipv6_high = src.ipv6_high;
	(*dest).ipv6_low = src.ipv6_low;
}

void ipv6_add_one(uint128_t * src, int opt_type){
	//add
	if(ADD_OPT == opt_type){
		if((*src).ipv6_low != 0xffffffffffffffff){
			(*src).ipv6_high = (*src).ipv6_high;
			(*src).ipv6_low = (*src).ipv6_low + 1;
		}
		else{
			(*src).ipv6_high = (*src).ipv6_high + 1;
			(*src).ipv6_low = 0;
		}
	}
	//minus
	else if(MINUS_OPT == opt_type){
		if((*src).ipv6_low != 0){
			(*src).ipv6_high = (*src).ipv6_high;
			(*src).ipv6_low = (*src).ipv6_low - 1;
		}
		else{
			(*src).ipv6_high = (*src).ipv6_high - 1;
			(*src).ipv6_low = 0xffffffffffffffff;
		}
	}
}

int ipv6_belong2seg(uint128_t ip_num, uint128_t start_ip_num, uint128_t end_ip_num){
	if(ip_num.ipv6_high > start_ip_num.ipv6_high || (ip_num.ipv6_high == start_ip_num.ipv6_high && ip_num.ipv6_low >= start_ip_num.ipv6_low)){
		if(ip_num.ipv6_high < end_ip_num.ipv6_high || (ip_num.ipv6_high == start_ip_num.ipv6_high && ip_num.ipv6_low <= end_ip_num.ipv6_low)){
			return 1;
		}
		else{
			return 0;
		}
	}
	else{
		return 0;
	}
}


//init conf of kafka
rd_kafka_conf_t * init_kafka_conf(const char * group_id){
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	char errString[512] = {0};

	//kafka conf
	conf = rd_kafka_conf_new();
	rd_kafka_conf_set(conf, "group.id", group_id, errString, 512);

	//topic conf
	topic_conf = rd_kafka_topic_conf_new();
	rd_kafka_topic_conf_set(topic_conf, "auto.offset.reset", "smallest", errString, 512);
	rd_kafka_topic_conf_set(topic_conf, "offset.store.method", "broker", errString, 512);
	rd_kafka_conf_set_default_topic_conf(conf, topic_conf); 

	return conf;
}

//init kafka consumer
int init_kafka_consumer(rd_kafka_t ** kafka_consumer, const char * topic_name, const char * group_id, const char * brokers){
	rd_kafka_topic_partition_list_t * topics;
	rd_kafka_conf_t * conf;
	char errString[512] = {0};
	rd_kafka_resp_err_t err;

	conf = init_kafka_conf(group_id);
	if(NULL == conf){
		return -1;
	}

	if(!(* kafka_consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errString, 512))){
		MESA_handle_runtime_log(rlog_handle, RLOG_LV_FATAL, module_name, "rd_kafka_new error");
		return -1;
	}

	if(rd_kafka_brokers_add(* kafka_consumer, brokers) == 0){
		MESA_handle_runtime_log(rlog_handle, RLOG_LV_FATAL, module_name, "rd_kafka_brokers_add error");
		return -1;
	}

	rd_kafka_poll_set_consumer(* kafka_consumer);          //rd_kafka_poll --> consumer_poll
	topics = rd_kafka_topic_partition_list_new(1);         //create store space for topic+partition (list/vector)
	rd_kafka_topic_partition_list_add(topics, topic_name, -1);  //add topic+partition to list
	err = rd_kafka_subscribe(* kafka_consumer, topics);    //start consumer subscribe, topic matched will be added to subscribe list
	if(err){
		MESA_handle_runtime_log(rlog_handle, RLOG_LV_FATAL, module_name, "failed to subscribe");
		return -1;
	}
	return 0;
}

//rk_message in json format, json analysis
void json_anal(rd_kafka_message_t * rm_message, input_info_t ** input_info){
	cJSON * root = cJSON_Parse((const char *)rm_message->payload);
	cJSON * item_addr_type = cJSON_GetObjectItem(root, "addr_type");
	cJSON * item_ip = cJSON_GetObjectItem(root, "s_ip");
	cJSON * item_gid = cJSON_GetObjectItem(root, "device_id");
	
	(*input_info)->addr_type = item_addr_type->valueint;
	memcpy((*input_info)->ip_str, item_ip->valuestring, strlen(item_ip->valuestring));
	if(4 == (*input_info)->addr_type){
		(*input_info)->ipv4 = ip2int(item_ip->valuestring);
	}
	else if(6 == (*input_info)->addr_type){
		ipv6_str2int(item_ip->valuestring, &((*input_info)->ipv6));
	}
	else{
		printf("\nError: addr_type not 4 or 6");
	}
	(*input_info)->gid = item_gid->valueint;
	cJSON_Delete(root);
}


void init_std_list(void * * ip_std_list, int case_num){

	uint ip_start = 0;
	uint ip_end = 0;

	char ipv6_start[MAX_IP6_LEN] = {0};
	char ipv6_end[MAX_IP6_LEN] = {0};

	
	FILE * fp = NULL;
	if(4 == case_num){
		fp = fopen(ipv4_dms_file, "r");
	}
	else{
		fp = fopen(ipv6_dms_file, "r");
	}
	
	char line[MAX_DMS_LINE_SIZE];
	char * result = NULL;
	
	int cnt = 0;
	int flag = 0;
		
	while(fgets(line, MAX_DMS_LINE_SIZE, fp) != NULL){
		if(0 == flag){
			flag = 1;    //flag is used to get rid of the first line from the file
			continue;
		}
		
		result = strtok(line, ",");
		int j = 0;
		while(j < 2 && result != NULL){
			if(j == 0){
				if(4 == case_num){
					ip_start = strtoul(result, NULL, 10);
				}
				else{
					memcpy(ipv6_start, result, strlen(result));
				}
			}
			else{
				if(4 == case_num){
					ip_end = strtoul(result, NULL, 10);
				}
				else{
					memcpy(ipv6_end, result, strlen(result));
				}
			}
			result = strtok(NULL, ",");
			j ++;
		}

		//init root node
		if(4 == case_num){
			ip_std_list[cnt] = (STNode_v4_t *)calloc(1, sizeof(STNode_v4_t));
			((STNode_v4_t *)ip_std_list[cnt])->valid_time = 0;
			((STNode_v4_t *)ip_std_list[cnt])->start_ip_num = ip_start;
			((STNode_v4_t *)ip_std_list[cnt])->end_ip_num = ip_end;
			((STNode_v4_t *)ip_std_list[cnt])->used_ip_exist = 0;
		}
		else{
			ip_std_list[cnt] = (STNode_v6_t *)calloc(1, sizeof(STNode_v6_t));
			ipv6_str2int(ipv6_start, &((STNode_v6_t *)ip_std_list[cnt])->start_ip_num);
			ipv6_str2int(ipv6_end, &((STNode_v6_t *)ip_std_list[cnt])->end_ip_num);
			memset(ipv6_start, '\0', sizeof(char)*MAX_IP6_LEN);
			memset(ipv6_end, '\0', sizeof(char)*MAX_IP6_LEN);
		}
		cnt ++;
	}

	fclose(fp);
}

void create_node(uint ip_num, int gid, int ip_valid_time, STNode_v4_t * root){
    if(ip_num >= root->start_ip_num && ip_num <= root->end_ip_num){
        root->valid_time = ip_valid_time;
        if(root->lChild == NULL && root->rChild == NULL){
			if(ip_num != root->end_ip_num){
            	root->lChild = (STNode_v4_t *)calloc(1, sizeof(STNode_v4_t));          //if ip belones to the segment
            	root->rChild = (STNode_v4_t *)calloc(1, sizeof(STNode_v4_t));          //then calloc space for new segments

            	root->lChild->valid_time = ip_valid_time;                        //value lChild
            	root->lChild->start_ip_num = root->start_ip_num;
            	root->lChild->end_ip_num = ip_num;
				root->lChild->gid = gid;
            	root->lChild->used_ip_exist = 1;

            	root->rChild->valid_time = ip_valid_time;                        //value rChild
            	root->rChild->start_ip_num = ip_num + 1;
            	root->rChild->end_ip_num = root->end_ip_num;
				root->rChild->gid = gid;
            	root->rChild->used_ip_exist = 0;
			}
        }
        else if(root->lChild != NULL && root->rChild != NULL){
            if(ip_num <= root->lChild->end_ip_num){
                create_node(ip_num, gid, ip_valid_time, root->lChild);
            }
            else{
                create_node(ip_num, gid, ip_valid_time, root->rChild);
            }
        }
        else{
            printf("\nError:lChild and rChild are not created simultaniously");
        }
    }
    else{
        printf("\nError:ip not in this seg");
    }
}

void create_node_v6(uint128_t ip_num, int gid, int ip_valid_time, STNode_v6_t * root){
	if(ipv6_belong2seg(ip_num, root->start_ip_num, root->end_ip_num)){
		root->valid_time = ip_valid_time;
        if(root->lChild == NULL && root->rChild == NULL){
			if(!(ipv6_eq(ip_num, root->end_ip_num))){	
            	root->lChild = (STNode_v6_t *)calloc(1, sizeof(STNode_v6_t));          //if ip belones to the segment
            	root->rChild = (STNode_v6_t *)calloc(1, sizeof(STNode_v6_t));          //then calloc space for new segments

            	root->lChild->valid_time = ip_valid_time;                        //value lChild
            	root->lChild->start_ip_num.ipv6_high = root->start_ip_num.ipv6_high;
				root->lChild->start_ip_num.ipv6_low = root->start_ip_num.ipv6_low;
				root->lChild->end_ip_num.ipv6_high = ip_num.ipv6_high;
				root->lChild->end_ip_num.ipv6_low = ip_num.ipv6_low;
				root->lChild->gid = gid;
            	root->lChild->used_ip_exist = 1;

            	root->rChild->valid_time = ip_valid_time;                        //value rChild

				ipv6_add_one(&ip_num, ADD_OPT);
				ipv6_value(ip_num, &root->rChild->start_ip_num);

				root->rChild->end_ip_num.ipv6_high = root->end_ip_num.ipv6_high;
				root->rChild->end_ip_num.ipv6_low = root->end_ip_num.ipv6_low;
				root->rChild->gid = gid;
            	root->rChild->used_ip_exist = 0;
			}
        }
        else if(root->lChild != NULL && root->rChild != NULL){
            if(ipv6_belong2seg(ip_num, root->start_ip_num, root->lChild->end_ip_num)){
                create_node_v6(ip_num, gid, ip_valid_time, root->lChild);
            }
            else{
                create_node_v6(ip_num, gid, ip_valid_time, root->rChild);
            }
        }
	}
}

void subtree_delete(STNode_v4_t * root){
	if(root->lChild == NULL && root->rChild == NULL){
		free(root);
		root = NULL;
	}
	else{
		subtree_delete(root->lChild);
		subtree_delete(root->rChild);
		free(root);
		root = NULL;
	}
}

void delete_node(int cur_time, STNode_v4_t * root){
	if(root == NULL){return;}
	if(root->valid_time < cur_time){  //not valid --> unused
		//subtree delete
		if(root->lChild != NULL && root->rChild != NULL){
			subtree_delete(root->lChild);
			subtree_delete(root->rChild);
		}
		root->used_ip_exist = 0;
		root->lChild = NULL;
		root->rChild = NULL;
	}
	else{
		delete_node(cur_time, root->lChild);
		delete_node(cur_time, root->rChild);
	}
}

void msg_consume(rd_kafka_message_t * rk_message, STNode_v4_t * * ipv4_std_list, STNode_v6_t * * ipv6_std_list, MMDB_s mmdb_v4, MMDB_s mmdb_v6){
	//get ip from kafka
	input_info_t *input_info = (input_info_t *)calloc(1, sizeof(input_info_t));
	json_anal(rk_message, &input_info);

	//get ip-location index
	int ip_idx = 0;
	if(4 == input_info->addr_type){
		ip_idx = get_ip_idx(input_info->ip_str, mmdb_v4);
	}
	else{
		ip_idx = get_ip_idx(input_info->ip_str, mmdb_v6);
	}

	//create node to seg tree
	if(ip_idx >= 0){
		//set time for exist ip
		int ip_valid_time = time((time_t *)NULL) + valid_time;
		if(4 == input_info->addr_type){
			create_node(input_info->ipv4, input_info->gid, ip_valid_time, ipv4_std_list[ip_idx-1]);
		}
		else{
			create_node_v6(input_info->ipv6, input_info->gid, ip_valid_time, ipv6_std_list[ip_idx-1]);
		}
	}
	free(input_info);
}

void STree_output(STNode_v4_t * root, FILE * fp, uint seg_start_ip, uint seg_end_ip, int cur_time){
    if(root->lChild == NULL && root->rChild == NULL){
		char result[160] = {0};
		char unused_ip_start[MAX_IP4_LEN] = {0};
		char unused_ip_end[MAX_IP4_LEN] = {0};
		int2ip(unused_ip_start, root->start_ip_num);
		int2ip(unused_ip_end, root->end_ip_num);
		
        if(root->valid_time < cur_time){
			if(root->start_ip_num != seg_start_ip && root->end_ip_num != seg_end_ip){
				snprintf(result, 160, "[%s, %s]\t", unused_ip_start, unused_ip_end);
			}
			else if(root->start_ip_num == seg_start_ip && root->end_ip_num != seg_end_ip){
				snprintf(result, 160, "\n%s:%d\n", unused_ip_end, root->gid);
				//output root->end_ip_num:root->gid
			}
			else if(root->start_ip_num != seg_start_ip && root->end_ip_num == seg_end_ip){
				int2ip(unused_ip_end, root->start_ip_num-1);
				snprintf(result, 160, "\n%s:%d\n", unused_ip_end, root->gid);
				//output root->start_ip_num:root->gid
			}
			else{
				printf("\nError: root should be NULL");
			}
        }
        else{
            if(root->used_ip_exist == 1){
				if(root->start_ip_num != seg_start_ip && root->end_ip_num != seg_end_ip){
                	snprintf(result, 160, "[%s, %s)\t", unused_ip_start, unused_ip_end);     //if leaf node is a lChild
				}
				else if(root->start_ip_num == seg_start_ip && root->end_ip_num != seg_end_ip){
					snprintf(result, 160, "\n%s:%d\n", unused_ip_start, root->gid);
					//output root->start_ip_num:root->gid
				}
				else{
					printf("\nError: used ip seen in the last seg");
				}
            }
            else{
				if(root->end_ip_num != seg_end_ip){
                	snprintf(result, 160, "[%s, %s]\t", unused_ip_start, unused_ip_end);     //if leaf node is a rChild
                }
				else{
					int2ip(unused_ip_end, root->start_ip_num-1);
					snprintf(result, 160, "\n%s:%d\n", unused_ip_end, root->gid);
					//output root->end_ip_num:root->gid
				}
            }
        }

		fprintf(fp, result);
    }
	else{
    	STree_output(root->lChild, fp, seg_start_ip, seg_end_ip, cur_time);
    	STree_output(root->rChild, fp, seg_start_ip, seg_end_ip, cur_time);
	}
}

void STree_output_v6(STNode_v6_t * root, FILE * fp, uint128_t seg_start_ip, uint128_t seg_end_ip, int cur_time){
    if(root->lChild == NULL && root->rChild == NULL){
		char result[160] = {0};
		char unused_ip_start[MAX_IP6_LEN] = {0};
		char unused_ip_end[MAX_IP6_LEN] = {0};
		ipv6_int2str(unused_ip_start, root->start_ip_num);
		ipv6_int2str(unused_ip_end, root->end_ip_num);
		
        if(root->valid_time < cur_time){
			if(!(ipv6_eq(root->start_ip_num, seg_start_ip) || ipv6_eq(root->end_ip_num, seg_end_ip))){
				snprintf(result, 160, "[%s, %s]\t", unused_ip_start, unused_ip_end);
			}
			else if(ipv6_eq(root->start_ip_num, seg_start_ip) && !(ipv6_eq(root->end_ip_num, seg_end_ip))){
				snprintf(result, 160, "\n%s:%d\n", unused_ip_end, root->gid);
			}
			else if(!(ipv6_eq(root->start_ip_num, seg_start_ip)) && (ipv6_eq(root->end_ip_num, seg_end_ip))){
				uint128_t ip_num_tmp;
				ipv6_value(root->start_ip_num, &ip_num_tmp);
				ipv6_add_one(&ip_num_tmp, MINUS_OPT);
				ipv6_int2str(unused_ip_end, ip_num_tmp);         //root->start_ip_num - 1
				snprintf(result, 160, "\n%s:%d\n", unused_ip_end, root->gid);
			}
			else{
				printf("\nError: root should be NULL");
			}
        }
       else{
            if(root->used_ip_exist == 1){
				if(!(ipv6_eq(root->start_ip_num, seg_start_ip) || ipv6_eq(root->end_ip_num, seg_end_ip))){
                	snprintf(result, 160, "[%s, %s)\t", unused_ip_start, unused_ip_end);     //if leaf node is a lChild
				}
				else if(ipv6_eq(root->start_ip_num, seg_start_ip) && !(ipv6_eq(root->end_ip_num, seg_end_ip))){
					snprintf(result, 160, "\n%s:%d\n", unused_ip_start, root->gid);
				}
				else{
					printf("\nError: used ip seen in the last seg");
				}
            }
            else{
				if(!(ipv6_eq(root->end_ip_num, seg_end_ip))){
                	snprintf(result, 160, "[%s, %s]\t", unused_ip_start, unused_ip_end);     //if leaf node is a rChild
                }
				else{
					uint128_t ip_num_tmp;
					ipv6_value(root->start_ip_num, &ip_num_tmp);
					ipv6_add_one(&ip_num_tmp, MINUS_OPT);
					ipv6_int2str(unused_ip_end, ip_num_tmp);
					snprintf(result, 160, "\n%s:%d\n", unused_ip_end, root->gid);
				}
            }
        }

		fprintf(fp, result);
    }
	else{
    	STree_output_v6(root->lChild, fp, seg_start_ip, seg_end_ip, cur_time);
    	STree_output_v6(root->rChild, fp, seg_start_ip, seg_end_ip, cur_time);
	}
}


void output_unused_ip(STNode_v4_t * * ipv4_std_list, const int ipv4_list_line_num, STNode_v6_t * * ipv6_std_list, const int ipv6_list_line_num){
	const char * result = "no matched ip";

	FILE *fp = NULL;
	fp = fopen("../src/unused_ipv4.txt", "wb+");

	int cur_time_v4 = time((time_t*)NULL);
	for(int i = 0; i < ipv4_list_line_num; i ++){
		if(ipv4_std_list[i]->lChild != NULL && ipv4_std_list[i]->valid_time >= cur_time_v4){
			delete_node(cur_time_v4, ipv4_std_list[i]);
			STree_output(ipv4_std_list[i], fp, ipv4_std_list[i]->start_ip_num, ipv4_std_list[i]->end_ip_num, cur_time_v4);
		}
	}

	if(NULL == fp){
		fprintf(fp, result);
	}
	fclose(fp);

	FILE *fp2 = NULL;
	fp2 = fopen("../src/unused_ipv6.txt", "wb");

	int cur_time_v6 = time((time_t*)NULL);
	for(int i = 0; i < ipv6_list_line_num; i ++){
		if(ipv6_std_list[i]->lChild != NULL && ipv6_std_list[i]->valid_time >= cur_time_v6){
			//delete_node(cur_time_v6, ipv6_std_list[i]);
			STree_output_v6(ipv6_std_list[i], fp2, ipv6_std_list[i]->start_ip_num, ipv6_std_list[i]->end_ip_num, cur_time_v6);
		}
	}

	if(NULL == fp2){
		fprintf(fp2, result);
	}
	fclose(fp2);

}

void destroy(void * * ip_std_list, int ip_list_line_num, int case_num){
	for(int i = 0; i < ip_list_line_num; i ++){
		//free all pointers
	}
}

screen_stat_handle_t init_stat_handle(){
	screen_stat_handle_t handle = NULL;
	const char * stat_path = "./find_unused_ip.status";
	const char * app_name = "find_unused_ip";
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

	char group_id[512];
	char brokers[512];
	char topic[128];
	int ipv4_list_line_num = 9930714;
	int ipv6_list_line_num = 15380;
	int present_time;
	int output_time;
	
	MESA_load_profile_string_def(conf_file,"KAFKA_INFO","BROKERS",brokers,MAX_BROKER_LEN,"localhost:9092");
	MESA_load_profile_string_def(conf_file,"KAFKA_INFO","GROUP_ID",group_id,MAX_GROUP_ID_LEN,"localhost:9092");
	MESA_load_profile_string_def(conf_file,"KAFKA_INFO","TOPIC",topic,MAX_TOPIC_LEN,"IPD-HTTP-IP-LOG");
	MESA_load_profile_int_def(conf_file,"USER_INFO","VALID_TIME",&valid_time,10800);
	MESA_load_profile_int_def(conf_file,"USER_INFO","OUTPUT_TIME",&output_time_interval,10800);

	//init ipv4/ipv6 two std list - header
	STNode_v4_t * * ipv4_std_list = (STNode_v4_t * *)calloc(ipv4_list_line_num, sizeof(STNode_v4_t *));
	init_std_list((void **)ipv4_std_list, CASE_IPV4);
	STNode_v6_t * * ipv6_std_list = (STNode_v6_t * *)calloc(ipv6_list_line_num, sizeof(STNode_v6_t *));
	init_std_list((void **)ipv6_std_list, CASE_IPV6);

	
	//create mmdb v4
	MMDB_s mmdb_v4;
	int status_v4 = MMDB_open(mmdb_v4_file, MMDB_MODE_MMAP, &mmdb_v4);
	if(MMDB_SUCCESS != status_v4){
		fprintf(stderr, "\n can't open %s - %s", mmdb_v4_file, MMDB_strerror(status_v4));
		if(MMDB_IO_ERROR == status_v4){
			fprintf(stderr, "IO error: %s\n", strerror(errno));
		}
		exit(1);
	}
	//create mmdb v6
	MMDB_s mmdb_v6;
	int status_v6 = MMDB_open(mmdb_v6_file, MMDB_MODE_MMAP, &mmdb_v6);
	if(MMDB_SUCCESS != status_v6){
		fprintf(stderr, "\n can't open %s - %s", mmdb_v6_file, MMDB_strerror(status_v6));
		if(MMDB_IO_ERROR == status_v6){
			fprintf(stderr, "IO error: %s\n", strerror(errno));
		}
		exit(1);
	}

	//field_state init
	screen_stat_handle_t handle = init_stat_handle();
	char buff[128];
	int i = 1;
	int field_ids;

	snprintf(buff, sizeof(buff), "field_%02d", i);
	field_ids = FS_register(handle, FS_STYLE_FIELD, FS_CALC_CURRENT, buff);
	

	output_time = time((time_t *)NULL);

	//read from kafka
	rd_kafka_t *kafka_consumer;
	init_kafka_consumer(&kafka_consumer, topic, group_id, brokers);

	FS_start(handle);
	while(1){
		rd_kafka_message_t *rk_message;
		rk_message = rd_kafka_consumer_poll(kafka_consumer, 1000);
		if(rk_message){
			if(rk_message->err == RD_KAFKA_RESP_ERR_NO_ERROR){
				msg_consume(rk_message, ipv4_std_list, ipv6_std_list, mmdb_v4, mmdb_v6);
				//msg_cnt ++;
				FS_operate(handle, field_ids, 0, FS_OP_ADD, 1);
				rd_kafka_message_destroy(rk_message);
			}
			else{
				MESA_handle_runtime_log(rlog_handle, RLOG_LV_FATAL, module_name, "failed to get rk_message");
			}
		}

		//output to file every OUTPUT_TIME_INTERVAL seconds
		present_time = time((time_t *)NULL);
		if(output_time_interval == (present_time - output_time)){
			output_unused_ip(ipv4_std_list, ipv4_list_line_num, ipv6_std_list, ipv6_list_line_num);
			output_time = present_time;
		}
	}
	FS_stop(&handle);
	rd_kafka_destroy(kafka_consumer);
	MMDB_close(&mmdb_v4);
	MMDB_close(&mmdb_v6);
	destroy((void **)ipv4_std_list, ipv4_list_line_num, CASE_IPV4);
	destroy((void **)ipv6_std_list, ipv6_list_line_num, CASE_IPV6);
}
