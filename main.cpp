#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <libcouchbase/couchbase.h>
#include <event2/event.h>
#include <sys/signal.h>
#include <openssl/md5.h>
#include "json/json.h"

static int32_t random(int32_t cbegin, int32_t cend);
static const char* fmt_cstr(const char* fmt, ...);
static const char* to_cstr(const Json::Value &json);
static std::string fmt_str(const char* fmt, ...);
static Json::Value to_json(const char* str);
static void on_signal(int32_t sig, void(*handler)(int));
static const char* Md5(const char* str);
static const char* hex_to_cstr(uint8_t* buf, int len);

static int DATASIZEMIN = 100;
static int DATASIZEMAX = 10000;
static int DOCNUM = 1000;
static int TOTALDOCNUM = 100000;

enum state_t {
	Null = -1,      // uninitialized may be just started or destroied
	Initing = 0,    // initing
	Running = 1,    // good status
	Destroying = 2, // it may be some mistake so need destroy for next initing
};

static state_t s_state = Null;
static int s_pending_get = 0;
static int s_pending_set = 0;
static int s_pending_conf = 0;
static lcb_t s_inst = NULL;

static lcb_io_opt_t create_libevent_io_ops(struct event_base *evbase);
static void create_instance(lcb_io_opt_t ioops);
static void destroy_instance();

static void gen_data(int seed, Json::Value &data);
static bool check_data(const Json::Value &data);

static bool run = true;
static void handle_ctrl_c(int signal) {
    run = false;
    s_state = Destroying;
}


int main(int argc, char** argv) {

    on_signal(SIGINT, handle_ctrl_c);

    struct event_base *evbase = event_base_new();
    lcb_io_opt_t ioops = create_libevent_io_ops(evbase);

	while (true) {
		switch (s_state) {
		case Null:
		    assert(s_pending_conf == 0 && s_pending_set == 0 && s_pending_get == 0);
		    create_instance(ioops);
			break;
		case Running:
			if (s_pending_conf == 0 && s_pending_get == 0 && s_pending_set == 0) {
				Json::Value data;
				for (int i=0; i<DOCNUM; ++i) {
					gen_data(i, data);

					std::string value = to_cstr(data);
					std::string key = fmt_cstr("key:%d", i);

					lcb_store_cmd_t cmd(LCB_SET, key.c_str(), key.length(), value.c_str(), value.length());
					lcb_store_cmd_t *cmds[] = {&cmd};

					lcb_error_t err = lcb_store(s_inst, NULL, 1, cmds);
					if (err == LCB_SUCCESS) {
						++s_pending_set;
					} else {
						fprintf(stderr, "Failed to set up store request: %s\n", lcb_strerror(s_inst, err));
					    s_state = Destroying;
					    break;
					}
				}
			}

			break;
		case Destroying:
			if (s_pending_conf == 0 && s_pending_get == 0 && s_pending_set == 0) {
				destroy_instance();
				if (!run)
					goto finish;
			}
			break;
		}
		event_base_loop(evbase, EVLOOP_NONBLOCK);
		usleep(1000*100);
		printf("status:%d\n", s_state);
		printf("pending\n\tconf:%d\n\tset:%d\n\tget:%d\n",
				s_pending_conf,
				s_pending_set,
				s_pending_get);
		printf("____________________\n");
	}

finish:

	lcb_destroy_io_ops(ioops);
    event_base_free(evbase);

    printf("pace finish\n");
	return true;
}

static void error_callback(lcb_t instance,
                           lcb_error_t error,
                           const char *errinfo)
{
    fprintf(stderr, "ERROR: %s %s\n", lcb_strerror(instance, error), errinfo);
    s_state = Destroying;
}

static void configuration_callback(lcb_t instance, lcb_configuration_t config)
{
	s_pending_conf = 0;
	s_state = Running;

	if (s_pending_conf == 0 && s_pending_get == 0 && s_pending_set == 0) {
		Json::Value data;
		for (int i=0; i<DOCNUM; ++i) {
			gen_data(i, data);

			std::string value = to_cstr(data);
			std::string key = fmt_cstr("key:%d", i);

			lcb_store_cmd_t cmd(LCB_SET, key.c_str(), key.length(), value.c_str(), value.length());
			lcb_store_cmd_t *cmds[] = {&cmd};

			lcb_error_t err = lcb_store(instance, NULL, 1, cmds);
			if (err == LCB_SUCCESS) {
				++s_pending_set;
			} else {
				fprintf(stderr, "Failed to set up store request: %s\n", lcb_strerror(instance, err));
				s_state = Destroying;
				break;
			}
		}
	}
}

static void get_callback(lcb_t instance,
                         const void *cookie,
                         lcb_error_t error,
                         const lcb_get_resp_t *resp)
{
	--s_pending_get;

	assert(s_state == Running || s_state == Destroying);

	if (error != LCB_SUCCESS) {
        fprintf(stderr, "Failed to get key: %s\n", lcb_strerror(instance, error));
    } else {
    	std::string str((char*)resp->v.v0.bytes, resp->v.v0.nbytes);
    	Json::Value data = to_json(str.c_str());
    	assert(check_data(data));
    	if (s_pending_get == 0 && s_state == Running) {
    		for (int i=0; i<DOCNUM; ++i) {
    			gen_data(i, data);

    			std::string value = to_cstr(data);
    			std::string key = fmt_cstr("key:%d", i);

    			lcb_store_cmd_t cmd(LCB_SET, key.c_str(), key.length(), value.c_str(), value.length());
    			lcb_store_cmd_t *cmds[] = {&cmd};

        		lcb_error_t err = lcb_store(instance, NULL, 1, cmds);
        		if (err == LCB_SUCCESS) {
        			++s_pending_set;
        		} else {
        			fprintf(stderr, "Failed to set up store request: %s\n", lcb_strerror(instance, err));
        		    s_state = Destroying;
        		    break;
        		}
    		}
    	}
    }
}

static void store_callback(lcb_t instance,
                           const void *cookie,
                           lcb_storage_t operation,
                           lcb_error_t error,
                           const lcb_store_resp_t *resp)
{
	--s_pending_set;

	assert(s_state == Running || s_state == Destroying);

    if (error != LCB_SUCCESS) {
        fprintf(stderr, "Failed to store key: %s\n", lcb_strerror(instance, error));
    } else {
    	if (s_state == Running && s_pending_set == 0) {
    		for (int i=0; i<DOCNUM; ++i) {
    			std::string key = fmt_cstr("key:%d", i);

                lcb_get_cmd_t cmd(key.c_str(), key.length());
                lcb_get_cmd_t *cmds[] = {&cmd};

                lcb_error_t err = lcb_get(instance, NULL, 1, cmds);
                if (err == LCB_SUCCESS) {
                	++s_pending_get;
                } else {
                    fprintf(stderr, "Failed to setup get request: %s\n", lcb_strerror(instance, error));
            	    s_state = Destroying;
            	    break;
                }
    		}
    	}
    }
}

static lcb_io_opt_t create_libevent_io_ops(struct event_base *evbase)
{
    struct lcb_create_io_ops_st ciops;
    lcb_io_opt_t ioops;
    lcb_error_t error;

    memset(&ciops, 0, sizeof(ciops));
    ciops.v.v0.type = LCB_IO_OPS_LIBEVENT;
    ciops.v.v0.cookie = evbase;

    error = lcb_create_io_ops(&ioops, &ciops);
    if (error != LCB_SUCCESS) {
        fprintf(stderr, "Failed to create an IOOPS structure for libevent: %s\n", lcb_strerror(NULL, error));
        exit(EXIT_FAILURE);
    }

    return ioops;
}

static void destroy_instance() {
	assert(s_state == Destroying);

	if (s_inst != NULL) {
		lcb_destroy(s_inst);
		s_inst = NULL;
	}
	s_state = Null;
}

static void create_instance(lcb_io_opt_t ioops) {
	assert(s_state == Null && s_inst == NULL);

    lcb_t instance;
    lcb_error_t error;
    lcb_create_st copts("192.168.1.3", "", "", "", ioops);
    lcb_error_t err = lcb_create(&instance, &copts);
    if (err != LCB_SUCCESS) {
        fprintf(stderr, "Failed to create a libcouchbase instance: %s\n", lcb_strerror(NULL, error));
        exit(EXIT_FAILURE);
    }

    /* Set up the callbacks */
    lcb_set_error_callback(instance, error_callback);
    lcb_set_configuration_callback(instance, configuration_callback);
    lcb_set_get_callback(instance, get_callback);
    lcb_set_store_callback(instance, store_callback);

    err = lcb_connect(instance);
    if (err != LCB_SUCCESS) {
        fprintf(stderr, "Failed to connect libcouchbase instance: %s\n", lcb_strerror(NULL, error));
        lcb_destroy(instance);
        return;
    }

    ++s_pending_conf;
    s_inst = instance;
    s_state = Initing;
}

static void gen_data(int seed, Json::Value &data) {
	char buffer[DATASIZEMAX+1];
	int len = random(DATASIZEMIN, DATASIZEMAX);
	for (int i=0; i<len; ++i) {
		buffer[i] = 48 + i % (125-48);
	}
	buffer[len] = 0;
	data["data"] = buffer;
	data["md5"] = Md5(buffer);
}
static bool check_data(const Json::Value &data) {
	return data["md5"].asString() == Md5(data["data"].asCString());
}


const int BUFFSIZE = 1024*8;
// print to static buffer
static const char* fmt_cstr(const char* fmt, ...) {
    va_list p;
    va_start(p, fmt);

    static char buf[BUFFSIZE] = { 0 };
    vsprintf(buf, fmt, p);

    va_end(p);
    return buf;
}


static const char* to_cstr(const Json::Value &json) {
    static Json::FastWriter writer;
    static std::string tmp = writer.write(json);
    return tmp.c_str();
}

static Json::Value to_json(const char* str) {
	if (str == NULL || 0 == strcmp(str, "")) {
		return Json::nullValue;
	}

    Json::Value json;
    Json::Reader reader;
    bool ok = reader.parse(str, json);
    assert(ok);

    return std::move(json);
}

static void on_signal(int32_t sig, void(*handler)(int)) {
    struct sigaction act;
    act.sa_handler = handler;
    act.sa_flags = SA_RESTART;
    sigaction(sig, &act, NULL);
}
static const char* Md5(const char* str) {
    uint8_t* buf = MD5((uint8_t*)str, strlen(str), NULL);
    return hex_to_cstr(buf, 16);
}

static int32_t random(int32_t cbegin, int32_t cend) {
    return cbegin + rand()%(cend - cbegin + 1);
}

static const char* hex_to_cstr(uint8_t* buf, int len) {
    static char ret_buf[2048];
    for (int i=0; i<len; ++i)
        sprintf(ret_buf+i*2, "%02x", buf[i]);

    return ret_buf;
}

