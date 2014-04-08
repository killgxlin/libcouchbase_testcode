#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <libcouchbase/couchbase.h>
#include <event2/event.h>
#include <sys/signal.h>
#include <openssl/md5.h>
#include "json/json.h"

static const char* fmt_cstr(const char* fmt, ...);
static const char* to_cstr(const Json::Value &json);
static Json::Value to_json(const char* str);
static void on_signal(int32_t sig, void(*handler)(int));
static const char* Md5(const char* str);
static const char* hex_to_cstr(uint8_t* buf, int len);

static const int DOCNUM = 10000;
static const int DOCSIZE = 1024 * 16;

enum state_t {
	Null = -1,
	Initing = 0,
	Running = 1,
	Destroying = 2,
};

static int 			s_pending_view = 0;
static int 			s_pending_set = 0;
static int 			s_pending_conf = 0;

static bool 		s_run = true;
static lcb_t 		s_inst = NULL;
static state_t 		s_state = Null;
static Json::Value 	s_json;

static lcb_io_opt_t create_libevent_io_ops(struct event_base *evbase);
static void 		create_instance(lcb_io_opt_t ioops);
static void 		destroy_instance();
static void 		emit_sets();
static void 		emit_check();
static void 		gen_json();

static void handle_ctrl_c(int signal) {
    s_run = false;
    s_state = Destroying;
}

int main() {
    on_signal(SIGINT, handle_ctrl_c);

    struct event_base *evbase = event_base_new();
    lcb_io_opt_t ioops = create_libevent_io_ops(evbase);

	while (true) {
		switch (s_state) {
		case Null:
		    assert(s_pending_conf == 0 && s_pending_set == 0 && s_pending_view == 0);
		    create_instance(ioops);
			break;
		case Running:
			if (s_pending_conf == 0 && s_pending_view == 0 && s_pending_set == 0)
				emit_check();

			break;
		case Destroying:
			if (s_pending_conf == 0 && s_pending_view == 0 && s_pending_set == 0) {
				destroy_instance();
				if (!s_run)
					goto finish;
			}
			break;
        default:
            break;
		}
		event_base_loop(evbase, EVLOOP_ONCE);
		printf("status:%d\n", s_state);
		printf("pending\n\tconf:%d\n\tset:%d\n\tview:%d\n",
				s_pending_conf,
				s_pending_set,
				s_pending_view);
		printf("____________________\n");
	}

finish:

	lcb_destroy_io_ops(ioops);
    event_base_free(evbase);

    printf("pace finish\n");
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

	if (s_state == Running && s_pending_set == 0 && s_pending_conf == 0 && s_pending_view == 0)
		emit_check();
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
        //fprintf(stderr, "Failed to store key: %s\n", lcb_strerror(instance, error));
    } else {
    	if (s_state == Running && s_pending_set == 0) {
    		emit_check();
    	}
    }
}

static void view_callback(
		lcb_http_request_t request,
		lcb_t conn,
		const void *cookie,
		lcb_error_t error,
		const lcb_http_resp_t *resp)
{
	--s_pending_view;

	assert(s_state == Running || s_state == Destroying);

    if (error != LCB_SUCCESS) {
        //fprintf(stderr, "Failed to store key: %s\n", lcb_strerror(s_inst, error));
    } else {
    	std::string str((const char*)resp->v.v0.bytes, resp->v.v0.nbytes);
    	Json::Value view_json = to_json(str.c_str());
    	assert(view_json["total_rows"].asInt() == 0);
    	if (s_state == Running && s_pending_view == 0)
    		emit_sets();
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
    lcb_create_st copts("192.168.1.3", "", "", "", ioops);
    lcb_error_t err = lcb_create(&instance, &copts);
    if (err != LCB_SUCCESS) {
        fprintf(stderr, "Failed to create a libcouchbase instance: %s\n", lcb_strerror(NULL, err));
        exit(EXIT_FAILURE);
    }

    /* Set up the callbacks */
    lcb_set_error_callback(instance, error_callback);
    lcb_set_configuration_callback(instance, configuration_callback);
    lcb_set_store_callback(instance, store_callback);
    lcb_set_http_complete_callback(instance, view_callback);
    lcb_set_timeout(instance, 1000 * 1000 * 3);

    err = lcb_connect(instance);
    if (err != LCB_SUCCESS) {
        fprintf(stderr, "Failed to connect libcouchbase instance: %s\n", lcb_strerror(NULL, err));
        lcb_destroy(instance);
        return;
    }

    ++s_pending_conf;
    s_inst = instance;
    s_state = Initing;
}

static void emit_sets() {
	gen_json();

	for (int i=0; i<DOCNUM; ++i) {
		std::string value = to_cstr(s_json);
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

static void emit_check() {
	const char* path = "/_design/check/_view/check?stale=false&connection_timeout=60000";
	lcb_http_request_t req;
    lcb_http_cmd_t cmd(path, strlen(path), NULL, 0, LCB_HTTP_METHOD_GET, 0, "application/json");

    lcb_error_t err = lcb_make_http_request(s_inst, NULL, LCB_HTTP_TYPE_VIEW, &cmd, &req);
	if (err == LCB_SUCCESS) {
		++s_pending_view;
	} else {
		fprintf(stderr, "Failed to setup view request: %s\n", lcb_strerror(s_inst, err));
		s_state = Destroying;
		return;
	}
}

static void gen_json() {
	static char buffer[DOCSIZE];
	static bool inited = false;
	if (inited)
		return;

	inited = true;
	for (int i=0; i<DOCSIZE - 1; ++i)
		buffer[i] = '0' + i % 10;
	buffer[DOCSIZE - 1] = 0;

	s_json["data"] = buffer;
	s_json["checksum"] = Md5(buffer);
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

static const char* hex_to_cstr(uint8_t* buf, int len) {
    static char ret_buf[2048];
    for (int i=0; i<len; ++i)
        sprintf(ret_buf+i*2, "%02x", buf[i]);

    return ret_buf;
}

