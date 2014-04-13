
/*
 * test_db5.cpp
 *
 *  Created on: Apr 8, 2014
 *      Author: killerg
 */


#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <libcouchbase/couchbase.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

enum status_t {
	Test = 0,
	Check = 1,
};

static int s_pending_get = 0;
static int s_pending_set = 0;
static int s_pending_conf = 0;

static int s_op = Test;

static int s_serial = 0;

static lcb_t create_instance();
static void destroy_instance(lcb_t inst);

static void emit_set(lcb_t inst, int serial);
static void emit_get(lcb_t inst, int serial);
static void do_next(lcb_t inst);

static const char* fmt_cstr(const char* fmt, ...);
static const char* get_data(int serial);
static void write_file(int serial, uint8_t* bytes, size_t nbytes);

static const int DOCSIZE = 1024 * 1024 * 19;


int main(int argc, char** argv) {
	if (argc == 1) {
		s_op = Test;
	} else if (argc >= 2) {
		s_op = (status_t)atoi(argv[1]);
		if (s_op > Check || s_op < Test) {
			printf("arg err\n");
			return 1;
		}
	} else {
		printf("arg err\n");
		return 1;
	}

    lcb_t inst = create_instance();
    lcb_wait(inst);
    destroy_instance(inst);
    printf("pace finish\n");
    return 0;
}

static void error_callback(lcb_t instance, lcb_error_t error,
        const char *errinfo) {
	printf("pending set:%d get:%d conf:%d\n", s_pending_set, s_pending_get, s_pending_conf);
    fprintf(stderr, "ERROR: %s %s\n", lcb_strerror(instance, error), errinfo);
	printf("exit\n");
    exit(1);
}

static void configuration_callback(lcb_t inst, lcb_configuration_t config) {
    s_pending_conf = 0;

    do_next(inst);
}

static void store_callback(lcb_t inst, const void *cookie,
        lcb_storage_t operation, lcb_error_t error,
        const lcb_store_resp_t *resp) {
    --s_pending_set;
    //printf("serial:%lu store_callback %s\n", (size_t)cookie, lcb_strerror(inst, error));

    do_next(inst);
}

static void get_callback(lcb_t inst, const void *cookie, lcb_error_t error,
        const lcb_get_resp_t *resp) {
    --s_pending_get;

    int serial = (size_t)cookie;

    //printf("serial:%d get_callback %s\n", serial, lcb_strerror(inst, error));
    if (error == LCB_SUCCESS) {
    	int64_t nbytes = resp->v.v0.nbytes;
    	uint8_t* bytes = (uint8_t*)resp->v.v0.bytes;
    	if (s_op == 1) {
        	if (memcmp(bytes, get_data(serial), DOCSIZE)) {
        		write_file(serial, bytes, nbytes);
        		exit(0);
        	}
    	} else if (s_op == 2) {
    		write_file(serial, bytes, nbytes);
    	    exit(0);
    	}
    } else if (error == LCB_KEY_ENOENT) {
    	printf("key:%d not exist\n", serial);
    }

    do_next(inst);
}

static void destroy_instance(lcb_t inst) {
    if (inst != NULL) {
        lcb_destroy(inst);
    }
}

static lcb_t create_instance() {
    lcb_t inst;
    lcb_create_st copts("localhost", "", "", "");
    lcb_error_t err = lcb_create(&inst, &copts);
    if (err != LCB_SUCCESS) {
        fprintf(stderr, "Failed to create a libcouchbase instance: %s\n",
                lcb_strerror(NULL, err));
        return NULL;
    }

    /* Set up the callbacks */
    lcb_set_error_callback(inst, error_callback);
    lcb_set_configuration_callback(inst, configuration_callback);
    lcb_set_store_callback(inst, store_callback);
    lcb_set_get_callback(inst, get_callback);

    switch (s_op) {
    case Test:
    	lcb_set_timeout(inst, 1000);
    	break;
    case Check:
        lcb_set_timeout(inst, 1000 * 1000 * 10);
    	break;
    }


    err = lcb_connect(inst);
    if (err != LCB_SUCCESS) {
        fprintf(stderr, "Failed to connect libcouchbase instance: %s\n",
                lcb_strerror(NULL, err));
        lcb_destroy(inst);
        return NULL;
    }

    ++s_pending_conf;

    return inst;
}

static void emit_set(lcb_t inst, int serial) {
//	printf("start set s_serial %d\n", serial);
    const char* key = fmt_cstr("key:%d", serial);

    lcb_store_cmd_t cmd(LCB_SET, key, strlen(key), get_data(serial), DOCSIZE);
    lcb_store_cmd_t *cmds[] = { &cmd };

    lcb_error_t err = lcb_store(inst, (void*)(size_t)serial, 1, cmds);
    if (err == LCB_SUCCESS) {
        ++s_pending_set;
    } else {
        fprintf(stderr, "Failed to set up store request: %s\n", lcb_strerror(inst, err));
        exit(1);
    }
}

static void emit_get(lcb_t inst, int serial) {
//	printf("start check s_serial %d\n", serial);
    const char* key = fmt_cstr("key:%d", serial);

    lcb_get_cmd_t cmd(key, strlen(key));
    lcb_get_cmd_t *cmds[] = { &cmd };

    lcb_error_t err = lcb_get(inst, (void*)(size_t)serial, 1, cmds);
    if (err == LCB_SUCCESS) {
        ++s_pending_get;
    } else {
        fprintf(stderr, "Failed to setup get request: %s\n", lcb_strerror(inst, err));
        exit(1);
    }
}

static const char* get_data(int serial) {
    static char buffer[DOCSIZE];
    memset(buffer, serial, DOCSIZE);
    return buffer;
}

static void write_file(int serial, uint8_t* bytes, size_t nbytes) {
	int fd = open(fmt_cstr("invalid_%d", serial), O_CREAT|O_WRONLY|O_TRUNC, DEFFILEMODE);
	if (fd < 0) {
		perror("create file");
		exit(0);
	}

	int n = 0;
	do {
		n = write(fd, bytes, nbytes);
		if (n < 0)
			break;
		nbytes -= n;
		bytes += n;
	} while (nbytes > 0);
	close(fd);
}

static const char* fmt_cstr(const char* fmt, ...) {
    va_list p;
    va_start(p, fmt);

    static char buf[1024 * 8] = { 0 };
    vsprintf(buf, fmt, p);

    va_end(p);
    return buf;
}

static void do_next(lcb_t inst) {
	switch (s_op) {
	case Test:
		if (s_pending_conf == 0 && s_pending_set == 0)
			emit_set(inst, s_serial++);
		break;
	case Check:
		if (s_pending_conf == 0 && s_pending_get == 0)
			emit_get(inst, s_serial++);
		break;
	}
	s_serial = s_serial % 100;
}
