#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <netdb.h>
#include <unistd.h>

#include <rdma/rsocket.h>

union rsocket_address {
	struct sockaddr		sa;
	struct sockaddr_in	sin;
	struct sockaddr_in6	sin6;
};

static const char *port = "7427";
static char *dst_addr;
static char *dst_file;
static char *src_file;
static uint64_t bytes;
static int fd;
static void *file_addr;

enum {
	CMD_NOOP,
	CMD_OPEN,
	CMD_CLOSE,
	CMD_WRITE,
	CMD_RESP = 0x80,
};

/* TODO: handle byte swapping */
struct msg_hdr {
	uint8_t  version;
	uint8_t  command;
	uint16_t len;
	uint32_t data;
	uint64_t id;
};

struct msg_write {
	struct msg_hdr hdr;
	uint64_t size;
};

static size_t _recv(int rs, char *msg, size_t len) {
	size_t ret;

	for (size_t offset = 0; offset < len; offset += ret) {
		ret = rrecv(rs, msg + offset, len - offset, 0);
		if (ret <= 0)
			return ret;
	}

	return len;
}

static int msg_recv_hdr(int rs, struct msg_hdr *hdr) {
	int ret = _recv(rs, (char *) hdr, sizeof *hdr);
	if (ret != sizeof *hdr)
		return -1;

	if (hdr->version || hdr->len < sizeof *hdr) {
		printf("invalid version %d or length %d\n",
		       hdr->version, hdr->len);
		return -1;
	}

	return sizeof *hdr;
}

static int msg_get_resp(int rs, struct msg_hdr *msg, uint8_t cmd) {
	int ret = msg_recv_hdr(rs, msg);
	if (ret != sizeof *msg)
		return ret;

	if ((msg->len != sizeof *msg) || (msg->command != (cmd | CMD_RESP))) {
		printf("invalid length %d or bad command response %x:%x\n",
		       msg->len, msg->command, cmd | CMD_RESP);
		return -1;
	}

	return msg->data;
}

static void msg_send_resp(int rs, struct msg_hdr *msg, uint32_t status) {
	struct msg_hdr resp;
	resp.version = 0;
	resp.command = msg->command | CMD_RESP;
	resp.len = sizeof resp;
	resp.data = status;
	resp.id = msg->id;
	rsend(rs, (char *) &resp, sizeof resp, 0);
}

static int server_listen(void) {
	struct addrinfo hints, *res;
	memset(&hints, 0, sizeof hints);
	hints.ai_flags = RAI_PASSIVE;
 	int ret = getaddrinfo(NULL, port, &hints, &res);
	if (ret) {
		printf("getaddrinfo failed: %s\n", gai_strerror(ret));
		return ret;
	}

	int rs = rsocket(res->ai_family, res->ai_socktype, res->ai_protocol);
	if (rs < 0) {
		if (res->ai_socktype == SOCK_STREAM && errno == ENODEV)
			fprintf(stderr, "No RDMA devices were detected\n");
		else
			perror("rsocket failed");
		ret = rs;
		goto free;
	}

	ret = 1;
	ret = rsetsockopt(rs, SOL_SOCKET, SO_REUSEADDR, &ret, sizeof ret);
	if (ret) {
		perror("rsetsockopt failed");
		goto close;
	}

	ret = rbind(rs, res->ai_addr, res->ai_addrlen);
	if (ret) {
		perror("rbind failed");
		goto close;
	}

	ret = rlisten(rs, 1);
	if (ret) {
		perror("rlisten failed");
		goto close;
	}

	ret = rs;
	goto free;

close:
	rclose(rs);
free:
	freeaddrinfo(res);
	return ret;
}

static int server_open(int rs, struct msg_hdr *msg) {
	char *path = NULL;
	int ret, len;

	printf("opening: ");
	fflush(NULL);
	if (file_addr || fd > 0) {
		printf("cannot open another file\n");
		ret = EBUSY;
		goto out;
	}

	len = msg->len - sizeof *msg;
	path = malloc(len);
	if (!path) {
		printf("cannot allocate path name\n");
		ret = ENOMEM;
		goto out;
	}

	ret = _recv(rs, path, len);
	if (ret != len) {
		printf("error receiving path\n");
		goto out;
	}

	printf("%s, ", path);
	fflush(NULL);
	fd = open(path, O_RDWR | O_CREAT | O_TRUNC, msg->data);
	if (fd < 0) {
		printf("unable to open destination file\n");
		ret = errno;
		goto out;
	}

	ret = 0;
out:
	if (path)
		free(path);

	msg_send_resp(rs, msg, ret);
	return ret;
}

static void server_close(int rs, struct msg_hdr *msg)
{
	printf("closing...");
	fflush(NULL);
	msg_send_resp(rs, msg, 0);

	if (file_addr) {
		munmap(file_addr, bytes);
		file_addr = NULL;
	}

	if (fd > 0) {
		close(fd);
		fd = 0;
	}
	printf("done\n");
}

static int server_write(int rs, struct msg_hdr *msg)
{
	size_t len;
	int ret;

	printf("transferring");
	fflush(NULL);
	if (fd <= 0) {
		printf("...file not opened\n");
		ret = EINVAL;
		goto out;
	}

	if (msg->len != sizeof(struct msg_write)) {
		printf("...invalid message length %d\n", msg->len);
		ret = EINVAL;
		goto out;
	}

	ret = _recv(rs, (char *) &bytes, sizeof bytes);
	if (ret != sizeof bytes)
		goto out;

	ret = ftruncate(fd, bytes);
	if (ret)
		goto out;

	file_addr = mmap(NULL, bytes, PROT_WRITE, MAP_SHARED, fd, 0);
	if (file_addr == (void *) -1) {
		printf("...error mapping file\n");
		ret = errno;
		goto out;
	}

	printf("...%lld bytes...", (long long) bytes);
	fflush(NULL);
	len = _recv(rs, file_addr, bytes);
	if (len != bytes) {
		printf("...error receiving data\n");
		ret = (int) len;
	}
out:
	msg_send_resp(rs, msg, ret);
	return ret;
}

static void server_process(int rs) {
	struct msg_hdr msg;
	int ret;

	do {
		ret = msg_recv_hdr(rs, &msg);
		if (ret != sizeof msg)
			break;

		switch (msg.command) {
		case CMD_OPEN:
			ret = server_open(rs, &msg);
			break;
		case CMD_CLOSE:
			server_close(rs, &msg);
			ret = 0;
			break;
		case CMD_WRITE:
			ret = server_write(rs, &msg);
			break;
		default:
			msg_send_resp(rs, &msg, EINVAL);
			ret = -1;
			break;
		}

	} while (!ret);
}

static char *_ntop(union rsocket_address *rsa) {
	static char addr[32];

	switch (rsa->sa.sa_family) {
	case AF_INET:
		inet_ntop(AF_INET, &rsa->sin.sin_addr, addr, sizeof addr);
		break;
	case AF_INET6:
		inet_ntop(AF_INET6, &rsa->sin6.sin6_addr, addr, sizeof addr);
		break;
	default:
		addr[0] = '\0';
		break;
	}

	return addr;
}

static int server_run(void) {
	int rs;
	union rsocket_address rsa;
	socklen_t len;

	int lrs = server_listen();
	if (lrs < 0)
		return lrs;

	while (1) {
		len = sizeof rsa;
		printf("waiting for connection...");
		fflush(NULL);
		rs = raccept(lrs, &rsa.sa, &len);

		printf("client: %s\n", _ntop(&rsa));
		server_process(rs);

		rshutdown(rs, SHUT_RDWR);
		rclose(rs);
	}
	return 0;
}

static int client_connect(void) {
	struct addrinfo *res;

 	int ret = getaddrinfo(dst_addr, port, NULL, &res);
	if (ret) {
		printf("getaddrinfo failed: %s\n", gai_strerror(ret));
		return ret;
	}

	int rs = rsocket(res->ai_family, res->ai_socktype, res->ai_protocol);
	if (rs < 0) {
		if (res->ai_socktype == SOCK_STREAM && errno == ENODEV)
			fprintf(stderr, "No RDMA devices were detected\n");
		else
			perror("rsocket failed");
		goto free;
	}
	printf("rsocket success\n");

	ret = rconnect(rs, res->ai_addr, res->ai_addrlen);
	if (ret) {
		perror("rconnect failed");
		rclose(rs);
		rs = ret;
	} else {
		printf("rconnect success\n");
	}

free:
	freeaddrinfo(res);
	return rs;
}

static int client_open(int rs) {
	struct msg_open {
		struct msg_hdr hdr;
		char path[];
	};
	struct msg_open *msg;
	struct stat stats;

	printf("opening...");
	fflush(NULL);
	fd = open(src_file, O_RDONLY);
	if (fd < 0)
		return fd;

	int ret = fstat(fd, &stats);
	if (ret < 0)
		goto err1;

	bytes = (uint64_t) stats.st_size;
	file_addr = mmap(NULL, bytes, PROT_READ, MAP_SHARED, fd, 0);
	if (file_addr == (void *) -1) {
		ret = errno;
		goto err1;
	}

	uint32_t len = (((uint32_t) strlen(dst_file)) + 8) & 0xFFFFFFF8;
	msg = calloc(1, sizeof(*msg) + len);
	if (!msg) {
		ret = -1;
		goto err2;
	}

	msg->hdr.command = CMD_OPEN;
	msg->hdr.len = sizeof(*msg) + len;
	msg->hdr.data = (uint32_t) stats.st_mode;
	strcpy(msg->path, dst_file);
	ret = rsend(rs, msg, msg->hdr.len, 0);
	if (ret != msg->hdr.len)
		goto err3;

	ret = msg_get_resp(rs, &msg->hdr, CMD_OPEN);
	if (ret)
		goto err3;

	return 0;

err3:
	free(msg);
err2:
	munmap(file_addr, bytes);
err1:
	close(fd);
	return ret;
}

static int client_start_write(int rs) {
	struct msg_write msg;

	printf("transferring");
	fflush(NULL);
	memset(&msg, 0, sizeof msg);
	msg.hdr.command = CMD_WRITE;
	msg.hdr.len = sizeof(msg);
	msg.size = bytes;

	int ret = rsend(rs, &msg, sizeof msg, 0);
	if (ret != msg.hdr.len)
		return ret;

	return 0;
}

static int client_close(int rs) {
	printf("closing...");
	fflush(NULL);
	struct msg_hdr msg;
	memset(&msg, 0, sizeof msg);
	msg.command = CMD_CLOSE;
	msg.len = sizeof msg;
	int ret = rsend(rs, (char *) &msg, msg.len, 0);
	if (ret != msg.len)
		goto out;

	ret = msg_get_resp(rs, &msg, CMD_CLOSE);
	if (ret)
		goto out;

	printf("done\n");
out:
	munmap(file_addr, bytes);
	close(fd);
	return ret;
}

static int client_run(void) {
	struct msg_hdr ack;
	size_t len;

	int rs = client_connect();
	if (rs < 0)
		return rs;

	int ret = client_open(rs);
	if (ret)
		goto shutdown;

	ret = client_start_write(rs);
	if (ret)
		goto close;

	printf("...");
	fflush(NULL);
	struct timeval start, end;
	gettimeofday(&start, NULL);
	len = rsend(rs, file_addr, bytes, 0);
	if (len == bytes)
		ret = msg_get_resp(rs, &ack, CMD_WRITE);
	else
		ret = (int) len;

	gettimeofday(&end, NULL);

close:
	client_close(rs);
shutdown:
	rshutdown(rs, SHUT_RDWR);
	rclose(rs);
	if (!ret) {
		float usec = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
		printf("%lld bytes in %.2f seconds = %.2f Gb/sec\n",
			   (long long) bytes, usec / 1000000., (bytes * 8) / (1000. * usec));
	}
	return ret;
}

static void show_usage(char *program) {
	printf("usage 1: %s [options]\n", program);
	printf("\t     starts the server application\n");
	printf("\t[-p  port_number]\n");
	printf("usage 2: %s source server[:destination] [options]\n", program);
	printf("\t     source - file name and path\n");
	printf("\t     server - name or address\n");
	printf("\t     destination - file name and path\n");
	printf("\t[-p  port_number]\n");
	exit(1);
}

static void server_opts(int argc, char **argv) {
	int op;

	while ((op = getopt(argc, argv, "p:")) != -1) {
		switch (op) {
		case 'p':
			port = optarg;
			break;
		default:
			show_usage(argv[0]);
		}
	}
}

static void client_opts(int argc, char **argv) {
	if (argc < 3) {
		show_usage(argv[0]);
	}
	
	src_file = argv[1];
	dst_addr = argv[2];
	// 如果目标地址中包含冒号，则分离出目标文件名
	char *colon = strchr(dst_addr, ':');
	if (colon) {
		*colon = '\0';
		dst_file = colon + 1;
	} else {
		dst_file = src_file;
	}

	int op;
	while ((op = getopt(argc, argv, "p:")) != -1) {
		switch (op) {
		case 'p':
			port = optarg;
			break;
		default:
			show_usage(argv[0]);
		}
	}
}

int main(int argc, char **argv) {
	int ret;

	if (argc == 1 || argv[1][0] == '-') {
		server_opts(argc, argv);
		ret = server_run();
	} else {
		client_opts(argc, argv);
		ret = client_run();
	}

	return ret;
}
