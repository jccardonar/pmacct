/*  
    pmacct (Promiscuous mode IP Accounting package)
    pmacct is Copyright (C) 2003-2017 by Paolo Lucente
*/

/*
    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
*/

#define __PMBGP_C

/* includes */
#include "pmacct.h"
#include "pmacct-data.h"
#include "addr.h"
#include "zmq_common.h"
#include "bgp/bgp.h"
#include "pmbgpd.h"
#include "pmbgp.h"

/* functions */
#ifdef WITH_ZMQ
void usage_pmbgp(char *prog)
{
  printf("%s %s (%s)\n", PMBGP_USAGE_HEADER, PMACCT_VERSION, PMACCT_BUILD);
  printf("Usage: %s [options] [query]\n\n", prog);
  printf("Query options:\n");
  printf("  -a\tIP address to look up\n");
  printf("  -d\tRoute Distinguisher to look up\n");
  printf("  -r\tBGP peer routing table to look up\n");
  printf("General options:\n");
  printf("  -z\tLooking Glass IP address [default: 127.0.0.1]\n");
  printf("  -Z\tLooking Glass port [default: 17900]\n");
  printf("  -u\tLooking glass username [default: none]\n");
  printf("  -p\tLooking Glass password [default: none]\n");
  printf("\n");
  printf("  -h\tShow this page\n");
  printf("  -V\tPrint version and exit\n");
  printf("\n");
  printf("For suggestions, critics, bugs, contact me: %s.\n", MANTAINER);
}

void version_pmbgp(char *prog)
{
  printf("%s %s (%s)\n", PMBGP_USAGE_HEADER, PMACCT_VERSION, PMACCT_BUILD);
  printf("\n");
  printf("For suggestions, critics, bugs, contact me: %s.\n", MANTAINER);
}

int main(int argc,char **argv)
{
  char address_str[SRVBUFLEN], peer_str[SRVBUFLEN], rd_str[SRVBUFLEN], *req_str = NULL;
  char *req_type_str = NULL, *rep_str = NULL;
  char *zmq_host_str_ptr, zmq_host_str[SRVBUFLEN], default_zmq_host_str[] = "127.0.0.1";
  int ret, zmq_port = 0, default_zmq_port = 17900, results = 0, query_type = 0, idx = 0;

  struct p_zmq_host zmq_host;
  struct host_addr address_ha;

  /* getopt() stuff */
  extern char *optarg;
  extern int optind, opterr, optopt;
  int errflag, cp;

  memset(address_str, 0, sizeof(address_str));
  memset(rd_str, 0, sizeof(rd_str));
  memset(peer_str, 0, sizeof(peer_str));
  memset(zmq_host_str, 0, sizeof(zmq_host_str));
  memset(&zmq_host, 0, sizeof(zmq_host));

  while (!errflag && ((cp = getopt(argc, argv, ARGS_PMBGP)) != -1)) {
    switch (cp) {
    case 'h':
      usage_pmbgp(argv[0]);
      exit(0);
      break;
    case 'V':
      version_pmbgp(argv[0]);
      exit(0);
      break;
    case 'a':
      strlcpy(address_str, optarg, sizeof(address_str));
      break;
    case 'd':
      strlcpy(rd_str, optarg, sizeof(rd_str));
      break;
    case 'r':
      strlcpy(peer_str, optarg, sizeof(peer_str));
      break;
    case 'z':
      strlcpy(zmq_host_str, optarg, sizeof(zmq_host_str));
      break;
    case 'Z':
      zmq_port = atoi(optarg);
      break;
    case 'u':
      strlcpy(zmq_host.zap.username, optarg, sizeof(zmq_host.zap.username));
      break;
    case 'p':
      strlcpy(zmq_host.zap.password, optarg, sizeof(zmq_host.zap.password));
      break;
    default:
      printf("ERROR: parameter %c unknown! \n  Exiting...\n\n", cp);
      usage_pmbgp(argv[0]);
      exit(1);
      break;
    }
  }

  if (!strlen(address_str) || !strlen(peer_str)) {
    printf("ERROR: mandatory options, -a and/or -r, are not specified. Exiting ..\n");
    exit(1);
  }
  
  if (!strlen(zmq_host_str)) zmq_host_str_ptr = default_zmq_host_str; 
  else zmq_host_str_ptr = zmq_host_str; 

  if (!zmq_port) zmq_port = default_zmq_port;

  /* craft query */
  {
    json_t *req_obj = json_object();

    json_object_set_new_nocheck(req_obj, "query_type", json_integer(BGP_LG_QT_IP_LOOKUP));
    req_type_str = json_dumps(req_obj, JSON_PRESERVE_ORDER);
    json_decref(req_obj);
  }

  {
    json_t *req_obj = json_object();

    str_to_addr(peer_str, &address_ha);
    if (address_ha.family) json_object_set_new_nocheck(req_obj, "peer_ip_src", json_string(peer_str));
    else {
      printf("ERROR: invalid -P value. Exiting ..\n");
      exit(1);
    }

    str_to_addr(address_str, &address_ha);
    if (address_ha.family) json_object_set_new_nocheck(req_obj, "ip_address", json_string(address_str));
    else {
      printf("ERROR: invalid -p value. Exiting ..\n");
      exit(1);
    }

    /* no specific validation done for the RD */
    if (strlen(rd_str)) json_object_set_new_nocheck(req_obj, "rd", json_string(rd_str));

    req_str = json_dumps(req_obj, JSON_PRESERVE_ORDER);
    json_decref(req_obj);
  }

  pmbgp_zmq_req_setup(&zmq_host, zmq_host_str_ptr, zmq_port);

  pmbgp_zmq_sendmore_str(&zmq_host.sock, req_type_str);
  pmbgp_zmq_send_str(&zmq_host.sock, req_str);

  /* query type + results */
  rep_str = pmbgp_zmq_recv_str(&zmq_host.sock);
  if (rep_str) {
    json_error_t rep_err;
    json_t *rep_results_obj, *results_json, *query_type_json;

    rep_results_obj = json_loads(rep_str, 0, &rep_err);

    if (rep_results_obj) {
      if (!json_is_object(rep_results_obj)) {
        printf("WARN: json_is_object() failed for results: %s\n", rep_err.text);
	exit(1);
      }
      else {
        query_type_json = json_object_get(rep_results_obj, "query_type");
        if (query_type_json == NULL) {
          printf("WARN: no 'query_type' element.\n");
          exit(1);
        }
        else query_type = json_integer_value(query_type_json);

        results_json = json_object_get(rep_results_obj, "results");
        if (results_json == NULL) {
          printf("WARN: no 'results' element.\n");
	  exit(1);
	}
        else results = json_integer_value(results_json);
      }

      json_decref(query_type_json);
      json_decref(results_json);
      json_decref(rep_results_obj);
    }

    printf("%s\n", rep_str);
    free(rep_str);
  }
  
  /* data */
  for (idx = 0; idx < results; idx++) {
    rep_str = pmbgp_zmq_recv_str(&zmq_host.sock);
    if (rep_str) {
      if (query_type == BGP_LG_QT_IP_LOOKUP) printf("%s\n", rep_str);
      free(rep_str);
    }
  }
}

void pmbgp_zmq_req_setup(struct p_zmq_host *zmq_host, char *host, int port)
{
  int ret;

  if (!zmq_host->ctx) zmq_host->ctx = zmq_ctx_new();

  zmq_host->sock.obj = zmq_socket(zmq_host->ctx, ZMQ_REQ);
  if (!zmq_host->sock.obj) {
    printf("ERROR: zmq_socket() failed for ZMQ_REQ: %s. Exiting.\n", zmq_strerror(errno));
    exit(1);
  }

  snprintf(zmq_host->sock.str, sizeof(zmq_host->sock.str), "tcp://%s:%u", host, port);

  if (strlen(zmq_host->zap.username) && strlen(zmq_host->zap.password)) {
    ret = zmq_setsockopt(zmq_host->sock.obj, ZMQ_PLAIN_USERNAME, zmq_host->zap.username, strlen(zmq_host->zap.username));
    if (ret == ERR) {
      printf("ERROR: zmq_setsockopt() ZMQ_PLAIN_USERNAME failed: %s\nExiting.\n", zmq_strerror(errno));
      exit(1);
    }

    ret = zmq_setsockopt(zmq_host->sock.obj, ZMQ_PLAIN_PASSWORD, zmq_host->zap.password, strlen(zmq_host->zap.password));
    if (ret == ERR) {
      printf("ERROR: zmq_setsockopt() ZMQ_PLAIN_PASSWORD failed: %s\nExiting.\n", zmq_strerror(errno));
      exit(1);
    }
  }

  ret = zmq_connect(zmq_host->sock.obj, zmq_host->sock.str);
  if (ret == ERR) {
    printf("ERROR: zmq_connect() failed for ZMQ_REQ: %s. Exiting.\n", zmq_strerror(errno));
    exit(1);
  }
}

char *pmbgp_zmq_recv_str(struct p_zmq_sock *sock)
{
  char buf[LARGEBUFLEN];
  int len;

  memset(buf, 0, sizeof(buf));
  len = zmq_recv(sock->obj, buf, (sizeof(buf) - 1), 0);
  if (len == ERR) return NULL;
  else return strndup(buf, sizeof(buf));
}

int pmbgp_zmq_send_str(struct p_zmq_sock *sock, char *buf)
{
  int len;

  len = zmq_send(sock->obj, buf, strlen(buf), 0);

  return len;
}

int pmbgp_zmq_sendmore_str(struct p_zmq_sock *sock, char *buf)
{
  int len;

  len = zmq_send(sock->obj, buf, strlen(buf), ZMQ_SNDMORE);

  return len;
}
#else
int main(int argc,char **argv)
{
  printf("WARN: pmbgp: tool depends on missing --enable-zmq. Exiting.\n");
}
#endif
