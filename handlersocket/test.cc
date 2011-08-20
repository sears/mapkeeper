#include "hstcpcli.hpp"
#include "string_util.hpp"
using namespace dena;

int
hstcpcli_main(int argc, char **argv)
{
  config conf;
  conf["host"] = "localhost";
  conf["port"] = "9999";
  socket_args sockargs;
  sockargs.set(conf);
  hstcpcli_ptr cli = hstcpcli_i::create(sockargs);
  const std::string dbname = "mapkeeper";
  const std::string table = "usertable";
  const std::string index = "PRIMARY";
  const std::string fields = "record_key";
  //const std::string fields = "record_key,record_value";
  const std::string op = "=";
  const std::string modop = "U";
  const int limit = 1;
  const int skip = 0;
  std::vector<string_ref> keyrefs;
  std::string key = "user102642test";
  std::string value = "more struggle";
  const string_ref ref(key.data(), key.size());
  keyrefs.push_back(ref);
  const string_ref ref2(value.data(), value.size());
  keyrefs.push_back(ref2);
  size_t num_keys = keyrefs.size();
  const string_ref op_ref(op.data(), op.size());
  const string_ref modop_ref(modop.data(), modop.size());
  cli->request_buf_open_index(0, dbname.c_str(), table.c_str(), index.c_str(), fields.c_str());
  //cli->request_buf_exec_generic(0, op_ref, &keyrefs[0], num_keys, limit, skip, string_ref(), 0, 0);
  cli->request_buf_exec_generic(0, op_ref, &keyrefs[0], num_keys, 1, 0, modop_ref, 0, 0);

  int code = 0;
  size_t numflds = 0;
  if (cli->request_send() != 0) {
    fprintf(stderr, "request_send: %s\n", cli->get_error().c_str());
    exit(1);   
  }
  if ((code = cli->response_recv(numflds)) != 0) {
    fprintf(stderr, "response_recv: %s\n", cli->get_error().c_str());
    exit(1);   
  }
  cli->response_buf_remove();

  if ((code = cli->response_recv(numflds)) != 0) {
      fprintf(stderr, "response_recv: %s\n", cli->get_error().c_str());
      exit(1);   
  }
  const string_ref *const row = cli->get_next_row();
  if (row == 0) {
      fprintf(stderr, "record '%s' not found", key.c_str());
      exit(1);   
  }
  printf("REC:");
  for (size_t i = 0; i < numflds; ++i) {
      const std::string val(row[i].begin(), row[i].size());
      printf(" %s", val.c_str());
  }
  printf("\n");
  cli->response_buf_remove();
  return 0;
}

int
main(int argc, char **argv)
{
  return hstcpcli_main(argc, argv);
}

