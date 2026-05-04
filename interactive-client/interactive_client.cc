// Interactive Bench 客户端
// =====================================================================
//   连接计算节点交互模式监听端口 (默认 9115 + node_id)。
//
//   每一行 = 一个完整事务 (类似存储过程)，里面可以串多个 op:
//
//     <op1> ; <op2> ; ... ; <opN>
//
//   每个 op:
//     <table_id>,<key>,<is_write>[,<write_value>]
//
//     - is_write = 0  读操作 (不带 write_value)
//     - is_write = 1  写操作 (必须带 write_value)
//         ycsb     -> write_value 是字符串 (服务端会填充/截断到 100B)
//         smallbank-> write_value 是浮点数 (写 bal)
//
//   示例:
//     0,100,0                              # 读 table=0, key=100
//     0,100,1,helloworld                   # 写 table=0, key=100, val="helloworld"
//     0,100,1,foo;0,200,0;0,300,1,bar      # 一笔事务 含 3 个 op
//
//   控制命令: quit / exit
//
//   构建后用法:
//     ./InteractiveBench_client -h 127.0.0.1 -p 9115
// =====================================================================

#include <netdb.h>
#include <netinet/in.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <string>

#define MAX_RECV_BUF 65536

static bool is_exit_command(const std::string& cmd) {
  return cmd == "exit" || cmd == "exit;" || cmd == "bye" ||
         cmd == "bye;" || cmd == "\\q";
  // 注意：服务端也会响应 quit，所以用户输入 quit 时我们直接发给服务器，
  // 由服务器发回 "OK: bye" 后我们再 break。
}

static int init_tcp_sock(const char* host, int port) {
  struct hostent* h = gethostbyname(host);
  if (h == nullptr) {
    fprintf(stderr, "gethostbyname failed: %s\n", strerror(errno));
    return -1;
  }
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    fprintf(stderr, "socket: %s\n", strerror(errno));
    return -1;
  }
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr = *((struct in_addr*)h->h_addr);
  if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
    fprintf(stderr, "connect %s:%d failed: %s\n", host, port, strerror(errno));
    close(fd);
    return -1;
  }
  return fd;
}

static void usage(const char* prog) {
  std::cout << "Usage: " << prog << " -h <host> -p <port>\n"
            << "       (default: -h 127.0.0.1 -p 9115)\n"
            << "\n"
            << "Each input line = ONE transaction (stored procedure).\n"
            << "Operations within a transaction are joined by ';'.\n"
            << "\n"
            << "  op : <table_id>,<key>,<is_write>[,<write_value>]\n"
            << "        is_write=0 -> read  (no write_value)\n"
            << "        is_write=1 -> write (write_value required)\n"
            << "          ycsb     : value is a string written to file_0 (100B)\n"
            << "          smallbank: value is a float written to bal\n"
            << "\n"
            << "Examples:\n"
            << "  0,100,0\n"
            << "  0,100,1,helloworld\n"
            << "  0,100,1,foo;0,200,0;0,300,1,bar\n"
            << "\n"
            << "Control: quit | exit\n";
}

// 接收一行响应(以 \n 结束) 或被服务器关闭。
// 返回 true 表示成功收到一行；false 表示连接断开。
static bool recv_line(int fd, std::string& out) {
  out.clear();
  char buf[MAX_RECV_BUF];
  while (true) {
    ssize_t n = recv(fd, buf, sizeof(buf) - 1, 0);
    if (n < 0) {
      if (errno == EINTR) continue;
      fprintf(stderr, "recv error: %s\n", strerror(errno));
      return false;
    }
    if (n == 0) {
      return !out.empty();  // 还有半截就先吐出来
    }
    buf[n] = '\0';
    out.append(buf, n);
    if (!out.empty() && out.back() == '\n') {
      while (!out.empty() && (out.back() == '\n' || out.back() == '\r')) out.pop_back();
      return true;
    }
  }
}

int main(int argc, char* argv[]) {
  const char* host = "127.0.0.1";
  int port = 9115;
  int opt;
  while ((opt = getopt(argc, argv, "h:p:H")) > 0) {
    switch (opt) {
      case 'h':
        host = optarg;
        break;
      case 'p': {
        char* end = nullptr;
        port = (int)strtol(optarg, &end, 10);
        break;
      }
      case 'H':
      default:
        usage(argv[0]);
        return opt == 'H' ? 0 : 1;
    }
  }

  int sockfd = init_tcp_sock(host, port);
  if (sockfd < 0) return 1;

  std::cout << "Connected to " << host << ":" << port
            << ". Type 'quit' to exit. (-H for help)\n";

  bool is_interactive = isatty(STDIN_FILENO);
  std::string prompt = std::string("Bench@") + host + ":" + std::to_string(port) + "> ";

  while (true) {
    std::string command;
    if (is_interactive) {
      char* line = readline(prompt.c_str());
      if (line == nullptr) break;       // EOF (Ctrl-D)
      command = line;
      free(line);
    } else {
      if (!std::getline(std::cin, command)) break;
      std::cout << prompt << command << "\n";
    }

    // 去尾空白
    while (!command.empty() && (command.back() == ' ' || command.back() == '\t' ||
                                command.back() == '\r' || command.back() == '\n')) {
      command.pop_back();
    }
    if (command.empty()) continue;

    if (is_interactive) add_history(command.c_str());

    // 客户端层面的退出命令(不发给服务器；与服务器的 quit 区分)
    if (is_exit_command(command)) {
      std::cout << "Bye.\n";
      break;
    }

    // 协议是按行的，确保以 \n 结尾
    std::string send_buf = command + "\n";
    ssize_t total = 0;
    while (total < (ssize_t)send_buf.size()) {
      ssize_t n = send(sockfd, send_buf.data() + total, send_buf.size() - total, 0);
      if (n <= 0) {
        if (n < 0 && errno == EINTR) continue;
        fprintf(stderr, "send error: %s\n", strerror(errno));
        close(sockfd);
        return 1;
      }
      total += n;
    }

    std::string resp;
    if (!recv_line(sockfd, resp)) {
      std::cerr << "Connection closed by server.\n";
      break;
    }
    std::cout << resp << std::endl;

    // 服务器端响应 "OK: bye" 表示已主动关闭
    if (resp.find("OK: bye") != std::string::npos) {
      break;
    }
  }

  close(sockfd);
  return 0;
}
