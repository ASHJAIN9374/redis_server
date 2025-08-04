#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <map>
#include <chrono>
#include <mutex>
#include <bits/stdc++.h>
using namespace std;
using namespace std::chrono;


struct ThreadArgs {
    int client_fd;
    map<string, string>* cache;
    map<string, system_clock::time_point>* expiration_times;
    map<string, vector<string>>* lists;
    map<string,map<string,map<string,string>>>* stream; 
};

map<string, mutex> cache_mutexes;
map<string, mutex> list_mutexes;
map<string, mutex> queue_mutexes; 
map<string, mutex> stream_mutexes;
map<string,queue<int>> que;
mutex key_mutex_map_lock;
condition_variable cv[100000];
map<std::thread::id,int> thread_map;
map<string, string> stream_id_last;

void create_mutex_cache(const string& key) {
    lock_guard<mutex> lock(key_mutex_map_lock);
    if (cache_mutexes.find(key) == cache_mutexes.end()) {
        cache_mutexes.try_emplace(key);
    }
}


void create_mutex_list(const string& key) {
    lock_guard<mutex> lock(key_mutex_map_lock);
    if (list_mutexes.find(key) == list_mutexes.end()) {
        list_mutexes.try_emplace(key);
    }
}

void create_mutex_queue(const string& key){
    lock_guard<mutex> lock(key_mutex_map_lock);
    if(queue_mutexes.find(key)==queue_mutexes.end()){
      queue_mutexes.try_emplace(key);
    }
}

void create_mutex_stream(const string& key) {
    lock_guard<mutex> lock(key_mutex_map_lock);
    if (stream_mutexes.find(key) == stream_mutexes.end()) {
        stream_mutexes.try_emplace(key);
    }
}

string to_redis_bulk_string(const std::string& data) {
    return "$" + std::to_string(data.size()) + "\r\n" + data + "\r\n";
}

string to_redis_number(int num) {
    return ":" + to_string(num) + "\r\n";
}

string to_redis_array(vector<string> &data) {
    string result = "*" + to_string(data.size()) + "\r\n";
    for (const auto& item : data) {
        result += to_redis_bulk_string(item);
    }
    return result;
}

string to_redis_array_map(string key,map<string,string> &data) {
    string result = "*" + to_string(2) + "\r\n";
    result += to_redis_bulk_string(key);
    //result += to_redis_bulk_string(to_string(2*data.size()));
    vector<string> item_data;
    for (const auto& item : data) {     
      item_data.push_back(item.first);
      item_data.push_back(item.second);
    }
    result += to_redis_array(item_data);
    return result;
}
string to_redis_array_3d(vector<pair<string,map<string,string>>> &data) {
    string result = "*" + to_string(data.size()) + "\r\n";
    for (auto item : data) {
        result += (to_redis_array_map(item.first,item.second));
    }
    return result;
}
string handle_parsing(char *buffer,int *ptr, ssize_t buffer_size) {
    if(*ptr>buffer_size) return "";
    
    if (buffer[*ptr] == '$'|| buffer[*ptr]=='+'||buffer[*ptr]=='-') {
        (*ptr)++;
    }
    if(buffer[*ptr]==':'){
      (*ptr)++;
      string result;
      while (buffer[*ptr] >= '0' && buffer[*ptr] <= '9' ){
          result += buffer[*ptr];
          (*ptr)++;
      }
      (*ptr) += 2; 
      return result;

    }

    int string_size = 0;
        while (buffer[*ptr] >= '0' && buffer[*ptr] <= '9') {
            string_size = string_size * 10 + (buffer[*ptr] - '0');
            (*ptr)++;
        }
        (*ptr) += 2; // Skip the size and CRLF
        std::string result(buffer + *ptr, string_size);
        (*ptr) += string_size;
        result[string_size] = '\0'; // Null-terminate the string
        (*ptr) += 3; // Skip the trailing CRLF
        return result;
}

vector<string> subrange(vector<string> &list, int start, int end) {
    vector<string> result;
    if(start < 0)start+=list.size();
    if(end < 0)end+=list.size();
    if (start < 0) start = 0;
    if (end >= list.size()) end = list.size() - 1;
    for (int i = start; i <= end && i < list.size(); ++i) {
        result.push_back(list[i]);
    }
    return result;
}
 void wait_function(int num, string cmd, double timeout,map<string,vector<string>> &lists) {
  mutex mtx;
          std::unique_lock<std::mutex> lock(mtx);
          std::chrono::duration<double> duration(timeout);
          if(abs(timeout-0)<=1e-9)
          {
            cv[num].wait(lock, [&] {
              return lists[cmd].size()>0; // Wait until the queue is not empty
            });
          }
          else{
            cv[num].wait_for(lock,duration, [&] {
              return lists[cmd].size()>0; // Wait until the queue is not empty
             });
            }
 }
void parse(char *buffer,ssize_t buffer_size, int client_fd, map<string,string> &cache , map<string, system_clock::time_point> &expiration_times, map<string, vector<string>> &lists, map<string,map<string,map<string,string>>> &stream) {
    int ptr = 4;
    while(ptr<buffer_size){
      string str = handle_parsing(buffer, &ptr, buffer_size);
      int string_size = strlen(str.c_str());
      for(int i=0;i<string_size;i++){
        str[i] = tolower(str[i]); // Convert to lowercase
      }
      if(str == "ping") {
          write(client_fd, "+PONG\r\n", 7);
      } 
      else if(str == "echo"){
        std::string echo_str = handle_parsing(buffer, &ptr, buffer_size);
        std::string response = to_redis_bulk_string(echo_str);
        //cout<<response<<endl;
        write(client_fd, response.c_str(), response.size());
      } 
      else if(str == "set"){
        string key = handle_parsing(buffer, &ptr, buffer_size);
        string value = handle_parsing(buffer, &ptr, buffer_size);
        create_mutex_cache(key);
        lock_guard<mutex> lock(cache_mutexes[key]);
        cache[key] = value;
        string option = handle_parsing(buffer, &ptr, buffer_size);
        if(option == "px"){
          string num = handle_parsing(buffer, &ptr, buffer_size);
          int milliseconds = stoi(num);
          auto expiration_time = system_clock::now() + milliseconds * 1ms;
          expiration_times[key] = expiration_time;
        }                                                                                                                               
        write(client_fd, "+OK\r\n", 5);
      }
      else if(str == "get"){
        string key = handle_parsing(buffer, &ptr, buffer_size);
        create_mutex_cache(key);
        lock_guard<mutex> lock(cache_mutexes[key]);
        if(cache.find(key) != cache.end()&& ((expiration_times.find(key) != expiration_times.end() && expiration_times[key] >= system_clock::now()) || expiration_times.find(key) == expiration_times.end())) {
            string value = cache[key];
            string response = to_redis_bulk_string(value);
            write(client_fd, response.c_str(), response.size());
        } else {
            write(client_fd, "$-1\r\n", 5); // Key not found
        }
      } 
      else if(str == "xadd"){
        string key = handle_parsing(buffer, &ptr, buffer_size);
        string id = handle_parsing(buffer, &ptr, buffer_size);
        string sequence;
        string millisecondTime;
        bool flag = false;
        for(auto i:id)
        {
          if(flag)sequence.push_back(i);
          if(i=='-')flag=true;
          if(!flag)millisecondTime.push_back(i);
        }
        create_mutex_stream(key);
        lock_guard<mutex> lock(stream_mutexes[key]);
         if(stream_id_last.find(key) == stream_id_last.end()) 
                {
                  stream_id_last[key] = "0-0";
                }
        if(flag){
                if(sequence == "*")
                {
                  string last_id = stream_id_last[key];
                  string millisecondTime_last_id = last_id.substr(0, last_id.find("-"));
                  if(millisecondTime_last_id != millisecondTime)
                  {
                    sequence = "0";
                  }
                  else{
                    sequence = to_string(stoi(stream_id_last[key].substr(stream_id_last[key].find("-") + 1)) + 1);
                  }
                  
                }
            }
          else{
            millisecondTime = to_string(duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count());
            string last_id = stream_id_last[key];
            string millisecondTime_last_id = last_id.substr(0, last_id.find("-"));
            if(millisecondTime_last_id != millisecondTime)
            {
              sequence = "0";
            }
            else{
              sequence = to_string(stoi(stream_id_last[key].substr(stream_id_last[key].find("-") + 1)) + 1);
            }
          }
        id = millisecondTime + "-" + sequence; 
        if(stream_id_last[key] <id)
        {
          while(ptr < buffer_size) {
          string field = handle_parsing(buffer, &ptr, buffer_size);
          string value = handle_parsing(buffer, &ptr, buffer_size);
          stream[key][id][field] = value; // Add to the stream
        }
        stream_id_last[key] = id; // Update the last ID for the stream
        string response = to_redis_bulk_string(id);
        write(client_fd, response.c_str(), response.size());   
        }
        else{
          if(id == "0-0"){
            write(client_fd, "-ERR The ID specified in XADD must be greater than 0-0\r\n", 56);
          }
          else{
            write(client_fd, "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n", 83);
            
          }
          return;
        }
              
      } 
      else if(str == "xrange"){
        string key = handle_parsing(buffer, &ptr, buffer_size);
        string start = handle_parsing(buffer, &ptr, buffer_size);
        string end = handle_parsing(buffer, &ptr, buffer_size);
        create_mutex_stream(key);
        lock_guard<mutex> lock(stream_mutexes[key]);
        if(start == "-"){
          start = "0-0";
        }
        if(end == "+"){
          if(stream_id_last.find(key) == stream_id_last.end())
            stream_id_last[key] = "0-0"; // Initialize if not found
          end = stream_id_last[key];
        }
        if(start.find('-') == string::npos) {
            start += "-0"; // Append "-0" if no sequence part is present
        }
        if(end.find('-') == string::npos) {
            end += "-18446744073709551615";
        }
        if(stream.find(key) != stream.end()) {
            vector<pair<string,map<string,string>>> resp;
            for (const auto& item : stream[key]) {
                if(item.first < start || item.first > end) {
                    continue; // Skip items outside the range
                }
                resp.push_back({item.first, item.second});
            }
            string response = to_redis_array_3d(resp);
            write(client_fd, response.c_str(), response.size());
        } else {
            write(client_fd, "-ERR Stream not found\r\n", 24);
        }
      }
      else if(str == "xread"){
        string streams = handle_parsing(buffer, &ptr, buffer_size);
        string key = handle_parsing(buffer, &ptr, buffer_size);
        string id = handle_parsing(buffer, &ptr, buffer_size);
        create_mutex_stream(key); 
        lock_guard<mutex> lock(stream_mutexes[key]);
        if(stream.find(key)!=stream.end()){
          vector<pair<string,map<string,string>>> resp;
          for(auto item:stream[key]){
            if(item.first > id){
              resp.push_back({item.first, item.second});
            }
          }
          string response;
          string part1 = "*1\r\n";
          string part2 = "*2\r\n";
          response = part1 + part2 + to_redis_bulk_string(key) + to_redis_array_3d(resp);
          write(client_fd, response.c_str(), response.size());
        }
      }
      else if(str == "type"){
        string key = handle_parsing(buffer, &ptr, buffer_size);
        create_mutex_cache(key);
        lock_guard<mutex> lock(cache_mutexes[key]);
        if(cache.find(key) != cache.end()) {
            write(client_fd, "+string\r\n", 9); // Type is string
        }
        else if(stream.find(key)!=stream.end()){
            write(client_fd, "+stream\r\n", 9);
        }
        else {
            write(client_fd, "+none\r\n", 7); // Key not found or unknown type
        }
      }
      else if(str == "rpush")
      {
        string cmd = handle_parsing(buffer, &ptr, buffer_size);
        create_mutex_list(cmd);
        lock_guard<mutex> lock(list_mutexes[cmd]);
          while(ptr < buffer_size) {
           string value = handle_parsing(buffer, &ptr, buffer_size);
           lists[cmd].push_back(value);
          }
          string response = to_redis_number(lists[cmd].size());
          create_mutex_queue(cmd);
          lock_guard<mutex> queue_lock(queue_mutexes[cmd]);
          if(!que[cmd].empty()){
           cv[que[cmd].front()].notify_all();
            que[cmd].pop();
          }
          write(client_fd, response.c_str(), response.size());
      }
      else if(str == "lpush")
      {
        string cmd = handle_parsing(buffer, &ptr, buffer_size);
        create_mutex_list(cmd);
        lock_guard<mutex> lock(list_mutexes[cmd]);
          while(ptr < buffer_size) {
           string value = handle_parsing(buffer, &ptr, buffer_size);
           lists[cmd].insert(lists[cmd].begin(), value);
          }
          string response = to_redis_number(lists[cmd].size());
          write(client_fd, response.c_str(), response.size());
      }
      else if(str == "lrange"){
        string cmd = handle_parsing(buffer, &ptr, buffer_size);
        int start = stoi(handle_parsing(buffer, &ptr, buffer_size));
        int end = stoi(handle_parsing(buffer, &ptr, buffer_size));
        create_mutex_list(cmd);
        lock_guard<mutex> lock(list_mutexes[cmd]);
        vector<string> resp = subrange(lists[cmd], start, end);
        string response = to_redis_array(resp);
        write(client_fd, response.c_str(), response.size());

      }
      else if(str == "llen")
      {
          string cmd = handle_parsing(buffer, &ptr, buffer_size);
          create_mutex_list(cmd);
          lock_guard<mutex> lock(list_mutexes[cmd]);
          if(lists.find(cmd) != lists.end()) {
              string response = to_redis_number(lists[cmd].size());
              write(client_fd, response.c_str(), response.size());
          } else {
              write(client_fd, ":0\r\n", 4); // List not found
          }
      }
      else if(str == "lpop")
      {
          string cmd = handle_parsing(buffer, &ptr, buffer_size);
          string num = handle_parsing(buffer, &ptr, buffer_size);
          create_mutex_cache(cmd);
          lock_guard<mutex> lock(cache_mutexes[cmd]);
          if(num.empty()) num = "1"; // Default to 1 if no number is provided
          int count = stoi(num);
          vector<string> rem;
          while(count-- >0 && lists.find(cmd) != lists.end() && !lists[cmd].empty()) {
              string value = lists[cmd][0];
              lists[cmd].erase(lists[cmd].begin());
              rem.push_back(value);
          } 
          if(rem.empty()) {
              write(client_fd, "$-1\r\n", 5); // List not found or empty
          }
          else if(rem.size()==1)
          {
            string response = to_redis_bulk_string(rem[0]);
            write(client_fd, response.c_str(), response.size());
          }
          else{
            string response = to_redis_array(rem);
            write(client_fd, response.c_str(), response.size());
          }
      }
      else if(str == "blpop")
      {
        string cmd = handle_parsing(buffer, &ptr, buffer_size);
        string num = handle_parsing(buffer, &ptr, buffer_size);
        if(num.empty()) num = "0"; // Default to 0 if no number is
        double timeout = stod(num);
        create_mutex_list(cmd);
        vector<string> rem;
        bool list_empty = false;
        {
        lock_guard<mutex> lock(list_mutexes[cmd]);
        rem.push_back(cmd);
        if(!lists[cmd].empty()){
          string value = lists[cmd][0];
          lists[cmd].erase(lists[cmd].begin());
          rem.push_back(value);
          string response = to_redis_array(rem);
          write(client_fd, response.c_str(), response.size());
        } 
        else {
         list_empty = true;
        }
        }
        if(list_empty) {
          int to_push = thread_map[std::this_thread::get_id()];
          {
            create_mutex_queue(cmd);
            lock_guard<mutex> lock(queue_mutexes[cmd]);
            que[cmd].push(to_push);
          }
            wait_function(to_push, cmd, timeout, lists);
            create_mutex_list(cmd);
             lock_guard<mutex> lock(list_mutexes[cmd]);
             if(lists[cmd].empty()) {
              write(client_fd, "$-1\r\n", 5); // List not found or empty
              return;
             }
              string value = lists[cmd][0];
              cout<<value<<endl;
              rem.push_back(value);
              lists[cmd].erase(lists[cmd].begin());
              string response = to_redis_array(rem);
              write(client_fd, response.c_str(), response.size());
        }
      }
      else {
          write(client_fd, "-ERR unknown command\r\n", 23);
      }
    }
}
int cnt=0;
void* threadfunction(void* arg) {
    ThreadArgs* args = static_cast<ThreadArgs*>(arg);
    thread_map[std::this_thread::get_id()] = ++cnt; // Store thread ID and count
    int client_fd = args->client_fd;
    map<string, string>& cache = *(args->cache);
    map<string, system_clock::time_point>& expiration_times = *(args->expiration_times);
    map<string, vector<string>>& lists = *(args->lists);
    map<string,map<string,map<string,string>>>& stream = *(args->stream);
    char buffer[1024];
    memset(buffer, 0, sizeof(buffer));
    int bytes_read;
    while ((bytes_read = read(client_fd, buffer, sizeof(buffer) - 1)) > 0) {
        buffer[bytes_read] = '\0'; // Null-terminate the received string
        parse(buffer, bytes_read, client_fd, cache, expiration_times, lists, stream);
        memset(buffer, 0, sizeof(buffer));
    }

    close(client_fd);
    return nullptr;
}


int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }
  
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }
  
  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  std::cout << "Waiting for a client to connect...\n";

  // You can use print statements as follows for debugging, they'll be visible when running tests.
  std::cout << "Logs from your program will appear here!\n";

    map<string,string> cache;
    map<string, system_clock::time_point> expiration_times;
    map<string, vector<string>> lists;
    map<string,map<string,map<string,string>>> stream;
  // Uncomment this block to pass the first stage
  while(1)
  {
    int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
    std::cout << "Client connected\n";
        pthread_t p_id;
        ThreadArgs* args = new ThreadArgs{client_fd, &cache, &expiration_times, &lists, &stream};
        pthread_create(&p_id, nullptr, threadfunction, (void*)args);
        pthread_detach(p_id);
  }
  

  close(server_fd);

  return 0;
}
