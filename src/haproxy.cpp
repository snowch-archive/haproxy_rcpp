
#include <Rcpp.h>
#include <iostream>
#include <fstream>
#include <string>
#include <regex>

using namespace Rcpp;
using namespace std;

static CharacterVector clientIp;
static CharacterVector clientPort;

int get_number_lines(String fileName) {
  std::ifstream inFile(fileName); 
  return std::count(std::istreambuf_iterator<char>(inFile), 
             std::istreambuf_iterator<char>(), '\n');
}

void parse(string current_line, int i) {
  
  const std::regex re (
    // process_name '[' pid ']:' # E.g. haproxy[14389]:
    "^(.*?):" 
    
    // client_ip ':' client_port # E.g. 10.0.1.2:33317
    " (\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b):(\\d*) .*$"
    );
    
  std::smatch sm;
  std::regex_match (current_line, sm, re);
  
  clientIp[i] = Rcpp::String(sm[2]);
  clientPort[i] = Rcpp::String(sm[3]);
}

// [[Rcpp::export]]
DataFrame haproxy_read(String fileName) {
  
    int vsize = get_number_lines(fileName) + 1;
    
    clientIp = CharacterVector(vsize);
    clientPort = CharacterVector(vsize);
      
    std::ifstream in(fileName);
    
    int i = 0;
    string current_line;    
    while (!in.eof()) {
      getline(in, current_line, '\n');
      parse(current_line, i);
      current_line.clear(); 
      i++;
    }
    return DataFrame::create(
      _["clientIp"]=clientIp,
      _["clientPort"]=clientPort
      );
}
