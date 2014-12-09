
#include <Rcpp.h>
#include <iostream>
#include <fstream>
#include <string>
#include <regex>

using namespace Rcpp;
using namespace std;

static CharacterVector clientIp;
static CharacterVector clientPort;
static CharacterVector acceptDate;
static CharacterVector frontendName;
static CharacterVector backendName;
static CharacterVector serverName;
static IntegerVector   tq;
static IntegerVector   tw;
static IntegerVector   tc;
static IntegerVector   tr;
static IntegerVector   tt;

int get_number_lines(String fileName) {
  std::ifstream inFile(fileName); 
  return std::count(std::istreambuf_iterator<char>(inFile), 
             std::istreambuf_iterator<char>(), '\n');
}

int stoi_ignore_err(string s) {
  int i = 0;
  try {
    i = stoi(s);
  }
  catch(...) {
    // ignore
  }
  return i;
}

void parse(string current_line, int i) {
  
  const std::regex re (
    // process_name '[' pid ']:' # E.g. haproxy[14389]:
    "^(.*?):" 
    
    // client_ip ':' client_port # E.g. 10.0.1.2:33317
    " (\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b):(\\d*)"
    
    // '[' accept_date ']' # E.g. [06/Feb/2009:12:14:14.655]
    " \\[(\\d{2}/[a-zA-Z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}.\\d{3})\\]"
    
    // frontend_name # E.g. http-in
    " (\\S*)"
    
    // backend_name # E.g. backend_name '/' server_name  
    " (.*?)/(.*?)"
    
    // Tq '/' Tw '/' Tc '/' Tr '/' Tt* # E.g. 10/0/30/69/109
    " (\\d*)/(\\d*)/(\\d*)/(\\d*)/(\\d*)"
    
    // discard everything else
    ".*$"
    );
    
  std::smatch sm;
  std::regex_match (current_line, sm, re);
  
  clientIp[i]        = Rcpp::String(sm[2]);
  clientPort[i]      = Rcpp::String(sm[3]);
  acceptDate[i]      = Rcpp::String(sm[4]);
  frontendName[i]    = Rcpp::String(sm[5]);
  backendName[i]     = Rcpp::String(sm[6]);
  serverName[i]      = Rcpp::String(sm[7]);
  tq[i]              = stoi_ignore_err(sm[8]);
  tw[i]              = stoi_ignore_err(sm[9]);
  tc[i]              = stoi_ignore_err(sm[10]);
  tr[i]              = stoi_ignore_err(sm[11]);
  tt[i]              = stoi_ignore_err(sm[12]);
}

// [[Rcpp::export]]
DataFrame haproxy_read(String fileName) {
  
    int vsize = get_number_lines(fileName) + 1;
    
    clientIp     = CharacterVector(vsize);
    clientPort   = CharacterVector(vsize);
    acceptDate   = CharacterVector(vsize);
    frontendName = CharacterVector(vsize);
    backendName  = CharacterVector(vsize);
    serverName   = CharacterVector(vsize);
    tq           = IntegerVector(vsize);
    tw           = IntegerVector(vsize);
    tc           = IntegerVector(vsize);
    tr           = IntegerVector(vsize);
    tt           = IntegerVector(vsize);
      
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
      _["clientIp"]     = clientIp,
      _["clientPort"]   = clientPort,
      _["acceptDate"]   = acceptDate,
      _["frontendName"] = frontendName,
      _["backendName"]  = backendName,
      _["serverName"]   = serverName,
      _["tq"]           = tq,
      _["tw"]           = tw,
      _["tc"]           = tc,
      _["tr"]           = tr,
      _["tt"]           = tt
      );
}
