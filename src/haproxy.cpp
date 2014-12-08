
#include <Rcpp.h>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>

using namespace Rcpp;
using namespace std;

int get_number_records(String fileName) {
  std::ifstream inFile(fileName); 
  return std::count(std::istreambuf_iterator<char>(inFile), 
             std::istreambuf_iterator<char>(), '\n');
}

// [[Rcpp::export]]
DataFrame haproxy_read(String fileName) {
  
    int vsize = get_number_records(fileName);
    CharacterVector z = CharacterVector(vsize+1);
      
    std::ifstream in(fileName);
    
    int i = 0;
    string tmp;
    while (!in.eof()) {
      getline(in, tmp, '\n');
      z[i] = tmp;
      tmp.clear( ); 
      i++;
    }
    return DataFrame::create(_["z"]= z);
}
