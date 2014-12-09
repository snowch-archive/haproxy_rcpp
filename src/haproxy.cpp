#include <Rcpp.h>
#include <iostream>
#include <fstream>
#include <string>
#include <regex>

using namespace Rcpp;
using namespace std;

// TODO move this to it's own file

class ListBuilder {

public:

   ListBuilder() {};
   ~ListBuilder() {};

   inline ListBuilder& add(std::string const& name, SEXP x) {
      names.push_back(name);

      // NOTE: we need to protect the SEXPs we pass in; there is
      // probably a nicer way to handle this but ...
      elements.push_back(PROTECT(x));

      return *this;
   }

   inline operator List() const {
      List result(elements.size());
      for (size_t i = 0; i < elements.size(); ++i) {
         result[i] = elements[i];
      }
      result.attr("names") = wrap(names);
      UNPROTECT(elements.size());
      return result;
   }

   inline operator DataFrame() const {
      List result = static_cast<List>(*this);
      result.attr("class") = "data.frame";
      result.attr("row.names") = IntegerVector::create(NA_INTEGER, XLENGTH(elements[0]));
      return result;
   }

private:

   std::vector<std::string> names;
   std::vector<SEXP> elements;

   ListBuilder(ListBuilder const&) {}; // not safe to copy

};

// the R function code starts here

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
static IntegerVector   statusCode;
static IntegerVector   bytesRead;
static CharacterVector capturedRequestCookie;
static CharacterVector capturedResponseCookie;
static CharacterVector terminationState;
static IntegerVector   actconn;
static IntegerVector   feconn;
static IntegerVector   beconn;
static IntegerVector   srvConn;
static IntegerVector   retries;
static IntegerVector   serverQueue;
static IntegerVector   backendQueue;
static CharacterVector capturedRequestHeaders;
static CharacterVector capturedResponseHeaders;
static CharacterVector httpRequest;

template <int RTYPE>
IntegerVector fast_factor_template( const Vector<RTYPE>& x ) {
    Vector<RTYPE> levs = sort_unique(x);
    IntegerVector out = match(x, levs);
    out.attr("levels") = as<CharacterVector>(levs);
    out.attr("class") = "factor";
    return out;
}

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
    
  // for fields definitions, see http://cbonte.github.io/haproxy-dconv/configuration-1.4.html#8.2.3
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
    
    // status_code # E.g. 200
    " (\\d{3})"
    
    //  bytes_read # E.g. 2750
    " (\\d*)"
 
#if CAPTURED_REQUEST_COOKIE_FIELD == 1
    //  captured_request_cookie # E.g. -
    " (\\S*)"
#endif
    
#if CAPTURED_RESPONSE_COOKIE_FIELD == 1
    //  captured_response_cookie # E.g. -
    " (\\S*)"
#endif
    
    //  termination_state # E.g. ----
    "\\s+(\\S*)"    
    
    //  actconn '/' feconn '/' beconn '/' srv_conn '/' retries* # E.g   1/1/1/1/0
    " (\\d*)/(\\d*)/(\\d*)/(\\d*)/(\\d*)"
 
    // srv_queue '/' backend_queue # E.g. 0/0
    " (\\d*)/(\\d*)"
 
    // '{' captured_request_headers* '}' # E.g. {haproxy.1wt.eu}
    " \\{([^}]*)\\}"
    
    // '{' captured_response_headers* '}' # E.g  {}
    " \\{([^}]*)\\}"
    
    // '"' http_request '"' # E.g. "GET /index.html HTTP/1.1" 
    " \"(.*?)\""
    );
    
  std::smatch sm;
  std::regex_match (current_line, sm, re);
  
  int startMatchGroup = 2;
  
  clientIp[i]        = Rcpp::String(sm[startMatchGroup++]);
  clientPort[i]      = Rcpp::String(sm[startMatchGroup++]);
  acceptDate[i]      = Rcpp::String(sm[startMatchGroup++]);
  frontendName[i]    = Rcpp::String(sm[startMatchGroup++]);
  backendName[i]     = Rcpp::String(sm[startMatchGroup++]);
  serverName[i]      = Rcpp::String(sm[startMatchGroup++]);
  tq[i]              = stoi_ignore_err(sm[startMatchGroup++]);
  tw[i]              = stoi_ignore_err(sm[startMatchGroup++]);
  tc[i]              = stoi_ignore_err(sm[startMatchGroup++]);
  tr[i]              = stoi_ignore_err(sm[startMatchGroup++]);
  tt[i]              = stoi_ignore_err(sm[startMatchGroup++]);
  statusCode[i]      = stoi_ignore_err(sm[startMatchGroup++]);
  bytesRead[i]       = stoi_ignore_err(sm[startMatchGroup++]);

#if CAPTURED_REQUEST_COOKIE_FIELD == 1
  capturedRequestCookie[i]  = Rcpp::String(sm[startMatchGroup++]);
#endif

#if CAPTURED_RESPONSE_COOKIE_FIELD == 1
  capturedResponseCookie[i]  = Rcpp::String(sm[startMatchGroup++]);
#endif

  terminationState[i]        = Rcpp::String(sm[startMatchGroup++]);
  actconn[i]                 = stoi_ignore_err(sm[startMatchGroup++]);
  feconn[i]                  = stoi_ignore_err(sm[startMatchGroup++]);
  beconn[i]                  = stoi_ignore_err(sm[startMatchGroup++]);
  srvConn[i]                 = stoi_ignore_err(sm[startMatchGroup++]);
  retries[i]                 = stoi_ignore_err(sm[startMatchGroup++]);
  serverQueue[i]             = stoi_ignore_err(sm[startMatchGroup++]); 
  backendQueue[i]            = stoi_ignore_err(sm[startMatchGroup++]);
  capturedRequestHeaders[i]  = Rcpp::String(sm[startMatchGroup++]);
  capturedResponseHeaders[i] = Rcpp::String(sm[startMatchGroup++]);
  httpRequest[i]             = Rcpp::String(sm[startMatchGroup++]);
}

// [[Rcpp::export]]
DataFrame haproxy_read(String fileName) {
  
    int vsize = get_number_lines(fileName);
    
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
    statusCode   = IntegerVector(vsize);
    bytesRead    = IntegerVector(vsize);

#if CAPTURED_RESPONSE_COOKIE_FIELD == 1
    capturedRequestCookie  = CharacterVector(vsize);
#endif

#if CAPTURED_RESPONSE_COOKIE_FIELD == 1
    capturedResponseCookie  = CharacterVector(vsize);
#endif

    terminationState        = CharacterVector(vsize);
    actconn                 = IntegerVector(vsize);
    feconn                  = IntegerVector(vsize);
    beconn                  = IntegerVector(vsize);
    srvConn                 = IntegerVector(vsize);
    retries                 = IntegerVector(vsize);
    serverQueue             = IntegerVector(vsize);
    backendQueue            = IntegerVector(vsize);
    capturedRequestHeaders  = CharacterVector(vsize);
    capturedResponseHeaders = CharacterVector(vsize);
    httpRequest             = CharacterVector(vsize);
    
    std::ifstream in(fileName);
    
    int i = 0;
    string current_line;    
    while (!in.eof()) {
      getline(in, current_line, '\n');
      if (current_line.length() > 0) {
        parse(current_line, i);
        current_line.clear(); 
      }
      i++;
    }
        
    // Why ListBuilder? See http://stackoverflow.com/q/27371543/1033422
    return ListBuilder()
      .add("clientIp", fast_factor_template<STRSXP>(clientIp))
      .add("clientPort", fast_factor_template<STRSXP>(clientPort))
      .add("acceptDate", acceptDate)
      .add("frontendName", fast_factor_template<STRSXP>(frontendName))
      .add("backendName", fast_factor_template<STRSXP>(backendName))
      .add("serverName", fast_factor_template<STRSXP>(serverName))
      .add("tq", tq)
      .add("tw", tw)
      .add("tc", tc)
      .add("tr", tr)
      .add("tt", tt)
      .add("status_code", fast_factor_template<INTSXP>(statusCode))
      .add("bytes_read", bytesRead)
      
#if CAPTURED_REQUEST_COOKIE_FIELD == 1
      .add("capturedRequestCookie", capturedRequestCookie)
#endif     

#if CAPTURED_REQUEST_COOKIE_FIELD == 1
      .add("capturedResponseCookie", capturedResponseCookie)
#endif    

      .add("terminationState", terminationState)
      .add("actconn", actconn)
      .add("feconn", feconn)
      .add("beconn", beconn)
      .add("srv_conn", srvConn)
      .add("retries", retries)
      .add("serverQueue", serverQueue)
      .add("backendQueue", backendQueue)
      .add("capturedRequestHeaders", capturedRequestHeaders)
      .add("capturedResponseHeaders", capturedResponseHeaders)
      .add("httpRequest", httpRequest);
};
