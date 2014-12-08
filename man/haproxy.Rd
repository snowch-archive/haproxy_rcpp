\name{haproxy}
\alias{haproxy}
\docType{package}
\title{
Haproxy raw log file import utility
}
\description{
This version has hardcoded fields.  Your haproxy log file will need to have the same 
number of fields that this package is expecting.
}
\usage{
df <- haproxy_read( "/path/to/haproxy_raw_file")	
}
\examples{
\dontrun{
df <- haproxy_read( "/home/snowch/haproxy-logs/haproxy.log")
}
}
