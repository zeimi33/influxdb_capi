///
/// \author Adam Wegrzynek <adam.wegrzynek@cern.ch>
///

#include "HTTP.h"
#include "InfluxDBException.h"
#include <iostream>

namespace influxdb
{
namespace transports
{

HTTP::HTTP(const std::string& url)
{
  initCurl(url);
  initCurlRead(url);
}

void HTTP::initCurl(const std::string& url)
{
    CURLcode globalInitResult = curl_global_init(CURL_GLOBAL_ALL);
    if (globalInitResult != CURLE_OK) {
        throw InfluxDBException("HTTP::initCurl", curl_easy_strerror(globalInitResult));
    }

    initHandle(&writeHandle,"create",url);
    initHandle(&createBucketHandle,"buckets",url);
}

void HTTP::initHandle(CURL** handle,std::string param,const std::string url){
    std::string createBucketUrl = url;
    auto position = createBucketUrl.find("?");
    if (position == std::string::npos) {
        throw InfluxDBException("HTTP::initCurl", "Database not specified");
    }
    if (createBucketUrl.at(position - 1) != '/') {
        createBucketUrl.insert(position, (std::string("/")+param).c_str());
    } else {
        createBucketUrl.insert(position, param.c_str());
    }
    *handle = curl_easy_init();
    curl_easy_setopt(*handle, CURLOPT_URL,  createBucketUrl.c_str());
    curl_easy_setopt(*handle, CURLOPT_SSL_VERIFYPEER, 0);
    curl_easy_setopt(*handle, CURLOPT_CONNECTTIMEOUT, 10);
    curl_easy_setopt(*handle, CURLOPT_TIMEOUT, 10);
    curl_easy_setopt(*handle, CURLOPT_POST, 1);
    curl_easy_setopt(*handle, CURLOPT_TCP_KEEPIDLE, 120L);
    curl_easy_setopt(*handle, CURLOPT_TCP_KEEPINTVL, 60L);
    FILE *devnull = fopen("/dev/null", "w+");
    curl_easy_setopt(*handle, CURLOPT_WRITEDATA, devnull);
}

static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

void HTTP::initCurlRead(const std::string& url)
{
  mReadUrl = url;
  readHandle = curl_easy_init();
  curl_easy_setopt(readHandle, CURLOPT_SSL_VERIFYPEER, 0); 
  curl_easy_setopt(readHandle, CURLOPT_CONNECTTIMEOUT, 10);
  curl_easy_setopt(readHandle, CURLOPT_TIMEOUT, 10);
  curl_easy_setopt(readHandle, CURLOPT_TCP_KEEPIDLE, 120L);
  curl_easy_setopt(readHandle, CURLOPT_TCP_KEEPINTVL, 60L);
  curl_easy_setopt(readHandle, CURLOPT_WRITEFUNCTION, WriteCallback);
}

std::string HTTP::query(const std::string& query)
{
  CURLcode response;
  long responseCode;
  std::string buffer;
  struct curl_slist* headers = NULL;
  const char * json = "Content-type: application/json";;
  headers =   curl_slist_append(headers,json);
//  auto fullUrl = mReadUrl + std::string(encodedQuery);
  curl_easy_setopt(readHandle, CURLOPT_URL, mReadUrl);
    curl_easy_setopt(readHandle,CURLOPT_POST,1);
  curl_easy_setopt(readHandle, CURLOPT_POSTFIELDS, query.c_str());
  curl_easy_setopt(readHandle, CURLOPT_POSTFIELDSIZE, (long) query.length());
//  curl_easy_setopt(readHandle, CURLOPT_WRITEDATA, &buffer);
  response = curl_easy_perform(readHandle);
  curl_easy_getinfo(readHandle, CURLINFO_RESPONSE_CODE, &responseCode);
  if (response != CURLE_OK) {
    throw InfluxDBException("HTTP::query", curl_easy_strerror(response)+std::string ("\nurl ")+mReadUrl + std::string ("\nquery: ")+ query);
  }
  if (responseCode !=  200) {
    throw InfluxDBException("HTTP::query", "Status code: " + std::to_string(responseCode) + mReadUrl + query);
  }
  return buffer;
}

void HTTP::enableBasicAuth(const std::string& auth)
{
  curl_easy_setopt(writeHandle, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
  curl_easy_setopt(writeHandle, CURLOPT_USERPWD, auth.c_str());
  curl_easy_setopt(readHandle, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
  curl_easy_setopt(readHandle, CURLOPT_USERPWD, auth.c_str());
}

void HTTP::enableSsl()
{
  curl_easy_setopt(readHandle, CURLOPT_SSL_VERIFYPEER, 0L);
  curl_easy_setopt(writeHandle, CURLOPT_SSL_VERIFYPEER, 0L);
}

HTTP::~HTTP()
{
  curl_easy_cleanup(writeHandle);
  curl_easy_cleanup(readHandle);
  curl_global_cleanup();
}

void HTTP::create(std::string&& post)
{
    CURLcode response;
    long responseCode;
    struct curl_slist* headers = NULL;
    char * auth = "Authorization: Token n4LKrFSiL3cOnsFn8WuAFe16XekVT2l2_zJq2r19kLlbPswJYmGA6Py3tm19uo51kYT9ENLrp46pWqhPVtOYng==";
    char * json = "Content-type: application/json";
    char * encode = "Accept-Encoding: gzip";
    headers = curl_slist_append(headers,json);
    headers = curl_slist_append(headers,auth);
    headers = curl_slist_append(headers,encode);
    curl_easy_setopt(createBucketHandle,CURLOPT_POST,1);
    curl_easy_setopt(createBucketHandle, CURLOPT_HTTPHEADER,headers);
    curl_easy_setopt(createBucketHandle, CURLOPT_POSTFIELDS, post.c_str());
    curl_easy_setopt(createBucketHandle, CURLOPT_POSTFIELDSIZE, (long) post.length());
    response = curl_easy_perform(createBucketHandle);
    curl_easy_getinfo(createBucketHandle, CURLINFO_RESPONSE_CODE, &responseCode);
    if (response != CURLE_OK) {
        throw InfluxDBException("HTTP::create_bucket", curl_easy_strerror(response)+std::string());
    }
    if (responseCode < 200 || responseCode > 206) {
        throw InfluxDBException("HTTP::create_bucket", "Response code: " + std::to_string(responseCode) + std::string(curl_easy_strerror(response)));
    }
}


void HTTP::send(std::string&& post,CURL *handle)
{
  CURLcode response;
  long responseCode;
  curl_easy_setopt(handle, CURLOPT_POSTFIELDS, post.c_str());
  curl_easy_setopt(handle, CURLOPT_POSTFIELDSIZE, (long) post.length());
  response = curl_easy_perform(handle);
  curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &responseCode);
  if (response != CURLE_OK) {
    throw InfluxDBException("HTTP::send", curl_easy_strerror(response));
  }
  if (responseCode < 200 || responseCode > 206) {
    throw InfluxDBException("HTTP::send", "Response code: " + std::to_string(responseCode));
  }
}

} // namespace transports
} // namespace influxdb
