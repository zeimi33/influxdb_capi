///
/// \author Adam Wegrzynek <adam.wegrzynek@cern.ch>
///

#include "InfluxDB.h"
#include "InfluxDBException.h"

#include <iostream>
#include <memory>
#include <string>
#include <curl/curl.h>

#ifdef INFLUXDB_WITH_BOOST
#include <boost/lexical_cast.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#endif

namespace influxdb
{
InfluxDB::InfluxDB(std::unique_ptr<Transport> transport) :
  mTransport(std::move(transport))
{
  mBuffer = {};
  mBuffering = false;
  mBufferSize = 0;
  mGlobalTags = {};
}

void InfluxDB::create(std::string buf)
{
    mTransport->create(std::move(buf));
}

void InfluxDB::batchOf(const std::size_t size)
{
  mBufferSize = size;
  mBuffering = true;
}

void InfluxDB::addBuffer(std::string buf)
{
    if(!mBuffering){
        mBuffering = true;
    }
    mBuffer.push_back(std::move(buf));
}

void InfluxDB::flushBuffer() {
  if (!mBuffering || mBuffer.empty()) {
    return;
  }
  std::string stringBuffer{};
  for (const auto &i : mBuffer) {
    stringBuffer+= i + "\n";
  }
  mBuffer.clear();
  transmit(std::move(stringBuffer),NULL);
}

void InfluxDB::addGlobalTag(std::string_view key, std::string_view value)
{
  if (!mGlobalTags.empty()) mGlobalTags += ",";
  mGlobalTags += key;
  mGlobalTags += "=";
  mGlobalTags += value;
}

InfluxDB::~InfluxDB()
{
  if (mBuffering) {
    flushBuffer();
  }
}

void InfluxDB::transmit(std::string&& point,CURL *handle)
{
  mTransport->send(std::move(point),handle);
}

void InfluxDB::write(Point&& metric,CURL* handle)
{
  if (mBuffering) {
    mBuffer.emplace_back(metric.toLineProtocol());
    if (mBuffer.size() >= mBufferSize) {
      flushBuffer();
    }
  } else {
    transmit(metric.toLineProtocol(),handle);
  }
}

writeApi::writeApi(InfluxDB* influxDb,std::string bucket,std::string org){
    this->bucket = bucket;
    this->influxdb = influxDb;
    this->org = org;
    std::string url;
    std::string params = std::string("bucket=") +bucket+std::string("&org=")+org;
    auto position = influxdb->url.find("?");
    if(position == std::string::npos){
        url = influxdb->url + std::string("?") + params;
    }else{
        url = influxdb->url.insert(position+1,  params+std::string("&"));
    }

    initHandle("write",url);
}

void writeApi::initHandle(std::string param,const std::string url){
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
    struct curl_slist* headers = NULL;
    writeHandle = curl_easy_init();
    char * auth = "Authorization: Token n4LKrFSiL3cOnsFn8WuAFe16XekVT2l2_zJq2r19kLlbPswJYmGA6Py3tm19uo51kYT9ENLrp46pWqhPVtOYng==";
    char * encode = "Accept-Encoding: gzip";
    char * json = "Content-type: ";
    char * accept = "Accept: ";
    char * agent = "User-Agent: influxdb-client-go/2.5.0  (linux; amd64)";
    headers = curl_slist_append(headers,json);
    headers = curl_slist_append(headers,auth);
    headers = curl_slist_append(headers,accept);
    headers = curl_slist_append(headers,agent);
    headers = curl_slist_append(headers,encode);
    curl_easy_setopt(writeHandle,CURLOPT_POST,1);
    curl_easy_setopt(writeHandle, CURLOPT_HTTPHEADER,headers);
    curl_easy_setopt(writeHandle, CURLOPT_URL,  createBucketUrl.c_str());
    curl_easy_setopt(writeHandle, CURLOPT_SSL_VERIFYPEER, 0);
    curl_easy_setopt(writeHandle, CURLOPT_CONNECTTIMEOUT, 10);
    curl_easy_setopt(writeHandle, CURLOPT_TIMEOUT, 10);
    curl_easy_setopt(writeHandle, CURLOPT_POST, 1);
    curl_easy_setopt(writeHandle, CURLOPT_TCP_KEEPIDLE, 120L);
    curl_easy_setopt(writeHandle, CURLOPT_TCP_KEEPINTVL, 60L);
    FILE *devnull = fopen("/dev/null", "w+");
    curl_easy_setopt(writeHandle, CURLOPT_WRITEDATA, devnull);
}

void  writeApi::write(Point&& metric){
    influxdb->write(std::move(metric),writeHandle);
}

#ifdef INFLUXDB_WITH_BOOST
std::vector<Point> InfluxDB::query(const std::string&  query)
{
  auto response = mTransport->query(query);
  std::stringstream ss;
  ss << response;
  std::vector<Point> points;
  boost::property_tree::ptree pt;
  boost::property_tree::read_json(ss, pt);

  for (auto& result : pt.get_child("results")) {
    auto isResultEmpty = result.second.find("series");
    if (isResultEmpty == result.second.not_found()) return {};
    for (auto& series : result.second.get_child("series")) {
      auto columns = series.second.get_child("columns");

      for (auto& values : series.second.get_child("values")) {
        Point point{series.second.get<std::string>("name")};
        auto iColumns = columns.begin();
        auto iValues = values.second.begin();
        for (; iColumns != columns.end() && iValues != values.second.end(); iColumns++, iValues++) {
          auto value = iValues->second.get_value<std::string>();
          auto column = iColumns->second.get_value<std::string>();
          if (!column.compare("time")) {
            std::tm tm = {};
            std::stringstream ss;
            ss << value;
            ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
            point.setTimestamp(std::chrono::system_clock::from_time_t(std::mktime(&tm)));
            continue;
          }
          // cast all values to double, if strings add to tags
          try { point.addField(column, boost::lexical_cast<double>(value)); }
          catch(...) { point.addTag(column, value); }
        }
        points.push_back(std::move(point));
      }
    }
  }
  return points;
}
#else
std::vector<Point> InfluxDB::query(const std::string& /*query*/)
{
  throw InfluxDBException("InfluxDB::query", "Boost is required");
}
#endif

} // namespace influxdb
