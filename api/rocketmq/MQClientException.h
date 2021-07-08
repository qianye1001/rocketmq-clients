#pragma once

#include <exception>
#include <ostream>
#include <sstream>
#include <string>

#include "RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class MQException : public std::exception {
public:
  MQException(const std::string& msg, int error, const char* file, int line) throw()
      : m_error(error), m_line(line), m_file(file) {
    try {
      std::stringstream ss;
      ss << "msg: " << msg << ",error:" << error << ",in file <" << file << "> line:" << line;
      m_msg = ss.str();
    } catch (...) {
    }
  }

  MQException(const std::string& msg, int error, const char* file, const char* type, int line) throw()
      : m_error(error), m_line(line), m_file(file), m_type(type) {
    try {
      std::stringstream ss;
      ss << "msg: " << msg << ",error:" << error << ",in file <" << file << "> line:" << line;
      m_msg = ss.str();
    } catch (...) {
    }
  }

  virtual ~MQException() throw() {}

  const char* what() const throw() { return m_msg.c_str(); }

  int GetError() const throw() { return m_error; }

  virtual const char* GetType() const throw() { return m_type.c_str(); }

protected:
  int m_error;
  int m_line;
  std::string m_msg;
  std::string m_file;
  std::string m_type;
};

inline std::ostream& operator<<(std::ostream& os, const MQException& e) {
  os << "Type: " << e.GetType() << " , " << e.what();
  return os;
}

#define DEFINE_MQ_CLIENT_EXCEPTION(name)                                                                               \
  class name : public MQException {                                                                                    \
  public:                                                                                                              \
    name(const std::string& msg, int error, const char* file, int line) throw()                                        \
        : MQException(msg, error, file, #name, line) {}                                                                \
    virtual const char* GetType() const throw() { return m_type.c_str(); }                                             \
  };

DEFINE_MQ_CLIENT_EXCEPTION(MQClientException)
DEFINE_MQ_CLIENT_EXCEPTION(MQBrokerException)
DEFINE_MQ_CLIENT_EXCEPTION(InterruptedException)
DEFINE_MQ_CLIENT_EXCEPTION(RemotingException)
DEFINE_MQ_CLIENT_EXCEPTION(UnknownHostException)

#define THROW_MQ_EXCEPTION(e, msg, err) throw e(msg, err, __FILE__, __LINE__)
#define NEW_MQ_EXCEPTION(e, msg, err) e(msg, err, __FILE__, __LINE__)

ROCKETMQ_NAMESPACE_END