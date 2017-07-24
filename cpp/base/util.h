#pragma once

#include <stddef.h>
#include <algorithm>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <random>
#include <mutex>
#include <condition_variable>
#include <memory>

#include <boost/format.hpp>
#include <boost/tokenizer.hpp>
#include <boost/algorithm/string.hpp>

#include <thrift/protocol/TProtocol.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

class Args {
 public:
  virtual bool HasNext() const = 0;
  virtual const char* Pop() const = 0;
  virtual size_t Count() const = 0;
  virtual ~Args() {
  }
};

class StrArgs : public Args {
 public:
  StrArgs(const std::vector<std::string>& strs)
      : curr_(0),
        strs_(std::move(strs)) {
  }

  StrArgs(const std::string& line)
      : curr_(0) {
    boost::split(strs_, line, boost::is_any_of(" "));
  }

  StrArgs(int argc, char* argv[])
      : curr_(0) {
    strs_.reserve(argc);
    for (int i = 0; i < argc; ++i) {
      strs_.push_back(std::string(argv[i]));
    }
  }

  bool HasNext() const override {
    return curr_ < strs_.size();
  }

  const char* Pop() const override {
    const char* res = strs_[curr_].c_str();
    ++curr_;
    return res;
  }

  size_t Count() const override {
    return strs_.size() - curr_;
  }

 private:
  std::vector<std::string> strs_;
  mutable int curr_;
};

class CommandProcessor {
 public:
  static CommandProcessor* GetProcessor(const std::string& name) {
    auto it = processor_map_.find(name);
    if(it != processor_map_.end()){
      return it->second;
    } else {
      return nullptr;
    }
  }

  static void PrintHelp() {
    std::cout << "-------- Command Usage --------" << std::endl;
    for(auto entry : processor_map_){
      entry.second->Usage();
    }
  }

  static std::map<std::string, CommandProcessor* > processor_map_;

  static int ProcessCommand(const Args& args) {
    if (args.HasNext()) {
      std::string lower_cmd(args.Pop());
      std::transform(lower_cmd.begin(), lower_cmd.end(), lower_cmd.begin(),
                     ::tolower);

      CommandProcessor* processor = GetProcessor(lower_cmd);

      if(processor != nullptr){
        return processor->Process(args);
      } else {
        std::cerr << "Unknown command: " << lower_cmd << std::endl;
        return -1;
      }
    }
    return 0;
  }

  CommandProcessor() {}

  CommandProcessor(const std::string& name) {
    std::string lower_case(name);
    std::transform(name.begin(), name.end(), lower_case.begin(),
                   ::tolower);

    name_ = lower_case;
    auto it = processor_map_.insert(std::make_pair(name_, this));
    if(!it.second){
      throw std::string("command " + lower_case + " exists!");
    }
  }

  virtual int Process(const Args& args) = 0;
  virtual void Usage() {
    std::cout << name_ << std::endl;
  }
  virtual ~CommandProcessor() {
  }
 private:
  std::string name_;
};

inline void PrintIndicator(const std::string& indicator) {
  std::cout << indicator;
}

template<typename Factory> class Pool {
 public:
  typedef typename Factory::ObjectType ObjectType;
  virtual ObjectType* Borrow(int borrow_timeout /* ms */) = 0;
  virtual void Return(ObjectType* t) = 0;
  virtual std::string ToString() = 0;
  virtual ~Pool() {}
};

template<typename T> class LoadBalancer {
 public:
  virtual T& Get(std::vector<T>& elements) = 0;
  virtual ~LoadBalancer() {}
};

template<typename T> class RandomLoadBalancer : public LoadBalancer<T> {
 public:
  RandomLoadBalancer() : rd_(), e_(rd_()) {}

  T& Get(std::vector<T>& elements) override {
    std::uniform_int_distribution<std::default_random_engine::result_type> u(0, elements.size() - 1);
    int index = u(e_);
    return elements[index];
  }

 private:
  std::random_device rd_;
  std::default_random_engine e_;
};

template<typename T> class ReplicaGroup {
 public:
  virtual T GetOne() = 0;
  virtual std::vector<T> GetAll() = 0;
  virtual bool IsEmpty() = 0;
  virtual ~ReplicaGroup() {}
};

template<typename T> class ReplicaGroupImpl : public ReplicaGroup<T>{
 public:
  ReplicaGroupImpl() : load_balancer_(new RandomLoadBalancer<T>()) {}

  void Add(const T& t) {
    replicas_.push_back(t);
  }

  bool IsEmpty() override {
    return replicas_.empty();
  }

  // must not be empty
  T GetOne() override {
    return load_balancer_->Get(replicas_);
  }

  std::vector<T> GetAll() override {
    return replicas_;
  }
 private:
  std::vector<T> replicas_;
  std::unique_ptr<LoadBalancer<T> > load_balancer_;
};

class UniformRandomIntGenerator {
 public:
  UniformRandomIntGenerator(int begin, int end) : rd_(), e_(rd_()), dist_(begin, end) {

  }

  int Next() {
    return dist_(e_);
  }

 private:
  std::random_device rd_;
  std::default_random_engine e_;
  std::uniform_int_distribution<std::default_random_engine::result_type> dist_;
};

class ClockObject {
 public:
  typedef std::chrono::steady_clock ClockType;
  typedef std::chrono::steady_clock::duration DurationType;

  ClockObject(int64_t* d) : d_(d) {
    start_ = ClockType::now();
  }

  ~ClockObject() {
    *d_ = std::chrono::duration_cast<std::chrono::microseconds>(ClockType::now() - start_).count();
  }
 private:
  ClockType::time_point start_;
  int64_t* d_;
};


template<class Factory> class RandomPool : public Pool<Factory>{
 public:
  typedef typename Pool<Factory>::ObjectType ObjectType;
  RandomPool(size_t capacity, const Factory& factory) : capacity_(capacity), factory_(factory), size_(0), rd_(), e_(rd_()) {
  }

  virtual ObjectType* Borrow(int borrow_timeout) override {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(borrow_timeout);
    bool timeout = false;
    std::unique_lock<std::mutex> lock(mtx_);
    while(pool_.empty()){
      if(CanCreate()){
        return Create();
      }
      timeout = WaitUntil(lock, deadline);
      if(timeout){
        return nullptr;
      }
    }

    std::uniform_int_distribution<std::default_random_engine::result_type> u(0, pool_.size() - 1);
    int index = u(e_);
    ObjectType* t = pool_[index];
    Remove(index);
    return t;
  }

  virtual void Return(ObjectType* t)  override {
    if(t != nullptr){
      std::lock_guard<std::mutex> lock(mtx_);
      pool_.push_back(t);
      cond_.notify_one();
    }
  }

  virtual std::string ToString(){
    std::ostringstream os;
    os << "{";
    os << "type : RandomPool,";
    os << "capacity : " << capacity_ << ",";
    os << "size : " << size_ << ",";
    os << "factory: " << factory_.ToString();
    os << "}";
    return os.str();
  }

  virtual ~RandomPool() {
    while(!pool_.empty()){
      ObjectType* o = pool_.back();
//      Destroy(o);
      pool_.pop_back();
    }
  }
 private:
  bool CanCreate(){
    return size_ < capacity_;
  }

  ObjectType* Create() {
    ObjectType* t = factory_.Create();
    if(t != nullptr) {
      ++size_;
    }
    return t;
  }

  void Destory(ObjectType* t){
    if(t != nullptr){
      --size_;
    }
    factory_.Destory(t);
  }

  bool WaitUntil(std::unique_lock<std::mutex>& lock, const std::chrono::time_point<std::chrono::steady_clock>& deadline){
    std::cv_status s = cond_.wait_until(lock, deadline);
    if(s == std::cv_status::timeout){
      return true;
    } else {
      return false;
    }
  }

  void Remove(int index){
    if(index >=0 && index < pool_.size()){
      pool_[index] = pool_.back();
      pool_.erase(pool_.end() - 1);
    }
  }

  size_t capacity_;
  Factory factory_;
  size_t size_;

  std::vector<ObjectType*> pool_;
  std::random_device rd_;
  std::default_random_engine e_;

  std::mutex mtx_;
  std::condition_variable cond_;

};


template<typename T> std::vector<T> AsList(const T& t){
  std::vector<T> list;
  list.push_back(t);
  return list;
}

template<typename T> static bool Deserialize(const std::string& data, T* result){
  using apache::thrift::transport::TTransportException;
  using apache::thrift::protocol::TCompactProtocol;
  using apache::thrift::protocol::TProtocol;
  using apache::thrift::transport::TMemoryBuffer;
  using apache::thrift::transport::TTransport;

  try {
    // const_cast is safe here since thrift will not modify the buffer when policy == OBSERVE
    uint8_t* data_addr = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>((data.data())));
    boost::shared_ptr<TTransport> buffer(new TMemoryBuffer(data_addr, data.size(), TMemoryBuffer::OBSERVE));
    boost::shared_ptr<TProtocol> protocol(new TCompactProtocol(buffer));
    result->read(protocol.get());
    return true;
  } catch (const TTransportException& e){
    return false;
  }
}

// TODO
class NonCopyable {
 public:
  NonCopyable() = default;
  NonCopyable& operator=(const NonCopyable&) = delete;
  NonCopyable(const NonCopyable&) = delete;
};

// TODO optimize
template<typename T> class Singleton : public NonCopyable {
 public:
  static T* Instance(){
    std::call_once(flag_, []{
      instance_ = new T();
    });
    return instance_;
  }

  static std::once_flag flag_;
  static T* instance_;
};

template<typename T> std::once_flag Singleton<T>::flag_;
template<typename T> T* Singleton<T>::instance_ = nullptr;

template<typename T> std::ostream& operator<<(std::ostream& os, const std::vector<T>& objs) {
  os << "[";
  bool first = true;
  for(auto& o : objs) {
    if(first) {
      os << o;
      first = false;
    } else {
      os << "," << o;
    }
  }
  os << "]";
  return os;
}

template<typename K, typename V> std::ostream& operator<<(std::ostream& os, const std::map<K, V>& objs) {
  os << "{";
  bool first = true;
  for(auto& entry : objs) {
    if(first) {
      os << entry.first << ":" << entry.second;
      first = false;
    } else {
      os << "," << entry.first << ":" << entry.second;
    }
  }
  os << "}";
  return os;
}
