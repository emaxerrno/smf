// Copyright 2017 Alexander Gallego
//
#pragma once

#include "smf/rpc_envelope.h"
#include <smf/log.h>

#include <boost/program_options.hpp>
#include <seastar/core/semaphore.hh>

#include <memory>
#include <vector>

namespace smf {
// missing timeout
// tracer probability
// exponential distribution, poisson distribution of load
//
struct load_generator_args {
  load_generator_args(seastar::sstring _ip, uint16_t _port, size_t _parallelism,
                      size_t _concurrency, size_t _memory_per_core,
                      smf::rpc::compression_flags _compression,
                      boost::program_options::variables_map _cfg)
    : ip(_ip), port(_port), parallelism(_parallelism),
      concurrency(_concurrency), memory_per_core(_memory_per_core),
      compression(_compression), cfg(std::move(_cfg)) {}

  seastar::sstring ip;
  uint16_t port;
  size_t parallelism;
  size_t concurrency;
  size_t memory_per_core;
  smf::rpc::compression_flags compression;
  boost::program_options::variables_map cfg;
};

}  // namespace smf

namespace std {
inline std::ostream &
operator<<(std::ostream &o, const smf::load_generator_args &args) {
  o << "generator_args{ip=" << args.ip << ", port=" << args.port
    << ", parallelism=" << args.parallelism
    << ", concurrency=" << args.concurrency
    << ", compression=" << smf::rpc::EnumNamecompression_flags(args.compression)
    << ", cfg_size=" << args.cfg.size() << "}";
  return o;
}
}  // namespace std
