// Copyright 2017 Alexander Gallego
//
#pragma once

#include <algorithm>
#include <memory>

#include <seastar/core/shared_ptr.hh>

#include "smf/load_generator_args.h"
#include "smf/load_generator_duration.h"
#include "smf/lz4_filter.h"
#include "smf/macros.h"
#include "smf/random.h"
#include "smf/rpc_client.h"
#include "smf/rpc_envelope.h"
#include "smf/rpc_generated.h"
#include "smf/zstd_filter.h"

namespace smf {

template <typename T>
inline seastar::shared_ptr<T>
make_client(const load_generator_args &args) {
  smf::rpc_client_opts opts{};
  opts.server_addr = seastar::ipv4_addr{args.ip, args.port};
  opts.memory_avail_for_client = args.memory_per_core;
  auto client = seastar::make_shared<T>(std::move(opts));
  client->enable_histogram_metrics();
  if (args.compression == smf::rpc::compression_flags::compression_flags_zstd) {
    client->incoming_filters().push_back(smf::zstd_decompression_filter());
    client->outgoing_filters().push_back(smf::zstd_compression_filter(1024));
  } else if (args.compression ==
             smf::rpc::compression_flags::compression_flags_lz4) {
    client->incoming_filters().push_back(smf::lz4_decompression_filter());
    client->outgoing_filters().push_back(smf::lz4_compression_filter(1024));
  }
  return client;
}

template <typename ClientService>
class __attribute__((visibility("default"))) load_generator {
 public:
  struct method_cb_t {
    virtual ~method_cb_t() = default;
    virtual seastar::future<> apply(ClientService *, smf::rpc_envelope &&) = 0;
  };
  struct generator_cb_t {
    virtual ~generator_cb_t() = default;
    virtual seastar::future<smf::rpc_envelope>
    apply(seastar::semaphore &sem,
          const boost::program_options::variables_map &) = 0;
  };

  explicit load_generator(load_generator_args _args) : args(_args) {
    for (auto i = 0u; i < args.parallelism; ++i) {
      clients_.push_back(make_client<ClientService>(args));
    }
  }
  ~load_generator() {}

  SMF_DISALLOW_COPY_AND_ASSIGN(load_generator);

  const load_generator_args args;

  std::unique_ptr<smf::histogram>
  copy_histogram() const {
    auto h = smf::histogram::make_unique();
    for (auto &c : clients_) {
      auto p = c->get_histogram();
      *h += *p;
    }
    return std::move(h);
  }

  seastar::future<>
  stop() {
    return seastar::do_for_each(clients_.begin(), clients_.end(),
                                [](auto &c) { return c->stop(); });
  }
  seastar::future<>
  connect() {
    LOG_INFO("Making {} connections on this core.", clients_.size());
    return seastar::do_for_each(clients_.begin(), clients_.end(),
                                [](auto &c) { return c->connect(); });
  }
  seastar::future<load_generator_duration>
  benchmark(seastar::shared_ptr<generator_cb_t> gen,
            seastar::shared_ptr<method_cb_t> method_cb) {
    namespace co = std::chrono;
    auto stats =
      seastar::make_lw_shared<load_generator_duration>(args.parallelism);
    LOG_INFO("Launching parallel:{}, concurrent:{}", args.parallelism,
             args.concurrency);
    auto sem =
      seastar::make_lw_shared<seastar::semaphore>(args.memory_per_core);
    stats->begin();
    return seastar::parallel_for_each(
             clients_.begin(), clients_.end(),
             [this, gen, method_cb, stats, sem](auto &c) mutable {
               return seastar::parallel_for_each(
                 boost::irange<size_t>(0, args.concurrency),
                 [this, gen, method_cb, c, stats,
                  sem](uint32_t) mutable -> seastar::future<> {
                   return gen->apply(*sem, args.cfg)
                     .then([stats, c, method_cb](smf::rpc_envelope e) {
                       const auto sz = e.size();
                       stats->total_bytes += sz;
                       return method_cb->apply(c.get(), std::move(e));
                     });
                 });
             })
      .then([stats, sem] {
        stats->end();
        return seastar::make_ready_future<load_generator_duration>(
          std::move(*stats));
      });
  }

 private:
  std::vector<seastar::shared_ptr<ClientService>> clients_;
};

}  // namespace smf
