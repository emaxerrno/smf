// Copyright (c) 2017 Alexander Gallego. All rights reserved.
//

#include <chrono>
#include <iostream>

#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>

#include "smf/histogram_seastar_utils.h"
#include "smf/load_generator.h"
#include "smf/log.h"
#include "smf/unique_histogram_adder.h"

// templates
#include "demo_service.smf.fb.h"

using client_t = smf_gen::demo::SmfStorageClient;
using load_gen_t = smf::load_generator<client_t>;

inline smf_gen::demo::payloadT
payload() {
  auto c1 = []() {
    smf_gen::demo::c1T ret;
    ret.a = 1;
    ret.b = 1;
    ret.c = 1;
    ret.d = 1;
    return ret;
  };
  auto c2 = [&]() {
    smf_gen::demo::c2T ret;
    ret.x = std::make_unique<smf_gen::demo::c1T>(c1());
    ret.a = 1;
    ret.b = 1;
    ret.c = 1;
    ret.d = 1;
    return ret;
  };
  auto c3 = [&]() {
    smf_gen::demo::c3T ret;
    ret.x = std::make_unique<smf_gen::demo::c2T>(c2());
    ret.a = 1;
    ret.b = 1;
    ret.c = 1;
    ret.d = 1;
    return ret;
  };
  auto c4 = [&]() {
    smf_gen::demo::c4T ret;
    ret.x = std::make_unique<smf_gen::demo::c3T>(c3());
    ret.a = 1;
    ret.b = 1;
    ret.c = 1;
    ret.d = 1;
    return ret;
  };
  auto c5 = [&]() {
    smf_gen::demo::c5T ret;
    ret.x = std::make_unique<smf_gen::demo::c4T>(c4());
    ret.a = 1;
    ret.b = 1;
    ret.c = 1;
    ret.d = 1;
    return ret;
  };
  smf_gen::demo::payloadT ret;
  ret.one = std::make_unique<smf_gen::demo::c1T>(c1());
  ret.two = std::make_unique<smf_gen::demo::c2T>(c2());
  ret.three = std::make_unique<smf_gen::demo::c3T>(c3());
  ret.four = std::make_unique<smf_gen::demo::c4T>(c4());
  ret.five = std::make_unique<smf_gen::demo::c5T>(c5());
  return ret;
}
inline smf_gen::demo::fragbufT
fragbuf(std::size_t data_size, std::size_t chunk_size) {
  const size_t chunks = data_size / chunk_size;
  smf_gen::demo::fragbufT ret;
  ret.data.reserve(chunks);
  for (size_t i = 0; i < chunks; ++i) {
    smf_gen::demo::bufT frag;
    frag.data.resize(chunk_size, 66);
    ret.data.emplace_back(
      std::make_unique<smf_gen::demo::bufT>(std::move(frag)));
  }
  return ret;
}
// This is just for the load generation.
// On normal apps you would just do
// client-><method_name>(params).then().then()
// as you would for normal seastar calls
// This example is just using the load generator to test performance
//
struct method_callback final : load_gen_t::method_cb_t {
  seastar::future<>
  apply(client_t *c, smf::rpc_envelope &&e) final {
    return c->Get(std::move(e)).then([](auto ret) {
      return seastar::make_ready_future<>();
    });
  }
};

struct generator final : load_gen_t::generator_cb_t {
  seastar::future<smf::rpc_envelope>
  apply(seastar::semaphore &sem,
        const boost::program_options::variables_map &cfg) final {
    std::size_t test = cfg["test-case"].as<std::size_t>();
    if (test == 1) {
      std::size_t data_size = cfg["data-size"].as<std::size_t>();
      return seastar::with_semaphore(
        sem, data_size, [&cfg, this] { return generate_test_1(cfg); });
    } else if (test == 2) {
      return seastar::with_semaphore(
        sem, (sizeof(smf_gen::demo::c5T) * 10) + 200,
        [&cfg, this] { return generate_test_2(cfg); });
    }
    throw std::runtime_error("invalid test case, must be 1, or 2");
  }
  seastar::future<smf::rpc_envelope>
  generate_test_2(const boost::program_options::variables_map &cfg) {
    smf::rpc_typed_envelope<smf_gen::demo::ComplexRequest> req;
    req.data->data = std::make_unique<smf_gen::demo::payloadT>(payload());
    return seastar::make_ready_future<smf::rpc_envelope>(req.serialize_data());
  }
  seastar::future<smf::rpc_envelope>
  generate_test_1(const boost::program_options::variables_map &cfg) {
    smf::rpc_typed_envelope<smf_gen::demo::Request> req;
    std::size_t data_size = cfg["data-size"].as<std::size_t>();
    std::size_t chunk_size = cfg["chunk-size"].as<std::size_t>();
    req.data->data =
      std::make_unique<smf_gen::demo::fragbufT>(fragbuf(data_size, chunk_size));
    auto e = req.serialize_data();
    LOG_THROW_IF(e.size() < data_size, "Incorrect data size");
    return seastar::make_ready_future<smf::rpc_envelope>(std::move(e));
  }
};

void
cli_opts(boost::program_options::options_description_easy_init o) {
  namespace po = boost::program_options;

  o("ip", po::value<std::string>()->default_value("127.0.0.1"),
    "ip to connect to");

  o("port", po::value<uint16_t>()->default_value(20776), "port for service");

  o("test-case", po::value<std::size_t>()->default_value(1),
    "1: large payload, 2: complex struct");

  o("data-size", po::value<std::size_t>()->default_value(1 << 20),
    "1MB default data _per_ request");

  o("chunk-size", po::value<std::size_t>()->default_value(1 << 15),
    "fragment data_size by this chunk size step (32KB default)");

  o("concurrency", po::value<std::size_t>()->default_value(1000),
    "number of request per tcp connection");

  // currently these are sockets
  o("parallelism", po::value<std::size_t>()->default_value(2),
    "number of tcp cons per core");
}

int
main(int args, char **argv, char **env) {
  seastar::distributed<load_gen_t> load;
  seastar::app_template app;
  cli_opts(app.add_options());

  return app.run(args, argv, [&] {
    seastar::engine().at_exit([&] { return load.stop(); });
    auto &cfg = app.configuration();

    ::smf::load_generator_args largs(
      cfg["ip"].as<std::string>().c_str(), cfg["port"].as<uint16_t>(),
      cfg["parallelism"].as<std::size_t>(),
      cfg["concurrency"].as<std::size_t>(),
      static_cast<uint64_t>(0.9 * seastar::memory::stats().total_memory()),
      smf::rpc::compression_flags::compression_flags_none, cfg);

    LOG_INFO("Load args: {}", largs);

    return load.start(std::move(largs))
      .then([&load] {
        LOG_INFO("Connecting to server");
        return load.invoke_on_all(&load_gen_t::connect);
      })
      .then([&load] {
        LOG_INFO("Benchmarking server");
        return load.invoke_on_all([](load_gen_t &server) {
          return server
            .benchmark(seastar::make_shared<generator>(),
                       seastar::make_shared<method_callback>())
            .then([](auto test) {
              LOG_INFO("Bench: {}", test);
              return seastar::make_ready_future<>();
            });
        });
      })
      .then([&load] {
        LOG_INFO("MapReducing stats");
        return load
          .map_reduce(smf::unique_histogram_adder(),
                      [](load_gen_t &shard) { return shard.copy_histogram(); })
          .then([](std::unique_ptr<smf::histogram> h) {
            LOG_INFO("Writing client histograms");
            return smf::histogram_seastar_utils::write("clients_latency.hgrm",
                                                       std::move(h));
          });
      })
      .then([] {
        LOG_INFO("Exiting");
        return seastar::make_ready_future<int>(0);
      });
  });
}
