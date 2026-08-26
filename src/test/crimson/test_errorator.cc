// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <boost/iterator/counting_iterator.hpp>
#include <numeric>

#include "test/crimson/gtest_seastar.h"

#include "crimson/common/errorator.h"
#include "crimson/common/errorator-utils.h"
#include "crimson/common/log.h"
#include "seastar/core/sleep.hh"
#include <iostream>


struct errorator_test_t : public seastar_test_suite_t {
  using ertr = crimson::errorator<crimson::ct_error::invarg>; // 参数无效
  using two_error_ertr = crimson::errorator<crimson::ct_error::invarg,
                                            crimson::ct_error::enoent>; 

  ertr::future<> invarg_foo() {
    // 表示产生一个 invarg 错误
    return crimson::ct_error::invarg::make();
  };

  ertr::future<> clean_foo() {
    return ertr::now();
  };

  two_error_ertr::future<> two_error_foo(bool invalid) {
    if(invalid) {
      return crimson::ct_error::invarg::make();
    }

    return crimson::ct_error::enoent::make();
  }

  using enoent_ertr = crimson::errorator<
        crimson::ct_error::enoent>;

  enoent_ertr::future<> handle_invarg_only(bool invalid) {
    return two_error_foo(invalid).handle_error(
      crimson::ct_error::invarg::handle(
        [](const auto&) {
          fmt::print("INNER: handled invarg\n");
          return seastar::now();
        }
      ),
      crimson::ct_error::enoent::pass_further{}
    );
  }

  struct noncopyable_t {
    constexpr noncopyable_t() = default;
    ~noncopyable_t() = default;
    noncopyable_t(noncopyable_t&&) = default;
  private:
    noncopyable_t(const noncopyable_t&) = delete;
    noncopyable_t& operator=(const noncopyable_t&) = delete;
  };
};

TEST_F(errorator_test_t, basic)
{
  run_async([] {
    return crimson::repeat([i=0]() mutable {
      if (i < 5) {
        ++i;
        return ertr::make_ready_future<seastar::stop_iteration>(
          seastar::stop_iteration::no);
      } else {
        return ertr::make_ready_future<seastar::stop_iteration>(
          seastar::stop_iteration::yes);
      }
    }).unsafe_get();
  });
}

TEST_F(errorator_test_t, parallel_for_each)
{
  run_async([] {
    static constexpr int SIZE = 42;
    auto sum = std::make_unique<int>(0);
    return ertr::parallel_for_each(
      boost::make_counting_iterator(0),
      boost::make_counting_iterator(SIZE),
      [sum=sum.get()](int i) {
	*sum += i;
      }).safe_then([sum=std::move(sum)] {
	int expected = std::accumulate(boost::make_counting_iterator(0),
				       boost::make_counting_iterator(SIZE),
				       0);
	ASSERT_EQ(*sum, expected);
      }).unsafe_get();
  });
}

TEST_F(errorator_test_t, non_copy_then)
{
  run_async([] {
    auto create_noncopyable = [] {
      return ertr::make_ready_future<noncopyable_t>();
    };
    return create_noncopyable().safe_then([](auto) {
      return ertr::now();
    }).unsafe_get();
  });
}

TEST_F(errorator_test_t, test_futurization)
{
  run_async([] {
    // we don't want to be enforced to always do `make_ready_future(...)`.
    // as in seastar::future, the futurization should take care about
    // turning non-future types (e.g. int) into futurized ones (e.g.
    // ertr::future<int>).
    return ertr::now().safe_then([] {
      return 42;
    }).safe_then([](int life) {
      return ertr::make_ready_future<int>(life);
    }).unsafe_get();
  });
}

TEST_F(errorator_test_t, no_handle_error)
{
  run_async([this] {
    return clean_foo().handle_error(
      crimson::ct_error::assert_all("unexpected error")
    ).get();
  });
}

TEST_F(errorator_test_t, trace_failed_path) 
{
  run_async([this] {
    return invarg_foo()
      .safe_then([] {
        std::cout << "SAFE_THEN: suceess path\n";
        return ertr::make_ready_future<int>(42);
      })
      .safe_then([](int value) {
        std::cout << "VALUE: " << value << "\n";
        return seastar::now();
      })
      .handle_error(
        crimson::ct_error::invarg::handle(
          [](const auto& ec) {
            std::cout << "ERROR: " << ec.message() << '\n';
            return seastar::now();
          }
        ),
        crimson::ct_error::assert_all("unexpected error")
      )
      .get();
  });
}

TEST_F(errorator_test_t, handle_specific_error)
{
  int res = 0;
  run_async([&res, this] {
  return invarg_foo().handle_error(
    crimson::ct_error::invarg::handle([&res] (const auto& ec) {
      EXPECT_EQ(ec.value(), EINVAL);
      res = 1; // handle确实执行了
      return seastar::now();
    }),
    crimson::ct_error::assert_all("unexpected error")).get();
  });
  EXPECT_EQ(res, 1);
}

TEST_F(errorator_test_t, pass_further_error)
{
  int res = 0;
  run_async([&res, this] {
    return invarg_foo().handle_error(
      ertr::pass_further{}
    ).handle_error(
      crimson::ct_error::invarg::handle([&res] (const auto& ec) {
      res = 1;
      return seastar::now();
    }),
    crimson::ct_error::assert_all("unexpected error")).get();
  });
  EXPECT_EQ(res, 1);
}


TEST_F(errorator_test_t, exhaustive_error_handler) {
  run_async([this] {
    return two_error_foo(false)
            .handle_error(
              crimson::ct_error::invarg::handle(
                [](const auto&) {
                  fmt::print("HANDLED: invarg\n");
                  return seastar::now();
              }
            ),
            crimson::ct_error::enoent::handle(
              [](const auto&) {
                fmt::print("HANDLED: enoent\n");
                return seastar::now();
              }
            )
          )
        .get();
    });
}

TEST_F(errorator_test_t, partially_handle_error) {
  run_async([this] {
    return handle_invarg_only(false)
        .handle_error(
            crimson::ct_error::enoent::handle(
              [](const auto&) {
                fmt::print("OUTER: handled enoent\n");
                return seastar::now();
              }
            )
        )
      .get();
  });
}