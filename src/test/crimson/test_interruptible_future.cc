// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <seastar/core/sleep.hh>

#include "test/crimson/gtest_seastar.h"

#include "crimson/common/interruptible_future.h"
#include "crimson/common/log.h"

using namespace crimson;

class test_interruption : public std::exception
{};

class TestInterruptCondition {
public:
  TestInterruptCondition(bool interrupt)
    : interrupt(interrupt) {}

  template <typename T>
  std::optional<T> may_interrupt() {
    if (interrupt) {
      return seastar::futurize<T>::make_exception_future(test_interruption());
    } else {
      return std::optional<T>();
    }
  }

  template <typename T>
  static constexpr bool is_interruption_v = std::is_same_v<T, test_interruption>;

  static bool is_interruption(std::exception_ptr& eptr) {
    if (*eptr.__cxa_exception_type() == typeid(test_interruption))
      return true;
    return false;
  }

  void set_interrupt() {
    interrupt = true;
  }
private:
  bool interrupt = false;
};

namespace crimson::interruptible {
template
thread_local interrupt_cond_t<TestInterruptCondition>
interrupt_cond<TestInterruptCondition>;
}

TEST_F(seastar_test_suite_t, basic)
{
  using interruptor =
    interruptible::interruptor<TestInterruptCondition>;
  run_async([] {
    interruptor::with_interruption(
      [] {
        ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
        // 创建一个立即成功的普通 future
        // → 把它包装成可中断 future
        // → 成功后准备运行第一个可中断 continuation
        return interruptor::make_interruptible(seastar::now())
        .then_interruptible([] {
          ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
        }).then_interruptible([] {
          // 断言的作用，当前continuation确实携带着中断条件
          ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
          return errorator<ct_error::enoent>::make_ready_future<>();
        }).safe_then_interruptible([] {
          ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
          return seastar::now(); // 成功处理函数
        }, errorator<ct_error::enoent>::all_same_way([] {
          ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
          return seastar::now(); // 错误处理函数
          })
        );
      }, [](std::exception_ptr) {}, false).get();// get 等待future完成，并检查最终成功或失败

    interruptor::with_interruption(
      [] {
        ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
        return interruptor::make_interruptible(seastar::now())
        .then_interruptible([] {
          ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
        });
      }, [](std::exception_ptr) {
        ceph_assert(!interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
        return seastar::now();
      }, true).get();
  });
}

TEST_F(seastar_test_suite_t, loops)
{
  using interruptor =
    interruptible::interruptor<TestInterruptCondition>;
  std::cout << "testing interruptible loops" << std::endl;
  run_async([] {
    std::cout << "beginning" << std::endl;
    interruptor::with_interruption(
      [] {
	std::cout << "interruptiion enabled" << std::endl;
	ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
	return interruptor::make_interruptible(seastar::now())
	.then_interruptible([] {
	  std::cout << "test seastar future do_for_each" << std::endl;
	  std::vector<int> vec = {1, 2};
	  return seastar::do_with(std::move(vec), [](auto& vec) {
	    return interruptor::do_for_each(std::begin(vec), std::end(vec), [](int) {
	      ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
	      return seastar::now();
	    });
	  });
	}).then_interruptible([] {
	  std::cout << "test interruptible seastar future do_for_each" << std::endl;
	  std::vector<int> vec = {1, 2};
	  return seastar::do_with(std::move(vec), [](auto& vec) {
	    return interruptor::do_for_each(std::begin(vec), std::end(vec), [](int) {
	      ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
	      return interruptor::make_interruptible(seastar::now());
	    });
	  });
	}).then_interruptible([] {
	  std::cout << "test seastar future repeat" << std::endl;
	  return interruptor::repeat([] {
	    ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
	    return interruptor::make_interruptible(
		seastar::make_ready_future<
		  seastar::stop_iteration>(
		    seastar::stop_iteration::yes));
	  });
	}).then_interruptible([] {
	  std::cout << "test interruptible seastar future repeat" << std::endl;
	  return interruptor::repeat([] {
	    ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
	    return seastar::make_ready_future<
		    seastar::stop_iteration>(
		      seastar::stop_iteration::yes);
	  });
	}).then_interruptible([] {
	  std::cout << "test interruptible errorated future do_for_each" << std::endl;
	  std::vector<int> vec = {1, 2};
	  return seastar::do_with(std::move(vec), [](auto& vec) {
	    using namespace std::chrono_literals;
	    return interruptor::make_interruptible(seastar::now()).then_interruptible([&vec] {
	      return interruptor::do_for_each(std::begin(vec), std::end(vec), [](int) {
		ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
		return interruptor::make_interruptible(
		  errorator<ct_error::enoent>::make_ready_future<>());
	      }).safe_then_interruptible([] {
		ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
		return seastar::now();
	      }, errorator<ct_error::enoent>::all_same_way([] {
		ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
		return seastar::now();
	      }));
	    });
	  });
	}).then_interruptible([] {
	  std::cout << "test errorated future do_for_each" << std::endl;
	  std::vector<int> vec;
	  // set a big enough iteration times to test if there is stack overflow in do_for_each
	  for (int i = 0; i < 1000000; i++) {
	    vec.push_back(i);
	  }
	  return seastar::do_with(std::move(vec), [](auto& vec) {
	    using namespace std::chrono_literals;
	    return interruptor::make_interruptible(seastar::now()).then_interruptible([&vec] {
	      return interruptor::do_for_each(std::begin(vec), std::end(vec), [](int) {
		ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
		return errorator<ct_error::enoent>::make_ready_future<>();
	      }).safe_then_interruptible([] {
		ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
		return seastar::now();
	      }, errorator<ct_error::enoent>::all_same_way([] {
		ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
		return seastar::now();
	      }));
	    });
	  });
	}).then_interruptible([] {
	  ceph_assert(interruptible::interrupt_cond<TestInterruptCondition>.interrupt_cond);
	  return seastar::now();
	});
      }, [](std::exception_ptr) {}, false).get();
  });
}

using base_intr = interruptible::interruptor<TestInterruptCondition>;

using base_ertr = errorator<ct_error::enoent, ct_error::eagain>;
using base_iertr = interruptible::interruptible_errorator<
  TestInterruptCondition,
  base_ertr>;

using base2_ertr = base_ertr::extend<ct_error::input_output_error>;
using base2_iertr = interruptible::interruptible_errorator<
  TestInterruptCondition,
  base2_ertr>;

template <typename F>
auto with_intr(F &&f) {
  return base_intr::with_interruption_to_error<ct_error::eagain>(
    std::forward<F>(f),
    TestInterruptCondition(false));
}

TEST_F(seastar_test_suite_t, errorated)
{
  run_async([] {
    base_ertr::future<> ret = with_intr(
      []() {
	      return base_iertr::now();
      }
    );
    ret.unsafe_get();
  });
}

TEST_F(seastar_test_suite_t, errorated_value)
{
  run_async([] {
    base_ertr::future<int> ret = with_intr(
      []() {
	return base_iertr::make_ready_future<int>(
	  1
	);
      });
    EXPECT_EQ(ret.unsafe_get(), 1);
  });
}

TEST_F(seastar_test_suite_t, expand_errorated_value)
{
  run_async([] {
    base2_ertr::future<> ret = with_intr(
      []() {
	return base_iertr::make_ready_future<int>(
	  1
	).si_then([](auto) {
	  return base2_iertr::make_ready_future<>();
	});
      });
    ret.unsafe_get();
  });
}

TEST_F(seastar_test_suite_t, interruptible_async)
{
  using interruptor =
    interruptible::interruptor<TestInterruptCondition>;

  run_async([] {
    interruptor::with_interruption([] {
      auto fut = interruptor::async([] {
	  interruptor::make_interruptible(
	    seastar::sleep(std::chrono::milliseconds(10))).get();
	ceph_assert(interruptible::interrupt_cond<
	  TestInterruptCondition>.interrupt_cond);
	ceph_assert(interruptible::interrupt_cond<
	  TestInterruptCondition>.ref_count == 1);
      });
      ceph_assert(interruptible::interrupt_cond<
	TestInterruptCondition>.interrupt_cond);
      ceph_assert(interruptible::interrupt_cond<
	TestInterruptCondition>.ref_count == 1);
      return fut;
    }, [](std::exception_ptr) {}, false).get();
  });

}

TEST_F(seastar_test_suite_t, interruptible_yield)
{
  using interruptor =
    interruptible::interruptor<TestInterruptCondition>;

  run_async([] {
    bool interrupted = false;
    auto fut = interruptor::with_interruption([] {
      std::cout << "1. opfunc started" << std::endl;
      return interruptor::async([] {
        std::cout << "2. before set_interrupt" << std::endl;
        interruptible::interrupt_cond<
	        TestInterruptCondition>.interrupt_cond->set_interrupt();
        std::cout << "3. after set_interrupt" << std::endl;
        std::cout << "4. before interruptor::yield" << std::endl;
        interruptor::yield();
        // the execution should be interrupted, the run should
        // never reach here.
        std::cout << "5. after interruptor::yield" << std::endl;
        ceph_abort();
      });
    }, [&interrupted](std::exception_ptr) {
      std::cout << "6. interrupted handler" << std::endl;
      interrupted = true;
    }, false);
    fut.wait();
    ceph_assert(interrupted);

    interrupted = false;
    fut = interruptor::with_interruption([] {
      return interruptor::async([] {
        interruptible::interrupt_cond<
	  TestInterruptCondition>.interrupt_cond->set_interrupt();
        // 当前任务暂时让出 Reactor
        // 让其他任务有机会运行
        // 之后再回来继续
        interruptor::green_get(seastar::yield()); 
        // the execution should be interrupted, the run should
        // never reach here.
        ceph_abort();
      });
    }, [&interrupted](std::exception_ptr) {
      std::cout << "interrupted" << std::endl;
      interrupted = true;
    }, false);
    fut.wait();
    ceph_assert(interrupted);

    interrupted = false;
    fut = interruptor::with_interruption([] {
      return interruptor::async([] {
        interruptible::interrupt_cond<
	  TestInterruptCondition>.interrupt_cond->set_interrupt();
        interruptor::make_interruptible(seastar::yield()).get();
        // the execution should be interrupted, the run should
        // never reach here.
        ceph_abort();
      });
    }, [&interrupted](std::exception_ptr) {
      std::cout << "interrupted" << std::endl;
      interrupted = true;
    }, false);
    fut.wait();
    ceph_assert(interrupted);
  });
}

TEST_F(seastar_test_suite_t, DISABLED_nested_interruptors)
{
  run_async([] {
    base_ertr::future<> ret = with_intr(
      []() {
	return base_iertr::now().safe_then_interruptible([]() {
          return with_intr(
            []() {
              return base_iertr::now();
            }
          );
        });
      }
    );
    ret.unsafe_get();
  });
}

TEST_F(seastar_test_suite_t, interruptible_repeat_eagain)
{
  using interruptor =
    interruptible::interruptor<TestInterruptCondition>;
  run_async([] {
    interruptor::with_interruption([] {
      return seastar::do_with(
	0,
	[](auto &i) {
	return interruptor::repeat_eagain([&i]() -> base_iertr::future<> {
	  if (++i < 5) {
	    return crimson::ct_error::eagain::make();
	  }
	  return base_iertr::now();
	}).si_then([&i] {
	  std::cout << i << std::endl;
	  ceph_assert(i == 5);
	});
      });
    }, [](std::exception_ptr) {}, false).unsafe_get();
  });
}

TEST_F(seastar_test_suite_t, handle_error)
{
  run_async([] {
    base_ertr::future<> ret = with_intr(
      []() {
	return base2_iertr::make_ready_future<int>(
	  1
	).handle_error_interruptible(
	  base_iertr::pass_further{},
	  ct_error::assert_all("crash on eio")
	).si_then([](auto) {
	  return base_iertr::now();
	});
      });
    ret.unsafe_get();
  });
}

TEST_F(seastar_test_suite_t, interruption_to_eagain) 
{
  run_async([]{
    auto ret = 
      base_intr::with_interruption_to_error<ct_error::eagain
      >(
        [] {
          std::cout << "OPFUNC: should not run" << std::endl;
          return base_iertr::now();
        },
        TestInterruptCondition(true)
      );

      std::move(ret).handle_error(
        ct_error::enoent::handle(
          [](const auto&) {
            std::cout << "ERROR: unexpectded enoent" << std::endl;
            return seastar::now();
          }
        ),

      ct_error::eagain::handle(
        [](const auto&) {
          std::cout << "ERROR: handled egain" << std::endl;
          return seastar::now();
        }
      )
    ).get();
  });
}
