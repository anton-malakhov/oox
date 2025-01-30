#include <exec/any_sender_of.hpp>

namespace ex = stdexec;

template <class... Ts>
using any_sender_of_impl = typename exec::any_receiver_ref<
    ex::completion_signatures<Ts...>
>::template any_sender<>;

template <class T>
using any_sender_of = any_sender_of_impl<
    ex::set_value_t(T),
    ex::set_stopped_t(),
    ex::set_error_t(std::exception_ptr)
>;