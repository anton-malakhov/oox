#pragma once

#include <atomic>
#include <cstdint>

template <typename T> struct IntrusivePtr {

  template <typename Y> friend struct IntrusivePtr;

  using element_type = T;

  IntrusivePtr() noexcept : Ptr_(nullptr) {}
  explicit IntrusivePtr(T *p) : Ptr_(p) {
    if (p) {
      IntrusivePtrAddRef(p);
    }
  }

  IntrusivePtr(IntrusivePtr const &r) : Ptr_(r.get()) {
    if (Ptr_) {
      IntrusivePtrAddRef(Ptr_);
    }
  }

  IntrusivePtr(IntrusivePtr &&r) noexcept : Ptr_(r.get()) { r.Ptr_ = nullptr; }

  ~IntrusivePtr() {
    if (Ptr_) {
      IntrusivePtrRelease(Ptr_);
    }
  }

  IntrusivePtr &operator=(IntrusivePtr const &r) {
    if (*this != r) {
      IntrusivePtr(r).swap(*this);
    }
    return *this;
  }

  IntrusivePtr &operator=(T *r) {
    if (Ptr_ != r) {
      IntrusivePtr(r).swap(*this);
    }
    return *this;
  }

  IntrusivePtr &operator=(IntrusivePtr &&r) noexcept {
    if (this != &r) {
      IntrusivePtr(std::move(r)).swap(*this);
    }
    return *this;
  }

  void Reset() { IntrusivePtr().swap(*this); }

  T &operator*() const noexcept { return *get(); }

  T *operator->() const noexcept { return get(); }

  T *get() const noexcept { return Ptr_; }

  explicit operator bool() const noexcept { return Ptr_ != nullptr; }

  void swap(IntrusivePtr &b) noexcept { std::swap(Ptr_, b.Ptr_); }

private:
  T *Ptr_;
};

template <class T, class U>
bool operator==(IntrusivePtr<T> const &a, IntrusivePtr<U> const &b) noexcept {
  return a.get() == b.get();
}

template <class T, class U>
bool operator!=(IntrusivePtr<T> const &a, IntrusivePtr<U> const &b) noexcept {
  return !(a == b);
}

template <class T, class U>
bool operator==(IntrusivePtr<T> const &a, U *b) noexcept {
  return a.get() == b;
}

template <class T, class U>
bool operator!=(IntrusivePtr<T> const &a, U *b) noexcept {
  return !(a == b);
}

template <class T, class U>
bool operator==(T *a, IntrusivePtr<U> const &b) noexcept {
  return b == a;
}

template <class T, class U>
bool operator!=(T *a, IntrusivePtr<U> const &b) noexcept {
  return b != a;
}

template <class T>
bool operator<(IntrusivePtr<T> const &a, IntrusivePtr<T> const &b) noexcept {
  return std::less<T *>(a.get(), b.get());
}

template <class T> void swap(IntrusivePtr<T> &a, IntrusivePtr<T> &b) noexcept {
  a.swap(b);
}

template <typename T> struct intrusive_ref_counter {

  template <typename D>
  friend std::size_t IntrusivePtrLoadRef(const intrusive_ref_counter<D> *p) noexcept;

  template <typename D>
  friend void IntrusivePtrAddRef(const intrusive_ref_counter<D> *p) noexcept;

  template <typename D>
  friend void IntrusivePtrRelease(const intrusive_ref_counter<D> *p) noexcept;

  intrusive_ref_counter() noexcept : m_cnt(0) {}

  intrusive_ref_counter(const intrusive_ref_counter &) noexcept : m_cnt(0) {}

  intrusive_ref_counter &operator=(const intrusive_ref_counter &) noexcept {
    return *this;
  }

  unsigned int use_count() const noexcept { return m_cnt; }

protected:
  ~intrusive_ref_counter() = default;

private:
  mutable std::atomic<std::size_t> m_cnt{0};
};

template <class Derived>
std::size_t IntrusivePtrLoadRef(const intrusive_ref_counter<Derived> *p) noexcept {
  return p->m_cnt.load(std::memory_order_acquire);
}

template <class Derived>
void IntrusivePtrAddRef(const intrusive_ref_counter<Derived> *p) noexcept {
  p->m_cnt.fetch_add(1, std::memory_order_release);
}

template <class Derived>
void IntrusivePtrRelease(const intrusive_ref_counter<Derived> *p) noexcept {
  if (p->m_cnt.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    delete static_cast<const Derived *>(p);
  }
}
