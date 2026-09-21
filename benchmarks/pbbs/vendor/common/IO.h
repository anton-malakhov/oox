// This code is part of the Problem Based Benchmark Suite (PBBS)
// Copyright (c) 2011 Guy Blelloch and the PBBS team
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights (to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <iostream>
#include <fstream>
#include <ostream>
#include <string>
#include <string>
#include <cstring>
#include <type_traits>
#include "../parlay/primitives.h"
#include "../parlay/parallel.h"
#include "../parlay/io.h"
#include "../parlay/internal/get_time.h"
// #include "geometry.h"
// #include "graph.h"

namespace benchIO {
  using namespace std;
  using parlay::sequence;
  using parlay::tabulate;
  using parlay::make_slice;

  auto is_space = [] (char c) {
    switch (c)  {
    case '\r': 
    case '\t': 
    case '\n': 
    case 0:
    case ' ' : return true;
    default : return false;
    }
  };

  // parallel code for converting a string to word pointers
  // side effects string by setting to null after each word
  template <class Seq>
    parlay::sequence<char*> stringToWords(Seq &Str) {
    size_t n = Str.size();
    
    parlay::parallel_for(0, n, [&] (long i) {
	if (is_space(Str[i])) Str[i] = 0;}); 

    // mark start of words
    auto FL = parlay::tabulate(n, [&] (long i) -> bool {
	return (i==0) ? Str[0] : Str[i] && !Str[i-1];});
    
    // offset for each start of word
    auto Offsets = parlay::pack_index<long>(FL);

    // pointer to each start of word
    auto SA = parlay::tabulate(Offsets.size(), [&] (long j) -> char* {
	return Str.begin() + Offsets[j];});
    
    return SA;
  }

  //using this as a typename so we can replace with parlay::chars easily if desired
  using charstring = typename parlay::sequence<char>;

  inline int xToStringLen(charstring const &a) { return a.size();}
  inline void xToString(char* s, charstring const &a) {
    for (int i=0; i < a.size(); i++) s[i] = a[i];}

  inline int xToStringLen(long a) { return 21;}
  inline void xToString(char* s, long a) { sprintf(s,"%ld",a);}

  inline int xToStringLen(unsigned long a) { return 21;}
  inline void xToString(char* s, unsigned long a) { sprintf(s,"%lu",a);}

  inline uint xToStringLen(uint a) { return 12;}
  inline void xToString(char* s, uint a) { sprintf(s,"%u",a);}

  inline int xToStringLen(int a) { return 12;}
  inline void xToString(char* s, int a) { sprintf(s,"%d",a);}

  inline int xToStringLen(double a) { return 18;}
  inline void xToString(char* s, double a) { sprintf(s,"%.11le", a);}

  inline int xToStringLen(char* a) { return strlen(a)+1;}
  inline void xToString(char* s, char* a) { sprintf(s,"%s",a);}

  template <class A, class B>
  inline int xToStringLen(pair<A,B> a) { 
    return xToStringLen(a.first) + xToStringLen(a.second) + 1;
  }

  template <class A, class B>
  inline void xToString(char* s, pair<A,B> a) { 
    int l = xToStringLen(a.first);
    xToString(s, a.first);
    s[l] = ' ';
    xToString(s+l+1, a.second);
  }

  template <typename T>
  struct StreamWriter {
    std::ostream& operator()(std::ostream& os, T const& a) { return os << a; }
  };

  template <>
  struct StreamWriter<charstring> {
    std::ostream& operator()(std::ostream& os, charstring const& a) {
      return StreamWriter<std::string_view>{}(os, std::string_view(a.data(), a.size()));
    }
  };

  template <typename A, typename B>
  struct StreamWriter<std::pair<A, B>> {
    std::ostream& operator()(std::ostream& os, std::pair<A, B> const& a) {
      StreamWriter<A>{}(os, a.first);
      os << ' ';
      StreamWriter<B>{}(os, a.second);
      return os;
    }
  };

  template <typename T>
  std::ostream& writeToStream(std::ostream& os, T const& a) {
    return StreamWriter<T>{}(os, a);
  }

  template <class T>
  void writeSeqToStream(ofstream& os, parlay::sequence<T> const &A) {
    os << std::scientific << std::setprecision(11);
    for (const auto& elem : A) {
      writeToStream(os, elem) << '\n';
    }
  }

  template <class T>
  int writeSeqToFile(string header,
		     parlay::sequence<T> const &A,
		     char const *fileName) {
    auto a = A[0];
    //xToStringLena(a);
    ofstream file (fileName, ios::out | ios::binary);
    if (!file.is_open()) {
      std::cout << "Unable to open file: " << fileName << std::endl;
      return 1;
    }
    file << header << endl;
    writeSeqToStream(file, A);
    file.close();
    return 0;
  }

  template <class T1, class T2>
  int write2SeqToFile(string header,
		      parlay::sequence<T1> const &A,
		      parlay::sequence<T2> const &B,
		      char const *fileName) {
    ofstream file (fileName, ios::out | ios::binary);
    if (!file.is_open()) {
      std::cout << "Unable to open file: " << fileName << std::endl;
      return 1;
    }
    file << header << endl;
    writeSeqToStream(file, A);
    writeSeqToStream(file, B);
    file.close();
    return 0;
  }

  charstring readStringFromFile(char const *fileName) {
    ifstream file (fileName, ios::in | ios::binary | ios::ate);
    if (!file.is_open()) {
      std::cout << "Unable to open file: " << fileName << std::endl;
      abort();
    }
    long end = file.tellg();
    file.seekg (0, ios::beg);
    long n = end - file.tellg();
    charstring bytes(n, (char) 0);
    file.read (bytes.begin(), n);
    file.close();
    return bytes;
  }

  string intHeaderIO = "sequenceInt";

  template <class T>
  int writeIntSeqToFile(parlay::sequence<T> const &A, char const *fileName) {
    return writeSeqToFile(intHeaderIO, A, fileName);
  }

  inline sequence<sequence<char>> get_tokens(char const *fileName) {
    // parlay::internal::timer t("get_tokens");
    // auto S = parlay::chars_from_file(fileName);
    auto S = parlay::file_map(fileName);
    // t.next("file map");
    auto r =  parlay::tokens(S, benchIO::is_space);
    // t.next("tokens");
    return r;
  }

  inline sequence<std::string_view> get_tokens(char const* begin, char const* end) {
    sequence<std::string_view> out;

    for (char const *cur_start = begin, *cur_end = begin; cur_end < end; cur_start = cur_end + 1) {
      while (cur_end < end && !benchIO::is_space(*cur_end)) {
        ++cur_end;
      }

      out.push_back(std::string_view(cur_start, cur_end - cur_start));
    }

    return out;
  }

  template <typename T>
  struct To {};

  class FileReader {
  public:
    FileReader(char const* fileName)
      : strm{fileName, std::ios::in} {}

    std::string_view readHeader() {
      strm >> cur_token;
      return cur_token;
    }

    template <typename T>
    std::enable_if_t<std::is_integral_v<T>, bool> readTo(T& to) {
      if (!(strm >> cur_token)) {
        return false;
      }
      to = parlay::internal::chars_to_int_t<T>(make_slice(cur_token));
      return true;
    }

    template <typename T>
    std::enable_if_t<std::is_floating_point_v<T>, bool> readTo(T& to) {
      if (!(strm >> cur_token)) {
        return false;
      }
      to = static_cast<T>(parlay::chars_to_float_t<double>(make_slice(cur_token)));
      return true;
    }

    template <typename F, typename S>
    bool readTo(std::pair<F, S>& to) {
      return readTo(to.first) && readTo(to.second);
    }

    bool readTo(sequence<char>& to) {
      if (!(strm >> cur_token)) {
        return false;
      }
      to.clear();
      to.reserve(cur_token.size());
      for (char ch : cur_token) {
        to.push_back(ch);
      }
      return true;
    }

    template <typename T>
    sequence<T> readSeq() {
      sequence<T> out;
      T cur;
      while (readTo(cur)) {
        out.push_back(std::move(cur));
      }
      return out;
    }

  private:
    std::ifstream strm;
    std::string cur_token;
  };

  template <class T>
  parlay::sequence<T> readIntSeqFromFile(char const *fileName) {
    FileReader reader{fileName};

    std::string header{reader.readHeader()};
    if (header != intHeaderIO) {
      cout << "readIntSeqFromFile: bad input" << endl;
      abort();
    }

    return reader.readSeq<T>();
  }
}
