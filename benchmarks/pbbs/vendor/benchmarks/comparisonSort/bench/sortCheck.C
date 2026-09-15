// This code is part of the Problem Based Benchmark Suite (PBBS)
// Copyright (c) 2010 Guy Blelloch and the PBBS team
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

#include <iostream>
#include <algorithm>
#include <cstring>
#include "parlay/parallel.h"
#include "parlay/internal/quicksort.h"
#include "common/sequenceIO.h"
#include "common/parseCommandLine.h"
using namespace std;
using namespace benchIO;

template <typename T, typename LESS, typename Key>
void check_sort(sequence<T> in_vals,
		sequence<T> out_vals,
		LESS less, Key f) {
  size_t n = in_vals.size();
  auto sorted_in = parlay::stable_sort(in_vals, less);
  parlay::internal::quicksort(make_slice(in_vals), less);

  atomic<size_t> error = n;
  parlay::parallel_for (0, n, [&] (size_t i) {
    if (f(in_vals[i]) != f(out_vals[i])) 
	parlay::write_min(&error,i,std::less<size_t>());
  });
  
  if (error < n) {
    auto expected = parlay::to_chars(f(in_vals[error]));
    auto got = parlay::to_chars(f(out_vals[error]));
    cout << "comparison sort: check failed at location i=" << error
	 << " expected " << expected << " got " << got << endl;
    abort();
  }
}

int main(int argc, char* argv[]) {
  commandLine P(argc,argv,"<infile> <outfile>");
  pair<char*,char*> fnames = P.IOFileNames();
  char* infile = fnames.first;
  char* outfile = fnames.second;

  FileReader in_file{infile};
  elementType in_type = elementTypeFromHeader(in_file.readHeader());

  FileReader out_file{outfile};
  elementType out_type = elementTypeFromHeader(out_file.readHeader());

  if (in_type != out_type) {
    cout << "sortCheck: types don't match" << endl;
    return(1);
  }

  if (in_type == doubleT) {
    auto In = in_file.readSeq<double>();
    auto Out = out_file.readSeq<double>();
    if (In.size() != Out.size()) {
      cout << "sortCheck: lengths dont' match" << endl;
      return 2;
    }
    check_sort(In, Out, std::less<double>(), [&] (double x) {return x;});
  } else if (in_type == doublePairT) {
    using dpair = pair<double,double>;
    auto less = [] (dpair a, dpair b) {return a.first < b.first;};
    auto In = in_file.readSeq<dpair>();
    auto Out = out_file.readSeq<dpair>();
    if (In.size() != Out.size()) {
      cout << "sortCheck: lengths dont' match" << endl;
      return 2;
    }
    check_sort<dpair>(In, Out, less, [&] (dpair x) {return x.first;});
  } else if (in_type == stringT) {
    using str = sequence<char>;
    auto strless = [&] (str const &a, str const &b) {
      auto sa = a.begin();
      auto sb = b.begin();
      auto ea = sa + min(a.size(),b.size());
      while (sa < ea && *sa == *sb) {sa++; sb++;}
      return sa == ea ? (a.size() < b.size()) : *sa < *sb;
    };
    auto In = in_file.readSeq<str>();
    auto Out = out_file.readSeq<str>();
    if (In.size() != Out.size()) {
      cout << "sortCheck: lengths dont' match" << endl;
      return 2;
    }
    check_sort<str>(In, Out, strless, [&] (str x) {return x;});
  } else if (in_type == intType) {
    auto In = in_file.readSeq<int>();
    auto Out = out_file.readSeq<int>();
    if (In.size() != Out.size()) {
      cout << "sortCheck: lengths dont' match" << endl;
      return 2;
    }
    check_sort<int>(In, Out, std::less<int>(), [&] (int x) {return x;});
  } else {
    cout << "sortCheck: input files not of accepted type" << endl;
    return(1);
  }
}
