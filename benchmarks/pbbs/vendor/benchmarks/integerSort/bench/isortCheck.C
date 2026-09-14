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

#include <charconv>
#include <iostream>
#include <algorithm>
#include <cstring>
#include "parlay/parallel.h"
#include "parlay/primitives.h"
#include "common/sequenceIO.h"
#include "common/atomics.h"
#include "common/parse_command_line.h"
using namespace std;
using namespace benchIO;

template <class T, class LESS>
void checkSort(sequence<T> in_vals,
	       sequence<T> out_vals,
	       LESS less) {
  size_t n = in_vals.size();
  // std::stable_sort(in_vals.begin(), in_vals.end(), less);
  // auto sorted_in = in_vals;
  auto sorted_in = parlay::stable_sort(in_vals, less);
  size_t error = n;
  parlay::parallel_for (0, n, [&] (size_t i) {
      if (out_vals[i] != sorted_in[i])
	pbbs::write_min(&error,i,std::less<size_t>());
  });
  if (error < n) {
    auto expected = parlay::to_chars(sorted_in[error]);
    auto got = parlay::to_chars(out_vals[error]);
    cout << "integer sort: check failed at location i=" << error
	 << " expected " << expected << " got " << got << endl;
    abort();
  }
}

template <typename T>
bool assertSizes(char const* progName, sequence<T> const& lhs, sequence<T> const& rhs) {
  if (lhs.size() != rhs.size()) {
    cout << progName << ": in and out lengths don't match" << endl;
    return false;
  }
  return true;
}

int main(int argc, char* argv[]) {
  commandLine P(argc,argv,"<inFile> <outFile>");
  pair<char*,char*> fnames = P.IOFileNames();
  FileReader infile{fnames.first};
  FileReader outfile(fnames.second);

  elementType in_type = elementTypeFromHeader(infile.readHeader());
  elementType out_type = elementTypeFromHeader(outfile.readHeader());

  if (in_type != out_type) {
    cout << argv[0] << ": in and out types don't match" << endl;
    return(1);
  }

  auto less = std::less<uint>{};
  auto lessp = [&] (uintPair a, uintPair b) {return a.first < b.first;};
  
  switch (in_type) {
  case intType: {
    auto inValues = infile.readSeq<uint>();
    auto outValues = outfile.readSeq<uint>();
    if (!assertSizes(argv[0], inValues, outValues)) {
      return 1;
    }
    checkSort(inValues, outValues, less);
    break;
  }
  case intPairT: {
    auto inValues = infile.readSeq<uintPair>();
    auto outValues = outfile.readSeq<uintPair>();
    if (!assertSizes(argv[0], inValues, outValues)) {
      return 1;
    }
    checkSort(inValues, outValues, lessp);
    break;
  }
  default:
    cout << argv[0] << ": input files not of right type" << endl;
    return(1);
  }
}
