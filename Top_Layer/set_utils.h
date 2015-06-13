#ifndef H_SET_UTILS
#define H_SET_UTILS

#include <set>
#include <string>
using namespace std;

//template<class T>
inline bool set_contains(set<string>& large, set<string>& small){
	for (set<string>::iterator i = small.begin(); i != small.end(); i++){
		if (large.find(*i) == large.end()) return false;
	}
	return true;
}

inline void merge_sets(set<string>& dest, set<string>& src){
	for (set<string>::iterator i = src.begin(); i != src.end(); i++){
		dest.insert(*i);
	}
}

#endif