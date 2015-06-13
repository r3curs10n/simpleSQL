#include "condition_or.h"
#include <cassert>

condition_or::condition_or(){
	left = right = NULL;
}

condition::condition_type condition_or::type(){
	return condition::OR;
}

void condition_or::destroy(){
	if (left) {left->destroy(); delete left;}
	if (right) {right->destroy(); delete right;}
}

bool condition_or::eval(record_type& rt, record& r){
	assert(left && right);
	return (left->eval(rt, r) || right->eval(rt, r));
}