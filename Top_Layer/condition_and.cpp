#include "condition_and.h"
#include <cassert>

condition_and::condition_and(){
	left = right = NULL;
}

condition::condition_type condition_and::type(){
	return condition::AND;
}

void condition_and::destroy(){
	if (left) {left->destroy(); delete left;}
	if (right) {right->destroy(); delete right;}
}

bool condition_and::eval(record_type& rt, record& r){
	assert(left && right);
	return left->eval(rt, r) && right->eval(rt, r);
}