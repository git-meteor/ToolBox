#ifndef _MACROS_H_
#define _MACROS_H_

#include <sstream>
#include <iostream>


// print one line, support stream operator
#define PRINTLN(x) do{std::ostringstream __os__; __os__ << x << std::endl; std::cout << __os__.str();}while(0)

// print executed statement
#define PRINT_EXEC(...) do{PRINTLN(#__VA_ARGS__); __VA_ARGS__;}while(0)

#endif
