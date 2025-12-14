cmake_minimum_required(VERSION 3.10)
project(inf_var2)

set(CMAKE_CXX_STANDARD 17)

# Для MacOS
if(APPLE)
    include_directories(/opt/homebrew/opt/postgresql@14/include/postgresql@14)
    link_directories(/opt/homebrew/opt/postgresql@14/lib/postgresql@14)
endif()

add_executable(inf_var2 main.cpp)
target_link_libraries(inf_var2 pq)