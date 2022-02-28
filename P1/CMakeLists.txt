cmake_minimum_required(VERSION 3.13.4)

project(DBI-P1)

file(GLOB_RECURSE SRC_FILE src/*.cc)

add_executable(${PROJECT_NAME} ${SRC_FILE})

