cmake_minimum_required(VERSION 3.15)
project(PROJECT2_LINEAR_PBFT)

# Enable full debugging and keep function frames (helps addr2line/gdb)
add_compile_options(-g -O0 -fno-omit-frame-pointer)

# 1. Require C++17 (gRPC and Abseil need it)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# 2. Find Protobuf and gRPC (must be installed on your system)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(nlohmann_json 3.2.0 REQUIRED)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_path(BLS_INCLUDE_DIR bls/bls.hpp
          PATHS /usr/include /usr/local/include
          )
find_library(BLS_LIBRARY bls384_256 PATHS /usr/lib /usr/local/lib)
find_library(MCL_LIBRARY mcl PATHS /usr/lib /usr/local/lib)

if(NOT BLS_INCLUDE_DIR)
  message(FATAL_ERROR "Could not find bls/bls.hpp. Set BLS_INCLUDE_DIR.")
endif()
if(NOT BLS_LIBRARY)
  message(FATAL_ERROR "Could not find libbls384_256. Set BLS_LIBRARY.")
endif()
if(NOT MCL_LIBRARY)
  message(FATAL_ERROR "Could not find libmcl. Set MCL_LIBRARY.")
endif()

# 3. Proto file
set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/messages.proto")

# Generated sources will be placed in the build dir
set(GEN_SRC_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

file(MAKE_DIRECTORY ${GEN_SRC_DIR})
include_directories(
    ${BLS_INCLUDE_DIR}
    ${BLS_INCLUDE_DIR}/bls
    ${NLOHMANN_JSON_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}        # for local headers like bls_helper.h
)
link_directories(/usr/local/lib /usr/lib)
# Generate protobuf sources
add_custom_command(
  OUTPUT "${GEN_SRC_DIR}/messages.pb.cc" "${GEN_SRC_DIR}/messages.pb.h"
  COMMAND protobuf::protoc
  ARGS --cpp_out=${GEN_SRC_DIR}
       -I ${CMAKE_CURRENT_SOURCE_DIR}
       ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
)

# Generate gRPC sources
add_custom_command(
  OUTPUT "${GEN_SRC_DIR}/messages.grpc.pb.cc" "${GEN_SRC_DIR}/messages.grpc.pb.h"
  COMMAND protobuf::protoc
  ARGS --grpc_out=${GEN_SRC_DIR}
       --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
       -I ${CMAKE_CURRENT_SOURCE_DIR}
       ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
)

# Collect all generated files
set(GEN_SRCS
  "${GEN_SRC_DIR}/messages.pb.cc"
  "${GEN_SRC_DIR}/messages.grpc.pb.cc"
)
set(GEN_HDRS
  "${GEN_SRC_DIR}/messages.pb.h"
  "${GEN_SRC_DIR}/messages.grpc.pb.h"
)

add_library(proto_lib ${GEN_SRCS})

target_include_directories(proto_lib PUBLIC ${GEN_SRC_DIR})
target_link_libraries(proto_lib PUBLIC gRPC::grpc++ protobuf::libprotobuf)

# Add executable
add_executable(client client.cpp)
target_include_directories(client PUBLIC ${GEN_SRC_DIR} ${CMAKE_CURRENT_SOURCE_DIR})
target_link_directories(client PRIVATE /home/sushant/blst)
target_link_libraries(client PRIVATE nlohmann_json::nlohmann_json Threads::Threads proto_lib  gRPC::grpc++ protobuf::libprotobuf ${BLS_LIBRARY} ${MCL_LIBRARY} OpenSSL::Crypto)

add_executable(node node.cpp)
target_include_directories(node PUBLIC ${GEN_SRC_DIR} ${CMAKE_CURRENT_SOURCE_DIR})
target_link_directories(node PRIVATE /home/sushant/blst)
target_link_libraries(node PRIVATE nlohmann_json::nlohmann_json Threads::Threads proto_lib  gRPC::grpc++ protobuf::libprotobuf ${BLS_LIBRARY} ${MCL_LIBRARY} OpenSSL::Crypto)

add_executable(orchestrator orchestrator.cpp)
target_link_libraries(orchestrator PRIVATE nlohmann_json::nlohmann_json Threads::Threads proto_lib)

add_custom_target(run_node1
    COMMAND gnome-terminal --title="Node 1" -- bash -c "${CMAKE_BINARY_DIR}/node --id n1 --port 5001 --stateFile ${CMAKE_SOURCE_DIR}/node_files/n1_state.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS node
)

add_custom_target(run_node2
    COMMAND gnome-terminal --title="Node 2" -- bash -c "${CMAKE_BINARY_DIR}/node --id n2 --port 5002 --stateFile ${CMAKE_SOURCE_DIR}/node_files/n2_state.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS node
)

add_custom_target(run_node3
    COMMAND gnome-terminal --title="Node 3" -- bash -c "${CMAKE_BINARY_DIR}/node --id n3 --port 5003 --stateFile ${CMAKE_SOURCE_DIR}/node_files/n3_state.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS node
)

add_custom_target(run_node4
    COMMAND gnome-terminal --title="Node 4" -- bash -c "${CMAKE_BINARY_DIR}/node --id n4 --port 5004 --stateFile ${CMAKE_SOURCE_DIR}/node_files/n4_state.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS node
)

add_custom_target(run_node5
    COMMAND gnome-terminal --title="Node 5" -- bash -c "${CMAKE_BINARY_DIR}/node --id n5 --port 5005 --stateFile ${CMAKE_SOURCE_DIR}/node_files/n5_state.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS node
)

add_custom_target(run_node6
    COMMAND gnome-terminal --title="Node 6" -- bash -c "${CMAKE_BINARY_DIR}/node --id n6 --port 5006 --stateFile ${CMAKE_SOURCE_DIR}/node_files/n6_state.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS node
)

add_custom_target(run_node7
    COMMAND gnome-terminal --title="Node 7" -- bash -c "${CMAKE_BINARY_DIR}/node --id n7 --port 5007 --stateFile ${CMAKE_SOURCE_DIR}/node_files/n7_state.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS node
)

# Custom targets to run orchestrator

add_custom_target(run_orchestrator
    COMMAND gnome-terminal --title="Orchestrator" -- bash -c "${CMAKE_BINARY_DIR}/orchestrator --id orchestrator --port 5018 --resources ${CMAKE_SOURCE_DIR} --inputCSV ${CMAKE_SOURCE_DIR}/input.csv"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS orchestrator node client
)

# Custom targets to run individual clients

add_custom_target(run_client_A
    COMMAND gnome-terminal --title="Client A" -- bash -c "${CMAKE_BINARY_DIR}/client --id A --port 5008 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_client_B
    COMMAND gnome-terminal --title="Client B" -- bash -c "${CMAKE_BINARY_DIR}/client --id B --port 5009 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_client_C
    COMMAND gnome-terminal --title="Client C" -- bash -c "${CMAKE_BINARY_DIR}/client --id C --port 5010 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_client_D
    COMMAND gnome-terminal --title="Client D" -- bash -c "${CMAKE_BINARY_DIR}/client --id D --port 5011 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_client_E
    COMMAND gnome-terminal --title="Client E" -- bash -c "${CMAKE_BINARY_DIR}/client --id E --port 5012 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_client_F
    COMMAND gnome-terminal --title="Client F" -- bash -c "${CMAKE_BINARY_DIR}/client --id F --port 5013 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_client_G
    COMMAND gnome-terminal --title="Client G" -- bash -c "${CMAKE_BINARY_DIR}/client --id G --port 5014 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_client_H
    COMMAND gnome-terminal --title="Client H" -- bash -c "${CMAKE_BINARY_DIR}/client --id H --port 5015 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_client_I
    COMMAND gnome-terminal --title="Client I" -- bash -c "${CMAKE_BINARY_DIR}/client --id I --port 5016 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_client_J
    COMMAND gnome-terminal --title="Client J" -- bash -c "${CMAKE_BINARY_DIR}/client --id J --port 5017 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

# Custom targets to run individual nodes
add_custom_target(run_inplace_node1
    COMMAND node --id n1 --port 5001 --stateFile ${CMAKE_SOURCE_DIR}/node_files/n1_state.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS node
)

add_custom_target(run_inplace_node2
    COMMAND node --id n2 --port 5002 --stateFile ${CMAKE_SOURCE_DIR}/node_files/n2_state.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS node
)

add_custom_target(run_inplace_node3
    COMMAND node --id n3 --port 5003 --stateFile ${CMAKE_SOURCE_DIR}/node_files/n3_state.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS node
)

add_custom_target(run_inplace_node4
    COMMAND node --id n4 --port 5004 --stateFile ${CMAKE_SOURCE_DIR}/node_files/n4_state.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS node
)

add_custom_target(run_inplace_node5
    COMMAND node --id n5 --port 5005 --stateFile ${CMAKE_SOURCE_DIR}/node_files/n5_state.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS node
)

add_custom_target(run_inplace_node6
    COMMAND node --id n6 --port 5006 --stateFile ${CMAKE_SOURCE_DIR}/node_files/n6_state.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS node
)

add_custom_target(run_inplace_node7
    COMMAND node --id n7 --port 5007 --stateFile ${CMAKE_SOURCE_DIR}/node_files/n7_state.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS node
)

# Custom targets to run_inplace orchestrator

add_custom_target(run_inplace_orchestrator
    COMMAND orchestrator --id orchestrator --port 5018 --resources ${CMAKE_SOURCE_DIR}/resources.json --inputCSV ${CMAKE_SOURCE_DIR}/input.csv
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS orchestrator
)

# Custom targets to run_inplace individual clients

add_custom_target(run_inplace_client_A
    COMMAND client --id A --port 5008 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_inplace_client_B
    COMMAND client --id B --port 5009 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_inplace_client_C
    COMMAND client --id C --port 5010 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_inplace_client_D
    COMMAND client --id D --port 5011 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_inplace_client_E
    COMMAND client --id E --port 5012 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_inplace_client_F
    COMMAND client --id F --port 5013 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_inplace_client_G
    COMMAND client --id G --port 5014 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_inplace_client_H
    COMMAND client --id H --port 5015 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_inplace_client_I
    COMMAND client --id I --port 5016 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)

add_custom_target(run_inplace_client_J
    COMMAND client --id J --port 5017 --configFile ${CMAKE_SOURCE_DIR}/client_files/client_config.json
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS client
)


# Master target to run all nodes in parallel
add_custom_target(run_all
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_node1 &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_node2 &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_node3 &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_node4 &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_node5 &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_node6 &
    # COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_node7 &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_client_A &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_client_B &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_client_C &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_client_D &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_client_E &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_client_F &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_client_G &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_client_H &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_client_I &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_client_J &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_orchestrator
    DEPENDS node client orchestrator
)

# Master target to run all nodes in parallel
add_custom_target(run_inplace_all
    # COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_node1 &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_node2 &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_node3 &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_node4 &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_node5 &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_node6 &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_node7 &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_client_A &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_client_B &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_client_C &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_client_D &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_client_E &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_client_F &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_client_G &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_client_H &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_client_I &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_client_J &
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target run_inplace_orchestrator
    DEPENDS node client orchestrator
)
