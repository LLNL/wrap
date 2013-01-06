# Copyright (c) 2008, Lawrence Livermore National Security, LLC. 
# Written by Martin Schulz, schulzm@llnl.gov, LLNL-CODE-402774,
# All rights reserved - please read information in "LICENCSE"

cmake_minimum_required(VERSION 2.8)

# @authors Todd Gamblin, Tobias Hilbrich, Martin Schulz
# @date 2011-05-01

# Install wrap.py into the project binary directory
install(PROGRAMS wrap.py DESTINATION bin)
install(PROGRAMS makefortran.py DESTINATION bin)
# Install WrapConfig into the project CMake config directory so that
# wrap.py can be used by subprojects.
set(WRAP ${CMAKE_INSTALL_PREFIX}/bin/wrap.py)
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/wrap-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/wrap-config.cmake @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/wrap-config.cmake DESTINATION
share/cmake/wrap)
