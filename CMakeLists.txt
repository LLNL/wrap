# Copyright (c) 2008, Lawrence Livermore National Security, LLC. 
# Written by Martin Schulz, schulzm@llnl.gov, LLNL-CODE-402774,
# All rights reserved - please read information in "LICENCSE"

# TODO Martin please add the copyright statment of your choice, as well as 
#      a reference to the license or license file!
#
# @authors Todd Gamblin, Tobias Hilbrich, Martin Schulz
# @date 2011-05-01

# Install wrap.py into the project binary directory
install(PROGRAMS wrap.py DESTINATION bin)

# Install WrapConfig into the project CMake config directory so that
# wrap.py can be used by subprojects.
set(WRAP ${CMAKE_INSTALL_PREFIX}/bin/wrap.py)
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/WrapConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/WrapConfig.cmake @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/WrapConfig.cmake DESTINATION share/cmake/PnMPI)
