cmake_minimum_required(VERSION 3.16)
project(detect_norm_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ── Release by default ──
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG")

include(FetchContent)

# ── nlohmann/json ──
find_package(nlohmann_json QUIET)
if(NOT nlohmann_json_FOUND)
  FetchContent_Declare(nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
  )
  FetchContent_MakeAvailable(nlohmann_json)
endif()

# ── cpp-httplib ──
FetchContent_Declare(httplib
  GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
  GIT_TAG v0.15.3
)
FetchContent_MakeAvailable(httplib)

add_library(cpp_httplib INTERFACE)
target_include_directories(cpp_httplib INTERFACE ${httplib_SOURCE_DIR})

find_package(Threads REQUIRED)

# ── ICU for Unicode ──
find_package(PkgConfig REQUIRED)
pkg_check_modules(ICU REQUIRED icu-uc icu-io)

# ── libzip for DOCX/PPTX/ODT ──
pkg_check_modules(LIBZIP REQUIRED libzip)

# ── poppler-cpp for PDF text extraction ──
pkg_check_modules(POPPLER REQUIRED poppler-cpp)

# ── pugixml for XML ──
FetchContent_Declare(pugixml
  GIT_REPOSITORY https://github.com/zeux/pugixml.git
  GIT_TAG v1.14
)
FetchContent_MakeAvailable(pugixml)

# ═══════════ Main binary ═══════════
add_executable(detect_norm_cpp
  src/main.cpp
  src/models.cpp
  src/detectors/symbol_substitution.cpp
  src/detectors/spacing_detector.cpp
  src/detectors/hidden_text_detector.cpp
  src/parsers/txt_parser.cpp
  src/parsers/rtf_parser.cpp
  src/parsers/doc_parser.cpp
  src/parsers/docx_parser.cpp
  src/parsers/pptx_parser.cpp
  src/parsers/odt_parser.cpp
  src/parsers/pdf_parser.cpp
  src/parsers/zip_xml_util.cpp
)

target_include_directories(detect_norm_cpp PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${ICU_INCLUDE_DIRS}
  ${LIBZIP_INCLUDE_DIRS}
  ${POPPLER_INCLUDE_DIRS}
)

target_link_libraries(detect_norm_cpp PRIVATE
  cpp_httplib
  nlohmann_json::nlohmann_json
  pugixml
  Threads::Threads
  ${ICU_LIBRARIES}
  ${LIBZIP_LIBRARIES}
  ${POPPLER_LIBRARIES}
)

target_compile_options(detect_norm_cpp PRIVATE ${ICU_CFLAGS_OTHER} ${LIBZIP_CFLAGS_OTHER} ${POPPLER_CFLAGS_OTHER})
