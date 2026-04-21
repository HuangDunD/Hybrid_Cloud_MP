set(SUPPORT_SOURCES friend_simulate.cc)

add_library(smallbank_aff_support STATIC ${SUPPORT_SOURCES})

set_target_properties(smallbank_aff_support PROPERTIES LINKER_LANGUAGE CXX)
