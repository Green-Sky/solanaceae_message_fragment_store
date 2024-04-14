cmake_minimum_required(VERSION 3.9...3.24 FATAL_ERROR)

project(solanaceae)

add_library(solanaceae_message_fragment_store
	./solanaceae/message_fragment_store/meta_messages_components.hpp
	./solanaceae/message_fragment_store/meta_messages_components_id.inl
	./solanaceae/message_fragment_store/internal_mfs_contexts.hpp
	./solanaceae/message_fragment_store/internal_mfs_contexts.cpp
	./solanaceae/message_fragment_store/message_fragment_store.hpp
	./solanaceae/message_fragment_store/message_fragment_store.cpp
)

target_include_directories(solanaceae_message_fragment_store PUBLIC .)
target_compile_features(solanaceae_message_fragment_store PUBLIC cxx_std_17)
target_link_libraries(solanaceae_message_fragment_store PUBLIC
	solanaceae_util
	solanaceae_message3
	solanaceae_message_serializer
	solanaceae_object_store
	nlohmann_json::nlohmann_json
)

########################################

add_executable(convert_message_object_store
	./convert_frag_to_obj.cpp
)

target_link_libraries(convert_message_object_store PUBLIC
	solanaceae_object_store
	solanaceae_object_store_backend_filesystem
	solanaceae_message_fragment_store
)

########################################

