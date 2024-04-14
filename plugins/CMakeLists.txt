cmake_minimum_required(VERSION 3.9...3.24 FATAL_ERROR)

########################################

add_library(plugin_mfs_wip SHARED
	./plugin_mfs_wip.cpp
)
target_compile_features(plugin_mfs_wip PUBLIC cxx_std_17)
target_link_libraries(plugin_mfs_wip PUBLIC
	solanaceae_plugin
	solanaceae_message_fragment_store
	solanaceae_object_store_backend_filesystem
)

