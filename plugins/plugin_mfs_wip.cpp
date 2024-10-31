#include <solanaceae/plugin/solana_plugin_v1.h>

#include <solanaceae/message3/message_serializer.hpp>
#include <solanaceae/object_store/backends/filesystem_storage.hpp>
#include <solanaceae/message_fragment_store/message_fragment_store.hpp>

#include <entt/entt.hpp>
#include <entt/fwd.hpp>

#include <memory>
#include <limits>
#include <iostream>

static std::unique_ptr<Backends::FilesystemStorage> g_fsb = nullptr;
static std::unique_ptr<MessageFragmentStore> g_mfs = nullptr;

constexpr const char* plugin_name = "MessageFragmentStore";

extern "C" {

SOLANA_PLUGIN_EXPORT const char* solana_plugin_get_name(void) {
	return plugin_name;
}

SOLANA_PLUGIN_EXPORT uint32_t solana_plugin_get_version(void) {
	return SOLANA_PLUGIN_VERSION;
}

SOLANA_PLUGIN_EXPORT uint32_t solana_plugin_start(struct SolanaAPI* solana_api) {
	std::cout << "PLUGIN " << plugin_name << " START()\n";

	if (solana_api == nullptr) {
		return 1;
	}

	try {
		auto* cr = PLUG_RESOLVE_INSTANCE_VERSIONED(Contact3Registry, "1");
		auto* rmm = PLUG_RESOLVE_INSTANCE(RegistryMessageModelI);
		auto* os = PLUG_RESOLVE_INSTANCE(ObjectStore2);
		auto* msnj = PLUG_RESOLVE_INSTANCE(MessageSerializerNJ);

		// static store, could be anywhere tho
		// construct with fetched dependencies
		g_fsb = std::make_unique<Backends::FilesystemStorage>(*os, "test2_message_store/"); // TODO: use config?
		g_mfs = std::make_unique<MessageFragmentStore>(*cr, *rmm, *os, *g_fsb, *msnj);

		// register types
		PLUG_PROVIDE_INSTANCE(MessageFragmentStore, plugin_name, g_mfs.get());
	} catch (const ResolveException& e) {
		std::cerr << "PLUGIN " << plugin_name << " " << e.what << "\n";
		return 2;
	}

	return 0;
}

SOLANA_PLUGIN_EXPORT void solana_plugin_stop(void) {
	std::cout << "PLUGIN " << plugin_name << " STOP()\n";

	g_mfs.reset();
	g_fsb.reset();
}

SOLANA_PLUGIN_EXPORT float solana_plugin_tick(float time_delta) {
	// HACK
	static bool scan_triggered {false};
	if (!scan_triggered) {
		g_fsb->scanAsync();
		scan_triggered = true;
	}

	return g_mfs->tick(time_delta);
}

} // extern C

