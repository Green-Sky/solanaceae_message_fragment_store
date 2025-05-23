#pragma once

#include <solanaceae/object_store/object_store.hpp>
#include <solanaceae/object_store/meta_components.hpp>

#include <solanaceae/message3/message_serializer.hpp>

#include <solanaceae/util/uuid_generator.hpp>

#include "./meta_messages_components.hpp"

#include <entt/container/dense_map.hpp>
#include <entt/container/dense_set.hpp>

#include <solanaceae/contact/fwd.hpp>
#include <solanaceae/message3/registry_message_model.hpp>

#include <deque>
#include <vector>
#include <cstdint>

namespace Message::Components {

	// unused, consumes too much memory (highly compressable)
	//using FUID = FragComp::ID;

	struct MFSObj {
		// message fragment's object
		Object o {entt::null};
	};

	// TODO: add adjacency range comp or inside curser

	// TODO: unused
	// mfs will only load a limited number of fragments per tick (1),
	// so this tag will be set if we loaded a fragment and
	// every tick we check all cursers for this tag and continue
	// and remove once no fragment could be loaded anymore
	// (internal)
	struct MFSCurserUnsatisfiedTag {};

} // Message::Components

// handles fragments for messages
// on new message: assign fuid
// on new and update: mark as fragment dirty
// on delete: mark as fragment dirty?
class MessageFragmentStore : public RegistryMessageModelEventI, public ObjectStoreEventI {
	public:
		static constexpr const char* version {"3"};

	protected:
		ContactStore4I& _cs;
		RegistryMessageModelI& _rmm;
		RegistryMessageModelI::SubscriptionReference _rmm_sr;
		ObjectStore2& _os;
		ObjectStore2::SubscriptionReference _os_sr;
		StorageBackendIMeta& _sbm;
		StorageBackendIAtomic& _sba;
		MessageSerializerNJ& _scnj;

		bool _fs_ignore_event {false};
		UUIDGenerator_128_128 _session_uuid_gen;

		void handleMessage(const Message3Handle& m);

		void loadFragment(Message3Registry& reg, ObjectHandle oh);

		bool syncFragToStorage(ObjectHandle oh, Message3Registry& reg);

		struct SaveQueueEntry final {
			uint64_t ts_since_dirty{0};
			//std::vector<uint8_t> id;
			ObjectHandle id;
			Message3Registry* reg{nullptr};
		};
		std::deque<SaveQueueEntry> _frag_save_queue;

		struct ECQueueEntry final {
			ObjectHandle fid;
			Contact4 c;
		};
		std::deque<ECQueueEntry> _event_check_queue;

		// range changed or fragment loaded.
		// we only load a limited number of fragments at once,
		// so we need to keep them dirty until nothing was loaded.
		entt::dense_set<Contact4> _potentially_dirty_contacts;

		// for cleaning up the ctx vars we create
		entt::dense_set<Contact4> _touched_contacts;

	public:
		MessageFragmentStore(
			ContactStore4I& cr,
			RegistryMessageModelI& rmm,
			ObjectStore2& os,
			StorageBackendIMeta& sbm,
			StorageBackendIAtomic& sba,
			MessageSerializerNJ& scnj
		);
		virtual ~MessageFragmentStore(void);

		float tick(float time_delta);

	protected: // rmm
		bool onEvent(const Message::Events::MessageConstruct& e) override;
		bool onEvent(const Message::Events::MessageUpdated& e) override;

	protected: // fs
		bool onEvent(const ObjectStore::Events::ObjectConstruct& e) override;
		bool onEvent(const ObjectStore::Events::ObjectUpdate& e) override;
};

