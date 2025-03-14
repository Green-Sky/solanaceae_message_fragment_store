#include "./message_fragment_store.hpp"

#include "./internal_mfs_contexts.hpp"
#include "solanaceae/object_store/meta_components.hpp"
#include "solanaceae/object_store/object_store.hpp"

#include <solanaceae/object_store/serializer_json.hpp>

#include <solanaceae/util/utils.hpp>
#include <solanaceae/util/time.hpp>

#include <solanaceae/contact/contact_store_i.hpp>

#include <solanaceae/contact/components.hpp>
#include <solanaceae/message3/components.hpp>
#include <solanaceae/message3/contact_components.hpp>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <string>
#include <cstdint>
#include <cassert>
#include <iostream>

// https://youtu.be/CU2exyhYPfA

// everything assumes a single object registry (and unique objects)

namespace ObjectStore::Components {
	NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(MessagesVersion, v)
	NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(MessagesTSRange, begin, end)
	NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(MessagesContact, id)

	namespace Ephemeral {
		// does not contain any messges
		// (recheck on frag update)
		struct MessagesEmptyTag {};

		// cache the contact for faster lookups
		struct MessagesContactEntity {
			Contact4 e {entt::null};
		};
	}
} // ObjectStore::Component

static nlohmann::json loadFromStorageNJ(ObjectHandle oh) {
	assert(oh.all_of<ObjComp::Ephemeral::BackendAtomic>());
	auto* backend = oh.get<ObjComp::Ephemeral::BackendAtomic>().ptr;
	assert(backend != nullptr);

	std::vector<uint8_t> tmp_buffer;
	std::function<StorageBackendIAtomic::read_from_storage_put_data_cb> cb = [&tmp_buffer](const ByteSpan buffer) {
		tmp_buffer.insert(tmp_buffer.end(), buffer.cbegin(), buffer.cend());
	};
	if (!backend->read(oh, cb)) {
		std::cerr << "failed to read obj '" << bin2hex(oh.get<ObjComp::ID>().v) << "'\n";
		return false;
	}

	const auto obj_version = oh.get<ObjComp::MessagesVersion>().v;

	if (obj_version == 1) {
		return nlohmann::json::parse(tmp_buffer, nullptr, false);
	} else if (obj_version == 2) {
		return nlohmann::json::from_msgpack(tmp_buffer, true, false);
	} else {
		assert(false);
		return {};
	}
}

void MessageFragmentStore::handleMessage(const Message3Handle& m) {
	if (_fs_ignore_event) {
		// message event because of us loading a fragment, ignore
		// TODO: this barely makes a difference
		return;
	}

	if (!static_cast<bool>(m)) {
		return; // huh?
	}

	if (!m.all_of<Message::Components::Timestamp>()) {
		return; // we only handle msg with ts
	}

	_potentially_dirty_contacts.emplace(m.registry()->ctx().get<Contact4>()); // always mark dirty here
	_touched_contacts.emplace(m.registry()->ctx().get<Contact4>());
	if (m.any_of<Message::Components::ViewCurserBegin, Message::Components::ViewCurserEnd>()) {
		// not an actual message, but we probalby need to check and see if we need to load fragments
		//std::cout << "MFS: new or updated curser\n";
		return;
	}

	// TODO: this is bad, we need a non persistence tag instead
	//if (!m.any_of<Message::Components::MessageText, Message::Components::MessageFileObject>()) {
	if (!m.any_of<Message::Components::MessageText>()) { // fix file message object storage first!
		// skip everything else for now
		return;
	}
	// TODO: check if file object has id


	// TODO: use fid, saving full fuid for every message consumes alot of memory (and heap frag)
	if (!m.all_of<Message::Components::MFSObj>()) {
		std::cout << "MFS: new msg missing Object\n";
		if (!m.registry()->ctx().contains<Message::Contexts::OpenFragments>()) {
			m.registry()->ctx().emplace<Message::Contexts::OpenFragments>();
		}

		auto& fid_open = m.registry()->ctx().get<Message::Contexts::OpenFragments>().open_frags;

		const auto msg_ts = m.get<Message::Components::Timestamp>().ts;
		// missing fuid
		// find closesed non-sealed off fragment

		Object fragment_id{entt::null};

		// first search for fragment where the ts falls into the range
		for (const auto& fid : fid_open) {
			auto fh = _os.objectHandle(fid);
			assert(static_cast<bool>(fh));

			// assuming ts range exists
			auto& fts_comp = fh.get<ObjComp::MessagesTSRange>();

			if (fts_comp.begin <= msg_ts && fts_comp.end >= msg_ts) {
				fragment_id = fid;
				// TODO: check conditions for open here
				// TODO: mark msg (and frag?) dirty
			}
		}

		// if it did not fit into an existing fragment, we next look for fragments that could be extended
		if (!_os._reg.valid(fragment_id)) {
			for (const auto& fid : fid_open) {
				auto fh = _os.objectHandle(fid);
				assert(static_cast<bool>(fh));

				// assuming ts range exists
				auto& fts_comp = fh.get<ObjComp::MessagesTSRange>();

				const int64_t frag_range = int64_t(fts_comp.end) - int64_t(fts_comp.begin);
				constexpr static int64_t max_frag_ts_extent {1000*60*60};
				//constexpr static int64_t max_frag_ts_extent {1000*60*3}; // 3min for testing
				const int64_t possible_extention = max_frag_ts_extent - frag_range;

				// which direction
				if ((fts_comp.begin - possible_extention) <= msg_ts && fts_comp.begin > msg_ts) {
					fragment_id = fid;

					std::cout << "MFS: extended begin from " << fts_comp.begin << " to " << msg_ts << "\n";

					// assuming ts range exists
					fts_comp.begin = msg_ts; // extend into the past

					if (m.registry()->ctx().contains<Message::Contexts::ContactFragments>()) {
						// should be the case
						m.registry()->ctx().get<Message::Contexts::ContactFragments>().erase(fh);
						m.registry()->ctx().get<Message::Contexts::ContactFragments>().insert(fh);
					}


					// TODO: check conditions for open here
					// TODO: mark msg (and frag?) dirty
				} else if ((fts_comp.end + possible_extention) >= msg_ts && fts_comp.end < msg_ts) {
					fragment_id = fid;

					std::cout << "MFS: extended end from " << fts_comp.end << " to " << msg_ts << "\n";

					// assuming ts range exists
					fts_comp.end = msg_ts; // extend into the future

					if (m.registry()->ctx().contains<Message::Contexts::ContactFragments>()) {
						// should be the case
						m.registry()->ctx().get<Message::Contexts::ContactFragments>().erase(fh);
						m.registry()->ctx().get<Message::Contexts::ContactFragments>().insert(fh);
					}

					// TODO: check conditions for open here
					// TODO: mark msg (and frag?) dirty
				}
			}
		}

		// if its still not found, we need a new fragment
		if (!_os.registry().valid(fragment_id)) {
			const auto new_uuid = _session_uuid_gen();
			_fs_ignore_event = true;
			auto fh = _sbm.newObject(ByteSpan{new_uuid});
			// TODO: the backend should have done that?
			fh.emplace_or_replace<ObjComp::Ephemeral::BackendAtomic>(&_sba);
			_fs_ignore_event = false;
			if (!static_cast<bool>(fh)) {
				std::cout << "MFS error: failed to create new object for message\n";
				return;
			}

			fragment_id = fh;

			fh.emplace_or_replace<ObjComp::Ephemeral::MetaCompressionType>().comp = Compression::ZSTD;
			fh.emplace_or_replace<ObjComp::DataCompressionType>().comp = Compression::ZSTD;
			fh.emplace_or_replace<ObjComp::MessagesVersion>(); // default is current

			auto& new_ts_range = fh.emplace_or_replace<ObjComp::MessagesTSRange>();
			new_ts_range.begin = msg_ts;
			new_ts_range.end = msg_ts;

			{
				const auto msg_reg_contact = m.registry()->ctx().get<Contact4>();
				if (_cs.registry().all_of<Contact::Components::ID>(msg_reg_contact)) {
					fh.emplace<ObjComp::MessagesContact>(_cs.registry().get<Contact::Components::ID>(msg_reg_contact).data);
				} else {
					// ? rage quit?
				}
			}

			// contact frag
			if (!m.registry()->ctx().contains<Message::Contexts::ContactFragments>()) {
				m.registry()->ctx().emplace<Message::Contexts::ContactFragments>();
			}
			m.registry()->ctx().get<Message::Contexts::ContactFragments>().insert(fh);

			// loaded contact frag
			if (!m.registry()->ctx().contains<Message::Contexts::LoadedContactFragments>()) {
				m.registry()->ctx().emplace<Message::Contexts::LoadedContactFragments>();
			}
			m.registry()->ctx().get<Message::Contexts::LoadedContactFragments>().loaded_frags.emplace(fh);

			fid_open.emplace(fragment_id);

			std::cout << "MFS: created new fragment " << bin2hex(fh.get<ObjComp::ID>().v) << "\n";

			_fs_ignore_event = true;
			_os.throwEventConstruct(fh);
			_fs_ignore_event = false;
		}

		// if this is still empty, something is very wrong and we exit here
		if (!_os.registry().valid(fragment_id)) {
			std::cout << "MFS error: failed to find/create fragment for message\n";
			return;
		}

		m.emplace_or_replace<Message::Components::MFSObj>(fragment_id);

		// in this case we know the fragment needs an update
		// TODO: refactor extract
		for (const auto& it : _frag_save_queue) {
			if (it.id == fragment_id) {
				// already in queue
				return; // done
			}
		}
		_frag_save_queue.push_back({getTimeMS(), {_os.registry(), fragment_id}, m.registry()});
		return; // done
	}

	const auto msg_fh = _os.objectHandle(m.get<Message::Components::MFSObj>().o);
	if (!static_cast<bool>(msg_fh)) {
		std::cerr << "MFS error: fid in message is invalid\n";
		return; // TODO: properly handle this case
	}

	if (!m.registry()->ctx().contains<Message::Contexts::OpenFragments>()) {
		m.registry()->ctx().emplace<Message::Contexts::OpenFragments>();
	}

	auto& fid_open = m.registry()->ctx().get<Message::Contexts::OpenFragments>().open_frags;

	if (fid_open.contains(msg_fh)) {
		// TODO: cooldown per fragsave
		// TODO: refactor extract
		for (const auto& it : _frag_save_queue) {
			if (it.id == msg_fh) {
				// already in queue
				return; // done
			}
		}
		_frag_save_queue.push_back({getTimeMS(), msg_fh, m.registry()});
		return;
	}

	// TODO: save updates to old fragments, but writing them to a new fragment that would overwrite on merge
	// new fragment?, since we dont write to others fragments?


	// on new message: assign fuid
	// on new and update: mark as fragment dirty
}

// assumes not loaded frag
// need update from frag
void MessageFragmentStore::loadFragment(Message3Registry& reg, ObjectHandle fh) {
	if (!fh) {
		std::cerr << "MFS error: loadFragment called with invalid object!!!\n";
		assert(false);
		return;
	}

	std::cout << "MFS: loadFragment\n";
	// version HAS to be set, or we just fail
	if (!fh.all_of<ObjComp::MessagesVersion>()) {
		std::cerr << "MFS error: nope, object without version, cant load\n";
		return;
	}

	nlohmann::json j;
	const auto obj_version = fh.get<ObjComp::MessagesVersion>().v;
	if (obj_version == 1 || obj_version == 2) {
		j = loadFromStorageNJ(fh); // also handles version and json/msgpack
	} else {
		std::cerr << "MFS error: nope, object with unknown version, cant load\n";
		return;
	}

	if (!j.is_array()) {
		// wrong data
		fh.emplace_or_replace<ObjComp::Ephemeral::MessagesEmptyTag>();
		return;
	}

	if (j.size() == 0) {
		// empty array
		fh.emplace_or_replace<ObjComp::Ephemeral::MessagesEmptyTag>();
		return;
	}

	// TODO: this should probably never be the case, since we already know here that it is a msg frag
	if (!reg.ctx().contains<Message::Contexts::ContactFragments>()) {
		reg.ctx().emplace<Message::Contexts::ContactFragments>();
	}
	reg.ctx().get<Message::Contexts::ContactFragments>().insert(fh);

	// mark loaded
	if (!reg.ctx().contains<Message::Contexts::LoadedContactFragments>()) {
		reg.ctx().emplace<Message::Contexts::LoadedContactFragments>();
	}
	reg.ctx().get<Message::Contexts::LoadedContactFragments>().loaded_frags.emplace(fh);

	size_t messages_new_or_updated {0};
	for (const auto& j_entry : j) {
		auto new_real_msg = Message3Handle{reg, reg.create()};
		// load into staging reg
		for (const auto& [k, v] : j_entry.items()) {
			//std::cout << "K:" << k << " V:" << v.dump() << "\n";
			const auto type_id = entt::hashed_string(k.data(), k.size());
			const auto deserl_fn_it = _scnj._deserl_json.find(type_id);
			if (deserl_fn_it != _scnj._deserl_json.cend()) {
				try {
					if (!deserl_fn_it->second(_scnj, new_real_msg, v)) {
						std::cerr << "MFS error: failed deserializing '" << k << "'\n";
					}
				} catch(...) {
					std::cerr << "MFS error: failed deserializing (threw) '" << k << "'\n";
				}
			} else {
				std::cerr << "MFS warning: missing deserializer for meta key '" << k << "'\n";
			}
		}

		new_real_msg.emplace_or_replace<Message::Components::MFSObj>(fh);

		// dup check (hacky, specific to protocols)
		Message3 dup_msg {entt::null};
		{
			// get comparator from contact
			if (reg.ctx().contains<Contact4>()) {
				const auto c = reg.ctx().get<Contact4>();
				if (_cs.registry().all_of<Contact::Components::MessageIsSame>(c)) {
					auto& comp = _cs.registry().get<Contact::Components::MessageIsSame>(c).comp;
					// walking EVERY existing message OOF
					// this needs optimizing
					for (const Message3 other_msg : reg.view<Message::Components::Timestamp, Message::Components::ContactFrom, Message::Components::ContactTo>()) {
						if (other_msg == new_real_msg) {
							continue; // skip self
						}

						if (comp({reg, other_msg}, new_real_msg)) {
							// dup
							dup_msg = other_msg;
							break;
						}
					}
				}
			}
		}

		if (reg.valid(dup_msg)) {
			//  -> merge with preexisting (needs to be order independent)
			//  -> throw update
			reg.destroy(new_real_msg);
			//messages_new_or_updated++; // TODO: how do i know on merging, if data was useful
			//_rmm.throwEventUpdate(reg, new_real_msg);
		} else {
			if (!new_real_msg.all_of<Message::Components::Timestamp, Message::Components::ContactFrom, Message::Components::ContactTo>()) {
				// does not have needed components to be stand alone
				reg.destroy(new_real_msg);
				std::cerr << "MFS warning: message with missing basic compoments\n";
				continue;
			}

			messages_new_or_updated++;
			//  -> throw create
			_rmm.throwEventConstruct(reg, new_real_msg);
		}
	}

	if (messages_new_or_updated == 0) {
		// useless frag
		// TODO: unload?
		fh.emplace_or_replace<ObjComp::Ephemeral::MessagesEmptyTag>();
	}
}

bool MessageFragmentStore::syncFragToStorage(ObjectHandle fh, Message3Registry& reg) {
	auto& ftsrange = fh.get_or_emplace<ObjComp::MessagesTSRange>(getTimeMS(), getTimeMS());

	auto j = nlohmann::json::array();

	// TODO: does every message have ts?
	auto msg_view = reg.view<Message::Components::Timestamp>();
	// we also assume all messages have an associated object
	for (auto it = msg_view.rbegin(), it_end = msg_view.rend(); it != it_end; it++) {
		const Message3 m = *it;

		if (!reg.all_of<Message::Components::MFSObj, Message::Components::ContactFrom, Message::Components::ContactTo>(m)) {
			continue;
		}

		// filter: require msg for now
		// this will be removed in the future
		if (!reg.any_of<Message::Components::MessageText>(m)) {
		// fix message file objects first
		//if (!reg.any_of<Message::Components::MessageText, Message::Components::MessageFileObject>(m)) {
			continue;
		}

		if (fh != reg.get<Message::Components::MFSObj>(m).o) {
			continue; // not ours
		}

		{ // potentially adjust tsrange (some external processes can change timestamps)
			const auto msg_ts = msg_view.get<Message::Components::Timestamp>(m).ts;
			if (ftsrange.begin > msg_ts) {
				ftsrange.begin = msg_ts;
			} else if (ftsrange.end < msg_ts) {
				ftsrange.end = msg_ts;
			}
		}

		auto& j_entry = j.emplace_back(nlohmann::json::object());

		for (const auto& [type_id, storage] : reg.storage()) {
			if (!storage.contains(m)) {
				continue;
			}

			//std::cout << "storage type: type_id:" << type_id << " name:" << storage.type().name() << "\n";

			// use type_id to find serializer
			auto s_cb_it = _scnj._serl_json.find(type_id);
			if (s_cb_it == _scnj._serl_json.end()) {
				// could not find serializer, not saving
				//std::cout << "missing " << storage.type().name() << "(" << type_id << ")\n";
				continue;
			}

			try {
				s_cb_it->second(_scnj, {reg, m}, j_entry[storage.type().name()]);
			} catch (...) {
				std::cerr << "MFS error: failed to serialize " << storage.type().name() << "(" << type_id << ")\n";
			}
		}
	}

	// we cant skip if array is empty (in theory it will not be empty later on)

	std::vector<uint8_t> data_to_save;
	const auto obj_version = fh.get_or_emplace<ObjComp::MessagesVersion>().v;
	if (obj_version == 1) {
		auto j_dump = j.dump(2, ' ', true);
		data_to_save = std::vector<uint8_t>(j_dump.cbegin(), j_dump.cend());
	} else if (obj_version == 2) {
		data_to_save = nlohmann::json::to_msgpack(j);
	} else {
		std::cerr << "MFS error: unknown object version\n";
		assert(false);
	}
	assert(fh.all_of<ObjComp::Ephemeral::BackendAtomic>());
	auto* backend = fh.get<ObjComp::Ephemeral::BackendAtomic>().ptr;
	if (backend->write(fh, {reinterpret_cast<const uint8_t*>(data_to_save.data()), data_to_save.size()})) {
		// TODO: make this better, should this be called on fail? should this be called before sync? (prob not)
		_fs_ignore_event = true;
		_os.throwEventUpdate(fh);
		_fs_ignore_event = false;

		//std::cout << "MFS: dumped " << j_dump << "\n";
		// succ
		return true;
	}

	// TODO: error
	return false;
}

MessageFragmentStore::MessageFragmentStore(
	ContactStore4I& cs,
	RegistryMessageModelI& rmm,
	ObjectStore2& os,
	StorageBackendIMeta& sbm,
	StorageBackendIAtomic& sba,
	MessageSerializerNJ& scnj
) : _cs(cs), _rmm(rmm), _rmm_sr(_rmm.newSubRef(this)), _os(os), _os_sr(_os.newSubRef(this)), _sbm(sbm), _sba(sba), _scnj(scnj) {
	_rmm_sr
		.subscribe(RegistryMessageModel_Event::message_construct)
		.subscribe(RegistryMessageModel_Event::message_updated)
		.subscribe(RegistryMessageModel_Event::message_destroy)
	;

	// TODO: move somewhere else?
	auto& sjc = _os.registry().ctx().get<SerializerJsonCallbacks<Object>>();
	sjc.registerSerializer<ObjComp::MessagesVersion>();
	sjc.registerDeSerializer<ObjComp::MessagesVersion>();
	sjc.registerSerializer<ObjComp::MessagesTSRange>();
	sjc.registerDeSerializer<ObjComp::MessagesTSRange>();
	sjc.registerSerializer<ObjComp::MessagesContact>();
	sjc.registerDeSerializer<ObjComp::MessagesContact>();

	// old frag names
	sjc.registerSerializer<FragComp::MessagesTSRange>(sjc.component_get_json<ObjComp::MessagesTSRange>);
	sjc.registerDeSerializer<FragComp::MessagesTSRange>(sjc.component_emplace_or_replace_json<ObjComp::MessagesTSRange>);
	sjc.registerSerializer<FragComp::MessagesContact>(sjc.component_get_json<ObjComp::MessagesContact>);
	sjc.registerDeSerializer<FragComp::MessagesContact>(sjc.component_emplace_or_replace_json<ObjComp::MessagesContact>);

	_os_sr
		.subscribe(ObjectStore_Event::object_construct)
		.subscribe(ObjectStore_Event::object_update)
	;
}

MessageFragmentStore::~MessageFragmentStore(void) {
	while (!_frag_save_queue.empty()) {
		auto fh = _frag_save_queue.front().id;
		auto* reg = _frag_save_queue.front().reg;
		assert(reg != nullptr);
		_fs_ignore_event = true;
		syncFragToStorage(fh, *reg);
		_fs_ignore_event = false;
		_frag_save_queue.pop_front(); // pop unconditionally
	}

	for (const auto c : _touched_contacts) {
		auto* mr_ptr = static_cast<const RegistryMessageModelI&>(_rmm).get(c);
		if (mr_ptr != nullptr) {
			mr_ptr->ctx().erase<Message::Contexts::OpenFragments>();
			mr_ptr->ctx().erase<Message::Contexts::ContactFragments>();
			mr_ptr->ctx().erase<Message::Contexts::LoadedContactFragments>();
		}
	}
}

// checks range against all cursers in msgreg
static bool rangeVisible(uint64_t range_begin, uint64_t range_end, const Message3Registry& msg_reg) {
	// 1D collision checks:
	//  - for range vs range:
	//    r1 rhs >= r0 lhs AND r1 lhs <= r0 rhs
	//  - for range vs point:
	//    p >= r0 lhs AND p <= r0 rhs
	// NOTE: directions for us are reversed (begin has larger values as end)

	auto c_b_view = msg_reg.view<Message::Components::Timestamp, Message::Components::ViewCurserBegin>();
	c_b_view.use<Message::Components::ViewCurserBegin>();
	for (const auto& [m, ts_begin_comp, vcb] : c_b_view.each()) {
		// p and r1 rhs can be seen as the same
		// but first we need to know if a curser begin is a point or a range

		// TODO: margin?
		auto ts_begin = ts_begin_comp.ts;
		auto ts_end = ts_begin_comp.ts; // simplyfy code by making a single begin curser act as an infinitly small range
		if (msg_reg.valid(vcb.curser_end) && msg_reg.all_of<Message::Components::ViewCurserEnd>(vcb.curser_end)) {
			// TODO: respect curser end's begin?
			// TODO: remember which ends we checked and check remaining
			ts_end = msg_reg.get<Message::Components::Timestamp>(vcb.curser_end).ts;

			// sanity check curser order
			if (ts_end > ts_begin) {
				std::cerr << "MFS warning: begin curser and end curser of view swapped!!\n";
				std::swap(ts_begin, ts_end);
			}
		}

		// perform both checks here
		if (ts_begin < range_end || ts_end > range_begin) {
			continue;
		}

		// range hits a view
		return true;
	}

	return false;
}

float MessageFragmentStore::tick(float) {
	const auto ts_now = getTimeMS();
	// sync dirty fragments here
	if (!_frag_save_queue.empty()) {
		// wait 10sec before saving
		if (_frag_save_queue.front().ts_since_dirty + 10*1000 <= ts_now) {
			auto fh = _frag_save_queue.front().id;
			auto* reg = _frag_save_queue.front().reg;
			assert(reg != nullptr);
			if (syncFragToStorage(fh, *reg)) {
				_frag_save_queue.pop_front();
			}
		}
	}

	// load needed fragments here

	// last check event frags
	// only checks if it collides with ranges, not adjacent
	// bc ~range~ msgreg will be marked dirty and checked next tick
	const bool had_events = !_event_check_queue.empty();
	for (size_t i = 0; i < 50 && !_event_check_queue.empty(); i++) {
		//std::cout << "MFS: event check\n";
		auto fh = _event_check_queue.front().fid;
		auto c = _event_check_queue.front().c;
		_event_check_queue.pop_front();

		if (!static_cast<bool>(fh)) {
			return 0.05f;
		}

		if (!fh.all_of<ObjComp::MessagesTSRange>()) {
			return 0.05f;
		}

		if (!fh.all_of<ObjComp::MessagesVersion>()) {
			// missing version, adding
			fh.emplace<ObjComp::MessagesVersion>();
		}
		const auto object_version = fh.get<ObjComp::MessagesVersion>().v;
		// TODO: move this early version check somewhere else
		if (object_version != 1 && object_version != 2) {
			std::cerr << "MFS: object with version mismatch\n";
			return 0.05f;
		}

		// get ts range of frag and collide with all curser(s/ranges)
		const auto& frag_range = fh.get<ObjComp::MessagesTSRange>();

		auto* msg_reg = _rmm.get(c);
		if (msg_reg == nullptr) {
			return 0.05f;
		}

		if (rangeVisible(frag_range.begin, frag_range.end, !msg_reg)) {
			loadFragment(*msg_reg, fh);
			_potentially_dirty_contacts.emplace(c);
			return 0.05f; // only one but soon again
		}
	}
	if (had_events) {
		//std::cout << "MFS: event check none\n";
		return 0.05f; // only check events, even if non where hit
	}

	if (!_potentially_dirty_contacts.empty()) {
		//std::cout << "MFS: pdc\n";
		// here we check if any view of said contact needs frag loading
		// only once per tick tho

		// TODO: this makes order depend on internal order and is not fair
		auto it = _potentially_dirty_contacts.cbegin();

		auto* msg_reg = _rmm.get(*it);

		// first do collision check agains every contact associated fragment
		// that is not already loaded !!
		if (msg_reg->ctx().contains<Message::Contexts::ContactFragments>()) {
			const auto& cf = msg_reg->ctx().get<Message::Contexts::ContactFragments>();
			if (!cf.sorted_frags.empty()) {
				if (!msg_reg->ctx().contains<Message::Contexts::LoadedContactFragments>()) {
					msg_reg->ctx().emplace<Message::Contexts::LoadedContactFragments>();
				}
				const auto& loaded_frags = msg_reg->ctx().get<Message::Contexts::LoadedContactFragments>().loaded_frags;

				for (const auto& [fid, si] : msg_reg->ctx().get<Message::Contexts::ContactFragments>().sorted_frags) {
					if (loaded_frags.contains(fid)) {
						continue;
					}

					auto fh = _os.objectHandle(fid);

					if (!static_cast<bool>(fh)) {
						std::cerr << "MFS error: frag is invalid\n";
						// WHAT
						msg_reg->ctx().get<Message::Contexts::ContactFragments>().erase(fid);
						return 0.05f;
					}

					if (!fh.all_of<ObjComp::MessagesTSRange>()) {
						std::cerr << "MFS error: frag has no range\n";
						// ????
						msg_reg->ctx().get<Message::Contexts::ContactFragments>().erase(fid);
						return 0.05f;
					}

					if (fh.all_of<ObjComp::Ephemeral::MessagesEmptyTag>()) {
						continue; // skip known empty
					}

					// get ts range of frag and collide with all curser(s/ranges)
					const auto& [range_begin, range_end] = fh.get<ObjComp::MessagesTSRange>();

					if (rangeVisible(range_begin, range_end, *msg_reg)) {
						std::cout << "MFS: frag hit by vis range\n";
						loadFragment(*msg_reg, fh);
						return 0.05f;
					}
				}
				// no new visible fragment
				//std::cout << "MFS: no new frag directly visible\n";

				// now, finally, check for adjecent fragments that need to be loaded
				// we do this by finding the outermost fragment in a rage, and extend it by one

				// TODO: rewrite using some bounding range tree to perform collision checks !!!
				// (this is now performing better, but still)


				// for each view
				auto c_b_view = msg_reg->view<Message::Components::Timestamp, Message::Components::ViewCurserBegin>();
				c_b_view.use<Message::Components::ViewCurserBegin>();
				for (const auto& [_, ts_begin_comp, vcb] : c_b_view.each()) {
					// aka "scroll down"
					{ // find newest(-ish) frag in range
						// or in reverse frag end <= range begin


						// lower bound of frag end and range begin
						const auto right = std::lower_bound(
							cf.sorted_end.crbegin(),
							cf.sorted_end.crend(),
							ts_begin_comp.ts,
							[&](const Object element, const auto& value) -> bool {
								return _os.registry().get<ObjComp::MessagesTSRange>(element).end >= value;
							}
						);

						Object next_frag{entt::null};
						if (right != cf.sorted_end.crend()) {
							next_frag = cf.next(*right);
						}
						// we checked earlier that cf is not empty
						if (!_os.registry().valid(next_frag)) {
							// fall back to closest, cf is not empty
							next_frag = cf.sorted_end.front();
						}

						// a single adjacent frag is often not enough
						// only ok bc next is cheap
						for (size_t i = 0; i < 5 && _os.registry().valid(next_frag); next_frag = cf.next(next_frag)) {
							auto fh = _os.objectHandle(next_frag);
							if (fh.any_of<ObjComp::Ephemeral::MessagesEmptyTag>()) {
								continue; // skip known empty
							}

							if (!loaded_frags.contains(next_frag)) {
								std::cout << "MFS: next frag of range\n";
								loadFragment(*msg_reg, fh);
								return 0.05f;
							}

							i++;
						}
					}

					// curser end
					if (!msg_reg->valid(vcb.curser_end) || !msg_reg->all_of<Message::Components::Timestamp>(vcb.curser_end)) {
						continue;
					}
					const auto ts_end = msg_reg->get<Message::Components::Timestamp>(vcb.curser_end).ts;

					// aka "scroll up"
					{ // find oldest(-ish) frag in range
						// frag begin >= range end

						// lower bound of frag begin and range end
						const auto left = std::lower_bound(
							cf.sorted_begin.cbegin(),
							cf.sorted_begin.cend(),
							ts_end,
							[&](const Object element, const auto& value) -> bool {
								return _os.registry().get<ObjComp::MessagesTSRange>(element).begin < value;
							}
						);

						Object prev_frag{entt::null};
						if (left != cf.sorted_begin.cend()) {
							prev_frag = cf.prev(*left);
						}
						// we checked earlier that cf is not empty
						if (!_os.registry().valid(prev_frag)) {
							// fall back to closest, cf is not empty
							prev_frag = cf.sorted_begin.back();
						}

						// a single adjacent frag is often not enough
						// only ok bc next is cheap
						for (size_t i = 0; i < 5 && _os.registry().valid(prev_frag); prev_frag = cf.prev(prev_frag)) {
							auto fh = _os.objectHandle(prev_frag);
							if (fh.any_of<ObjComp::Ephemeral::MessagesEmptyTag>()) {
								continue; // skip known empty
							}

							if (!loaded_frags.contains(prev_frag)) {
								std::cout << "MFS: prev frag of range\n";
								loadFragment(*msg_reg, fh);
								return 0.05f;
							}

							i++;
						}
					}
				}
			}
		} else {
			// contact has no fragments, skip
		}

		_potentially_dirty_contacts.erase(it);

		return 0.05f;
	}


	return 1000.f*60.f*60.f;
}

bool MessageFragmentStore::onEvent(const Message::Events::MessageConstruct& e) {
	handleMessage(e.e);
	return false;
}

bool MessageFragmentStore::onEvent(const Message::Events::MessageUpdated& e) {
	handleMessage(e.e);
	return false;
}

// TODO: handle deletes? diff between unload?

bool MessageFragmentStore::onEvent(const ObjectStore::Events::ObjectConstruct& e) {
	if (_fs_ignore_event) {
		return false; // skip self
	}

	if (!e.e.all_of<ObjComp::MessagesTSRange, ObjComp::MessagesContact>()) {
		return false; // not for us
	}
	if (!e.e.all_of<ObjComp::MessagesVersion>()) {
		// missing version, adding
		// version check is later
		e.e.emplace<ObjComp::MessagesVersion>();
	}

	// TODO: are we sure it is a *new* fragment?

	Contact4 frag_contact = entt::null;
	{ // get contact
		const auto& frag_contact_id = e.e.get<ObjComp::MessagesContact>().id;
		// TODO: id lookup table, this is very inefficent
		for (const auto& [c_it, id_it] : _cs.registry().view<Contact::Components::ID>().each()) {
			if (frag_contact_id == id_it.data) {
				frag_contact = c_it;
				break;
			}
		}
		if (!_cs.registry().valid(frag_contact)) {
			// unkown contact
			return false;
		}
		e.e.emplace_or_replace<ObjComp::Ephemeral::MessagesContactEntity>(frag_contact);
	}

	// create if not exist
	auto* msg_reg = _rmm.get(frag_contact);
	if (msg_reg == nullptr) {
		// msg reg not created yet
		// TODO: this is an erroious path
		return false;
	}
	_touched_contacts.emplace(frag_contact);

	if (!msg_reg->ctx().contains<Message::Contexts::ContactFragments>()) {
		msg_reg->ctx().emplace<Message::Contexts::ContactFragments>();
	}
	msg_reg->ctx().get<Message::Contexts::ContactFragments>().erase(e.e); // TODO: can this happen? update
	msg_reg->ctx().get<Message::Contexts::ContactFragments>().insert(e.e);

	_event_check_queue.push_back(ECQueueEntry{e.e, frag_contact});

	return false;
}

bool MessageFragmentStore::onEvent(const ObjectStore::Events::ObjectUpdate& e) {
	if (_fs_ignore_event) {
		return false; // skip self
	}

	if (!e.e.all_of<ObjComp::MessagesTSRange, ObjComp::MessagesContact>()) {
		return false; // not for us
	}

	// since its an update, we might have it associated, or not
	// its also possible it was tagged as empty
	e.e.remove<ObjComp::Ephemeral::MessagesEmptyTag>();

	Contact4 frag_contact = entt::null;
	{ // get contact
		// probably cached already
		if (e.e.all_of<ObjComp::Ephemeral::MessagesContactEntity>()) {
			frag_contact = e.e.get<ObjComp::Ephemeral::MessagesContactEntity>().e;
		}

		if (!_cs.registry().valid(frag_contact)) {
			const auto& frag_contact_id = e.e.get<ObjComp::MessagesContact>().id;
			// TODO: id lookup table, this is very inefficent
			for (const auto& [c_it, id_it] : _cs.registry().view<Contact::Components::ID>().each()) {
				if (frag_contact_id == id_it.data) {
					frag_contact = c_it;
					break;
				}
			}
			if (!_cs.registry().valid(frag_contact)) {
				// unkown contact
				return false;
			}
			e.e.emplace_or_replace<ObjComp::Ephemeral::MessagesContactEntity>(frag_contact);
		}
	}

	// create if not exist
	auto* msg_reg = _rmm.get(frag_contact);
	if (msg_reg == nullptr) {
		// msg reg not created yet
		// TODO: this is an erroious path
		return false;
	}
	_touched_contacts.emplace(frag_contact);

	if (!msg_reg->ctx().contains<Message::Contexts::ContactFragments>()) {
		msg_reg->ctx().emplace<Message::Contexts::ContactFragments>();
	}
	msg_reg->ctx().get<Message::Contexts::ContactFragments>().erase(e.e); // TODO: check/update/fragment update
	msg_reg->ctx().get<Message::Contexts::ContactFragments>().insert(e.e);

	// TODO: actually load it
	//_event_check_queue.push_back(ECQueueEntry{e.e, frag_contact});

	return false;
}

