/*
 *		Copyright (C) 2013, 2014 by the Konclude Developer Team.
 *
 *		This file is part of the reasoning system Konclude.
 *		For details and support, see <http://konclude.com/>.
 *
 *		Konclude is free software: you can redistribute it and/or modify it under
 *		the terms of version 3 of the GNU Lesser General Public License (LGPL3).
 */

#ifndef KONCLUDE_UTILITIES_CRULEPROFILER_H
#define KONCLUDE_UTILITIES_CRULEPROFILER_H

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <algorithm>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace Konclude {

	namespace Utilities {

		/*!
		 *
		 *		\class		CRuleProfiler
		 *		\brief		Says which concept expressions (axioms after preprocessing) the tableau spends its time and memory on.
		 *
		 *					Switched on by the environment variable KONCLUDE_PROFILE (off: one predictable branch per rule application, nothing else).
		 *					Every application of a tableau rule is counted per concept: calls, time (inclusive and without nested rule applications),
		 *					new individual nodes, new branches (OR disjuncts, merge and choose branches). A table of the most expensive concepts is written
		 *					to the file KONCLUDE_PROFILE_FILE (default: standard error) every KONCLUDE_PROFILE_INTERVAL seconds (default 10), when the process
		 *					ends, and the file is rewritten each time, so a run that is killed (time or memory limit) still leaves the latest table.
		 *					The counters are kept per thread and merged when written; the numbers of a run with several workers are approximate.
		 *					Header only on purpose.
		 *
		 */
		class CRuleProfiler {
			public:
				struct Entry {
					const void* mConcept = nullptr;
					bool mNegated = false;
					long long mOpCode = 0;
					unsigned long long mCalls = 0, mNs = 0, mSelfNs = 0, mNodes = 0, mBranches = 0;
					std::string mName;		// written when the concept is first seen: later the concept may no longer exist
				};
				typedef std::unordered_map<unsigned long long, Entry> Table;
				typedef std::string (*NameFunction)(const void* concept, bool negated);

				static bool enabled() {
					static const bool on = std::getenv("KONCLUDE_PROFILE") != nullptr;
					return on;
				}

				static void setNamer(NameFunction f) { namer() = f; }

				//! one application of a tableau rule on a concept
				struct Scope {
					bool mActive;
					Entry* mEntry = nullptr;
					Entry* mPrevCurrent = nullptr;
					unsigned long long* mPrevChild = nullptr;
					unsigned long long mChildNs = 0;
					std::chrono::steady_clock::time_point mStart;

					Scope(const void* concept, bool negated, long long opCode) : mActive(enabled()) {
						if (!mActive) { return; }
						ThreadData& td = threadData();
						unsigned long long key = ((unsigned long long)(uintptr_t)concept << 1) | (negated ? 1ULL : 0ULL);
						auto it = td.mTable->find(key);
						if (it == td.mTable->end()) {
							std::lock_guard<std::mutex> lock(*td.mMutex);
							it = td.mTable->emplace(key, Entry()).first;
							it->second.mConcept = concept; it->second.mNegated = negated; it->second.mOpCode = opCode;
							NameFunction nf = namer();
							if (nf) { it->second.mName = nf(concept, negated); }
						}
						mEntry = &it->second;
						mPrevCurrent = td.mCurrent; mPrevChild = td.mChildSlot;
						td.mCurrent = mEntry; td.mChildSlot = &mChildNs;
						mStart = std::chrono::steady_clock::now();
					}

					~Scope() {
						if (!mActive) { return; }
						unsigned long long ns = (unsigned long long)std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - mStart).count();
						mEntry->mCalls += 1; mEntry->mNs += ns; mEntry->mSelfNs += (ns > mChildNs ? ns - mChildNs : 0);
						ThreadData& td = threadData();
						if (mPrevChild) { *mPrevChild += ns; }
						td.mCurrent = mPrevCurrent; td.mChildSlot = mPrevChild;
						if ((++td.mTick & 0x1FFF) == 0) { maybeWrite(false); }
					}
				};

				//! a new individual node was created by the rule that is applied
				static void nodeCreated() { if (enabled()) { Entry* e = threadData().mCurrent; if (e) { e->mNodes += 1; } } }
				//! a new branch (alternative) of a non-deterministic rule was created
				static void branchCreated() { if (enabled()) { Entry* e = threadData().mCurrent; if (e) { e->mBranches += 1; } } }

				static void writeNow() { if (enabled()) { maybeWrite(true); } }

			private:
				struct ThreadData {
					Table* mTable; std::mutex* mMutex; Entry* mCurrent; unsigned long long* mChildSlot; unsigned long long mTick;
				};
				struct Global {
					std::mutex mMutex;
					std::vector<std::pair<Table*, std::mutex*> > mTables;
					std::chrono::steady_clock::time_point mStart = std::chrono::steady_clock::now();
					std::chrono::steady_clock::time_point mLastWrite = std::chrono::steady_clock::now();
					NameFunction mNamer = nullptr;
					bool mExitHook = false;
				};
				static Global& global() { static Global* g = new Global(); return *g; }
				static NameFunction& namer() { return global().mNamer; }

				static ThreadData& threadData() {
					static thread_local ThreadData td = { nullptr, nullptr, nullptr, nullptr, 0 };
					if (!td.mTable) {
						td.mTable = new Table(); td.mMutex = new std::mutex();
						Global& g = global();
						std::lock_guard<std::mutex> lock(g.mMutex);
						g.mTables.push_back(std::make_pair(td.mTable, td.mMutex));
						if (!g.mExitHook) { g.mExitHook = true; std::atexit(&CRuleProfiler::atExit); }
					}
					return td;
				}

				static void atExit() { maybeWrite(true); }

				static long rssMegabytes() {
#ifdef __linux__
					FILE* f = std::fopen("/proc/self/statm", "r");
					if (!f) { return -1; }
					long a = 0, b = 0;
					int n = std::fscanf(f, "%ld %ld", &a, &b);
					std::fclose(f);
					return n == 2 ? (b * 4096L) / (1024L * 1024L) : -1;
#else
					return -1;
#endif
				}

				static void maybeWrite(bool force) {
					Global& g = global();
					static const double interval = std::getenv("KONCLUDE_PROFILE_INTERVAL") ? std::atof(std::getenv("KONCLUDE_PROFILE_INTERVAL")) : 10.0;
					std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
					if (!force && std::chrono::duration<double>(now - g.mLastWrite).count() < interval) { return; }
					std::lock_guard<std::mutex> lock(g.mMutex);
					g.mLastWrite = now;
					std::vector<Entry> all;
					for (auto& tm : g.mTables) {
						std::lock_guard<std::mutex> tl(*tm.second);
						for (auto& kv : *tm.first) {
							Entry e = kv.second;
							bool merged = false;
							for (Entry& x : all) { if (x.mConcept == e.mConcept && x.mNegated == e.mNegated && x.mName == e.mName) { x.mCalls += e.mCalls; x.mNs += e.mNs; x.mSelfNs += e.mSelfNs; x.mNodes += e.mNodes; x.mBranches += e.mBranches; merged = true; break; } }
							if (!merged) { all.push_back(e); }
						}
					}
					unsigned long long calls = 0, selfNs = 0, nodes = 0, branches = 0;
					for (const Entry& e : all) { calls += e.mCalls; selfNs += e.mSelfNs; nodes += e.mNodes; branches += e.mBranches; }
					std::sort(all.begin(), all.end(), [](const Entry& a, const Entry& b) { return a.mSelfNs > b.mSelfNs; });
					const char* path = std::getenv("KONCLUDE_PROFILE_FILE");
					FILE* out = path ? std::fopen(path, "w") : stderr;
					if (!out) { out = stderr; }
					std::fprintf(out, "KONCLUDE-PROFILE v1 elapsed_s=%.1f rss_mb=%ld rule_calls=%llu self_ms=%llu nodes=%llu branches=%llu concepts=%zu\n",
						std::chrono::duration<double>(now - g.mStart).count(), rssMegabytes(), calls, selfNs / 1000000ULL, nodes, branches, all.size());
					std::fprintf(out, "rank\tself_ms\tincl_ms\tcalls\tnodes\tbranches\topcode\tneg\tconcept\n");
					size_t shown = 0;
					auto printTop = [&](const std::vector<Entry>& v) {
						for (const Entry& e : v) {
							std::string name = e.mName.empty() ? std::string("?") : e.mName;
							for (char& c : name) { if (c == '\t' || c == '\n' || c == '\r') { c = ' '; } }
							std::fprintf(out, "%zu\t%llu\t%llu\t%llu\t%llu\t%llu\t%lld\t%d\t%s\n", ++shown, e.mSelfNs / 1000000ULL, e.mNs / 1000000ULL, e.mCalls, e.mNodes, e.mBranches, e.mOpCode, e.mNegated ? 1 : 0, name.c_str());
						}
					};
					// the 25 most expensive by time without nested rules, and the 15 by new nodes and branches (without repeating)
					std::vector<Entry> top(all.begin(), all.begin() + std::min<size_t>(25, all.size()));
					printTop(top);
					std::vector<Entry> rest(all.begin() + std::min<size_t>(25, all.size()), all.end());
					std::sort(rest.begin(), rest.end(), [](const Entry& a, const Entry& b) { return a.mNodes + a.mBranches > b.mNodes + b.mBranches; });
					std::vector<Entry> top2;
					for (const Entry& e : rest) { if (top2.size() >= 15 || e.mNodes + e.mBranches == 0) { break; } top2.push_back(e); }
					printTop(top2);
					if (out != stderr) { std::fclose(out); }
				}
		};

	}; // end namespace Utilities

}; // end namespace Konclude

#endif // KONCLUDE_UTILITIES_CRULEPROFILER_H
