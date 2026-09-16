#include <atomic>
#include <iostream>
#include <stdexcept>
#include <thread>
#include "core/recovery/observation.h"

namespace obs = recovery_observation;
int main(int argc, char** argv) {
    const bool overflow = argc > 1 && std::string(argv[1]) == "--overflow";
    if (overflow) {
        for (int i = 0; i < 10000; ++i) obs::Emit("overflow_event");
        if (obs::Recorder::Get().Dropped() == 0) return 1;
        return 0;
    }
    obs::Recorder::Get().SetEpoch(7);
    {
        obs::RequestScope request(42, true, obs::WallUs() - 1000, 0);
        auto captured = obs::context;
        std::thread worker([captured] {
            obs::ContextScope attach(captured);
            obs::FetchScope fetch(10000, 2);
            for (int i = 0; i < 1000; ++i) fetch.Block("unit_ir_wait");
            fetch.Complete(true);
        });
        worker.join(); request.Finish(1);
    }
    for (int i = 0; i < 40; ++i) {
        std::thread worker([i] { obs::RequestScope request(100 + i); request.Finish(1); });
        worker.join();
    }
    {
        std::atomic<bool> cancelled{true};
        obs::RequestScope request(42, true, 0, 1, &cancelled);
        try { obs::CheckCancelled(); return 2; }
        catch (const recovery::RequestCancelled&) { request.Finish(3); }
    }
    {
        obs::RequestScope request(43, true, 0, 0, nullptr, obs::WallUs() - 1);
        try { obs::CheckCancelled(); return 3; }
        catch (const recovery::RequestCancelled&) { request.Finish(3); }
    }
    obs::Recorder::Get().Flush();
    if (obs::Recorder::Get().Dropped()) return 4;
    std::cout << "{\"kind\":\"OBSERVATION_CONTRACT_TEST\",\"correctness\":\"PASS\",\"cancelled\":2,\"validated\":41}\n";
    return 0;
}
