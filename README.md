[![Build Status](https://travis-ci.org/vitorenesduarte/planet_sim.svg?branch=master)](https://travis-ci.org/vitorenesduarte/planet_sim)
[![Coverage Status](https://coveralls.io/repos/github/vitorenesduarte/planet_sim/badge.svg)](https://coveralls.io/github/vitorenesduarte/planet_sim)

### `planet_sim`: simulation of planet-scale systems

Details coming soon.

#### Supported protocols
- EPaxos: [(source)](https://github.com/vitorenesduarte/planet_sim/tree/master/src/protocol/epaxos.rs)
- Atlas: [(source)](https://github.com/vitorenesduarte/planet_sim/tree/master/src/protocol/atlas.rs)
- Newt: [(source)](https://github.com/vitorenesduarte/planet_sim/tree/master/src/protocol/newt.rs)

Next:
- FPaxos (and Paxos as a special case of FPaxos)
- Mencius

#### Ideas / Goals

- the same protocol/system implementation should be used by the simulation and by an actual implementation
- these simulations will only output latency (infinite CPU will be assumed)
- even though infinite CPU is assumed, we can still use something like [flamegraph](https://github.com/jonhoo/inferno/) to detect CPU performance problems

## License

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
