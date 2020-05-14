[![Build Status](https://travis-ci.org/vitorenesduarte/fantoch.svg?branch=master)](https://travis-ci.org/vitorenesduarte/fantoch)
[![Coverage Status](https://coveralls.io/repos/github/vitorenesduarte/fantoch/badge.svg?branch=master)](https://coveralls.io/github/vitorenesduarte/fantoch?branch=master)

### `fantoch`: framework for evaluating (planet-scale) consensus protocols

#### Protocols currently implemented
- [EPaxos](https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf): [(source)](https://github.com/vitorenesduarte/fantoch/tree/master/fantoch_ps/src/protocol/epaxos.rs)
- [FPaxos](https://fpaxos.github.io/), and thus Paxos as a special case: [(source)](https://github.com/vitorenesduarte/fantoch/tree/master/fantoch_ps/src/protocol/fpaxos.rs)
- [EPaxos](https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf) [(source)](https://github.com/vitorenesduarte/fantoch/tree/master/fantoch_ps/src/protocol/epaxos.rs)
- [FPaxos](https://fpaxos.github.io/), and thus Paxos as a special case [(source)](https://github.com/vitorenesduarte/fantoch/tree/master/fantoch_ps/src/protocol/fpaxos.rs)
  - implemented following [Paxos Made Moderately Complex](http://paxos.systems/), which enables a certain degree of parallelism at the leader
- [Atlas](https://vitorenes.org/publication/enes-atlas/): [(source)](https://github.com/vitorenesduarte/fantoch/tree/master/fantoch_ps/src/protocol/atlas.rs)
- Newt:[(source)](https://github.com/vitorenesduarte/fantoch/tree/master/fantoch_ps/src/protocol/newt.rs)

Next:
- Mencius: a variation that we call Flexible Mencius (as in Flexible Paxos) + an optimization that should make it very efficient for low conflict workloads
- Caesar

#### What does it do?

- all protocols implement the [`Protocol`](https://github.com/vitorenesduarte/fantoch/blob/master/fantoch/src/protocol/mod.rs) trait
- this specification can then be used for both
  - geo-distributed simulations that only output latency (infinite CPU is assumed)
  - actually running the protocols (in any setting)
- this is achieved by providing a "simulator" and a "runner" that are protocol-agnostic and are only aware of the `Protocol` trait

## License

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
