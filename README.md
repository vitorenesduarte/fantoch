![Continuous integration](https://github.com/vitorenesduarte/fantoch/workflows/Continuous%20integration/badge.svg)
[![dependency status](https://deps.rs/repo/github/vitorenesduarte/fantoch/status.svg)](https://deps.rs/repo/github/vitorenesduarte/fantoch)
[![codecov](https://codecov.io/gh/vitorenesduarte/fantoch/branch/master/graph/badge.svg?token=yqK2KnILVg)](https://codecov.io/gh/vitorenesduarte/fantoch)

### `fantoch`: framework for evaluating (planet-scale) consensus protocols

#### Protocols currently implemented
- [EPaxos](https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf): [(source)](https://github.com/vitorenesduarte/fantoch/tree/master/fantoch_ps/src/protocol/epaxos.rs)
- [FPaxos](https://fpaxos.github.io/), and thus Paxos as a special case: [(source)](https://github.com/vitorenesduarte/fantoch/tree/master/fantoch_ps/src/protocol/fpaxos.rs)
- [Caesar](https://arxiv.org/abs/1704.03319): [(source)](https://github.com/vitorenesduarte/fantoch/tree/master/fantoch_ps/src/protocol/caesar.rs)
- [Atlas](https://vitorenes.org/publication/enes-atlas/): [(source)](https://github.com/vitorenesduarte/fantoch/tree/master/fantoch_ps/src/protocol/atlas.rs)
- Newt: [(source)](https://github.com/vitorenesduarte/fantoch/tree/master/fantoch_ps/src/protocol/newt.rs)

#### What does it do?

- all protocols implement the [`Protocol`](https://github.com/vitorenesduarte/fantoch/blob/master/fantoch/src/protocol/mod.rs) trait
- this specification can then be used for
  - __simulating__ the expected latency in a given geo-distributed scenario (infinite CPU and network bandwidth are assumed)
  - __running__ the protocols in a real setting
- this is achieved by providing a __"simulator"__ and a __"runner"__ that are protocol-agnostic and are only aware of the `Protocol` (and [`Executor`](https://github.com/vitorenesduarte/fantoch/blob/master/fantoch/src/executor/mod.rs)) trait

## License

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
