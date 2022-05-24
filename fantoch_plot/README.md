### fantoch_plot

#### Steps

1. Install prerequisites

```bash
sudo apt install python3-dev python3-venv python3-pip
```

2. Install `matplotlib`
```bash
pip3 install matplotlib==3.3.0
```

3. Generate plots
```bash
rm -rf plots/
cargo run --release --features=pyo3
ls plots/
```
