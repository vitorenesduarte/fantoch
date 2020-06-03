### fantoch_exp

#### Steps

1. Install prerequisites (openssl, unzip)

```bash
sudo apt-get install pkg-config libssl-dev
sudo apt-get install unzip
```

2. [Install AWS Cli](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-linux.html)

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

3. [Configure AWS Cli](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-linux.html)

Needs:
- AWS Access Key ID
- AWS Secret Access Key
- Default region name: maybe `eu-west-3` (`Paris`)

```bash
aws configure
```

4. Run

```bash
RUST_LOG=info cargo run --release
```
