from etlconfig.etl_config import EtlConfig

# demo only
if __name__ == "__main__":
    with open('/tmp/config_example.json', 'r') as f:
        config = "".join(f.readlines())

    etl = EtlConfig.decode(config)
    df = etl.run()
    df.show()
